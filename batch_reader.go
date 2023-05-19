package main

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type MessagesBatch struct {
	messages           []string
	commitFunc         func(context.Context) error
	firstMessageOffset int64
	lastMessageOffset  int64
}

func (b *MessagesBatch) Empty() bool {
	return len(b.messages) == 0
}

type BatchReader interface {
	ReadBatch(ctx context.Context) (*MessagesBatch, error)

	Close() error
}

type KafkaBatchReader struct {
	cfg         *Config
	kafkaReader *kafka.Reader
}

func NewKafkaBatchReader(cfg *Config) *KafkaBatchReader {
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: parseKafkaServers(cfg.KafkaBootstrapServers),
		GroupID: cfg.KafkaConsumerGroup,
		Topic:   cfg.KafkaTopic,
	})
	return &KafkaBatchReader{
		cfg:         cfg,
		kafkaReader: kafkaReader,
	}
}

func (br *KafkaBatchReader) Close() error {
	return br.kafkaReader.Close()
}

func (br *KafkaBatchReader) fetchMessageWithTimeout(ctx context.Context, timeout time.Duration) (*kafka.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	m, err := br.kafkaReader.FetchMessage(ctx)
	return &m, err
}

func (br *KafkaBatchReader) ReadBatch(ctx context.Context) (*MessagesBatch, error) {
	var (
		lastMessage        *kafka.Message
		lastMessageOffset  int64
		firstMessageOffset int64
		batch              = []string{}
		batchTimeout       = time.NewTimer(br.cfg.BatchMaxInterval)
	)
	defer batchTimeout.Stop()

_loop:
	for {
		select {
		case <-ctx.Done():
			logrus.Printf("exited")
			return nil, nil
		case <-batchTimeout.C:
			break _loop
		default:
			m, err := br.fetchMessageWithTimeout(ctx, br.cfg.BatchMaxInterval)
			if err != nil {
				logrus.Warnf("Failed to read message from Kafka: %v", err)
				continue
			}
			if firstMessageOffset == 0 {
				firstMessageOffset = m.Offset
			}

			data := string(m.Value)
			batch = append(batch, strings.ReplaceAll(data, "\n", ""))
			lastMessage = m

			if len(batch) >= br.cfg.BatchSize {
				break _loop
			}
		}
	}

	commitFunc := func(_ context.Context) error { return nil }
	if lastMessage != nil {
		commitFunc = func(ctx context.Context) error {
			return br.kafkaReader.CommitMessages(ctx, *lastMessage)
		}
		lastMessageOffset = lastMessage.Offset
	}

	return &MessagesBatch{
		messages:           batch,
		commitFunc:         commitFunc,
		firstMessageOffset: firstMessageOffset,
		lastMessageOffset:  lastMessageOffset,
	}, nil
}
