package main

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"bend-ingest-kafka/config"
)

type MessagesBatch struct {
	messages           []string
	partition          int
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

func NewBatchReader(cfg *config.Config) BatchReader {
	if cfg.MockData != "" {
		return NewMockBatchReader(cfg.MockData, cfg.BatchSize)
	}
	return NewKafkaBatchReader(cfg)
}

type MockBatchReader struct {
	sampleData string
	batchSize  int
}

func NewMockBatchReader(sampleData string, batchSize int) *MockBatchReader {
	return &MockBatchReader{
		sampleData: sampleData,
		batchSize:  batchSize,
	}
}

func (r *MockBatchReader) ReadBatch(ctx context.Context) (*MessagesBatch, error) {
	messages := []string{}
	for i := 0; i < r.batchSize; i++ {
		messages = append(messages, r.sampleData)
	}
	return &MessagesBatch{
		messages:           messages,
		commitFunc:         func(_ context.Context) error { return nil },
		firstMessageOffset: -1,
		lastMessageOffset:  -1,
	}, nil
}

func (r *MockBatchReader) Close() error {
	return nil
}

type KafkaBatchReader struct {
	kafkaReader      *kafka.Reader
	batchSize        int
	maxBatchInterval time.Duration
}

func NewKafkaBatchReader(cfg *config.Config) *KafkaBatchReader {
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   parseKafkaServers(cfg.KafkaBootstrapServers),
		GroupID:   cfg.KafkaConsumerGroup,
		Topic:     cfg.KafkaTopic,
		Partition: cfg.KafkaPartition,
	})
	return &KafkaBatchReader{
		batchSize:        cfg.BatchSize,
		maxBatchInterval: cfg.BatchMaxInterval,
		kafkaReader:      kafkaReader,
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
		partition          int
		lastMessageOffset  int64
		firstMessageOffset int64
		batch              = []string{}
		batchTimeout       = time.NewTimer(br.maxBatchInterval)
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
			m, err := br.fetchMessageWithTimeout(ctx, br.maxBatchInterval)
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

			if len(batch) >= br.batchSize {
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
		partition = lastMessage.Partition
	}

	return &MessagesBatch{
		messages:           batch,
		commitFunc:         commitFunc,
		partition:          partition,
		firstMessageOffset: firstMessageOffset,
		lastMessageOffset:  lastMessageOffset,
	}, nil
}
