package main

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/databendcloud/bend-ingest-kafka/config"
	"github.com/databendcloud/bend-ingest-kafka/message"
)

type BatchReader interface {
	ReadBatch(ctx context.Context) (*message.MessagesBatch, error)

	Close() error
}

func NewBatchReader(cfg *config.Config) BatchReader {
	if cfg.MockData != "" {
		messageData := message.MessageData{}
		err := json.Unmarshal([]byte(cfg.MockData), &messageData)
		if err != nil {
			logrus.Fatalf("Failed to parse mock data: %v", err)
		}
		return NewMockBatchReader(messageData, cfg.BatchSize)
	}
	return NewKafkaBatchReader(cfg)
}

type MockBatchReader struct {
	sampleData message.MessageData
	batchSize  int
}

func NewMockBatchReader(sampleData message.MessageData, batchSize int) *MockBatchReader {
	return &MockBatchReader{
		sampleData: sampleData,
		batchSize:  batchSize,
	}
}

func (r *MockBatchReader) ReadBatch(ctx context.Context) (*message.MessagesBatch, error) {
	messages := make([]message.MessageData, 0, r.batchSize)
	for i := 0; i < r.batchSize; i++ {
		messages = append(messages, r.sampleData)
	}
	return &message.MessagesBatch{
		Messages:           messages,
		CommitFunc:         func(_ context.Context) error { return nil },
		FirstMessageOffset: -1,
		LastMessageOffset:  -1,
	}, nil
}

func (r *MockBatchReader) Close() error {
	return nil
}

type KafkaBatchReader struct {
	kafkaReader      *kafka.Reader
	batchSize        int
	maxBatchInterval int
}

func NewKafkaBatchReader(cfg *config.Config) *KafkaBatchReader {
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: parseKafkaServers(cfg.KafkaBootstrapServers),
		GroupID: cfg.KafkaConsumerGroup,
		Topic:   cfg.KafkaTopic,
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
	ctx, cancel := context.WithTimeout(ctx, 2*timeout)
	defer cancel()

	m, err := br.kafkaReader.FetchMessage(ctx)
	return &m, err
}

func (br *KafkaBatchReader) ReadBatch(ctx context.Context) (*message.MessagesBatch, error) {
	var (
		lastMessage        *kafka.Message
		partition          int
		key                string
		createTime         time.Time
		lastMessageOffset  int64
		firstMessageOffset int64
		batch              []message.MessageData
		batchTimeout       = time.NewTimer(time.Duration(br.maxBatchInterval) * time.Second)
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
			m, err := br.fetchMessageWithTimeout(ctx, time.Duration(br.maxBatchInterval)*time.Second)
			if err != nil {
				logrus.Warnf("Failed to read message from Kafka: %v", err)
				continue
			}
			if firstMessageOffset == 0 {
				firstMessageOffset = m.Offset
			}

			data := string(m.Value)
			messageData := message.MessageData{
				Data:       strings.ReplaceAll(data, "\n", ""),
				DataOffset: m.Offset,
				Partition:  m.Partition,
				Key:        string(m.Key),
				CreateTime: m.Time,
			}
			batch = append(batch, messageData)
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
		key = string(lastMessage.Key)
		createTime = lastMessage.Time
	}

	return &message.MessagesBatch{
		Messages:           batch,
		CommitFunc:         commitFunc,
		Partition:          partition,
		FirstMessageOffset: firstMessageOffset,
		LastMessageOffset:  lastMessageOffset,
		Key:                key,
		CreateTime:         createTime,
	}, nil
}
