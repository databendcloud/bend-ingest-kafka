package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"

	"github.com/databendcloud/bend-ingest-kafka/config"
	"github.com/databendcloud/bend-ingest-kafka/message"
)

type BatchReader interface {
	ReadBatch() (*message.MessagesBatch, error)

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

func (r *MockBatchReader) ReadBatch() (*message.MessagesBatch, error) {
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
	mechanism := plain.Mechanism{
		Username: cfg.SaslUser,
		Password: cfg.SaslPassword,
	}

	dialer := &kafka.Dialer{
		Timeout:       300 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}
	kafkaReaderConfig := kafka.ReaderConfig{
		Brokers:          parseKafkaServers(cfg.KafkaBootstrapServers),
		GroupID:          cfg.KafkaConsumerGroup,
		Topic:            cfg.KafkaTopic,
		MinBytes:         cfg.MinBytes,
		MaxBytes:         cfg.MaxBytes,
		ReadBatchTimeout: 2 * time.Duration(cfg.BatchMaxInterval) * time.Second,
		MaxWait:          time.Duration(cfg.MaxWait) * time.Second,
	}

	if cfg.IsSASL {
		kafkaReaderConfig.Dialer = dialer
	}

	kafkaReader := kafka.NewReader(kafkaReaderConfig)
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
	maxRetries := 5
	var m kafka.Message
	var err error
	retryInterval := time.Second
	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(ctx, 2*timeout)
		m, err = br.kafkaReader.FetchMessage(ctx)
		cancel()
		if err != nil {
			if ctx.Err() == context.Canceled {
				logrus.Errorf("Failed to fetch message, attempt %d: %v", i+1, err)
				time.Sleep(retryInterval)
				retryInterval <<= 1
				fmt.Printf("Stack trace: %s\n", debug.Stack())
				continue
			}
			return nil, err
		}
		break
	}
	return &m, err
}

func (br *KafkaBatchReader) ReadBatch() (*message.MessagesBatch, error) {
	l := logrus.WithFields(logrus.Fields{"kafka_batch_reader": "ReadBatch"})
	var (
		lastMessageOffset  int64
		firstMessageOffset int64
		batch              []message.MessageData
		batchTimeout       = time.NewTimer(time.Duration(br.maxBatchInterval) * time.Second)
	)
	allMessages := make(map[int]*kafka.Message)
	defer batchTimeout.Stop()

_loop:
	for {
		select {
		case <-batchTimeout.C:
			break _loop
		default:
			ctx := context.Background()
			m, err := br.fetchMessageWithTimeout(ctx, time.Duration(br.maxBatchInterval)*time.Second)
			if err != nil {
				l.Warnf("Failed to read message from Kafka: %v", err)
				continue
			}
			if firstMessageOffset == 0 {
				firstMessageOffset = m.Offset
			}
			lastMessageOffset = m.Offset

			data := string(m.Value)
			data = strings.ReplaceAll(data, "\t", "")
			messageData := message.MessageData{
				Data:       strings.ReplaceAll(data, "\n", ""),
				DataOffset: m.Offset,
				Partition:  m.Partition,
				Key:        string(m.Key),
				CreateTime: m.Time,
			}
			batch = append(batch, messageData)
			allMessages[m.Partition] = m

			if len(batch) >= br.batchSize {
				break _loop
			}
		}
	}

	commitFunc := func(_ context.Context) error { return nil }
	if len(allMessages) != 0 {
		commitFunc = func(ctx context.Context) error {
			for partition, ms := range allMessages {
				err := br.kafkaReader.CommitMessages(ctx, *ms)
				if err != nil {
					l.Errorf("Failed to commit message at partition %d, offset %d: %v", partition, ms.Offset, err)
					return err
				}
			}
			return nil
		}
	}

	return &message.MessagesBatch{
		Messages:           batch,
		CommitFunc:         commitFunc,
		FirstMessageOffset: firstMessageOffset,
		LastMessageOffset:  lastMessageOffset,
	}, nil
}
