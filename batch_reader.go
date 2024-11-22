package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
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
	statsRecorder    *DatabendConsumeStatsRecorder
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
		statsRecorder:    NewDatabendConsumeStatsRecorder(),
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

func (br *KafkaBatchReader) ReadBatch(ctx context.Context) (*message.MessagesBatch, error) {
	l := logrus.WithFields(logrus.Fields{"kafka_batch_reader": "ReadBatch"})

	ctx, cancel := context.WithTimeout(ctx, time.Duration(br.maxBatchInterval)*time.Second)
	defer cancel()

	batchTimeout := time.NewTimer(time.Duration(br.maxBatchInterval) * time.Second)
	defer batchTimeout.Stop()
	startFetchBatchTime := time.Now()

	var (
		lastMessageOffset  int64
		firstMessageOffset int64
		allByteSize        int
		batch              = make([]message.MessageData, 0, br.batchSize)
		allMessages        = make(map[int]*kafka.Message, br.batchSize)
	)

	processMessage := func(m *kafka.Message) message.MessageData {
		data := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(string(m.Value), "\t", ""), "\n", ""))
		return message.MessageData{
			Data:       data,
			DataOffset: m.Offset,
			Partition:  m.Partition,
			Key:        string(m.Key),
			CreateTime: m.Time,
		}
	}

_loop:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-batchTimeout.C:
			break _loop
		default:
			m, err := br.fetchMessageWithTimeout(ctx, time.Duration(br.maxBatchInterval)*time.Second)
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) {
					l.WithError(err).Warn("Failed to read message from Kafka##")
				}
				continue
			}

			if firstMessageOffset == 0 {
				firstMessageOffset = m.Offset
			}
			lastMessageOffset = m.Offset

			batch = append(batch, processMessage(m))
			allByteSize += len(m.Value)
			allMessages[m.Partition] = m

			if len(batch) >= br.batchSize {
				logrus.Infof("Fetched %d messages cost %s, batch size: %d, batch byte size: %d", len(batch), time.Since(startFetchBatchTime), br.batchSize, allByteSize)
				break _loop
			}
		}
	}

	return &message.MessagesBatch{
		Messages:           batch,
		CommitFunc:         br.createCommitFunc(allMessages, l),
		FirstMessageOffset: firstMessageOffset,
		LastMessageOffset:  lastMessageOffset,
	}, nil
}

func (br *KafkaBatchReader) createCommitFunc(messages map[int]*kafka.Message, l *logrus.Entry) func(context.Context) error {
	if len(messages) == 0 {
		return func(_ context.Context) error { return nil }
	}

	return func(ctx context.Context) error {
		for partition, msg := range messages {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err := br.kafkaReader.CommitMessages(ctx, *msg); err != nil {
					l.WithError(err).WithFields(logrus.Fields{
						"partition": partition,
						"offset":    msg.Offset,
					}).Error("Failed to commit message")
					return fmt.Errorf("commit message failed: %w", err)
				}
			}
		}
		return nil
	}
}
