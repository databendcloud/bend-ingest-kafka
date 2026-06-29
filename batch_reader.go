package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
	consumer         *kafka.Consumer
	topic            string
	batchSize        int
	maxBatchInterval int
	statsRecorder    *DatabendConsumeStatsRecorder
}

func BuildKafkaConfigMap(cfg *config.Config) *kafka.ConfigMap {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.KafkaBootstrapServers,
		"group.id":           cfg.KafkaConsumerGroup,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"fetch.min.bytes":    cfg.MinBytes,
		"fetch.wait.max.ms":  cfg.MaxWait * 1000,
	}

	if cfg.IsSASL {
		_ = configMap.SetKey("sasl.mechanism", "PLAIN")
		_ = configMap.SetKey("sasl.username", cfg.SaslUser)
		_ = configMap.SetKey("sasl.password", cfg.SaslPassword)
		if cfg.DisableTLS {
			_ = configMap.SetKey("security.protocol", "SASL_PLAINTEXT")
		} else {
			_ = configMap.SetKey("security.protocol", "SASL_SSL")
		}
	} else {
		if cfg.DisableTLS {
			_ = configMap.SetKey("security.protocol", "PLAINTEXT")
		} else {
			_ = configMap.SetKey("security.protocol", "SSL")
		}
	}

	return configMap
}

func NewKafkaBatchReader(cfg *config.Config) *KafkaBatchReader {
	configMap := BuildKafkaConfigMap(cfg)

	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		logrus.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	err = consumer.Subscribe(cfg.KafkaTopic, nil)
	if err != nil {
		logrus.Fatalf("Failed to subscribe to topic %s: %v", cfg.KafkaTopic, err)
	}

	return &KafkaBatchReader{
		consumer:         consumer,
		topic:            cfg.KafkaTopic,
		batchSize:        cfg.BatchSize,
		maxBatchInterval: cfg.BatchMaxInterval,
		statsRecorder:    NewDatabendConsumeStatsRecorder(),
	}
}

func (br *KafkaBatchReader) Close() error {
	return br.consumer.Close()
}

func (br *KafkaBatchReader) ReadBatch(ctx context.Context) (*message.MessagesBatch, error) {
	l := logrus.WithFields(logrus.Fields{"kafka_batch_reader": "ReadBatch"})

	startFetchBatchTime := time.Now()
	var (
		lastMessageOffset  int64 = -1
		firstMessageOffset int64 = -1
		allByteSize        int
		batch              = make([]message.MessageData, 0, br.batchSize)
		allMessages        = make(map[int32]*kafka.Message, br.batchSize)
		lastMessageTime    = time.Now()
		noMessageCount     = 0
		maxEmptyAttempts   = 3
		maxMessageInterval = 2 * time.Second
	)

	processMessage := func(m *kafka.Message) message.MessageData {
		data := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(string(m.Value), "\t", ""), "\n", ""))
		return message.MessageData{
			Data:       data,
			DataOffset: int64(m.TopicPartition.Offset),
			Partition:  int(m.TopicPartition.Partition),
			Key:        string(m.Key),
			CreateTime: m.Timestamp,
		}
	}

	batchDeadline := time.Now().Add(time.Duration(br.maxBatchInterval) * time.Second)
	pollTimeoutMs := 500

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				break
			}
			return nil, ctx.Err()
		default:
		}

		if time.Now().After(batchDeadline) && len(batch) > 0 {
			l.Infof("Batch timeout reached with %d messages", len(batch))
			break
		}

		// If we've waited 2x the batch interval with no messages at all, return empty
		if time.Now().After(batchDeadline.Add(time.Duration(br.maxBatchInterval)*time.Second)) && len(batch) == 0 {
			break
		}

		if len(batch) > 0 && time.Since(lastMessageTime) > maxMessageInterval {
			l.Infof("Message interval exceeded %v, ending batch with %d messages",
				maxMessageInterval, len(batch))
			break
		}

		ev := br.consumer.Poll(pollTimeoutMs)
		if ev == nil {
			if len(batch) > 0 {
				noMessageCount++
				if noMessageCount >= maxEmptyAttempts {
					l.Infof("No new messages after %d attempts, returning current batch",
						maxEmptyAttempts)
					break
				}
			}
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			noMessageCount = 0
			lastMessageTime = time.Now()

			if firstMessageOffset == -1 {
				firstMessageOffset = int64(e.TopicPartition.Offset)
			}
			lastMessageOffset = int64(e.TopicPartition.Offset)

			batch = append(batch, processMessage(e))
			allByteSize += len(e.Value)
			allMessages[e.TopicPartition.Partition] = e

			if len(batch) >= br.batchSize {
				l.Infof("Batch full with %d messages in %v", len(batch),
					time.Since(startFetchBatchTime))
				break
			}
			continue

		case kafka.Error:
			if e.IsFatal() {
				if len(batch) > 0 {
					l.WithError(e).Warn("Fatal Kafka error, processing current batch")
					break
				}
				l.WithError(e).Error("Fatal Kafka error with empty batch")
				return nil, e
			}
			l.WithError(e).Warn("Transient Kafka error, continuing")
			continue

		default:
			continue
		}
		break
	}

	batchDuration := time.Since(startFetchBatchTime)
	l.WithFields(logrus.Fields{
		"batch_size":         len(batch),
		"bytes_size":         allByteSize,
		"first_offset":       firstMessageOffset,
		"last_offset":        lastMessageOffset,
		"duration_ms":        batchDuration.Milliseconds(),
		"messages_per_ms":    float64(len(batch)) / float64(batchDuration.Milliseconds()+1),
		"last_message_delay": time.Since(lastMessageTime).Milliseconds(),
	}).Info("Batch complete")

	if len(batch) == 0 {
		return &message.MessagesBatch{
			Messages:           batch,
			FirstMessageOffset: -1,
			LastMessageOffset:  -1,
			CommitFunc:         func(context.Context) error { return nil },
		}, nil
	}

	return &message.MessagesBatch{
		Messages:           batch,
		CommitFunc:         br.createCommitFunc(allMessages, l),
		FirstMessageOffset: firstMessageOffset,
		LastMessageOffset:  lastMessageOffset,
	}, nil
}

func (br *KafkaBatchReader) createCommitFunc(messages map[int32]*kafka.Message, l *logrus.Entry) func(context.Context) error {
	if len(messages) == 0 {
		return func(_ context.Context) error { return nil }
	}

	return func(ctx context.Context) error {
		for partition, msg := range messages {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if _, err := br.consumer.CommitMessage(msg); err != nil {
					l.WithError(err).WithFields(logrus.Fields{
						"partition": partition,
						"offset":   msg.TopicPartition.Offset,
					}).Error("Failed to commit message")
					return fmt.Errorf("commit message failed: %w", err)
				}
			}
		}
		return nil
	}
}
