package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type messagesBatch struct {
	messages           []string
	commitFunc         func(context.Context) error
	firstMessageOffset int64
	lastMessageOffset  int64
}

func (b *messagesBatch) Empty() bool {
	return len(b.messages) == 0
}

type ConsumeWorker struct {
	cfg         *Config
	ig          Ingester
	kafkaReader *kafka.Reader
}

func NewConsumeWorker(cfg *Config) *ConsumeWorker {
	ig := NewIngester(cfg)
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: parseKafkaServers(cfg.KafkaBootstrapServers),
		GroupID: cfg.KafkaConsumerGroup,
		Topic:   cfg.KafkaTopic,
	})
	return &ConsumeWorker{
		cfg:         cfg,
		ig:          ig,
		kafkaReader: kafkaReader,
	}
}

func (c *ConsumeWorker) Close() {
	c.kafkaReader.Close()
}

func (c *ConsumeWorker) readBatch(ctx context.Context) (*messagesBatch, error) {
	var (
		lastMessage        *kafka.Message
		lastMessageOffset  int64
		firstMessageOffset int64
		batch              = []string{}
		batchTimeout       = time.NewTimer(c.cfg.BatchMaxInterval)
	)
	defer batchTimeout.Stop()

_loop:
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("exited")
			return nil, nil
		case <-batchTimeout.C:
			break _loop
		default:
			m, err := c.kafkaReader.FetchMessage(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to read message from Kafka: %v\n", err)
				continue
			}
			if firstMessageOffset == 0 {
				firstMessageOffset = m.Offset
			}

			data := string(m.Value)
			batch = append(batch, strings.ReplaceAll(data, "\n", ""))
			lastMessage = &m

			if len(batch) >= c.cfg.BatchSize {
				break _loop
			}
		}
	}

	commitFunc := func(_ context.Context) error { return nil }
	if lastMessage != nil {
		commitFunc = func(ctx context.Context) error {
			return c.kafkaReader.CommitMessages(ctx, *lastMessage)
		}
		lastMessageOffset = lastMessage.Offset
	}

	return &messagesBatch{
		messages:           batch,
		commitFunc:         commitFunc,
		firstMessageOffset: firstMessageOffset,
		lastMessageOffset:  lastMessageOffset,
	}, nil
}

func (c *ConsumeWorker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "exited")
			return
		default:
			batch, err := c.readBatch(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to read batch from Kafka: %v\n", err)
				continue
			}

			if batch.Empty() {
				return
			}

			// TODO: make it at least once
			if err := c.ig.IngestData(batch.messages); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to ingest data between %d-%d into Databend: %v\n", batch.firstMessageOffset, batch.lastMessageOffset, err)
				continue
			}

			if err := batch.commitFunc(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to commit messages at %d: %v\n", batch.lastMessageOffset, err)
				continue
			}
		}
	}
}
