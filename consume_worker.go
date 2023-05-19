package main

import (
	"context"
	"fmt"
	"os"
)

type ConsumeWorker struct {
	cfg         *Config
	ig          Ingester
	batchReader BatchReader
}

func NewConsumeWorker(cfg *Config) *ConsumeWorker {
	ig := NewIngester(cfg)

	return &ConsumeWorker{
		cfg:         cfg,
		ig:          ig,
		batchReader: NewKafkaBatchReader(cfg),
	}
}

func (c *ConsumeWorker) Close() {
	c.batchReader.Close()
}

func (c *ConsumeWorker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "exited")
			c.Close()
			return
		default:
			batch, err := c.batchReader.ReadBatch(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to read batch from Kafka: %v\n", err)
				continue
			}

			if batch.Empty() {
				continue
			}

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
