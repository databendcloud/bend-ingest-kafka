package main

import (
	"context"
	"fmt"
	"log"
	"os"
)

type ConsumeWorker struct {
	name        string
	cfg         *Config
	ig          Ingester
	batchReader BatchReader
}

func NewConsumeWorker(cfg *Config, name string) *ConsumeWorker {
	ig := NewIngester(cfg)

	return &ConsumeWorker{
		name:        name,
		cfg:         cfg,
		ig:          ig,
		batchReader: NewKafkaBatchReader(cfg),
	}
}

func (c *ConsumeWorker) Close() {
	log.Printf("exited")
	c.batchReader.Close()
}

func (c *ConsumeWorker) stepBatch(ctx context.Context) error {
	log.Printf("DEBUG: read batch")
	batch, err := c.batchReader.ReadBatch(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read batch from Kafka: %v\n", err)
		return err
	}
	log.Printf("DEBUG: got batch")

	if batch.Empty() {
		return err
	}

	log.Printf("DEBUG: ingest data")
	if err := c.ig.IngestData(batch.messages); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to ingest data between %d-%d into Databend: %v\n", batch.firstMessageOffset, batch.lastMessageOffset, err)
		return err
	}

	log.Printf("DEBUG: commit")
	if err := batch.commitFunc(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to commit messages at %d: %v\n", batch.lastMessageOffset, err)
		return err
	}
	return nil
}

func (c *ConsumeWorker) Run(ctx context.Context) {
	log.Printf("Starting worker %s", c.name)

	for {
		select {
		case <-ctx.Done():
			c.Close()
			return
		default:
			c.stepBatch(ctx)
		}
	}
}
