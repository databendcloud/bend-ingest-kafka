package main

import (
	"context"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

type ConsumeWorker struct {
	name        string
	cfg         *Config
	ig          DatabendIngester
	batchReader BatchReader
}

func NewConsumeWorker(cfg *Config, name string, ig DatabendIngester) *ConsumeWorker {
	return &ConsumeWorker{
		name:        name,
		cfg:         cfg,
		ig:          ig,
		batchReader: NewBatchReader(cfg),
	}
}

func (c *ConsumeWorker) Close() {
	logrus.Printf("%v exited", c.name)
	c.batchReader.Close()
}

func (c *ConsumeWorker) stepBatch(ctx context.Context) error {
	logrus.Debug("read batch")
	batch, err := c.batchReader.ReadBatch(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read batch from Kafka: %v\n", err)
		return err
	}
	logrus.Debug("got batch")

	if batch.Empty() {
		return err
	}

	logrus.Debug("DEBUG: ingest data")
	if err := c.ig.IngestData(batch.messages); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to ingest data between %d-%d into Databend: %v\n", batch.firstMessageOffset, batch.lastMessageOffset, err)
		return err
	}

	logrus.Debug("DEBUG: commit")
	if err := batch.commitFunc(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to commit messages at %d: %v\n", batch.lastMessageOffset, err)
		return err
	}
	return nil
}

func (c *ConsumeWorker) Run(ctx context.Context) {
	logrus.Printf("Starting worker %s", c.name)

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
