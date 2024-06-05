package main

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/databendcloud/bend-ingest-kafka/config"
)

type ConsumeWorker struct {
	name        string
	cfg         *config.Config
	ig          DatabendIngester
	batchReader BatchReader
}

func NewConsumeWorker(cfg *config.Config, name string, ig DatabendIngester) *ConsumeWorker {
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
	if err := c.ig.IngestData(batch); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to ingest data between %d-%d into Databend: %v\n", batch.FirstMessageOffset, batch.LastMessageOffset, err)
		return err
	}

	logrus.Debug("DEBUG: commit")
	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		err = batch.CommitFunc(ctx)
		if err != nil {
			if err == context.Canceled {
				logrus.Errorf("Failed to commit messages at %d, attempt %d: %v", batch.LastMessageOffset, i+1, err)
				time.Sleep(1 * time.Second)
				fmt.Printf("Stack trace: %s\n", debug.Stack())
				continue
			}
			fmt.Fprintf(os.Stderr, "Failed to commit messages at %d: %v\n", batch.LastMessageOffset, err)
			return err
		}
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
