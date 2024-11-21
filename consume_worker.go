package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"time"

	"github.com/avast/retry-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/databendcloud/bend-ingest-kafka/config"
)

type ConsumeWorker struct {
	name          string
	cfg           *config.Config
	ig            DatabendIngester
	batchReader   BatchReader
	statsRecorder *DatabendConsumeStatsRecorder
}

func NewConsumeWorker(cfg *config.Config, name string, ig DatabendIngester) *ConsumeWorker {
	return &ConsumeWorker{
		name:          name,
		cfg:           cfg,
		ig:            ig,
		batchReader:   NewBatchReader(cfg),
		statsRecorder: NewDatabendConsumeStatsRecorder(),
	}
}

func (c *ConsumeWorker) Close() {
	logrus.Printf("%v exited", c.name)
	c.batchReader.Close()
}

func (c *ConsumeWorker) stepBatch(ctx context.Context) error {
	batchStartTime := time.Now()
	l := logrus.WithFields(logrus.Fields{"consumer_worker": "stepBatch"})
	l.Debug("read batch")
	batch, err := c.batchReader.ReadBatch(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read batch from Kafka: %v\n", err)
		l.Errorf("Failed to read batch from Kafka: %v", err)
		return err
	}
	l.Debug("got batch")

	if batch.Empty() {
		return err
	}
	allByteSize := 0
	for _, m := range batch.Messages {
		allByteSize += len([]byte(m.Data))
	}

	l.Debug("DEBUG: ingest data")
	if c.cfg.UseReplaceMode && !c.cfg.IsJsonTransform {
		err := DoRetry(
			func() error {
				return c.ig.IngestParquetData(batch)
			})
		if err != nil {
			l.Errorf("Failed to ingest data between %d-%d into Databend: %v", batch.FirstMessageOffset, batch.LastMessageOffset, err)
			return err
		}
	} else {
		err := DoRetry(
			func() error {
				return c.ig.IngestData(batch)
			})
		if err != nil {
			l.Errorf("Failed to ingest data between %d-%d into Databend: %v after retry 5 attempts\n", batch.FirstMessageOffset, batch.LastMessageOffset, err)
			return err
		}
	}

	l.Debug("DEBUG: commit")
	// guarantee the commit is successful before moving to the next batch
	maxRetries := 500
	retryInterval := time.Second
	startCommitTime := time.Now()
	for i := 0; i < maxRetries; i++ {
		ctx := context.Background()
		err = batch.CommitFunc(ctx)
		if err != nil {
			l.Errorf("Failed to commit messages at %d, attempt %d: %v", batch.LastMessageOffset, i+1, err)
			time.Sleep(retryInterval)
			retryInterval <<= 1
			fmt.Printf("Stack trace: %s\n", debug.Stack())
			continue
		}
		if i == 500 {
			panic("Failed to commit messages after 500 attempts, need panic")
		}
		endConsumeTime := time.Now()
		c.statsRecorder.RecordMetric(allByteSize, len(batch.Messages))
		stats := c.statsRecorder.Stats(time.Since(startCommitTime))
		log.Printf("consume %d rows (%f rows/s), %d bytes (%f bytes/s) in %d ms", len(batch.Messages), stats.RowsPerSecond, allByteSize, stats.BytesPerSecond, endConsumeTime.Sub(startCommitTime).Milliseconds())

		log.Printf("process %d rows (%f rows/s) in %d ms", len(batch.Messages), float64(len(batch.Messages))/time.Since(batchStartTime).Seconds(), time.Since(batchStartTime).Milliseconds())
		return nil
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

func DoRetry(f retry.RetryableFunc) error {
	delay := time.Second
	maxDelay := 30 * time.Minute
	return retry.Do(
		func() error {
			return f()
		},
		retry.RetryIf(func(err error) bool {
			if err == nil {
				return false
			}
			if errors.Is(err, ErrUploadStageFailed) || errors.Is(err, ErrCopyIntoFailed) {
				return true
			}
			return false
		}),
		retry.Delay(delay),
		retry.MaxDelay(maxDelay),
		retry.DelayType(retry.BackOffDelay),
	)
}
