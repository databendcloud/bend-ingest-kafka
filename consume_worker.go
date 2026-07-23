package main

import (
	"context"
	"fmt"
	"log"
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
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		l.Errorf("Failed to read batch from Kafka: %v", err)
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		l.Info("waitting to read batch from Kafka")
		return nil
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
	maxRetryDelay := time.Duration(c.cfg.MaxRetryDelay) * time.Second
	if c.cfg.UseReplaceMode && !c.cfg.IsJsonTransform {
		err := DoRetry(
			func() error {
				return c.ig.IngestParquetData(batch)
			}, maxRetryDelay, "IngestParquetData",
		)
		if err != nil {
			l.Errorf("Failed to ingest data between %d-%d into Databend: %v", batch.FirstMessageOffset, batch.LastMessageOffset, err)
			return err
		}
	} else {
		err := DoRetry(
			func() error {
				return c.ig.IngestData(batch)
			}, maxRetryDelay, "IngestData",
		)
		if err != nil {
			l.Errorf("Failed to ingest data between %d-%d into Databend: %v after retry 5 attempts\n", batch.FirstMessageOffset, batch.LastMessageOffset, err)
			return err
		}
	}

	l.Debug("DEBUG: commit")
	// Retry the commit with a capped backoff, and never block the poll loop
	// for longer than librdkafka's max.poll.interval.ms (default 300s):
	// a member that stops polling is kicked out of the group, after which
	// every commit fails with "Unknown member" and the worker would be stuck
	// in this loop forever.
	const (
		commitMaxRetries = 8
		commitMaxBackoff = 30 * time.Second
	)
	retryInterval := time.Second
	startCommitTime := time.Now()
	for i := 0; i < commitMaxRetries; i++ {
		ctx := context.Background()
		err = batch.CommitFunc(ctx)
		if err == nil {
			break
		}
		l.Errorf("Failed to commit messages at %d, attempt %d/%d: %v", batch.LastMessageOffset, i+1, commitMaxRetries, err)
		if i == commitMaxRetries-1 {
			// No point sleeping after the final attempt; exit the loop and panic.
			break
		}
		time.Sleep(retryInterval)
		retryInterval *= 2
		if retryInterval > commitMaxBackoff {
			retryInterval = commitMaxBackoff
		}
	}
	if err != nil {
		// Give up and crash: the process restarts, rejoins the group, and
		// re-consumes this batch from the last committed offset
		// (at-least-once). The retry budget above is kept well below the
		// 300s max.poll.interval.ms limit.
		panic(fmt.Sprintf("Failed to commit messages at %d after %d attempts: %v", batch.LastMessageOffset, commitMaxRetries, err))
	}

	endConsumeTime := time.Now()
	c.statsRecorder.RecordMetric(allByteSize, len(batch.Messages))
	consumeRowsTotal.Add(float64(len(batch.Messages)))
	consumeBytesTotal.Add(float64(allByteSize))
	stats := c.statsRecorder.Stats(time.Since(startCommitTime))
	log.Printf("consume %d rows (%f rows/s), %d bytes (%f bytes/s) in %d ms", len(batch.Messages), stats.RowsPerSecond, allByteSize, stats.BytesPerSecond, endConsumeTime.Sub(startCommitTime).Milliseconds())

	log.Printf("process %d rows (%f rows/s) in %d ms", len(batch.Messages), float64(len(batch.Messages))/time.Since(batchStartTime).Seconds(), time.Since(batchStartTime).Milliseconds())
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

func DoRetry(f retry.RetryableFunc, maxDelay time.Duration, name string) error {
	delay := time.Second
	return retry.Do(
		func() error {
			return f()
		},
		retry.RetryIf(func(err error) bool {
			if err == nil {
				return false
			}
			if errors.Is(err, ErrUploadStageFailed) || errors.Is(err, ErrCopyIntoFailed) || errors.Is(err, ErrStreamingLoadFailed) {
				return true
			}
			return false
		}),
		retry.OnRetry(func(n uint, err error) {
			logrus.Warnf("%s retry attempts: %d: %s", name, n, err)
		}),
		retry.Delay(delay),
		retry.MaxDelay(maxDelay),
		retry.DelayType(retry.BackOffDelay),
	)
}
