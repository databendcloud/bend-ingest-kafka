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
	"github.com/databendcloud/bend-ingest-kafka/message"
)

type pendingCopyBatch struct {
	batch        *message.MessagesBatch
	staged       *StagedBatch
	consumedByte int
	// stagedAt is set after StageData succeeds, so COPY waiting time does not
	// include file generation, compression, upload, or upload retries.
	stagedAt time.Time
}

type ConsumeWorker struct {
	name          string
	cfg           *config.Config
	ig            DatabendIngester
	batchReader   BatchReader
	statsRecorder *DatabendConsumeStatsRecorder
	pending       []pendingCopyBatch
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
	if err := c.flushPending(); err != nil {
		logrus.Errorf("Failed to flush pending COPY INTO files for %s during shutdown: %v", c.name, err)
	}
	if err := c.batchReader.Close(); err != nil {
		logrus.Errorf("Failed to close batch reader for %s: %v", c.name, err)
	}
	logrus.Printf("%v exited", c.name)
}

func (c *ConsumeWorker) stepBatch(ctx context.Context) error {
	// A previous COPY INTO may have exhausted its retry budget, or the oldest
	// staged file may have reached its maximum wait. Flush before polling more
	// Kafka data in either case.
	if len(c.pending) >= c.copyIntoFileCount() || c.copyIntoIntervalReached(time.Now()) {
		return c.flushPending()
	}
	batchStartTime := time.Now()
	l := logrus.WithFields(logrus.Fields{"consumer_worker": "stepBatch"})
	l.Debug("read batch")
	readCtx, cancelRead := c.batchReadContext(ctx)
	defer cancelRead()
	batch, err := c.batchReader.ReadBatch(readCtx)
	if c.isCopyIntoTimerDeadline(err, ctx) {
		return c.flushPending()
	}
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
	if c.usesBatchedCopyInto() {
		var staged *StagedBatch
		err := DoRetry(func() error {
			var stageErr error
			staged, stageErr = c.ig.StageData(batch)
			return stageErr
		}, maxRetryDelay, "StageData")
		if err != nil {
			l.Errorf("Failed to upload data between %d-%d to stage: %v", batch.FirstMessageOffset, batch.LastMessageOffset, err)
			return err
		}
		c.pending = append(c.pending, pendingCopyBatch{batch: batch, staged: staged, consumedByte: allByteSize, stagedAt: time.Now()})
		l.WithFields(logrus.Fields{
			"pending_file_count":   len(c.pending),
			"copy_into_file_count": c.copyIntoFileCount(),
		}).Info("Stage file pending COPY INTO")
		if len(c.pending) >= c.copyIntoFileCount() || c.copyIntoIntervalReached(time.Now()) {
			return c.flushPending()
		}
		return nil
	}

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

	if err := c.commitBatch(batch, l); err != nil {
		return err
	}

	c.recordConsumed(allByteSize, len(batch.Messages), batchStartTime)
	return nil
}

func (c *ConsumeWorker) usesBatchedCopyInto() bool {
	return !c.cfg.UseStreamingLoad && !(c.cfg.UseReplaceMode && !c.cfg.IsJsonTransform)
}

func (c *ConsumeWorker) copyIntoFileCount() int {
	if c.cfg.CopyIntoFileCount <= 0 {
		// Config values constructed directly in code do not pass through defaults.
		return 128
	}
	return c.cfg.CopyIntoFileCount
}

func (c *ConsumeWorker) copyIntoDeadline() (time.Time, bool) {
	if len(c.pending) == 0 || c.cfg.CopyIntoMaxInterval <= 0 {
		return time.Time{}, false
	}
	startedAt := c.pending[0].stagedAt
	if startedAt.IsZero() {
		// Keep manually constructed workers and pending state backward compatible.
		startedAt = c.pending[0].staged.StartedAt
	}
	return startedAt.Add(time.Duration(c.cfg.CopyIntoMaxInterval) * time.Second), true
}

func (c *ConsumeWorker) copyIntoIntervalReached(now time.Time) bool {
	deadline, ok := c.copyIntoDeadline()
	return ok && !now.Before(deadline)
}

func (c *ConsumeWorker) batchReadContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := c.copyIntoDeadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	if parentDeadline, hasParentDeadline := ctx.Deadline(); hasParentDeadline && !deadline.Before(parentDeadline) {
		return context.WithCancel(ctx)
	}
	return context.WithDeadline(ctx, deadline)
}

func (c *ConsumeWorker) isCopyIntoTimerDeadline(err error, parent context.Context) bool {
	return errors.Is(err, context.DeadlineExceeded) && parent.Err() == nil && c.copyIntoIntervalReached(time.Now())
}

func (c *ConsumeWorker) flushPending() error {
	if len(c.pending) == 0 {
		return nil
	}
	staged := make([]*StagedBatch, 0, len(c.pending))
	for _, pending := range c.pending {
		staged = append(staged, pending.staged)
	}
	maxRetryDelay := time.Duration(c.cfg.MaxRetryDelay) * time.Second
	if err := DoRetry(func() error { return c.ig.CopyInto(staged) }, maxRetryDelay, "CopyInto"); err != nil {
		return err
	}

	l := logrus.WithFields(logrus.Fields{"consumer_worker": c.name, "copy_file_count": len(c.pending)})
	for _, pending := range c.pending {
		if err := c.commitBatch(pending.batch, l); err != nil {
			return err
		}
		c.recordConsumed(pending.consumedByte, len(pending.batch.Messages), pending.staged.StartedAt)
	}
	c.pending = nil
	return nil
}

func (c *ConsumeWorker) recordConsumed(byteSize, rows int, startedAt time.Time) {
	c.statsRecorder.RecordMetric(byteSize, rows)
	consumeRowsTotal.Add(float64(rows))
	consumeBytesTotal.Add(float64(byteSize))
	stats := c.statsRecorder.Stats(time.Since(startedAt))
	log.Printf("consume %d rows (%f rows/s), %d bytes (%f bytes/s) in %d ms", rows, stats.RowsPerSecond, byteSize, stats.BytesPerSecond, time.Since(startedAt).Milliseconds())
}

func (c *ConsumeWorker) commitBatch(batch *message.MessagesBatch, l *logrus.Entry) error {
	var err error
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
		panic(fmt.Sprintf("Failed to commit messages at %d after %d attempts: %v", batch.LastMessageOffset, commitMaxRetries, err))
	}
	return nil
}

func (c *ConsumeWorker) Run(ctx context.Context) {
	logrus.Printf("Starting worker %s", c.name)
	defer c.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := c.stepBatch(ctx); err != nil {
				logrus.WithError(err).WithField("consumer_worker", c.name).Error("Worker step failed")
			}
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
