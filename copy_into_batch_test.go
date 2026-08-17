package main

import (
	"context"
	"sync"
	"testing"
	"time"

	godatabend "github.com/datafuselabs/databend-go"
	"github.com/test-go/testify/assert"

	"github.com/databendcloud/bend-ingest-kafka/config"
	"github.com/databendcloud/bend-ingest-kafka/message"
)

type batchingTestIngester struct {
	mu        sync.Mutex
	staged    int
	copyCalls [][]*StagedBatch
	events    []string
	copyErr   error
	stageErr  error
}

func (i *batchingTestIngester) IngestData(*message.MessagesBatch) error        { return nil }
func (i *batchingTestIngester) IngestParquetData(*message.MessagesBatch) error { return nil }
func (i *batchingTestIngester) CreateRawTargetTable() error                    { return nil }
func (i *batchingTestIngester) Close() error                                   { return nil }
func (i *batchingTestIngester) StageData(batch *message.MessagesBatch) (*StagedBatch, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.stageErr != nil {
		return nil, i.stageErr
	}
	i.staged++
	i.events = append(i.events, "upload")
	return &StagedBatch{
		Stage:     &godatabend.StageLocation{Name: "~", Path: "batch/file-" + string(rune('0'+i.staged))},
		Rows:      len(batch.Messages),
		Bytes:     len(batch.Messages[0].Data),
		StartedAt: time.Now(),
	}, nil
}
func (i *batchingTestIngester) CopyInto(staged []*StagedBatch) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.events = append(i.events, "copy")
	i.copyCalls = append(i.copyCalls, append([]*StagedBatch(nil), staged...))
	return i.copyErr
}

type sequenceBatchReader struct {
	batches []*message.MessagesBatch
	closed  bool
}

func (r *sequenceBatchReader) ReadBatch(context.Context) (*message.MessagesBatch, error) {
	if len(r.batches) == 0 {
		return &message.MessagesBatch{}, nil
	}
	b := r.batches[0]
	r.batches = r.batches[1:]
	return b, nil
}
func (r *sequenceBatchReader) Close() error { r.closed = true; return nil }

type blockingBatchReader struct {
	readStarted chan struct{}
	closed      bool
}

func (r *blockingBatchReader) ReadBatch(ctx context.Context) (*message.MessagesBatch, error) {
	select {
	case r.readStarted <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (r *blockingBatchReader) Close() error { r.closed = true; return nil }

type deadlinePartialBatchReader struct {
	batch  *message.MessagesBatch
	closed bool
}

func (r *deadlinePartialBatchReader) ReadBatch(ctx context.Context) (*message.MessagesBatch, error) {
	<-ctx.Done()
	return r.batch, nil
}
func (r *deadlinePartialBatchReader) Close() error { r.closed = true; return nil }

func testBatch(offset int64, commits *[]int64) *message.MessagesBatch {
	return &message.MessagesBatch{
		Messages:           []message.MessageData{{Data: `{"v":1}`, DataOffset: offset, Partition: 0}},
		FirstMessageOffset: offset,
		LastMessageOffset:  offset,
		CommitFunc: func(context.Context) error {
			*commits = append(*commits, offset)
			return nil
		},
	}
}

func newBatchingWorker(count int, batches []*message.MessagesBatch, ig DatabendIngester) (*ConsumeWorker, *sequenceBatchReader) {
	cfg := &config.Config{CopyIntoFileCount: count, MaxRetryDelay: 1}
	reader := &sequenceBatchReader{batches: batches}
	return &ConsumeWorker{
		name:          "batching-test",
		cfg:           cfg,
		ig:            ig,
		batchReader:   reader,
		statsRecorder: NewDatabendConsumeStatsRecorder(),
	}, reader
}

func TestConsumeWorkerUploadsImmediatelyAndCopiesAtFileCount(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{}
	worker, _ := newBatchingWorker(3, []*message.MessagesBatch{
		testBatch(1, &commits), testBatch(2, &commits), testBatch(3, &commits),
	}, ig)

	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.Equal(t, 2, ig.staged)
	assert.Empty(t, ig.copyCalls)
	assert.Empty(t, commits, "offsets must not be committed before COPY INTO")

	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.Len(t, ig.copyCalls, 1)
	assert.Len(t, ig.copyCalls[0], 3)
	assert.Equal(t, []int64{1, 2, 3}, commits)
	assert.Empty(t, worker.pending)
	assert.Equal(t, []string{"upload", "upload", "upload", "copy"}, ig.events)
}

func TestConsumeWorkerGracefulCloseFlushesAndCommitsRemainder(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{}
	worker, reader := newBatchingWorker(5, []*message.MessagesBatch{
		testBatch(10, &commits), testBatch(11, &commits),
	}, ig)

	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.Empty(t, commits)

	worker.Close()
	assert.Len(t, ig.copyCalls, 1)
	assert.Len(t, ig.copyCalls[0], 2)
	assert.Equal(t, []int64{10, 11}, commits)
	assert.True(t, reader.closed)
	assert.Empty(t, worker.pending)
}

func TestConsumeWorkerCopyFailureDoesNotCommitOrDiscardPending(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{copyErr: context.Canceled} // non-retryable
	worker, _ := newBatchingWorker(2, []*message.MessagesBatch{
		testBatch(20, &commits), testBatch(21, &commits),
	}, ig)

	assert.NoError(t, worker.stepBatch(context.Background()))
	err := worker.stepBatch(context.Background())
	assert.Error(t, err)
	assert.Empty(t, commits)
	assert.Len(t, worker.pending, 2)
	assert.Equal(t, 2, ig.staged, "COPY failure must not upload the files again")
}

func TestConsumeWorkerStageFailureDoesNotQueueOrCommit(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{stageErr: context.Canceled}
	worker, _ := newBatchingWorker(2, []*message.MessagesBatch{testBatch(25, &commits)}, ig)

	assert.Error(t, worker.stepBatch(context.Background()))
	assert.Empty(t, worker.pending)
	assert.Empty(t, commits)
	assert.Empty(t, ig.copyCalls)
}

func TestConsumeWorkerGracefulCloseCopyFailureStillClosesReader(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{copyErr: context.Canceled}
	worker, reader := newBatchingWorker(5, []*message.MessagesBatch{testBatch(26, &commits)}, ig)
	assert.NoError(t, worker.stepBatch(context.Background()))

	worker.Close()
	assert.True(t, reader.closed)
	assert.Empty(t, commits)
	assert.Len(t, worker.pending, 1)
}

func TestConsumeWorkerRetriesPendingCopyBeforeReadingMore(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{copyErr: context.Canceled}
	worker, reader := newBatchingWorker(2, []*message.MessagesBatch{
		testBatch(30, &commits), testBatch(31, &commits), testBatch(32, &commits),
	}, ig)

	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.Error(t, worker.stepBatch(context.Background()))
	assert.Len(t, reader.batches, 1)

	ig.copyErr = nil
	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.Len(t, reader.batches, 1, "retrying a full pending group must not read another Kafka batch")
	assert.Equal(t, []int64{30, 31}, commits)
	assert.Equal(t, 2, ig.staged)
}

func TestConsumeWorkerCopiesAtMaxIntervalWithoutNewMessages(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{}
	reader := &blockingBatchReader{readStarted: make(chan struct{}, 1)}
	worker := &ConsumeWorker{
		name: "interval-test",
		cfg: &config.Config{
			CopyIntoFileCount:   20,
			CopyIntoMaxInterval: 1,
			MaxRetryDelay:       1,
		},
		ig:            ig,
		batchReader:   reader,
		statsRecorder: NewDatabendConsumeStatsRecorder(),
		pending: []pendingCopyBatch{{
			batch:  testBatch(40, &commits),
			staged: &StagedBatch{Stage: &godatabend.StageLocation{Name: "~", Path: "batch/file-40"}, Rows: 1, Bytes: 7, StartedAt: time.Now()},
		}},
	}

	start := time.Now()
	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.True(t, time.Since(start) >= 900*time.Millisecond)
	assert.Len(t, ig.copyCalls, 1)
	assert.Len(t, ig.copyCalls[0], 1)
	assert.Equal(t, []int64{40}, commits)
	assert.Empty(t, worker.pending)
}

func TestConsumeWorkerCopiesImmediatelyWhenIntervalAlreadyReached(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{}
	reader := &sequenceBatchReader{batches: []*message.MessagesBatch{testBatch(42, &commits)}}
	worker := &ConsumeWorker{
		name: "expired-interval-test",
		cfg:  &config.Config{CopyIntoFileCount: 20, CopyIntoMaxInterval: 1, MaxRetryDelay: 1},
		ig:   ig, batchReader: reader, statsRecorder: NewDatabendConsumeStatsRecorder(),
		pending: []pendingCopyBatch{{
			batch: testBatch(41, &commits), stagedAt: time.Now().Add(-2 * time.Second),
			staged: &StagedBatch{Stage: &godatabend.StageLocation{Name: "~", Path: "batch/file-41"}, Rows: 1, Bytes: 7, StartedAt: time.Now()},
		}},
	}

	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.Len(t, reader.batches, 1, "an expired group must flush before reading another Kafka batch")
	assert.Equal(t, []int64{41}, commits)
	assert.Len(t, ig.copyCalls, 1)
}

func TestConsumeWorkerFileCountWinsBeforeMaxInterval(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{}
	worker, _ := newBatchingWorker(2, []*message.MessagesBatch{testBatch(50, &commits), testBatch(51, &commits)}, ig)
	worker.cfg.CopyIntoMaxInterval = 60

	start := time.Now()
	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.True(t, time.Since(start) < time.Second)
	assert.Len(t, ig.copyCalls, 1)
	assert.Len(t, ig.copyCalls[0], 2)
	assert.Equal(t, []int64{50, 51}, commits)
}

func TestConsumeWorkerDisabledMaxIntervalKeepsPending(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{}
	worker, _ := newBatchingWorker(20, []*message.MessagesBatch{testBatch(60, &commits)}, ig)
	worker.cfg.CopyIntoMaxInterval = 0

	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.Len(t, worker.pending, 1)
	assert.Empty(t, ig.copyCalls)
	assert.Empty(t, commits)
}

func TestConsumeWorkerParentDeadlineDoesNotForceCopyEarly(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{}
	reader := &blockingBatchReader{readStarted: make(chan struct{}, 1)}
	worker := &ConsumeWorker{
		name: "parent-deadline-test",
		cfg:  &config.Config{CopyIntoFileCount: 20, CopyIntoMaxInterval: 10, MaxRetryDelay: 1},
		ig:   ig, batchReader: reader, statsRecorder: NewDatabendConsumeStatsRecorder(),
		pending: []pendingCopyBatch{{
			batch:  testBatch(70, &commits),
			staged: &StagedBatch{Stage: &godatabend.StageLocation{Name: "~", Path: "batch/file-70"}, Rows: 1, Bytes: 7, StartedAt: time.Now()},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	assert.NoError(t, worker.stepBatch(ctx))
	assert.Empty(t, ig.copyCalls)
	assert.Empty(t, commits)
	assert.Len(t, worker.pending, 1)
}

func TestConsumeWorkerIntervalIncludesPartialBatchReadAtDeadline(t *testing.T) {
	var commits []int64
	ig := &batchingTestIngester{}
	reader := &deadlinePartialBatchReader{batch: testBatch(81, &commits)}
	worker := &ConsumeWorker{
		name: "partial-interval-test",
		cfg:  &config.Config{CopyIntoFileCount: 20, CopyIntoMaxInterval: 1, MaxRetryDelay: 1},
		ig:   ig, batchReader: reader, statsRecorder: NewDatabendConsumeStatsRecorder(),
		pending: []pendingCopyBatch{{
			batch: testBatch(80, &commits), stagedAt: time.Now(),
			staged: &StagedBatch{Stage: &godatabend.StageLocation{Name: "~", Path: "batch/file-80"}, Rows: 1, Bytes: 7, StartedAt: time.Now()},
		}},
	}

	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.Equal(t, 1, ig.staged, "the partial Kafka batch must be uploaded before the timed COPY")
	assert.Len(t, ig.copyCalls, 1)
	assert.Len(t, ig.copyCalls[0], 2)
	assert.Equal(t, []int64{80, 81}, commits)
	assert.Empty(t, worker.pending)
}

func TestConsumeWorkerAggregationDoesNotAffectOtherModes(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  config.Config
	}{
		{name: "streaming load", cfg: config.Config{UseStreamingLoad: true, CopyIntoFileCount: 5}},
		{name: "replace into", cfg: config.Config{UseReplaceMode: true, IsJsonTransform: false, CopyIntoFileCount: 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := &ConsumeWorker{cfg: &tc.cfg}
			assert.False(t, worker.usesBatchedCopyInto())
		})
	}
}
