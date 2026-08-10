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
