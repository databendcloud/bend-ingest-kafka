package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/test-go/testify/assert"

	"github.com/databendcloud/bend-ingest-kafka/config"
	"github.com/databendcloud/bend-ingest-kafka/message"
)

func TestValidateConfig_Valid(t *testing.T) {
	cfg := &config.Config{
		IsJsonTransform: true,
		UseReplaceMode:  false,
		UseStreamingLoad: false,
	}
	assert.NotPanics(t, func() { validateConfig(cfg) })
}

func TestValidateConfig_ReplaceWithJsonTransform(t *testing.T) {
	cfg := &config.Config{
		IsJsonTransform: true,
		UseReplaceMode:  true,
	}
	assert.Panics(t, func() { validateConfig(cfg) })
}

func TestValidateConfig_StreamingLoadWithJsonTransform(t *testing.T) {
	cfg := &config.Config{
		IsJsonTransform:  true,
		UseStreamingLoad: true,
	}
	assert.Panics(t, func() { validateConfig(cfg) })
}

func TestValidateConfig_StreamingLoadWithReplaceMode(t *testing.T) {
	cfg := &config.Config{
		IsJsonTransform:  false,
		UseStreamingLoad: true,
		UseReplaceMode:   true,
	}
	assert.Panics(t, func() { validateConfig(cfg) })
}

func TestValidateConfig_SASLWithoutUser(t *testing.T) {
	cfg := &config.Config{
		IsSASL:   true,
		SaslUser: "",
	}
	assert.Panics(t, func() { validateConfig(cfg) })
}

func TestValidateConfig_SASLWithUser(t *testing.T) {
	cfg := &config.Config{
		IsSASL:   true,
		SaslUser: "admin",
	}
	assert.NotPanics(t, func() { validateConfig(cfg) })
}

func TestMockBatchReader(t *testing.T) {
	sampleData := message.MessageData{
		Data:       `{"name":"test"}`,
		DataOffset: 42,
		Partition:  1,
		Key:        "key1",
		CreateTime: time.Now(),
	}

	reader := NewMockBatchReader(sampleData, 5)
	assert.NotNil(t, reader)

	batch, err := reader.ReadBatch(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, batch)
	assert.Equal(t, 5, len(batch.Messages))
	assert.Equal(t, int64(-1), batch.FirstMessageOffset)
	assert.Equal(t, int64(-1), batch.LastMessageOffset)

	for _, msg := range batch.Messages {
		assert.Equal(t, `{"name":"test"}`, msg.Data)
		assert.Equal(t, int64(42), msg.DataOffset)
		assert.Equal(t, 1, msg.Partition)
		assert.Equal(t, "key1", msg.Key)
	}

	err = batch.CommitFunc(context.Background())
	assert.NoError(t, err)

	err = reader.Close()
	assert.NoError(t, err)
}

func TestNewBatchReaderWithMockData(t *testing.T) {
	cfg := &config.Config{
		MockData:  `{"Data":"hello","DataOffset":1,"Partition":0,"Key":"k"}`,
		BatchSize: 3,
	}
	reader := NewBatchReader(cfg)
	assert.NotNil(t, reader)

	batch, err := reader.ReadBatch(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 3, len(batch.Messages))

	err = reader.Close()
	assert.NoError(t, err)
}

func TestConsumeWorkerClose(t *testing.T) {
	sampleData := message.MessageData{Data: `{"x":1}`}

	cfg := &config.Config{BatchSize: 1, BatchMaxInterval: 5}
	w := &ConsumeWorker{
		name:          "test-worker",
		cfg:           cfg,
		batchReader:   NewMockBatchReader(sampleData, 1),
		statsRecorder: NewDatabendConsumeStatsRecorder(),
	}

	w.Close()
}

func TestConsumeWorkerRunCancellation(t *testing.T) {
	cfg := &config.Config{
		MockData:         `{"Data":"hello","DataOffset":1,"Partition":0,"Key":"k"}`,
		BatchSize:        1,
		BatchMaxInterval: 5,
		DatabendDSN:      "http://root:@localhost:8002",
		DatabendTable:    "nonexistent",
		DataFormat:       "json",
		IsJsonTransform:  true,
		UserStage:        "~",
	}

	ig := NewDatabendIngester(cfg)
	w := NewConsumeWorker(cfg, "test-cancel-worker", ig)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}
}

func TestConsumeWorkerRunWithStepBatch(t *testing.T) {
	cfg := &config.Config{
		BatchSize:        10,
		BatchMaxInterval: 1,
		DatabendDSN:      "http://databend:databend@localhost:8002",
		DatabendTable:    "default.test",
		DataFormat:       "json",
		IsJsonTransform:  true,
		UserStage:        "~",
	}

	ig := NewDatabendIngester(cfg)
	w := &ConsumeWorker{
		name:          "test-run-step",
		cfg:           cfg,
		ig:            ig,
		batchReader:   &deadlineExceededReader{},
		statsRecorder: NewDatabendConsumeStatsRecorder(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after context timeout")
	}
}

func TestNewBatchReaderInvalidMockData(t *testing.T) {
	// NewBatchReader with invalid MockData should fatal - can't test directly
	// but we can test the valid path
	cfg := &config.Config{
		MockData:  `{"Data":"x"}`,
		BatchSize: 2,
	}
	reader := NewBatchReader(cfg)
	assert.NotNil(t, reader)
	batch, err := reader.ReadBatch(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, len(batch.Messages))
}

func TestValidateConfig_DisableTLSWithoutSASL(t *testing.T) {
	cfg := &config.Config{
		DisableTLS: true,
		IsSASL:     false,
	}
	assert.NotPanics(t, func() { validateConfig(cfg) })
}

func TestValidateConfig_DisableTLSWithSASL(t *testing.T) {
	cfg := &config.Config{
		DisableTLS: true,
		IsSASL:     true,
		SaslUser:   "user",
	}
	assert.NotPanics(t, func() { validateConfig(cfg) })
}

func TestReWriteParquetJsonData(t *testing.T) {
	cfg := &config.Config{
		DatabendDSN:   "http://databend:databend@localhost:8002",
		DatabendTable: "default.test_parquet",
		DataFormat:    "json",
		UserStage:     "~",
	}
	ig := NewDatabendIngester(cfg)
	ingester := ig.(*databendIngester)

	batch := &message.MessagesBatch{
		Messages: []message.MessageData{
			{
				Data:       `{"name":"Alice","age":30}`,
				DataOffset: 100,
				Partition:  0,
				Key:        "key1",
				CreateTime: time.Now(),
			},
			{
				Data:       `{"name":"Bob","age":25}`,
				DataOffset: 101,
				Partition:  0,
				Key:        "key2",
				CreateTime: time.Now(),
			},
		},
		FirstMessageOffset: 100,
		LastMessageOffset:  101,
	}

	results, err := ingester.reWriteParquetJsonData(batch)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))

	// Each result should be valid JSON containing parquet record fields
	for _, r := range results {
		assert.Contains(t, r, "uuid")
		assert.Contains(t, r, "koffset")
		assert.Contains(t, r, "kpartition")
		assert.Contains(t, r, "raw_data")
		assert.Contains(t, r, "record_metadata")
		assert.Contains(t, r, "add_time")
	}
}

func TestGenerateParquetFile(t *testing.T) {
	cfg := &config.Config{
		DatabendDSN:   "http://databend:databend@localhost:8002",
		DatabendTable: "default.test_parquet",
		DataFormat:    "json",
		UserStage:     "~",
	}
	ig := NewDatabendIngester(cfg)
	ingester := ig.(*databendIngester)

	batch := &message.MessagesBatch{
		Messages: []message.MessageData{
			{
				Data:       `{"name":"Alice","age":30}`,
				DataOffset: 1,
				Partition:  0,
				Key:        "key1",
				CreateTime: time.Now(),
			},
		},
		FirstMessageOffset: 1,
		LastMessageOffset:  1,
	}

	jsonData, err := ingester.reWriteParquetJsonData(batch)
	assert.NoError(t, err)

	filePath, size, err := ingester.generateParquetFile(jsonData)
	assert.NoError(t, err)
	assert.NotEmpty(t, filePath)
	assert.True(t, size > 0)

	// Clean up
	os.Remove(filePath)
}

func TestDoRetry_Success(t *testing.T) {
	attempts := 0
	err := DoRetry(func() error {
		attempts++
		return nil
	}, 5*time.Second, "test")
	assert.NoError(t, err)
	assert.Equal(t, 1, attempts)
}

func TestDoRetry_NonRetriableError(t *testing.T) {
	attempts := 0
	err := DoRetry(func() error {
		attempts++
		return fmt.Errorf("some random error")
	}, 5*time.Second, "test")
	assert.Error(t, err)
	assert.Equal(t, 1, attempts)
}

func TestDoRetry_RetriableError(t *testing.T) {
	attempts := 0
	err := DoRetry(func() error {
		attempts++
		if attempts < 3 {
			return ErrCopyIntoFailed
		}
		return nil
	}, 5*time.Second, "test")
	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestIngesterStatsRecorder(t *testing.T) {
	recorder := NewDatabendIntesterStatsRecorder()
	recorder.RecordMetric(1024, 10)
	recorder.RecordMetric(2048, 20)

	stats := recorder.Stats(5 * time.Second)
	assert.True(t, stats.BytesPerSecond >= 0)
	assert.True(t, stats.RowsPerSecond >= 0)
}

func TestConsumeStatsRecorder(t *testing.T) {
	recorder := NewDatabendConsumeStatsRecorder()
	recorder.RecordMetric(512, 5)

	stats := recorder.Stats(5 * time.Second)
	assert.True(t, stats.BytesPerSecond >= 0)
	assert.True(t, stats.RowsPerSecond >= 0)
}

type deadlineExceededReader struct{}

func (r *deadlineExceededReader) ReadBatch(_ context.Context) (*message.MessagesBatch, error) {
	return nil, context.DeadlineExceeded
}

func (r *deadlineExceededReader) Close() error {
	return nil
}

type errorReader struct {
	err error
}

func (r *errorReader) ReadBatch(_ context.Context) (*message.MessagesBatch, error) {
	return nil, r.err
}

func (r *errorReader) Close() error {
	return nil
}

func TestStepBatchWithDeadlineExceeded(t *testing.T) {
	cfg := &config.Config{
		BatchSize:        1,
		BatchMaxInterval: 1,
		DatabendDSN:      "http://databend:databend@localhost:8002",
		DatabendTable:    "nonexistent_table",
		DataFormat:       "json",
		IsJsonTransform:  true,
		UserStage:        "~",
	}

	ig := NewDatabendIngester(cfg)
	w := &ConsumeWorker{
		name:          "test-deadline",
		cfg:           cfg,
		ig:            ig,
		batchReader:   &deadlineExceededReader{},
		statsRecorder: NewDatabendConsumeStatsRecorder(),
	}

	err := w.stepBatch(context.Background())
	assert.Nil(t, err)
}

func TestStepBatchWithReadError(t *testing.T) {
	cfg := &config.Config{
		BatchSize:        1,
		BatchMaxInterval: 1,
		DatabendDSN:      "http://databend:databend@localhost:8002",
		DatabendTable:    "nonexistent_table",
		DataFormat:       "json",
		IsJsonTransform:  true,
		UserStage:        "~",
	}

	ig := NewDatabendIngester(cfg)
	w := &ConsumeWorker{
		name:          "test-error",
		cfg:           cfg,
		ig:            ig,
		batchReader:   &errorReader{err: fmt.Errorf("connection lost")},
		statsRecorder: NewDatabendConsumeStatsRecorder(),
	}

	err := w.stepBatch(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection lost")
}

func TestByteSliceToString(t *testing.T) {
	assert.Equal(t, "", byteSliceToString(nil))
	assert.Equal(t, "", byteSliceToString([]byte{}))
	assert.Equal(t, "hello", byteSliceToString([]byte("hello")))
}

func TestParseConfigWithFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config-test-*.json")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	content := `{
		"kafkaBootstrapServers": "localhost:9092",
		"kafkaTopic": "test",
		"KafkaConsumerGroup": "group1",
		"databendDSN": "http://root:@localhost:8000",
		"databendTable": "default.t",
		"batchSize": 50,
		"workers": 2
	}`
	_, err = tmpFile.WriteString(content)
	assert.NoError(t, err)
	tmpFile.Close()

	path := tmpFile.Name()
	cfg := parseConfigWithFile(&path)
	assert.Equal(t, "localhost:9092", cfg.KafkaBootstrapServers)
	assert.Equal(t, "test", cfg.KafkaTopic)
	assert.Equal(t, 50, cfg.BatchSize)
	assert.Equal(t, 2, cfg.Workers)
}

func TestDatabendIngesterClose(t *testing.T) {
	cfg := &config.Config{
		DatabendDSN:   "http://databend:databend@localhost:8002",
		DatabendTable: "default.test_close",
		DataFormat:    "json",
		UserStage:     "~",
	}
	ig := NewDatabendIngester(cfg)
	err := ig.Close()
	assert.NoError(t, err)
}

func TestStepBatchWithEmptyBatch(t *testing.T) {
	emptyReader := &deadlineExceededReader{}

	cfg := &config.Config{
		BatchSize:        10,
		BatchMaxInterval: 1,
		DatabendDSN:      "http://databend:databend@localhost:8002",
		DatabendTable:    "default.test",
		DataFormat:       "json",
		IsJsonTransform:  true,
		UserStage:        "~",
	}

	ig := NewDatabendIngester(cfg)
	w := &ConsumeWorker{
		name:          "test-empty",
		cfg:           cfg,
		ig:            ig,
		batchReader:   emptyReader,
		statsRecorder: NewDatabendConsumeStatsRecorder(),
	}

	err := w.stepBatch(context.Background())
	assert.Nil(t, err)
}

func TestGetFileSize(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "size-test-*")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString("hello world")
	assert.NoError(t, err)
	tmpFile.Close()

	size := getFileSize(tmpFile.Name())
	assert.Equal(t, 11, size)
}

func TestGenerateNDJsonFileNoCompression(t *testing.T) {
	cfg := &config.Config{
		DatabendDSN:               "http://databend:databend@localhost:8002",
		DatabendTable:             "default.test",
		DataFormat:                "json",
		UserStage:                 "~",
		CopyIntoUploadCompression: false,
	}
	ig := NewDatabendIngester(cfg)
	ingester := ig.(*databendIngester)

	data := []string{
		`{"name":"Alice","age":30}`,
		`{"name":"Bob","age":25}`,
	}

	filePath, size, err := ingester.generateNDJsonFile(data)
	assert.NoError(t, err)
	assert.NotEmpty(t, filePath)
	assert.True(t, size > 0)
	assert.Contains(t, filePath, ".ndjson")
	assert.NotContains(t, filePath, ".zst")

	os.Remove(filePath)
}

func TestGenerateNDJsonFileWithCompression(t *testing.T) {
	cfg := &config.Config{
		DatabendDSN:               "http://databend:databend@localhost:8002",
		DatabendTable:             "default.test",
		DataFormat:                "json",
		UserStage:                 "~",
		CopyIntoUploadCompression: true,
	}
	ig := NewDatabendIngester(cfg)
	ingester := ig.(*databendIngester)

	data := []string{
		`{"name":"Alice","age":30}`,
		`{"name":"Bob","age":25}`,
		`{"name":"Charlie","age":35}`,
	}

	filePath, size, err := ingester.generateNDJsonFile(data)
	assert.NoError(t, err)
	assert.NotEmpty(t, filePath)
	assert.True(t, size > 0)
	assert.Contains(t, filePath, ".ndjson.zst")

	os.Remove(filePath)
}

func TestIngestDataStreamingLoadWithMock(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"test","stats":{"rows":2,"bytes":200}}`))
	}))
	defer server.Close()

	cfg := &config.Config{
		DatabendDSN:      fmt.Sprintf("http://user:pass@%s?sslmode=disable", server.Listener.Addr().String()),
		DatabendTable:    "default.test_ingest",
		UseStreamingLoad: true,
		IsJsonTransform:  true,
		DataFormat:       "json",
	}

	ig := NewDatabendIngester(cfg)
	batch := &message.MessagesBatch{
		Messages: []message.MessageData{
			{Data: `{"name":"Alice","age":30}`, DataOffset: 1, Partition: 0, Key: "k1", CreateTime: time.Now()},
			{Data: `{"name":"Bob","age":25}`, DataOffset: 2, Partition: 0, Key: "k2", CreateTime: time.Now()},
		},
		FirstMessageOffset: 1,
		LastMessageOffset:  2,
		CommitFunc:         func(_ context.Context) error { return nil },
	}

	err := ig.IngestData(batch)
	assert.NoError(t, err)
}

func TestIngestDataEmptyBatch(t *testing.T) {
	cfg := &config.Config{
		DatabendDSN:   "http://databend:databend@localhost:8002",
		DatabendTable: "default.test",
		DataFormat:    "json",
		UserStage:     "~",
	}
	ig := NewDatabendIngester(cfg)

	batch := &message.MessagesBatch{}
	err := ig.IngestData(batch)
	assert.NoError(t, err)
}

func TestIngestDataStreamingLoadRawMode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"test","stats":{"rows":1,"bytes":100}}`))
	}))
	defer server.Close()

	cfg := &config.Config{
		DatabendDSN:      fmt.Sprintf("http://user:pass@%s?sslmode=disable", server.Listener.Addr().String()),
		DatabendTable:    "default.test_raw",
		UseStreamingLoad: true,
		IsJsonTransform:  false,
		DataFormat:       "json",
	}

	ig := NewDatabendIngester(cfg)
	batch := &message.MessagesBatch{
		Messages: []message.MessageData{
			{Data: `{"name":"Alice"}`, DataOffset: 5, Partition: 1, Key: "k1", CreateTime: time.Now()},
		},
		FirstMessageOffset: 5,
		LastMessageOffset:  5,
		CommitFunc:         func(_ context.Context) error { return nil },
	}

	err := ig.IngestData(batch)
	assert.NoError(t, err)
}
