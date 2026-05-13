package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	godatabend "github.com/datafuselabs/databend-go"
	"github.com/klauspost/compress/zstd"
	"github.com/test-go/testify/assert"

	"github.com/databendcloud/bend-ingest-kafka/config"
	"github.com/databendcloud/bend-ingest-kafka/message"
)

type ingestDatabendTest struct {
	databendDSN string
}

func prepareIngestDatabendTest() *ingestDatabendTest {
	testDatabendDSN := os.Getenv("TEST_DATABEND_DSN")
	if testDatabendDSN == "" {
		testDatabendDSN = "http://databend:databend@localhost:8000?presigned_url_disabled=true"
	}
	return &ingestDatabendTest{databendDSN: testDatabendDSN}
}

func TestParseKafkaServers(t *testing.T) {
	s1 := []string{"127.0.0.1:8000", "127.0.0.1:8000,127.0.0.2:8000"}
	for _, s := range s1 {
		res := parseKafkaServers(s)
		fmt.Println(res)
	}
}

func TestIngestDataIsJsonTransform(t *testing.T) {
	tt := prepareIngestDatabendTest()
	cfg := config.Config{
		KafkaBootstrapServers: "127.0.0.1:9002",
		KafkaTopic:            "test",
		KafkaConsumerGroup:    "test",
		DatabendDSN:           tt.databendDSN,
		// DatabendDSN:      os.Getenv("TEST_DATABEND_DSN"),
		DataFormat:       "json",
		IsJsonTransform:  true,
		DatabendTable:    "test_ingest",
		BatchSize:        10,
		BatchMaxInterval: 10,
		UserStage:        "~",
	}
	db, err := sql.Open("databend", cfg.DatabendDSN)
	assert.NoError(t, err)
	execute(db, "create table if not exists test_ingest(name varchar, age int, isMarried boolean);")
	defer execute(db, "drop table if exists test_ingest;")

	testData := []string{"{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}", "{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}"}
	messageData := message.MessageData{
		Data:       testData[0],
		DataOffset: 1,
		Partition:  1,
		Key:        "1",
		CreateTime: time.Now(),
	}
	messagesBatch := &message.MessagesBatch{Messages: []message.MessageData{messageData}}
	ig := NewDatabendIngester(&cfg)
	err = ig.IngestData(messagesBatch)
	assert.NoError(t, err)

	// check the data
	result, err := db.Query("select * from test_ingest")
	assert.NoError(t, err)
	count := 0
	for result.Next() {
		count += 1
		var name string
		var age int
		var isMarried bool
		err = result.Scan(&name, &age, &isMarried)
		fmt.Println(name, age, isMarried)
	}
	assert.NotEqual(t, 0, count)
}

type recordMetadata struct {
	CreateTime time.Time `json:"create_time"`
	Offset     int64     `json:"offset"`
	Partition  int32     `json:"partition"`
	Key        string    `json:"key"`
	Topic      string    `json:"topic"`
}

func TestIngestDataWithoutJsonTransform(t *testing.T) {
	tt := prepareIngestDatabendTest()
	cfg := config.Config{
		KafkaBootstrapServers: "127.0.0.1:9002",
		KafkaTopic:            "test",
		KafkaConsumerGroup:    "test",
		DatabendDSN:           tt.databendDSN,
		// DatabendDSN:      os.Getenv("TEST_DATABEND_DSN"),
		DataFormat:       "json",
		IsJsonTransform:  false,
		DatabendTable:    "default.test_ingest_without",
		BatchSize:        10,
		BatchMaxInterval: 10,
		UserStage:        "~",
	}
	db, err := sql.Open("databend", cfg.DatabendDSN)
	assert.NoError(t, err)
	execute(db, "create or replace table test_ingest_without(uuid String, raw_data JSON, record_metadata JSON, add_time timestamp);")
	defer execute(db, "drop table if exists test_ingest_without;")

	testData := []string{"{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}", "{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}"}
	messageData := message.MessageData{
		Data:       testData[0],
		DataOffset: 1,
		Partition:  1,
		Key:        "1",
		CreateTime: time.Now(),
	}
	messagesBatch := &message.MessagesBatch{Messages: []message.MessageData{messageData}}
	ig := NewDatabendIngester(&cfg)
	err = ig.IngestData(messagesBatch)
	assert.NoError(t, err)

	// check the data
	result, err := db.Query("select * from test_ingest_without")
	assert.NoError(t, err)
	count := 0
	var raw_data string
	var record_metadata string
	var add_time time.Time
	for result.Next() {
		count += 1
		var uuid string
		err = result.Scan(&uuid, &raw_data, &record_metadata, &add_time)
		fmt.Println(uuid, raw_data, record_metadata, add_time)
	}
	assert.NotEqual(t, 0, count)
	res := &recordMetadata{}
	err = json.Unmarshal([]byte(record_metadata), res)
	fmt.Println(*res)
	assert.NoError(t, err)
	assert.Equal(t, messageData.DataOffset, res.Offset)
}

func TestIngestWithReplaceMode(t *testing.T) {
	tt := prepareIngestDatabendTest()
	tableName := "default.test_ingest_replace"
	cfg := config.Config{
		KafkaBootstrapServers: "127.0.0.1:9002",
		KafkaTopic:            "test",
		KafkaConsumerGroup:    "test",
		DatabendDSN:           tt.databendDSN,
		// DatabendDSN:      os.Getenv("TEST_DATABEND_DSN"),
		DataFormat:       "json",
		IsJsonTransform:  false,
		DatabendTable:    tableName,
		BatchSize:        10,
		BatchMaxInterval: 10,
		UseReplaceMode:   true,
		UserStage:        "~",
	}
	db, err := sql.Open("databend", cfg.DatabendDSN)
	assert.NoError(t, err)
	err = execute(db, fmt.Sprintf("create or replace table %s (uuid String, koffset bigint, kpartition int, raw_data JSON, record_metadata JSON, add_time timestamp);", tableName))
	assert.NoError(t, err)
	defer execute(db, fmt.Sprintf("drop table if exists %s;", tableName))

	testData := []string{"{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}", "{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}"}
	messageData := message.MessageData{
		Data:       testData[0],
		DataOffset: 1,
		Partition:  1,
		Key:        "1",
		CreateTime: time.Now(),
	}
	messagesBatch := &message.MessagesBatch{Messages: []message.MessageData{messageData}}
	ig := NewDatabendIngester(&cfg)
	err = ig.IngestParquetData(messagesBatch)
	assert.NoError(t, err)
	// double replace into, but only on record inserted
	err = ig.IngestParquetData(messagesBatch)
	assert.NoError(t, err)

	// check the data
	result, err := db.Query(fmt.Sprintf("select * from %s", tableName))
	assert.NoError(t, err)
	count := 0
	var uuid string
	var koffset int64
	var kpartition int
	var raw_data string
	var record_metadata string
	var add_time string
	for result.Next() {
		count += 1
		err = result.Scan(&uuid, &koffset, &kpartition, &raw_data, &record_metadata, &add_time)
		fmt.Println(uuid, koffset, kpartition, raw_data, record_metadata, add_time)
	}
	assert.Equal(t, 1, count)
	res := &recordMetadata{}
	err = json.Unmarshal([]byte(record_metadata), res)
	fmt.Println(*res)
	assert.NoError(t, err)
	assert.Equal(t, messageData.DataOffset, res.Offset)
}

func TestStreamingLoadBuildRequest(t *testing.T) {
	var receivedSQL string
	var receivedAuth string
	var receivedWarehouse string
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSQL = r.Header.Get("X-Databend-SQL")
		receivedAuth = r.Header.Get("Authorization")
		receivedWarehouse = r.Header.Get("X-Databend-Warehouse")

		mediaType, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		assert.True(t, strings.HasPrefix(mediaType, "multipart/"))

		mr := multipart.NewReader(r.Body, params["boundary"])
		part, err := mr.NextPart()
		assert.NoError(t, err)
		assert.Equal(t, "upload", part.FormName())
		receivedBody, _ = io.ReadAll(part)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"test-id","stats":{"rows":1,"bytes":100}}`))
	}))
	defer server.Close()

	cfg := &config.Config{
		DatabendDSN:      fmt.Sprintf("http://testuser:testpass@%s?sslmode=disable", server.Listener.Addr().String()),
		DatabendTable:    "default.test_streaming",
		UseStreamingLoad: true,
		IsJsonTransform:  false,
	}

	ig := NewDatabendIngester(cfg).(*databendIngester)
	data := []string{`{"uuid":"abc","koffset":1,"kpartition":0,"raw_data":{"name":"Alice"},"record_metadata":{},"add_time":"2024-01-01T00:00:00Z"}`}

	bytesWritten, err := ig.streamingLoad(context.Background(), data)
	assert.NoError(t, err)
	assert.True(t, bytesWritten > 0)
	assert.Contains(t, receivedSQL, "INSERT INTO default.test_streaming FROM @_databend_load")
	assert.Contains(t, receivedSQL, "type=NDJSON")
	assert.Contains(t, receivedAuth, "Basic")
	assert.Equal(t, "", receivedWarehouse)
	assert.Contains(t, string(receivedBody), `"name":"Alice"`)
}

func TestGenerateNDJsonFileWithZstdCompression(t *testing.T) {
	ig := &databendIngester{
		databendIngesterCfg: &config.Config{
			CopyIntoUploadCompression: true,
		},
	}
	rows := []string{`{"name":"Alice"}`, `{"name":"Bob"}`}

	name, size, err := ig.generateNDJsonFile(rows)
	assert.NoError(t, err)
	defer os.Remove(name)

	assert.True(t, strings.HasSuffix(name, ".ndjson.zst"))
	assert.True(t, size > 0)

	f, err := os.Open(name)
	assert.NoError(t, err)
	defer f.Close()

	decoder, err := zstd.NewReader(f)
	assert.NoError(t, err)
	defer decoder.Close()

	body, err := io.ReadAll(decoder)
	assert.NoError(t, err)
	assert.Equal(t, "{\"name\":\"Alice\"}\n{\"name\":\"Bob\"}\n", string(body))
}

func TestBuildCopyIntoSQLUsesFilesOption(t *testing.T) {
	stage := &godatabend.StageLocation{
		Name: "otel_tmp",
		Path: "batch/1778578427-databend-ingest-2394177572.ndjson.zst",
	}

	sql := buildCopyIntoSQL("otel_traces.kafka_raw", stage, true, false, true)

	assert.Contains(t, sql, "COPY INTO otel_traces.kafka_raw FROM @otel_tmp/batch/ FILES = ('1778578427-databend-ingest-2394177572.ndjson.zst')")
	assert.NotContains(t, sql, "FROM @otel_tmp/batch/1778578427-databend-ingest-2394177572.ndjson.zst")
	assert.Contains(t, sql, "FILE_FORMAT = (type = NDJSON missing_field_as = FIELD_DEFAULT COMPRESSION = AUTO)")
	assert.Contains(t, sql, "PURGE = true FORCE = false DISABLE_VARIANT_CHECK = true")
}

func TestBuildCopyIntoSQLEscapesFileName(t *testing.T) {
	stage := &godatabend.StageLocation{
		Name: "otel_tmp",
		Path: "batch/batch's.ndjson.zst",
	}

	sql := buildCopyIntoSQL("otel_traces.kafka_raw", stage, false, false, false)

	assert.Contains(t, sql, "FROM @otel_tmp/batch/ FILES = ('batch''s.ndjson.zst')")
}

func TestStreamingLoadRetryOn5xx(t *testing.T) {
	var callCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&callCount, 1)
		if count <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"internal"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"ok","stats":{"rows":1,"bytes":10}}`))
	}))
	defer server.Close()

	cfg := &config.Config{
		DatabendDSN:      fmt.Sprintf("http://user:pass@%s?sslmode=disable", server.Listener.Addr().String()),
		DatabendTable:    "default.test_retry",
		UseStreamingLoad: true,
		IsJsonTransform:  false,
		MaxRetryDelay:    5,
	}

	ig := NewDatabendIngester(cfg)
	batch := &message.MessagesBatch{
		Messages: []message.MessageData{
			{Data: `{"name":"Bob"}`, DataOffset: 1, Partition: 0, Key: "k", CreateTime: time.Now()},
		},
	}

	err := DoRetry(func() error {
		return ig.IngestData(batch)
	}, 5*time.Second, "IngestData")
	assert.NoError(t, err)
	assert.True(t, atomic.LoadInt32(&callCount) >= int32(3))
}

func TestStreamingLoadNoRetryOn4xx(t *testing.T) {
	var callCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer server.Close()

	cfg := &config.Config{
		DatabendDSN:      fmt.Sprintf("http://user:pass@%s?sslmode=disable", server.Listener.Addr().String()),
		DatabendTable:    "default.test_no_retry",
		UseStreamingLoad: true,
		IsJsonTransform:  false,
	}

	ig := NewDatabendIngester(cfg).(*databendIngester)
	data := []string{`{"name":"Carol"}`}

	_, err := ig.streamingLoad(context.Background(), data)
	assert.Error(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount))
}

func TestIngestDataWithStreamingLoad(t *testing.T) {
	tt := prepareIngestDatabendTest()
	tableName := "default.test_ingest_streaming"

	cfg := config.Config{
		KafkaBootstrapServers: "127.0.0.1:9002",
		KafkaTopic:            "test",
		KafkaConsumerGroup:    "test",
		DatabendDSN:           tt.databendDSN,
		DataFormat:            "json",
		IsJsonTransform:       false,
		DatabendTable:         tableName,
		BatchSize:             10,
		BatchMaxInterval:      10,
		UseStreamingLoad:      true,
	}

	db, err := sql.Open("databend", cfg.DatabendDSN)
	assert.NoError(t, err)
	defer db.Close()

	err = execute(db, fmt.Sprintf("CREATE OR REPLACE TABLE %s (uuid String, koffset BIGINT, kpartition INT, raw_data JSON, record_metadata JSON, add_time TIMESTAMP)", tableName))
	assert.NoError(t, err)
	defer execute(db, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	messageData := message.MessageData{
		Data:       `{"name": "StreamingTest", "age": 99, "active": true}`,
		DataOffset: 42,
		Partition:  2,
		Key:        "test-key",
		CreateTime: time.Now(),
	}
	messagesBatch := &message.MessagesBatch{Messages: []message.MessageData{messageData}}

	ig := NewDatabendIngester(&cfg)
	defer ig.Close()

	err = ig.IngestData(messagesBatch)
	assert.NoError(t, err)

	result, err := db.Query(fmt.Sprintf("SELECT uuid, koffset, kpartition, raw_data, record_metadata, add_time FROM %s", tableName))
	assert.NoError(t, err)
	defer result.Close()

	count := 0
	for result.Next() {
		count++
		var uuid string
		var koffset int64
		var kpartition int
		var rawData string
		var recordMeta string
		var addTime string
		err = result.Scan(&uuid, &koffset, &kpartition, &rawData, &recordMeta, &addTime)
		assert.NoError(t, err)
		assert.NotEmpty(t, uuid)
		assert.Equal(t, int64(42), koffset)
		assert.Equal(t, 2, kpartition)
		assert.Contains(t, rawData, "StreamingTest")
	}
	assert.Equal(t, 1, count)
}
