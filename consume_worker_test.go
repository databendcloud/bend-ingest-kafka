package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	dto "github.com/prometheus/client_model/go"
	"github.com/test-go/testify/assert"

	"github.com/databendcloud/bend-ingest-kafka/config"
)

type consumeWorkerTest struct {
	databendDSN  string
	kafkaBrokers []string
}

func prepareConsumeWorkerTest(topic string, partition int) *consumeWorkerTest {
	testDatabendDSN := os.Getenv("TEST_DATABEND_DSN")
	if testDatabendDSN == "" {
		testDatabendDSN = "http://databend:databend@localhost:8000?presigned_url_disabled=true"
	}
	testKafkaBroker := os.Getenv("TEST_KAFKA_BROKER")
	if testKafkaBroker == "" {
		testKafkaBroker = "127.0.0.1:9092"
	}

	tt := &consumeWorkerTest{
		databendDSN:  testDatabendDSN,
		kafkaBrokers: []string{testKafkaBroker},
	}
	tt.setupKafkaTopic(topic, partition)
	return tt
}

func (tt *consumeWorkerTest) setupKafkaTopic(topic string, partition int) {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": tt.kafkaBrokers[0],
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create admin client: %v", err))
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     partition,
			ReplicationFactor: 1,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create topic: %v", err))
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			log.Printf("Failed to create topic %s: %v", result.Topic, result.Error)
		}
	}
}

func TestProduceMessage(t *testing.T) {
	produceMessage("produce_test", 1)
}

func produceMessage(topic string, partition int) {
	tt := prepareConsumeWorkerTest(topic, partition)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": tt.kafkaBrokers[0],
	})
	if err != nil {
		log.Fatal("Failed to create producer:", err)
	}
	defer producer.Close()

	for i := 0; i < 3; i++ {
		deliveryChan := make(chan kafka.Event, 1)
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte("name"),
			Value:          []byte(`{"i64": 10,"u64": 30,"f64": 20,"s": "hao","s2": "hello","a16":[1],"a8":[2],"d": "2011-03-06","t": "2016-04-04 11:30:00"}`),
		}, deliveryChan)
		if err != nil {
			log.Fatal("Failed to produce message:", err)
		}
		e := <-deliveryChan
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			log.Fatal("Delivery failed:", m.TopicPartition.Error)
		}
	}
	producer.Flush(5000)
}

func TestConsumeKafka(t *testing.T) {
	consumeTopic := "consume_test"
	consumePartition := 2
	tt := prepareConsumeWorkerTest(consumeTopic, consumePartition)

	db, err := sql.Open("databend", tt.databendDSN)
	assert.NoError(t, err)
	execute(db, `CREATE OR REPLACE TABLE test_ingest (
			i64 Int64,
			u64 UInt64,
			f64 Float64,
			s   String,
			s2  String,
			a16 Array(Int16),
			a8  Array(UInt8),
			d   Date,
			t   DateTime)`)
	defer execute(db, "drop table if exists test_ingest;")
	produceMessage(consumeTopic, consumePartition)
	fmt.Println("start consuming data")

	cfg := &config.Config{
		DatabendDSN:           tt.databendDSN,
		DatabendTable:         "test_ingest",
		KafkaTopic:            consumeTopic,
		KafkaBootstrapServers: tt.kafkaBrokers[0],
		IsJsonTransform:       true,
		KafkaConsumerGroup:    fmt.Sprintf("test-%d", time.Now().UnixNano()),
		BatchSize:             10,
		CopyIntoFileCount:     1,
		Workers:               1,
		DataFormat:            "json",
		BatchMaxInterval:      10,
		DisableVariantCheck:   false,
		UserStage:             "~",
		MinBytes:              1024,
		MaxBytes:              20 * 1024 * 1024,
		MaxWait:               10,
		DisableTLS:            true,
	}
	ig := NewDatabendIngester(cfg)
	w := NewConsumeWorker(cfg, "worker1", ig)
	log.Printf("start consume")
	w.stepBatch(context.TODO())

	result, err := db.Query("select * from test_ingest")
	assert.NoError(t, err)
	count := 0
	for result.Next() {
		count += 1
		var i64 int64
		var u64 uint64
		var f64 float64
		var s string
		var s2 string
		var a16 []int16
		var a8 []uint8
		var d time.Time
		var tt time.Time
		err = result.Scan(&i64, &u64, &f64, &s, &s2, &a16, &a8, &d, &tt)
		fmt.Println(i64, u64, f64, s, s2, a16, a8, d, tt)
	}

	assert.NotEqual(t, 0, count)
}

func TestConsumerWithoutTransform(t *testing.T) {
	consumeRawTopic := "consume_raw_test"
	consumeRawPartition := 3
	tt := prepareConsumeWorkerTest(consumeRawTopic, consumeRawPartition)

	db, err := sql.Open("databend", tt.databendDSN)
	assert.NoError(t, err)
	defer execute(db, "drop table if exists test_ingest_raw;")
	produceMessage(consumeRawTopic, consumeRawPartition)
	fmt.Println("start consuming data")

	cfg := &config.Config{
		DatabendDSN:           tt.databendDSN,
		DatabendTable:         "test_ingest_raw",
		KafkaTopic:            consumeRawTopic,
		KafkaBootstrapServers: tt.kafkaBrokers[0],
		IsJsonTransform:       false,
		KafkaConsumerGroup:    fmt.Sprintf("test-raw-%d", time.Now().UnixNano()),
		BatchSize:             10,
		CopyIntoFileCount:     1,
		Workers:               1,
		DataFormat:            "json",
		BatchMaxInterval:      10,
		DisableVariantCheck:   true,
		UserStage:             "~",
		MinBytes:              1024,
		MaxBytes:              20 * 1024 * 1024,
		MaxWait:               10,
		DisableTLS:            true,
	}
	ig := NewDatabendIngester(cfg)
	if !cfg.IsJsonTransform {
		err := ig.CreateRawTargetTable()
		if err != nil {
			panic(err)
		}
	}
	w := NewConsumeWorker(cfg, "worker1", ig)
	log.Printf("start consume")
	err = w.stepBatch(context.TODO())
	assert.NoError(t, err)

	result, err := db.Query("select * from test_ingest_raw")
	assert.NoError(t, err)
	count := 0
	for result.Next() {
		count += 1
	}

	assert.NotEqual(t, 0, count)
}

func TestCopyIntoFileAggregationE2E(t *testing.T) {
	consumeTopic := fmt.Sprintf("copy_into_files_e2e_%d", time.Now().UnixNano())
	tableName := fmt.Sprintf("copy_into_files_e2e_%d", time.Now().UnixNano())
	tt := prepareConsumeWorkerTest(consumeTopic, 1)
	produceMessage(consumeTopic, 1)

	db, err := sql.Open("databend", tt.databendDSN)
	assert.NoError(t, err)
	defer db.Close()
	assert.NoError(t, execute(db, fmt.Sprintf(`CREATE OR REPLACE TABLE %s (
		i64 Int64, u64 UInt64, f64 Float64, s String, s2 String,
		a16 Array(Int16), a8 Array(UInt8), d Date, t DateTime)`, tableName)))
	defer execute(db, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	cfg := &config.Config{
		DatabendDSN:           tt.databendDSN,
		DatabendTable:         tableName,
		KafkaTopic:            consumeTopic,
		KafkaBootstrapServers: tt.kafkaBrokers[0],
		KafkaConsumerGroup:    fmt.Sprintf("copy-files-e2e-%d", time.Now().UnixNano()),
		IsJsonTransform:       true,
		BatchSize:             1,
		BatchMaxInterval:      2,
		CopyIntoFileCount:     3,
		UserStage:             "~",
		CopyPurge:             true,
		MinBytes:              1,
		MaxWait:               1,
		DisableTLS:            true,
		MaxRetryDelay:         5,
	}
	ig := NewDatabendIngester(cfg)
	defer ig.Close()
	worker := NewConsumeWorker(cfg, "copy-files-e2e", ig)

	rowCount := func() int {
		var count int
		assert.NoError(t, db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&count))
		return count
	}
	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.Equal(t, 0, rowCount(), "the first uploaded file must not trigger COPY INTO")
	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.Equal(t, 0, rowCount(), "the second uploaded file must not trigger COPY INTO")
	assert.NoError(t, worker.stepBatch(context.Background()))
	assert.Equal(t, 3, rowCount(), "the third file must trigger one multi-file COPY INTO")
	worker.Close()
}

func TestCopyIntoFileAggregationGracefulShutdownE2E(t *testing.T) {
	consumeTopic := fmt.Sprintf("copy_into_shutdown_e2e_%d", time.Now().UnixNano())
	tableName := fmt.Sprintf("copy_into_shutdown_e2e_%d", time.Now().UnixNano())
	tt := prepareConsumeWorkerTest(consumeTopic, 1)
	produceMessage(consumeTopic, 1)

	db, err := sql.Open("databend", tt.databendDSN)
	assert.NoError(t, err)
	defer db.Close()
	assert.NoError(t, execute(db, fmt.Sprintf(`CREATE OR REPLACE TABLE %s (
		i64 Int64, u64 UInt64, f64 Float64, s String, s2 String,
		a16 Array(Int16), a8 Array(UInt8), d Date, t DateTime)`, tableName)))
	defer execute(db, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	cfg := &config.Config{
		DatabendDSN:           tt.databendDSN,
		DatabendTable:         tableName,
		KafkaTopic:            consumeTopic,
		KafkaBootstrapServers: tt.kafkaBrokers[0],
		KafkaConsumerGroup:    fmt.Sprintf("copy-shutdown-e2e-%d", time.Now().UnixNano()),
		IsJsonTransform:       true,
		BatchSize:             1,
		BatchMaxInterval:      2,
		CopyIntoFileCount:     5,
		UserStage:             "~",
		CopyPurge:             true,
		MinBytes:              1,
		MaxWait:               1,
		DisableTLS:            true,
		MaxRetryDelay:         5,
	}
	ig := NewDatabendIngester(cfg)
	defer ig.Close()
	worker := NewConsumeWorker(cfg, "copy-shutdown-e2e", ig)
	for i := 0; i < 3; i++ {
		assert.NoError(t, worker.stepBatch(context.Background()))
	}
	var before int
	assert.NoError(t, db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&before))
	assert.Equal(t, 0, before)

	worker.Close() // flushes three files (below the threshold) and commits them
	var after int
	assert.NoError(t, db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&after))
	assert.Equal(t, 3, after)

	// Rejoin with the same consumer group. A committed graceful flush must leave no messages to replay.
	reader := NewKafkaBatchReader(cfg)
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	batch, readErr := reader.ReadBatch(ctx)
	assert.True(t, errors.Is(readErr, context.DeadlineExceeded))
	assert.Nil(t, batch)
}

func TestCopyIntoFileCountStressE2E(t *testing.T) {
	if os.Getenv("RUN_STRESS_E2E") != "1" {
		t.Skip("set RUN_STRESS_E2E=1 to run the 100k-message full-chain stress test")
	}

	const (
		messageCount      = 100_000
		batchSize         = 1_000
		copyIntoFileCount = 5
	)
	consumeTopic := fmt.Sprintf("copy_into_stress_e2e_%d", time.Now().UnixNano())
	tableName := fmt.Sprintf("copy_into_stress_e2e_%d", time.Now().UnixNano())
	consumerGroup := fmt.Sprintf("copy-stress-e2e-%d", time.Now().UnixNano())
	tt := prepareConsumeWorkerTest(consumeTopic, 1)

	producerStart := time.Now()
	producedBytes := produceRawStressMessages(t, tt.kafkaBrokers[0], consumeTopic, messageCount)
	producerDuration := time.Since(producerStart)

	cfg := &config.Config{
		DatabendDSN:               tt.databendDSN,
		DatabendTable:             tableName,
		KafkaTopic:                consumeTopic,
		KafkaBootstrapServers:     tt.kafkaBrokers[0],
		KafkaConsumerGroup:        consumerGroup,
		IsJsonTransform:           false,
		BatchSize:                 batchSize,
		BatchMaxInterval:          10,
		CopyIntoFileCount:         copyIntoFileCount,
		Workers:                   1,
		UserStage:                 "~",
		CopyPurge:                 true,
		DisableVariantCheck:       true,
		CopyIntoUploadCompression: true,
		MinBytes:                  1,
		MaxBytes:                  20 * 1024 * 1024,
		MaxWait:                   1,
		DisableTLS:                true,
		MaxRetryDelay:             30,
	}

	db, err := sql.Open("databend", cfg.DatabendDSN)
	assert.NoError(t, err)
	defer db.Close()
	defer execute(db, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	ingester := NewDatabendIngester(cfg)
	defer ingester.Close()
	assert.NoError(t, ingester.CreateRawTargetTable())
	worker := NewConsumeWorker(cfg, "copy-stress-e2e", ingester)

	before := captureIngestMetrics()
	ingestStart := time.Now()
	for consumed := 0; consumed < messageCount; consumed += batchSize {
		assert.NoError(t, worker.stepBatch(context.Background()))
	}
	worker.Close()
	ingestDuration := time.Since(ingestStart)
	after := captureIngestMetrics()

	var rowCount, distinctOffsets, validRawRows, metadataRows int64
	var minOffset, maxOffset int64
	err = db.QueryRow(fmt.Sprintf(`SELECT
		count(*), count(DISTINCT koffset), min(koffset), max(koffset),
		count_if(raw_data IS NOT NULL), count_if(record_metadata IS NOT NULL)
		FROM %s WHERE kpartition = 0`, tableName)).Scan(
		&rowCount, &distinctOffsets, &minOffset, &maxOffset, &validRawRows, &metadataRows)
	assert.NoError(t, err)
	assert.Equal(t, int64(messageCount), rowCount)
	assert.Equal(t, int64(messageCount), distinctOffsets)
	assert.Equal(t, int64(0), minOffset)
	assert.Equal(t, int64(messageCount-1), maxOffset)
	assert.Equal(t, int64(messageCount), validRawRows)
	assert.Equal(t, int64(messageCount), metadataRows)

	metrics := after.subtract(before)
	assert.Equal(t, float64(messageCount), metrics.ingestRows)
	assert.Equal(t, float64(messageCount), metrics.consumeRows)
	assert.Equal(t, uint64(messageCount/batchSize), metrics.uploadCount)
	assert.Equal(t, uint64(messageCount/batchSize/copyIntoFileCount), metrics.copyCount)
	assert.Equal(t, uint64(messageCount/batchSize), metrics.batchCount)
	assert.Equal(t, float64(messageCount), metrics.batchSizeSum)
	assert.Zero(t, metrics.ingestErrors)

	t.Logf("100k stress result: rows=%d distinct_offsets=%d offset_range=%d..%d producer_duration=%s producer_rate=%.2f msg/s ingest_duration=%s ingest_rate=%.2f rows/s source_bytes=%d",
		rowCount, distinctOffsets, minOffset, maxOffset, producerDuration,
		float64(messageCount)/producerDuration.Seconds(), ingestDuration,
		float64(messageCount)/ingestDuration.Seconds(), producedBytes)
	t.Logf("100k stress metrics: ingest_rows=%.0f ingest_bytes=%.0f consume_rows=%.0f consume_bytes=%.0f upload_count=%d upload_total=%.3fs upload_avg=%.3fs copy_count=%d copy_total=%.3fs copy_avg=%.3fs batch_count=%d batch_size_sum=%.0f batch_fill_total=%.3fs errors=%.0f",
		metrics.ingestRows, metrics.ingestBytes, metrics.consumeRows, metrics.consumeBytes,
		metrics.uploadCount, metrics.uploadSum, averageDuration(metrics.uploadSum, metrics.uploadCount),
		metrics.copyCount, metrics.copySum, averageDuration(metrics.copySum, metrics.copyCount),
		metrics.batchCount, metrics.batchSizeSum, metrics.batchFillSum, metrics.ingestErrors)
}

func produceRawStressMessages(t *testing.T, broker, topic string, count int) int64 {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":            broker,
		"queue.buffering.max.messages": 200000,
		"batch.num.messages":           10000,
		"linger.ms":                    10,
	})
	assert.NoError(t, err)

	var deliveryFailures atomic.Int64
	deliveryDone := make(chan struct{})
	go func() {
		defer close(deliveryDone)
		for event := range producer.Events() {
			if msg, ok := event.(*kafka.Message); ok && msg.TopicPartition.Error != nil {
				deliveryFailures.Add(1)
			}
		}
	}()

	var bytesTotal int64
	for i := 0; i < count; i++ {
		value := []byte(fmt.Sprintf(`{"seq":%d,"source":"copy-into-100k-stress","payload":"abcdefghijklmnopqrstuvwxyz0123456789"}`, i))
		bytesTotal += int64(len(value))
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Key:            []byte(fmt.Sprintf("key-%d", i)),
			Value:          value,
		}, nil)
		assert.NoError(t, err)
	}
	remaining := producer.Flush(60_000)
	producer.Close()
	<-deliveryDone
	assert.Equal(t, 0, remaining)
	assert.Equal(t, int64(0), deliveryFailures.Load())
	return bytesTotal
}

type ingestMetricSnapshot struct {
	ingestRows, ingestBytes, consumeRows, consumeBytes, ingestErrors float64
	uploadCount, copyCount, batchCount                               uint64
	uploadSum, copySum, batchSizeSum, batchFillSum                   float64
}

func captureIngestMetrics() ingestMetricSnapshot {
	counter := func(metric interface{ Write(*dto.Metric) error }) float64 {
		m := &dto.Metric{}
		_ = metric.Write(m)
		return m.GetCounter().GetValue()
	}
	histogram := func(metric interface{ Write(*dto.Metric) error }) (uint64, float64) {
		m := &dto.Metric{}
		_ = metric.Write(m)
		return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
	}
	uploadCount, uploadSum := histogram(uploadStageDuration)
	copyCount, copySum := histogram(copyIntoDuration)
	batchCount, batchSizeSum := histogram(batchSizeHist)
	_, batchFillSum := histogram(batchFillDuration)
	return ingestMetricSnapshot{
		ingestRows: counter(ingestRowsTotal), ingestBytes: counter(ingestBytesTotal),
		consumeRows: counter(consumeRowsTotal), consumeBytes: counter(consumeBytesTotal),
		ingestErrors: counter(ingestErrors), uploadCount: uploadCount, uploadSum: uploadSum,
		copyCount: copyCount, copySum: copySum, batchCount: batchCount,
		batchSizeSum: batchSizeSum, batchFillSum: batchFillSum,
	}
}

func (m ingestMetricSnapshot) subtract(before ingestMetricSnapshot) ingestMetricSnapshot {
	m.ingestRows -= before.ingestRows
	m.ingestBytes -= before.ingestBytes
	m.consumeRows -= before.consumeRows
	m.consumeBytes -= before.consumeBytes
	m.ingestErrors -= before.ingestErrors
	m.uploadCount -= before.uploadCount
	m.uploadSum -= before.uploadSum
	m.copyCount -= before.copyCount
	m.copySum -= before.copySum
	m.batchCount -= before.batchCount
	m.batchSizeSum -= before.batchSizeSum
	m.batchFillSum -= before.batchFillSum
	return m
}

func averageDuration(sum float64, count uint64) float64 {
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

func TestConsumerWithoutTransformWithCompressedCopyInto(t *testing.T) {
	consumeRawTopic := "consume_raw_zstd_test"
	consumeRawPartition := 1
	tableName := "test_ingest_raw_zstd"
	tt := prepareConsumeWorkerTest(consumeRawTopic, consumeRawPartition)

	db, err := sql.Open("databend", tt.databendDSN)
	assert.NoError(t, err)
	defer execute(db, fmt.Sprintf("drop table if exists %s;", tableName))
	produceMessage(consumeRawTopic, consumeRawPartition)

	cfg := &config.Config{
		DatabendDSN:               tt.databendDSN,
		DatabendTable:             tableName,
		KafkaTopic:                consumeRawTopic,
		KafkaBootstrapServers:     tt.kafkaBrokers[0],
		IsJsonTransform:           false,
		KafkaConsumerGroup:        fmt.Sprintf("test-zstd-%d", time.Now().UnixNano()),
		BatchSize:                 10,
		CopyIntoFileCount:         1,
		Workers:                   1,
		DataFormat:                "json",
		BatchMaxInterval:          10,
		DisableVariantCheck:       true,
		UserStage:                 "~",
		CopyIntoUploadCompression: true,
		MinBytes:                  1024,
		MaxBytes:                  20 * 1024 * 1024,
		MaxWait:                   10,
		DisableTLS:                true,
	}
	ig := NewDatabendIngester(cfg)
	err = ig.CreateRawTargetTable()
	assert.NoError(t, err)

	w := NewConsumeWorker(cfg, "worker-zstd", ig)
	err = w.stepBatch(context.TODO())
	assert.NoError(t, err)

	result, err := db.Query(fmt.Sprintf("select count(*) from %s", tableName))
	assert.NoError(t, err)
	defer result.Close()

	var count int
	assert.True(t, result.Next())
	err = result.Scan(&count)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, count)
}
