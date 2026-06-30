package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
