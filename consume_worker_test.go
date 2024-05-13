package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/test-go/testify/assert"

	"bend-ingest-kafka/config"
)

type consumeWorkerTest struct {
	databendDSN  string
	kafkaBrokers []string
}

func prepareConsumeWorkerTest() *consumeWorkerTest {
	testDatabendDSN := os.Getenv("TEST_DATABEND_DSN")
	if testDatabendDSN == "" {
		testDatabendDSN = "http://root:root@localhost:8002"
	}
	testKafkaBroker := os.Getenv("TEST_KAFKA_BROKER")
	if testKafkaBroker == "" {
		testKafkaBroker = "127.0.0.1:64103"
	}

	tt := &consumeWorkerTest{
		databendDSN:  testDatabendDSN,
		kafkaBrokers: []string{testKafkaBroker},
	}
	tt.setupKafkaTopic("test")
	return tt
}

func (tt *consumeWorkerTest) setupKafkaTopic(topic string) {
	conn, err := kafka.Dial("tcp", tt.kafkaBrokers[0])
	if err != nil {
		panic(err)
	}
	controller, err := conn.Controller()
	if err != nil {
		panic(err)
	}
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort("localhost", strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()
	topicConfigs := []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		log.Printf(err.Error())
	}
}

func TestProduceMessage(t *testing.T) {
	produceMessage()
}

func produceMessage() {
	tt := prepareConsumeWorkerTest()
	// Set up a context
	ctx := context.Background()

	// Set up Kafka writer configuration
	writerConfig := kafka.WriterConfig{
		Brokers: tt.kafkaBrokers,
		Topic:   "test",
	}

	// Create a Kafka writer
	writer := kafka.NewWriter(writerConfig)

	// Send a message to Kafka
	message := kafka.Message{
		Key:   []byte("name"),
		Value: []byte("{\"i64\": 10,\"u64\": 30,\"f64\": 20,\"s\": \"hao\",\"s2\": \"hello\",\"a16\":[1],\"a8\":[2],\"d\": \"2011-03-06\",\"t\": \"2016-04-04 11:30:00\"}"),
	}
	err := writer.WriteMessages(ctx, message)
	if err != nil {
		log.Fatal("Failed to send message:", err)
	}

	// Close the Kafka writer
	err = writer.Close()
	if err != nil {
		log.Fatal("Failed to close writer:", err)
	}
}

func TestConsumeKafka(t *testing.T) {
	tt := prepareConsumeWorkerTest()

	db, err := sql.Open("databend", tt.databendDSN)
	assert.NoError(t, err)
	execute(db, `CREATE TABLE test_ingest (
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
	produceMessage()
	fmt.Println("start consuming data")

	cfg := &config.Config{
		DatabendDSN:           tt.databendDSN,
		DatabendTable:         "test_ingest",
		KafkaTopic:            "test",
		KafkaBootstrapServers: tt.kafkaBrokers[0],
		KafkaConsumerGroup:    "test",
		BatchSize:             10,
		Workers:               1,
		DataFormat:            "json",
		BatchMaxInterval:      10 * time.Second,
	}
	ig := NewDatabendIngester(cfg)
	w := NewConsumeWorker(cfg, "worker1", ig)
	log.Printf("start consume")
	w.stepBatch(context.TODO())

	result, err := db.Exec("select * from test_ingest")
	assert.NoError(t, err)
	r, _ := result.RowsAffected()
	fmt.Println(r)
}
