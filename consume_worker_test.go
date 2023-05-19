package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/test-go/testify/assert"
)

func TestProduceMessage(t *testing.T) {
	produceMessage()
}

func produceMessage() {

	// Set up a context
	ctx := context.Background()

	// Set up Kafka writer configuration
	writerConfig := kafka.WriterConfig{
		Brokers: []string{"127.0.0.1:64103"},
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
	db, err := sql.Open("databend", "http://root:root@localhost:8002")
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

	cfg := parseConfig()
	w := NewConsumeWorker(cfg)
	w.Run(context.TODO())

	result, err := db.Exec("select * from test_ingest")
	assert.NoError(t, err)
	r, _ := result.RowsAffected()
	fmt.Println(r)
}
