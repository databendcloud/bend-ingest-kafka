package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/test-go/testify/assert"

	"github.com/databendcloud/bend-ingest-kafka/config"
)

func getConfigValue(cm *kafka.ConfigMap, key string) interface{} {
	v, _ := cm.Get(key, nil)
	return v
}

func TestBuildKafkaConfigMap_PlainText(t *testing.T) {
	cfg := &config.Config{
		KafkaBootstrapServers: "localhost:9092",
		KafkaConsumerGroup:    "test-group",
		MinBytes:              1024,
		MaxWait:               10,
		DisableTLS:            true,
	}

	cm := BuildKafkaConfigMap(cfg)

	assert.Equal(t, "localhost:9092", getConfigValue(cm, "bootstrap.servers"))
	assert.Equal(t, "test-group", getConfigValue(cm, "group.id"))
	assert.Equal(t, "PLAINTEXT", getConfigValue(cm, "security.protocol"))
	assert.Equal(t, false, getConfigValue(cm, "enable.auto.commit"))
}

func TestBuildKafkaConfigMap_SASL_SSL(t *testing.T) {
	cfg := &config.Config{
		KafkaBootstrapServers: "broker1:9093,broker2:9093",
		KafkaConsumerGroup:    "my-group",
		IsSASL:                true,
		SaslUser:              "user1",
		SaslPassword:          "pass1",
		DisableTLS:            false,
		MinBytes:              2048,
		MaxWait:               5,
	}

	cm := BuildKafkaConfigMap(cfg)

	assert.Equal(t, "SASL_SSL", getConfigValue(cm, "security.protocol"))
	assert.Equal(t, "PLAIN", getConfigValue(cm, "sasl.mechanism"))
	assert.Equal(t, "user1", getConfigValue(cm, "sasl.username"))
	assert.Equal(t, "pass1", getConfigValue(cm, "sasl.password"))
	assert.Equal(t, 2048, getConfigValue(cm, "fetch.min.bytes"))
	assert.Equal(t, 5000, getConfigValue(cm, "fetch.wait.max.ms"))
}

func TestBuildKafkaConfigMap_SASL_PLAINTEXT(t *testing.T) {
	cfg := &config.Config{
		KafkaBootstrapServers: "localhost:9092",
		KafkaConsumerGroup:    "group1",
		IsSASL:                true,
		SaslUser:              "admin",
		SaslPassword:          "secret",
		DisableTLS:            true,
		MinBytes:              1024,
		MaxWait:               10,
	}

	cm := BuildKafkaConfigMap(cfg)

	assert.Equal(t, "SASL_PLAINTEXT", getConfigValue(cm, "security.protocol"))
	assert.Equal(t, "PLAIN", getConfigValue(cm, "sasl.mechanism"))
}

func TestBuildKafkaConfigMap_SSL_NoSASL(t *testing.T) {
	cfg := &config.Config{
		KafkaBootstrapServers: "localhost:9092",
		KafkaConsumerGroup:    "group1",
		IsSASL:                false,
		DisableTLS:            false,
		MinBytes:              1024,
		MaxWait:               10,
	}

	cm := BuildKafkaConfigMap(cfg)

	assert.Equal(t, "SSL", getConfigValue(cm, "security.protocol"))
}

func TestKafkaBatchReader_Integration(t *testing.T) {
	testKafkaBroker := os.Getenv("TEST_KAFKA_BROKER")
	if testKafkaBroker == "" {
		testKafkaBroker = "127.0.0.1:9092"
	}

	topic := fmt.Sprintf("batch_reader_test_%d", time.Now().UnixNano())

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": testKafkaBroker,
	})
	if err != nil {
		t.Skipf("Cannot connect to Kafka at %s: %v", testKafkaBroker, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = admin.CreateTopics(ctx, []kafka.TopicSpecification{
		{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
	})
	assert.NoError(t, err)
	admin.Close()

	// Produce test messages
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": testKafkaBroker,
	})
	assert.NoError(t, err)

	for i := 0; i < 5; i++ {
		deliveryChan := make(chan kafka.Event, 1)
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(fmt.Sprintf("key-%d", i)),
			Value:          []byte(fmt.Sprintf(`{"id": %d, "name": "test"}`, i)),
		}, deliveryChan)
		assert.NoError(t, err)
		e := <-deliveryChan
		m := e.(*kafka.Message)
		assert.NoError(t, m.TopicPartition.Error)
	}
	producer.Flush(5000)
	producer.Close()

	// Consume with KafkaBatchReader
	cfg := &config.Config{
		KafkaBootstrapServers: testKafkaBroker,
		KafkaTopic:            topic,
		KafkaConsumerGroup:    fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
		BatchSize:             10,
		BatchMaxInterval:      30,
		MinBytes:              1,
		MaxBytes:              20 * 1024 * 1024,
		MaxWait:               2,
		DisableTLS:            true,
	}

	reader := NewKafkaBatchReader(cfg)
	defer reader.Close()

	readCtx, readCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer readCancel()

	batch, err := reader.ReadBatch(readCtx)
	assert.NoError(t, err)
	assert.NotNil(t, batch)
	assert.Equal(t, 5, len(batch.Messages))
	assert.Equal(t, int64(0), batch.FirstMessageOffset)
	assert.Equal(t, int64(4), batch.LastMessageOffset)

	// Verify message content
	assert.Contains(t, batch.Messages[0].Data, `"id": 0`)
	assert.Equal(t, "key-0", batch.Messages[0].Key)
	assert.Equal(t, 0, batch.Messages[0].Partition)

	// Commit should succeed
	err = batch.CommitFunc(context.Background())
	assert.NoError(t, err)
}

func TestCreateCommitFuncContextCancelled(t *testing.T) {
	testKafkaBroker := os.Getenv("TEST_KAFKA_BROKER")
	if testKafkaBroker == "" {
		testKafkaBroker = "127.0.0.1:9092"
	}

	topic := fmt.Sprintf("commit_ctx_test_%d", time.Now().UnixNano())

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": testKafkaBroker,
	})
	if err != nil {
		t.Skipf("Cannot connect to Kafka: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = admin.CreateTopics(ctx, []kafka.TopicSpecification{
		{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
	})
	admin.Close()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": testKafkaBroker,
	})
	assert.NoError(t, err)
	deliveryChan := make(chan kafka.Event, 1)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(`{"x":1}`),
	}, deliveryChan)
	assert.NoError(t, err)
	<-deliveryChan
	producer.Flush(5000)
	producer.Close()

	cfg := &config.Config{
		KafkaBootstrapServers: testKafkaBroker,
		KafkaTopic:            topic,
		KafkaConsumerGroup:    fmt.Sprintf("ctx-cancel-group-%d", time.Now().UnixNano()),
		BatchSize:             10,
		BatchMaxInterval:      30,
		MinBytes:              1,
		MaxBytes:              20 * 1024 * 1024,
		MaxWait:               2,
		DisableTLS:            true,
	}

	reader := NewKafkaBatchReader(cfg)
	defer reader.Close()

	readCtx, readCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer readCancel()

	batch, err := reader.ReadBatch(readCtx)
	assert.NoError(t, err)
	assert.NotNil(t, batch)
	assert.True(t, len(batch.Messages) > 0)

	// Test commit with cancelled context
	cancelledCtx, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	err = batch.CommitFunc(cancelledCtx)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}
