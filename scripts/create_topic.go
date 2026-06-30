package main

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		fmt.Printf("Failed to create admin: %v\n", err)
		return
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{
		{Topic: "demo-ingest", NumPartitions: 1, ReplicationFactor: 1},
	})
	if err != nil {
		fmt.Printf("CreateTopics failed: %v\n", err)
		return
	}
	for _, r := range results {
		fmt.Printf("Topic %s: %v\n", r.Topic, r.Error)
	}
}
