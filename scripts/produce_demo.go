package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var names = []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Helen", "Ivan", "Julia"}
var cities = []string{"Beijing", "Shanghai", "Shenzhen", "Hangzhou", "Guangzhou", "Chengdu", "Wuhan", "Nanjing", "Suzhou", "Xiamen"}

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":            "localhost:9092",
		"queue.buffering.max.messages": 100000,
		"batch.num.messages":           1000,
		"linger.ms":                    50,
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %v\n", err)
		return
	}
	defer producer.Close()

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				}
			}
		}
	}()

	topic := "demo-ingest"
	total := 20000
	start := time.Now()

	for i := 0; i < total; i++ {
		name := names[rand.Intn(len(names))]
		city := cities[rand.Intn(len(cities))]
		age := 20 + rand.Intn(40)
		msg := fmt.Sprintf(`{"name":"%s","age":%d,"city":"%s"}`, name, age, city)

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(msg),
		}, nil)

		if (i+1)%10000 == 0 {
			fmt.Printf("Produced %d/%d messages...\n", i+1, total)
		}
	}

	remaining := producer.Flush(30000)
	elapsed := time.Since(start)
	fmt.Printf("Done! Sent %d messages in %v (%d unsent)\n", total-remaining, elapsed, remaining)
}
