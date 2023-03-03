package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
)

func ConsumeAndIngestDataFromKafka() {
	// parse config
	cfg := parseConfig()

	// consume data from kafka
	k := kafka.NewReader(kafka.ReaderConfig{
		Brokers: parseKafkaServers(cfg.KafkaBootstrapServers),
		GroupID: cfg.KafkaConsumerGroup,
		Topic:   cfg.KafkaTopic,
	})
	defer k.Close()

	// handle data
	var batch []string
	batchTicker := time.NewTicker(cfg.BatchMaxInterval)
	defer batchTicker.Stop()

	for {
		select {
		case <-batchTicker.C:
			// >BatchMaxInterval, Ingest the batch data
			if err := cfg.IngestData(batch); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to ingest data into Databend: %v\n", err)
			}
			batch = nil

		default:
			// < BatchMaxInterval, continue to consume from kafka
			m, err := k.ReadMessage(context.Background())
			if err != nil {
				if err.Error() == "context canceled" || err.Error() == "EOF" {
					// end
					logrus.Info(err.Error())
					return
				}
				fmt.Fprintf(os.Stderr, "Failed to read message from Kafka: %v\n", err)
				continue
			}

			// unmarshal json data
			data := string(m.Value)
			fmt.Println(string(m.Value))

			// add the data to batch
			batch = append(batch, strings.ReplaceAll(data, "\n", ""))

			// > batchSize, commit the data
			if len(batch) >= cfg.BatchSize {
				if err := cfg.IngestData(batch); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to ingest data into Databend: %v\n", err)
				}
				batch = nil
			}
		}
	}
}
