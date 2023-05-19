package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	cfg *Config
	ig  Ingester
}

func NewConsumer(cfg *Config) *Consumer {
	ig := NewIngester(cfg)
	return &Consumer{
		cfg: cfg,
		ig:  ig,
	}
}

func (c *Consumer) ConsumeMessages() {
	// consume data from kafka
	k := kafka.NewReader(kafka.ReaderConfig{
		Brokers: parseKafkaServers(c.cfg.KafkaBootstrapServers),
		GroupID: c.cfg.KafkaConsumerGroup,
		Topic:   c.cfg.KafkaTopic,
	})
	defer k.Close()

	// handle data
	var batch []string
	batchTicker := time.NewTicker(c.cfg.BatchMaxInterval)
	defer batchTicker.Stop()

	for {
		select {
		case <-batchTicker.C:
			// >BatchMaxInterval, Ingest the batch data
			if err := c.ig.IngestData(batch); err != nil {
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
			if len(batch) >= c.cfg.BatchSize {
				if err := c.ig.IngestData(batch); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to ingest data into Databend: %v\n", err)
				}
				batch = nil
			}
		}
	}
}
