package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	kafkaBrokers       = []string{"localhost:9092"}
	kafkaTopic         = "test"
	kafkaConsumerGroup = "test-group"
	batchSize          = 1000
	batchMaxInterval   = 5 * time.Second
	dataFormat         = "json"
	databendDSN        = "root:@tcp(127.0.0.1:3306)/test"
	databendTable      = "test"
)

type Config struct {
	KafkaBootstrapServers string
	KafkaTopic            string
	KafkaConsumerGroup    string
	MockData              string
	DatabendDSN           string
	DatabendTable         string
	BatchSize             int
	BatchMaxInterval      time.Duration
	DataFormat            string
	Workers               int
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigch := make(chan os.Signal, 1)
		signal.Notify(sigch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM)
		<-sigch
		cancel()
	}()

	cfg := parseConfig()
	wg := sync.WaitGroup{}
	wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		w := NewConsumeWorker(cfg, fmt.Sprintf("worker-%d", i))
		go func() {
			w.Run(ctx)
			wg.Done()
		}()
	}
	wg.Wait()
}

func parseConfig() *Config {
	cfg := Config{}

	flag.StringVar(&cfg.KafkaBootstrapServers, "kafka-bootstrap-servers", "127.0.0.1:64103", "Kafka bootstrap servers")
	flag.StringVar(&cfg.KafkaTopic, "kafka-topic", "test", "Kafka topic")
	flag.StringVar(&cfg.KafkaConsumerGroup, "kafka-consumer-group", "kafka-bend-ingest", "Kafkaconsumer group")
	flag.StringVar(&cfg.DatabendDSN, "databend-dsn", "http://root:root@localhost:8002", "Databend DSN")
	flag.StringVar(&cfg.DatabendTable, "databend-table", "test_ingest", "Databend table")
	flag.StringVar(&cfg.MockData, "mock-data", "", "generate mock data to databend")
	flag.IntVar(&cfg.BatchSize, "batch-size", 1024, "Batch size")
	flag.IntVar(&cfg.Workers, "workers", 1, "Number of workers")
	flag.DurationVar(&cfg.BatchMaxInterval, "batch-max-interval", 30*time.Second, "Batch max interval")
	flag.StringVar(&cfg.DataFormat, "data-format", "json", "kafka data format")

	flag.Parse()
	return &cfg
}

func parseKafkaServers(kafkaServerStr string) []string {
	kafkaServers := strings.Split(kafkaServerStr, ",")
	if len(kafkaServers) == 0 {
		panic("should have kafka servers")
	}
	return kafkaServers
}
