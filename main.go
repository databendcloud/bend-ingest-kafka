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

	"github.com/databendcloud/bend-ingest-kafka/config"
)

var (
	kafkaBrokers       = []string{"localhost:9092"}
	kafkaTopic         = "test"
	kafkaConsumerGroup = "test-group"
	batchSize          = 1000
	batchMaxInterval   = 5 * time.Second
	dataFormat         = "json"
	databendDSN        = "root:@tcp(127.0.0.1:3306)/test"
	databendTable      = "default.test"
)

var version = "dev"

func main() {
	configFile := flag.String("f", "./config/conf.json", "Path to the configuration file")
	showVersion := flag.Bool("v", false, "Print bend-ingest-kafka version and exit")
	flag.BoolVar(showVersion, "version", false, "Print bend-ingest-kafka version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("bend-ingest-kafka version %s\n", version)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigch := make(chan os.Signal, 1)
		signal.Notify(sigch, syscall.SIGQUIT, syscall.SIGTERM)
		<-sigch
		cancel()
	}()

	cfg := parseConfig(configFile)
	ig := NewDatabendIngester(cfg)
	defer ig.Close()

	if !cfg.IsJsonTransform {
		err := ig.CreateRawTargetTable()
		if err != nil {
			panic(err)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		w := NewConsumeWorker(cfg, fmt.Sprintf("worker-%d", i), ig)
		go func() {
			w.Run(ctx)
			wg.Done()
		}()
	}
	wg.Wait()
}

func parseConfigWithFile(configFile *string) *config.Config {
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		panic(err)
	}
	validateConfig(cfg)
	return cfg
}

func validateConfig(cfg *config.Config) {
	if cfg.IsJsonTransform && cfg.UseReplaceMode {
		panic("replace mode can only be used when is-json-transform is false")
	}
	if cfg.IsSASL && cfg.SaslUser == "" {
		panic("sasl user is required when sasl is enabled")
	}
}

func parseConfig(configFile *string) *config.Config {
	if *configFile == "" {
		*configFile = "config/conf.json"
	}
	if _, err := os.Stat(*configFile); err == nil {
		return parseConfigWithFile(configFile)
	}

	cfg := config.Config{}
	flag.StringVar(&cfg.KafkaBootstrapServers, "kafka-bootstrap-servers", "127.0.0.1:64103", "Kafka bootstrap servers")
	flag.StringVar(&cfg.KafkaTopic, "kafka-topic", "test", "Kafka topic")
	flag.StringVar(&cfg.KafkaConsumerGroup, "kafka-consumer-group", "kafka-bend-ingest", "Kafkaconsumer group")
	flag.StringVar(&cfg.DatabendDSN, "databend-dsn", "http://root:root@localhost:8002", "Databend DSN")
	flag.StringVar(&cfg.DatabendTable, "databend-table", "test_ingest", "Databend table")
	flag.StringVar(&cfg.MockData, "mock-data", "", "generate mock data to databend")
	flag.IntVar(&cfg.BatchSize, "batch-size", 1024, "Batch size")
	flag.IntVar(&cfg.Workers, "workers", 1, "Number of workers")
	flag.IntVar(&cfg.BatchMaxInterval, "batch-max-interval", 30, "Batch max interval")
	flag.StringVar(&cfg.DataFormat, "data-format", "json", "kafka data format")
	flag.BoolVar(&cfg.CopyPurge, "copy-purge", false, "purge data before copy")
	flag.BoolVar(&cfg.CopyForce, "copy-force", false, "force copy data")
	flag.BoolVar(&cfg.IsJsonTransform, "is-json-transform", true, "transform json data")
	flag.BoolVar(&cfg.DisableVariantCheck, "disable-variant-check", false, "disable variant check")
	flag.IntVar(&cfg.MinBytes, "min-bytes", 1024, "min bytes")
	flag.IntVar(&cfg.MaxBytes, "max-bytes", 20*1024*1024, "max bytes")
	flag.IntVar(&cfg.MaxWait, "max-wait", 10, "max wait")
	flag.BoolVar(&cfg.UseReplaceMode, "use-replace-mode", false, "use replace into mode")
	flag.StringVar(&cfg.UserStage, "user-stage", "~", "user stage")

	flag.Parse()
	validateConfig(&cfg)
	return &cfg
}

func parseKafkaServers(kafkaServerStr string) []string {
	kafkaServers := strings.Split(kafkaServerStr, ",")
	if len(kafkaServers) == 0 {
		panic("should have kafka servers")
	}
	return kafkaServers
}
