package config

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/mcuadros/go-defaults"
)

type Config struct {
	KafkaBootstrapServers string `json:"kafkaBootstrapServers" default:"localhost:9092"`
	KafkaTopic            string `json:"kafkaTopic" default:"test"`
	IsSASL                bool   `json:"isSASL" default:"false"`
	SaslUser              string `json:"saslUser"` // SASL user
	SaslPassword          string `json:"saslPassword"`
	DisableTLS            bool   `json:"disableTLS" default:"false"`
	KafkaConsumerGroup    string `json:"KafkaConsumerGroup" default:"test-group"`
	MockData              string `json:"mockData"`
	IsJsonTransform       bool   `json:"isJsonTransform"`
	DatabendDSN           string `json:"databendDSN" default:"localhost:8000"`
	DatabendTable         string `json:"databendTable"`
	BatchSize             int    `json:"batchSize" default:"1000"`
	BatchMaxInterval      int    `json:"batchMaxInterval" default:"30"`
	DataFormat            string `json:"dataFormat" default:"json"`
	Workers               int    `json:"workers" default:"1"`

	// related docs: https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table
	CopyPurge           bool `json:"copyPurge" default:"false"`
	CopyForce           bool `json:"copyForce" default:"false"`
	DisableVariantCheck bool `json:"disableVariantCheck" default:"false"`
	// MinBytes indicates to the broker the minimum batch size that the consumer
	// will accept. Setting a high minimum when consuming from a low-volume topic
	// may result in delayed delivery when the broker does not have enough data to
	// satisfy the defined minimum.
	//
	// Default: 1KB
	MinBytes int `json:"minBytes" default:"1024"`
	// MaxBytes indicates to the broker the maximum batch size that the consumer
	// will accept. The broker will truncate a message to satisfy this maximum, so
	// choose a value that is high enough for your largest message size.
	//
	// Default: 20MB
	MaxBytes int `json:"maxBytes" default:"20971520"`
	// Maximum amount of time to wait for new data to come when fetching batches
	// of messages from kafka.
	//
	// Default: 10s
	MaxWait int `json:"maxWait" default:"10"`

	// UseReplaceMode determines whether to use the REPLACE INTO statement to insert data.
	// replace into will upsert data
	UseReplaceMode bool   `json:"useReplaceMode" default:"false"`
	UserStage      string `json:"userStage" default:"~"`

	// UseStreamingLoad uses PUT /v1/streaming_load instead of uploadToStage+copyInto.
	// Only valid when IsJsonTransform=false (raw mode). Cannot be combined with UseReplaceMode.
	UseStreamingLoad bool `json:"useStreamingLoad" default:"false"`

	// CopyIntoUploadCompression enables zstd compression for staged NDJSON files used by COPY INTO.
	CopyIntoUploadCompression bool `json:"copyIntoUploadCompression" default:"true"`

	// MaxRetryDelay indicates the maximum delay between retries when ingesting data fails.
	// The retry delay uses exponential backoff, starting from 1 second, and will not exceed this value.
	// Unit: seconds
	// Default: 1800 (30 minutes)
	MaxRetryDelay int `json:"maxRetryDelay" default:"1800"`

	// MetricsPort is the port for the Prometheus metrics HTTP server.
	MetricsPort int `json:"metricsPort" default:"2112"`

	// EnableRebalanceOptimization configures faster partition-change detection,
	// an explicit assignment strategy, and rebalance event logging. Disabling it
	// restores the librdkafka defaults; consumer group rebalancing remains enabled.
	EnableRebalanceOptimization bool `json:"enableRebalanceOptimization" default:"true"`

	// PartitionAssignmentStrategy sets the consumer group partition assignment
	// strategy list. Only used when EnableRebalanceOptimization is true. The
	// default preserves librdkafka's legacy assignors so rolling upgrades remain
	// compatible. Use "cooperative-sticky" only after every consumer in the group
	// supports it.
	//
	// Default: range,roundrobin
	PartitionAssignmentStrategy string `json:"partitionAssignmentStrategy" default:"range,roundrobin"`

	// TopicMetadataRefreshIntervalMs controls how often the consumer refreshes
	// topic metadata to detect changes such as newly added partitions. A lower
	// value makes partition expansion picked up faster. Only used when
	// EnableRebalanceOptimization is true.
	// Unit: milliseconds
	//
	// Default: 60000 (1 minute)
	TopicMetadataRefreshIntervalMs int `json:"topicMetadataRefreshIntervalMs" default:"60000"`
}

func LoadConfig(configFile *string) (*Config, error) {
	conf := Config{}

	path := "config/conf.json"
	if configFile != nil && *configFile != "" {
		path = *configFile
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config file %q failed: %w", path, err)
	}
	defer f.Close()
	confByte, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read config file %q failed: %w", path, err)
	}
	defaults.SetDefaults(&conf)
	err = json.Unmarshal(confByte, &conf)
	if err != nil {
		return nil, fmt.Errorf("unmarshal config file %q failed: %w", path, err)
	}

	return &conf, nil
}
