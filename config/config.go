package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	KafkaBootstrapServers string `json:"kafkaBootstrapServers" default:"localhost:9092"`
	KafkaTopic            string `json:"kafkaTopic" default:"test"`
	KafkaConsumerGroup    string `json:"KafkaConsumerGroup" default:"test-group"`
	MockData              string `json:"mockData"`
	IsJsonTransform       bool   `json:"isJsonTransform" default:"true"`
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
	MaxBytes int `json:"maxBytes" default:"20 * 1024 * 1024"`
	// Maximum amount of time to wait for new data to come when fetching batches
	// of messages from kafka.
	//
	// Default: 10s
	MaxWait int `json:"maxWait" default:"10"`

	// UseReplaceMode determines whether to use the REPLACE INTO statement to insert data.
	// replace into will upsert data
	UseReplaceMode bool `json:"useReplaceMode" default:"false"`
}

func LoadConfig() (*Config, error) {
	conf := Config{}

	f, err := os.Open("config/conf.json")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	err = decoder.Decode(&conf)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		return &conf, err
	}

	return &conf, nil
}
