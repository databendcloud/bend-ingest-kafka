package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Config struct {
	KafkaBootstrapServers string        `json:"kafkaBootstrapServers"`
	KafkaTopic            string        `json:"kafkaTopic"`
	KafkaPartition        int           `json:"kafkaPartition"`
	KafkaConsumerGroup    string        `json:"KafkaConsumerGroup"`
	MockData              string        `json:"mockData"`
	IsJsonTransform       bool          `json:"isJsonTransform"`
	DatabendDSN           string        `json:"databendDSN"`
	DatabendTable         string        `json:"databendTable"`
	BatchSize             int           `json:"batchSize"`
	BatchMaxInterval      time.Duration `json:"batchMaxInterval"`
	DataFormat            string        `json:"dataFormat"`
	Workers               int           `json:"workers"`
	CopyPurge             bool          `json:"copyPurge"`
	CopyForce             bool          `json:"copyForce"`
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

	fmt.Println(conf)

	return &conf, nil
}
