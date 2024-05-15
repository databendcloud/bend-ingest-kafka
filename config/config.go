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
	CopyPurge             bool   `json:"copyPurge" default:"false"`
	CopyForce             bool   `json:"copyForce" default:"false"`
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
