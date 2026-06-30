package config

import (
	"os"
	"testing"

	"github.com/test-go/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	content := `{
		"kafkaBootstrapServers": "broker1:9092,broker2:9092",
		"kafkaTopic": "my-topic",
		"KafkaConsumerGroup": "my-group",
		"isSASL": true,
		"saslUser": "admin",
		"saslPassword": "secret",
		"disableTLS": false,
		"databendDSN": "http://root:@localhost:8000",
		"databendTable": "default.test",
		"batchSize": 500,
		"batchMaxInterval": 15,
		"workers": 4,
		"dataFormat": "json",
		"minBytes": 2048,
		"maxBytes": 10485760,
		"maxWait": 5,
		"maxRetryDelay": 600
	}`

	tmpFile, err := os.CreateTemp("", "config-*.json")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(content)
	assert.NoError(t, err)
	tmpFile.Close()

	path := tmpFile.Name()
	cfg, err := LoadConfig(&path)
	assert.NoError(t, err)

	assert.Equal(t, "broker1:9092,broker2:9092", cfg.KafkaBootstrapServers)
	assert.Equal(t, "my-topic", cfg.KafkaTopic)
	assert.Equal(t, "my-group", cfg.KafkaConsumerGroup)
	assert.Equal(t, true, cfg.IsSASL)
	assert.Equal(t, "admin", cfg.SaslUser)
	assert.Equal(t, "secret", cfg.SaslPassword)
	assert.Equal(t, false, cfg.DisableTLS)
	assert.Equal(t, 500, cfg.BatchSize)
	assert.Equal(t, 15, cfg.BatchMaxInterval)
	assert.Equal(t, 4, cfg.Workers)
	assert.Equal(t, 2048, cfg.MinBytes)
	assert.Equal(t, 10485760, cfg.MaxBytes)
	assert.Equal(t, 5, cfg.MaxWait)
	assert.Equal(t, 600, cfg.MaxRetryDelay)
}

func TestLoadConfigDefaults(t *testing.T) {
	content := `{}`

	tmpFile, err := os.CreateTemp("", "config-defaults-*.json")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(content)
	assert.NoError(t, err)
	tmpFile.Close()

	path := tmpFile.Name()
	cfg, err := LoadConfig(&path)
	assert.NoError(t, err)

	assert.Equal(t, "localhost:9092", cfg.KafkaBootstrapServers)
	assert.Equal(t, "test", cfg.KafkaTopic)
	assert.Equal(t, "test-group", cfg.KafkaConsumerGroup)
	assert.Equal(t, false, cfg.IsSASL)
	assert.Equal(t, 1000, cfg.BatchSize)
	assert.Equal(t, 30, cfg.BatchMaxInterval)
	assert.Equal(t, 1, cfg.Workers)
	assert.Equal(t, 1024, cfg.MinBytes)
	assert.Equal(t, 20971520, cfg.MaxBytes)
	assert.Equal(t, 10, cfg.MaxWait)
	assert.Equal(t, 1800, cfg.MaxRetryDelay)
	assert.Equal(t, true, cfg.CopyIntoUploadCompression)
}

func TestLoadConfigFileNotFound(t *testing.T) {
	path := "/nonexistent/config.json"
	_, err := LoadConfig(&path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "open config file")
}

func TestLoadConfigInvalidJSON(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config-invalid-*.json")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(`{invalid json}`)
	assert.NoError(t, err)
	tmpFile.Close()

	path := tmpFile.Name()
	_, err = LoadConfig(&path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal config file")
}
