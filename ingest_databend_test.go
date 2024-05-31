package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/test-go/testify/assert"

	"bend-ingest-kafka/config"
	"bend-ingest-kafka/message"
)

type ingestDatabendTest struct {
	databendDSN string
}

func prepareIngestDatabendTest() *ingestDatabendTest {
	testDatabendDSN := os.Getenv("TEST_DATABEND_DSN")
	if testDatabendDSN == "" {
		testDatabendDSN = "http://databend:databend@localhost:8000?presigned_url_disabled=true"
	}
	return &ingestDatabendTest{databendDSN: testDatabendDSN}
}

func TestParseKafkaServers(t *testing.T) {
	s1 := []string{"127.0.0.1:8000", "127.0.0.1:8000,127.0.0.2:8000"}
	for _, s := range s1 {
		res := parseKafkaServers(s)
		fmt.Println(res)
	}
}

func TestIngestDataIsJsonTransform(t *testing.T) {
	tt := prepareIngestDatabendTest()
	cfg := config.Config{
		KafkaBootstrapServers: "127.0.0.1:9002",
		KafkaTopic:            "test",
		KafkaConsumerGroup:    "test",
		DatabendDSN:           tt.databendDSN,
		//DatabendDSN:      os.Getenv("TEST_DATABEND_DSN"),
		DataFormat:       "json",
		IsJsonTransform:  true,
		DatabendTable:    "test_ingest",
		BatchSize:        10,
		BatchMaxInterval: 10,
	}
	db, err := sql.Open("databend", cfg.DatabendDSN)
	assert.NoError(t, err)
	execute(db, "create table if not exists test_ingest(name varchar, age int, isMarried boolean);")
	defer execute(db, "drop table if exists test_ingest;")

	testData := []string{"{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}", "{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}"}
	messageData := message.MessageData{
		Data:       testData[0],
		DataOffset: 1,
		Partition:  1,
		Key:        "1",
		CreateTime: time.Now(),
	}
	messagesBatch := &message.MessagesBatch{Messages: []message.MessageData{messageData}}
	ig := NewDatabendIngester(&cfg)
	err = ig.IngestData(messagesBatch)
	assert.NoError(t, err)

	// check the data
	result, err := db.Query("select * from test_ingest")
	assert.NoError(t, err)
	count := 0
	for result.Next() {
		count += 1
		var name string
		var age int
		var isMarried bool
		err = result.Scan(&name, &age, &isMarried)
		fmt.Println(name, age, isMarried)
	}
}

type recordMetadata struct {
	CreateTime time.Time `json:"create_time"`
	Offset     string    `json:"offset"`
	Partition  string    `json:"partition"`
	Key        string    `json:"key"`
	Topic      string    `json:"topic"`
}

func TestIngestDataWithoutJsonTransform(t *testing.T) {
	tt := prepareIngestDatabendTest()
	cfg := config.Config{
		KafkaBootstrapServers: "127.0.0.1:9002",
		KafkaTopic:            "test",
		KafkaConsumerGroup:    "test",
		DatabendDSN:           tt.databendDSN,
		//DatabendDSN:      os.Getenv("TEST_DATABEND_DSN"),
		DataFormat:       "json",
		IsJsonTransform:  false,
		DatabendTable:    "default.test_ingest_without",
		BatchSize:        10,
		BatchMaxInterval: 10,
	}
	db, err := sql.Open("databend", cfg.DatabendDSN)
	assert.NoError(t, err)
	execute(db, "create or replace table test_ingest_without(uuid String, raw_data JSON, record_metadata JSON, add_time timestamp);")
	defer execute(db, "drop table if exists test_ingest_without;")

	testData := []string{"{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}", "{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}"}
	messageData := message.MessageData{
		Data:       testData[0],
		DataOffset: 1,
		Partition:  1,
		Key:        "1",
		CreateTime: time.Now(),
	}
	messagesBatch := &message.MessagesBatch{Messages: []message.MessageData{messageData}}
	ig := NewDatabendIngester(&cfg)
	err = ig.IngestData(messagesBatch)
	assert.NoError(t, err)

	// check the data
	result, err := db.Query("select * from test_ingest_without")
	assert.NoError(t, err)
	count := 0
	var raw_data string
	var record_metadata string
	var add_time time.Time
	for result.Next() {
		count += 1
		var uuid string
		err = result.Scan(&uuid, &raw_data, &record_metadata, &add_time)
		fmt.Println(uuid, raw_data, record_metadata, add_time)
	}
	assert.NotEqual(t, 0, count)
	res := &recordMetadata{}
	err = json.Unmarshal([]byte(record_metadata), res)
	fmt.Println(*res)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%d", messageData.DataOffset), res.Offset)
}
