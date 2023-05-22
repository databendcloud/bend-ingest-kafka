package main

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/test-go/testify/assert"
)

type ingestDatabendTest struct {
	databendDSN string
}

func prepareIngestDatabendTest() *ingestDatabendTest {
	testDatabendDSN := os.Getenv("TEST_DATABEND_DSN")
	if testDatabendDSN == "" {
		testDatabendDSN = "http://root:root@localhost:8002"
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

func TestIngestData(t *testing.T) {
	tt := prepareIngestDatabendTest()
	cfg := Config{
		KafkaBootstrapServers: "127.0.0.1:64103",
		KafkaTopic:            "test",
		KafkaConsumerGroup:    "test",
		DatabendDSN:           tt.databendDSN,
		//DatabendDSN:      os.Getenv("TEST_DATABEND_DSN"),
		DataFormat:       "json",
		DatabendTable:    "test_ingest",
		BatchSize:        10,
		BatchMaxInterval: 100,
	}
	db, err := sql.Open("databend", cfg.DatabendDSN)
	assert.NoError(t, err)
	execute(db, "create table if not exists test_ingest(name varchar, age int, isMarried boolean);")
	defer execute(db, "drop table if exists test_ingest;")

	testData := []string{"{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}", "{\"name\": \"Alice\",\"age\": 30,\"isMarried\": true}"}
	ig := NewDatabendIngester(cfg.DatabendDSN, cfg.DatabendTable)
	err = ig.IngestData(testData)
	assert.NoError(t, err)
}
