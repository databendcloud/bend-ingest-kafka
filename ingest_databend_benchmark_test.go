package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/databendcloud/bend-ingest-kafka/config"
	"github.com/databendcloud/bend-ingest-kafka/message"
)

func benchmarkIngester() *databendIngester {
	return &databendIngester{
		databendIngesterCfg: &config.Config{
			KafkaTopic:    "benchmark-topic",
			DatabendTable: "default.benchmark_table",
		},
	}
}

func benchmarkMessageBatch(size int) *message.MessagesBatch {
	messages := make([]message.MessageData, 0, size)
	payload := `{"name":"Alice","age":30,"isMarried":true,"tags":["kafka","databend","ingest"],"nested":{"city":"Shanghai","score":99.5}}`
	for i := 0; i < size; i++ {
		messages = append(messages, message.MessageData{
			Data:       payload,
			DataOffset: int64(i),
			Partition:  i % 16,
			Key:        fmt.Sprintf("key-%d", i),
			CreateTime: time.Unix(1710000000+int64(i), int64(i)),
		})
	}
	return &message.MessagesBatch{Messages: messages}
}

func BenchmarkRewriteRawNDJSON1K(b *testing.B) {
	ig := benchmarkIngester()
	batch := benchmarkMessageBatch(1000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := ig.reWriteTheJsonData(batch)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != len(batch.Messages) {
			b.Fatalf("got %d rows, want %d", len(rows), len(batch.Messages))
		}
	}
}

func BenchmarkRewriteRawNDJSON100K(b *testing.B) {
	ig := benchmarkIngester()
	batch := benchmarkMessageBatch(100000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := ig.reWriteTheJsonData(batch)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != len(batch.Messages) {
			b.Fatalf("got %d rows, want %d", len(rows), len(batch.Messages))
		}
	}
}

func BenchmarkGenerateNDJSONFile1K(b *testing.B) {
	ig := benchmarkIngester()
	rows, err := ig.reWriteTheJsonData(benchmarkMessageBatch(1000))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name, _, err := ig.generateNDJsonFile(rows)
		if err != nil {
			b.Fatal(err)
		}
		if err := os.Remove(name); err != nil {
			b.Fatal(err)
		}
		_ = name
	}
}

func BenchmarkGenerateNDJSONFile100K(b *testing.B) {
	ig := benchmarkIngester()
	rows, err := ig.reWriteTheJsonData(benchmarkMessageBatch(100000))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name, _, err := ig.generateNDJsonFile(rows)
		if err != nil {
			b.Fatal(err)
		}
		if err := os.Remove(name); err != nil {
			b.Fatal(err)
		}
		_ = name
	}
}

func BenchmarkBuildParquetRecords1K(b *testing.B) {
	ig := benchmarkIngester()
	batch := benchmarkMessageBatch(1000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records, err := ig.buildParquetRecords(batch)
		if err != nil {
			b.Fatal(err)
		}
		if len(records) != len(batch.Messages) {
			b.Fatalf("got %d records, want %d", len(records), len(batch.Messages))
		}
	}
}

func BenchmarkRewriteParquetJSON100K(b *testing.B) {
	ig := benchmarkIngester()
	batch := benchmarkMessageBatch(100000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := ig.reWriteParquetJsonData(batch)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != len(batch.Messages) {
			b.Fatalf("got %d rows, want %d", len(rows), len(batch.Messages))
		}
	}
}
