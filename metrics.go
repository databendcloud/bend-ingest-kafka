package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ingestRowsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bend_ingest_kafka_ingest_rows_total",
		Help: "Total number of rows ingested into Databend",
	})
	ingestBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bend_ingest_kafka_ingest_bytes_total",
		Help: "Total bytes ingested into Databend",
	})
	consumeRowsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bend_ingest_kafka_consume_rows_total",
		Help: "Total number of rows consumed from Kafka",
	})
	consumeBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bend_ingest_kafka_consume_bytes_total",
		Help: "Total bytes consumed from Kafka",
	})
	uploadStageDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "bend_ingest_kafka_upload_stage_duration_seconds",
		Help:    "Duration of upload to stage operations",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10},
	})
	copyIntoDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "bend_ingest_kafka_copy_into_duration_seconds",
		Help:    "Duration of COPY INTO operations",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10},
	})
	batchSizeHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "bend_ingest_kafka_batch_size",
		Help:    "Number of messages per batch",
		Buckets: []float64{10, 50, 100, 500, 1000, 5000, 10000},
	})
	batchFillDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "bend_ingest_kafka_batch_fill_duration_seconds",
		Help:    "Time to fill a batch from Kafka",
		Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30},
	})
	ingestErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bend_ingest_kafka_ingest_errors_total",
		Help: "Total number of ingest errors",
	})
)

func initMetrics() {
	prometheus.MustRegister(
		ingestRowsTotal,
		ingestBytesTotal,
		consumeRowsTotal,
		consumeBytesTotal,
		uploadStageDuration,
		copyIntoDuration,
		batchSizeHist,
		batchFillDuration,
		ingestErrors,
	)
}
