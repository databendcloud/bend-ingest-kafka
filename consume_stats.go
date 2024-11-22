package main

import (
	"sync"
	"time"

	timeseries "github.com/codesuki/go-time-series"
)

type DatabendConsumeStatsRecorder struct {
	consumedBytes *timeseries.TimeSeries
	consumedRows  *timeseries.TimeSeries
	mu            sync.Mutex
}

type DatabendConsumeStatsData struct {
	BytesPerSecond float64
	RowsPerSecond  float64
}

func NewDatabendConsumeStatsRecorder() *DatabendConsumeStatsRecorder {
	consumedBytes, err := timeseries.NewTimeSeries()
	if err != nil {
		panic(err)
	}
	consumedRows, err := timeseries.NewTimeSeries()
	if err != nil {
		panic(err)
	}
	return &DatabendConsumeStatsRecorder{
		consumedBytes: consumedBytes,
		consumedRows:  consumedRows,
	}
}

func (stats *DatabendConsumeStatsRecorder) RecordMetric(bytes int, rows int) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.consumedBytes.Increase(bytes)
	stats.consumedRows.Increase(rows)
}

func (stats *DatabendConsumeStatsRecorder) Stats(statsWindow time.Duration) DatabendConsumeStatsData {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	bytesPerSecond := stats.calcPerSecond(stats.consumedBytes, statsWindow)
	rowsPerSecond := stats.calcPerSecond(stats.consumedRows, statsWindow)
	return DatabendConsumeStatsData{
		BytesPerSecond: bytesPerSecond,
		RowsPerSecond:  rowsPerSecond,
	}
}

func (stats *DatabendConsumeStatsRecorder) calcPerSecond(ts *timeseries.TimeSeries, duration time.Duration) float64 {
	amount, err := ts.Range(time.Now().Add(-duration), time.Now())
	if err != nil {
		return -1
	}

	return float64(amount) / duration.Seconds()
}
