package main

import (
	"sync"
	"time"

	timeseries "github.com/codesuki/go-time-series"
)

type DatabendIngesterStats struct {
	ingestedBytes *timeseries.TimeSeries
	ingestedRows  *timeseries.TimeSeries
	mu            sync.Mutex
}

func NewDatabendIntesterStats() *DatabendIngesterStats {
	ingestedBytes, _ := timeseries.NewTimeSeries()
	return &DatabendIngesterStats{ingestedBytes: ingestedBytes}
}

func (stats *DatabendIngesterStats) RecordMetric(bytes int, rows int) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.ingestedBytes.Increase(bytes)
	stats.ingestedRows.Increase(rows)
}

func (stats *DatabendIngesterStats) BytesPerSecond(duration time.Duration) float64 {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	return stats.calcPerSecond(stats.ingestedBytes, duration)
}

func (stats *DatabendIngesterStats) RowsPerSecond(duration time.Duration) float64 {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	return stats.calcPerSecond(stats.ingestedRows, duration)
}

func (stats *DatabendIngesterStats) calcPerSecond(ts *timeseries.TimeSeries, duration time.Duration) float64 {
	amount, err := ts.Range(time.Now().Add(-duration), time.Now())
	if err != nil {
		return -1
	}

	return float64(amount) / duration.Seconds()
}
