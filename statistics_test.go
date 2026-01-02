package rockyardkv

// statistics_test.go implements tests for statistics.

import (
	"sync"
	"testing"
)

func TestStatisticsBasic(t *testing.T) {
	stats := NewStatistics()

	// Record some tickers
	stats.RecordTick(TickerBytesWritten, 100)
	stats.RecordTick(TickerBytesWritten, 50)
	stats.RecordTick(TickerNumberKeysWritten, 1)

	if got := stats.GetTickerCount(TickerBytesWritten); got != 150 {
		t.Errorf("TickerBytesWritten = %d, want 150", got)
	}
	if got := stats.GetTickerCount(TickerNumberKeysWritten); got != 1 {
		t.Errorf("TickerNumberKeysWritten = %d, want 1", got)
	}
}

func TestStatisticsSetTicker(t *testing.T) {
	stats := NewStatistics()

	stats.SetTickerCount(TickerBytesRead, 1000)
	if got := stats.GetTickerCount(TickerBytesRead); got != 1000 {
		t.Errorf("TickerBytesRead = %d, want 1000", got)
	}

	stats.SetTickerCount(TickerBytesRead, 500)
	if got := stats.GetTickerCount(TickerBytesRead); got != 500 {
		t.Errorf("TickerBytesRead = %d, want 500", got)
	}
}

func TestStatisticsHistogram(t *testing.T) {
	stats := NewStatistics()

	// Record histogram values
	stats.MeasureTime(HistogramDBGet, 100)
	stats.MeasureTime(HistogramDBGet, 200)
	stats.MeasureTime(HistogramDBGet, 300)

	data := stats.GetHistogramData(HistogramDBGet)

	if data.Count != 3 {
		t.Errorf("Count = %d, want 3", data.Count)
	}
	if data.Sum != 600 {
		t.Errorf("Sum = %d, want 600", data.Sum)
	}
	if data.Min != 100 {
		t.Errorf("Min = %f, want 100", data.Min)
	}
	if data.Max != 300 {
		t.Errorf("Max = %f, want 300", data.Max)
	}
	if data.Average != 200 {
		t.Errorf("Average = %f, want 200", data.Average)
	}
}

func TestStatisticsReset(t *testing.T) {
	stats := NewStatistics()

	stats.RecordTick(TickerBytesWritten, 100)
	stats.MeasureTime(HistogramDBGet, 100)

	stats.Reset()

	if got := stats.GetTickerCount(TickerBytesWritten); got != 0 {
		t.Errorf("After reset, TickerBytesWritten = %d, want 0", got)
	}

	data := stats.GetHistogramData(HistogramDBGet)
	if data.Count != 0 {
		t.Errorf("After reset, histogram count = %d, want 0", data.Count)
	}
}

func TestStatisticsConcurrent(t *testing.T) {
	stats := NewStatistics()

	const numGoroutines = 10
	const numOps = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range numOps {
				stats.RecordTick(TickerBytesWritten, 1)
				stats.MeasureTime(HistogramDBGet, 100)
			}
		}()
	}

	wg.Wait()

	expected := uint64(numGoroutines * numOps)
	if got := stats.GetTickerCount(TickerBytesWritten); got != expected {
		t.Errorf("TickerBytesWritten = %d, want %d", got, expected)
	}

	data := stats.GetHistogramData(HistogramDBGet)
	if data.Count != expected {
		t.Errorf("Histogram count = %d, want %d", data.Count, expected)
	}
}

func TestStatisticsInvalidTypes(t *testing.T) {
	stats := NewStatistics()

	// Invalid ticker type should not panic
	stats.RecordTick(TickerEnumMax, 100)
	stats.RecordTick(-1, 100)
	_ = stats.GetTickerCount(TickerEnumMax)
	_ = stats.GetTickerCount(-1)

	// Invalid histogram type should not panic
	stats.MeasureTime(HistogramEnumMax, 100)
	stats.MeasureTime(-1, 100)
	_ = stats.GetHistogramData(HistogramEnumMax)
	_ = stats.GetHistogramData(-1)
}

func TestTickerTypeString(t *testing.T) {
	tests := []struct {
		ticker TickerType
		want   string
	}{
		{TickerBlockCacheMiss, "rocksdb.block.cache.miss"},
		{TickerBlockCacheHit, "rocksdb.block.cache.hit"},
		{TickerBytesWritten, "rocksdb.bytes.written"},
		{TickerBytesRead, "rocksdb.bytes.read"},
	}

	for _, tt := range tests {
		if got := tt.ticker.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.ticker, got, tt.want)
		}
	}

	// Invalid ticker
	if got := TickerEnumMax.String(); got != "unknown" {
		t.Errorf("TickerEnumMax.String() = %q, want 'unknown'", got)
	}
}

func TestHistogramTypeString(t *testing.T) {
	tests := []struct {
		histogram HistogramType
		want      string
	}{
		{HistogramDBGet, "rocksdb.db.get.micros"},
		{HistogramDBWrite, "rocksdb.db.write.micros"},
		{HistogramCompactionTime, "rocksdb.compaction.times.micros"},
	}

	for _, tt := range tests {
		if got := tt.histogram.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.histogram, got, tt.want)
		}
	}

	// Invalid histogram
	if got := HistogramEnumMax.String(); got != "unknown" {
		t.Errorf("HistogramEnumMax.String() = %q, want 'unknown'", got)
	}
}

func TestStatisticsString(t *testing.T) {
	stats := NewStatistics()

	stats.RecordTick(TickerBytesWritten, 100)
	stats.MeasureTime(HistogramDBGet, 100)

	str := stats.String()
	if str == "" {
		t.Error("String() returned empty string")
	}

	// Should contain the ticker we recorded
	if len(str) < 10 {
		t.Errorf("String() too short: %s", str)
	}
}

func TestHistogramMinMax(t *testing.T) {
	stats := NewStatistics()

	// Record values in non-sorted order
	stats.MeasureTime(HistogramDBGet, 500)
	stats.MeasureTime(HistogramDBGet, 100)
	stats.MeasureTime(HistogramDBGet, 900)
	stats.MeasureTime(HistogramDBGet, 200)

	data := stats.GetHistogramData(HistogramDBGet)

	if data.Min != 100 {
		t.Errorf("Min = %f, want 100", data.Min)
	}
	if data.Max != 900 {
		t.Errorf("Max = %f, want 900", data.Max)
	}
}

func TestStatisticsEmptyHistogram(t *testing.T) {
	stats := NewStatistics()

	data := stats.GetHistogramData(HistogramDBGet)

	if data.Count != 0 {
		t.Errorf("Empty histogram count = %d, want 0", data.Count)
	}
	if data.Sum != 0 {
		t.Errorf("Empty histogram sum = %d, want 0", data.Sum)
	}
	if data.Average != 0 {
		t.Errorf("Empty histogram average = %f, want 0", data.Average)
	}
}

func TestAllTickerTypes(t *testing.T) {
	stats := NewStatistics()

	// Test all ticker types can be recorded without panicking
	for i := range TickerEnumMax {
		stats.RecordTick(i, 1)
		if got := stats.GetTickerCount(i); got != 1 {
			t.Errorf("GetTickerCount(%d) = %d, want 1", i, got)
		}
	}
}

func TestAllHistogramTypes(t *testing.T) {
	stats := NewStatistics()

	// Test all histogram types can be measured without panicking
	for i := range HistogramEnumMax {
		stats.MeasureTime(i, 100)
		data := stats.GetHistogramData(i)
		if data.Count != 1 {
			t.Errorf("GetHistogramData(%d).Count = %d, want 1", i, data.Count)
		}
	}
}
