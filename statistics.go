package rockyardkv

// statistics.go implements the Statistics interface for collecting database metrics.
// Reference: RocksDB v10.7.5 include/rocksdb/statistics.h

import (
	"sync/atomic"
)

// TickerType represents different types of counters.
type TickerType int

const (
	// TickerBlockCacheMiss is the count of block cache misses.
	TickerBlockCacheMiss TickerType = iota
	// TickerBlockCacheHit is the count of block cache hits.
	TickerBlockCacheHit
	// TickerBytesWritten is the total bytes written to the database.
	TickerBytesWritten
	// TickerBytesRead is the total bytes read from the database.
	TickerBytesRead
	// TickerNumberKeysWritten is the count of keys written.
	TickerNumberKeysWritten
	// TickerNumberKeysRead is the count of keys read.
	TickerNumberKeysRead
	// TickerNumberSeekNext is the count of Iterator.Next() calls.
	TickerNumberSeekNext
	// TickerNumberSeekPrev is the count of Iterator.Prev() calls.
	TickerNumberSeekPrev
	// TickerNumberSeek is the count of Iterator.Seek() calls.
	TickerNumberSeek
	// TickerWALFileBytes is the total bytes written to WAL.
	TickerWALFileBytes
	// TickerWALFileSynced is the count of WAL file syncs.
	TickerWALFileSynced
	// TickerCompactReadBytes is bytes read during compaction.
	TickerCompactReadBytes
	// TickerCompactWriteBytes is bytes written during compaction.
	TickerCompactWriteBytes
	// TickerFlushWriteBytes is bytes written during flush.
	TickerFlushWriteBytes
	// TickerMemtableHit is the count of memtable hits.
	TickerMemtableHit
	// TickerMemtableMiss is the count of memtable misses.
	TickerMemtableMiss
	// TickerGetHitL0 is the count of L0 hits.
	TickerGetHitL0
	// TickerGetHitL1 is the count of L1 hits.
	TickerGetHitL1
	// TickerGetHitL2AndUp is the count of L2+ hits.
	TickerGetHitL2AndUp
	// TickerBloomFilterUseful is the count of bloom filter useful rejections.
	TickerBloomFilterUseful
	// TickerBloomFilterFullPositive is the count of bloom filter full positives.
	TickerBloomFilterFullPositive
	// TickerBloomFilterFullTruePositive is the count of bloom filter true positives.
	TickerBloomFilterFullTruePositive
	// TickerNumberDBSeek is the count of db.Get() calls.
	TickerNumberDBSeek
	// TickerNumberDBNext is the count of iterator next calls.
	TickerNumberDBNext
	// TickerNumberDBPrev is the count of iterator prev calls.
	TickerNumberDBPrev
	// TickerNumberDBSeekFound is the count of db.Get() calls that found the key.
	TickerNumberDBSeekFound
	// TickerNumberDBSeekNotFound is the count of db.Get() calls that didn't find the key.
	TickerNumberDBSeekNotFound
	// TickerIterBytesRead is the total bytes read by iterators.
	TickerIterBytesRead
	// TickerWriteWithWAL is the count of writes with WAL.
	TickerWriteWithWAL
	// TickerWriteWithoutWAL is the count of writes without WAL.
	TickerWriteWithoutWAL
	// TickerStallMicros is the total microseconds spent in write stalls.
	TickerStallMicros

	// Block cache granular metrics (reference: RocksDB statistics.h)
	// TickerBlockCacheIndexMiss is the count of index block cache misses.
	TickerBlockCacheIndexMiss
	// TickerBlockCacheIndexHit is the count of index block cache hits.
	TickerBlockCacheIndexHit
	// TickerBlockCacheFilterMiss is the count of filter block cache misses.
	TickerBlockCacheFilterMiss
	// TickerBlockCacheFilterHit is the count of filter block cache hits.
	TickerBlockCacheFilterHit
	// TickerBlockCacheDataMiss is the count of data block cache misses.
	TickerBlockCacheDataMiss
	// TickerBlockCacheDataHit is the count of data block cache hits.
	TickerBlockCacheDataHit
	// TickerBlockCacheAdd is the count of blocks added to cache.
	TickerBlockCacheAdd
	// TickerBlockCacheBytesRead is the total bytes read from block cache.
	TickerBlockCacheBytesRead
	// TickerBlockCacheBytesWrite is the total bytes written to block cache.
	TickerBlockCacheBytesWrite

	// Compaction key drop reasons (reference: RocksDB statistics.h)
	// TickerCompactionKeyDropNewerEntry is keys dropped due to newer entry.
	TickerCompactionKeyDropNewerEntry
	// TickerCompactionKeyDropObsolete is keys dropped as obsolete.
	TickerCompactionKeyDropObsolete
	// TickerCompactionKeyDropRangeDel is keys dropped due to range delete.
	TickerCompactionKeyDropRangeDel

	// File operations
	// TickerNoFileOpens is the count of file opens.
	TickerNoFileOpens
	// TickerNoFileErrors is the count of file errors.
	TickerNoFileErrors

	// MultiGet statistics
	// TickerNumberMultiGetCalls is the count of MultiGet calls.
	TickerNumberMultiGetCalls
	// TickerNumberMultiGetKeysRead is the count of keys read in MultiGet.
	TickerNumberMultiGetKeysRead
	// TickerNumberMultiGetKeysFound is the count of keys found in MultiGet.
	TickerNumberMultiGetKeysFound
	// TickerNumberMultiGetBytesRead is the bytes read in MultiGet.
	TickerNumberMultiGetBytesRead

	// TickerNumberMergeFailures is the count of merge operation failures.
	TickerNumberMergeFailures

	// TickerEnumMax is the maximum ticker type for sizing arrays.
	TickerEnumMax
)

// String returns the name of the ticker type.
func (t TickerType) String() string {
	names := []string{
		"rocksdb.block.cache.miss",
		"rocksdb.block.cache.hit",
		"rocksdb.bytes.written",
		"rocksdb.bytes.read",
		"rocksdb.number.keys.written",
		"rocksdb.number.keys.read",
		"rocksdb.number.seek.next",
		"rocksdb.number.seek.prev",
		"rocksdb.number.seek",
		"rocksdb.wal.bytes",
		"rocksdb.wal.synced",
		"rocksdb.compact.read.bytes",
		"rocksdb.compact.write.bytes",
		"rocksdb.flush.write.bytes",
		"rocksdb.memtable.hit",
		"rocksdb.memtable.miss",
		"rocksdb.l0.hit",
		"rocksdb.l1.hit",
		"rocksdb.l2andup.hit",
		"rocksdb.bloom.filter.useful",
		"rocksdb.bloom.filter.full.positive",
		"rocksdb.bloom.filter.full.true.positive",
		"rocksdb.db.seek",
		"rocksdb.db.next",
		"rocksdb.db.prev",
		"rocksdb.db.seek.found",
		"rocksdb.db.seek.notfound",
		"rocksdb.iter.bytes.read",
		"rocksdb.write.wal",
		"rocksdb.write.nowal",
		"rocksdb.stall.micros",
		// Block cache granular metrics
		"rocksdb.block.cache.index.miss",
		"rocksdb.block.cache.index.hit",
		"rocksdb.block.cache.filter.miss",
		"rocksdb.block.cache.filter.hit",
		"rocksdb.block.cache.data.miss",
		"rocksdb.block.cache.data.hit",
		"rocksdb.block.cache.add",
		"rocksdb.block.cache.bytes.read",
		"rocksdb.block.cache.bytes.write",
		// Compaction key drop reasons
		"rocksdb.compaction.key.drop.newer.entry",
		"rocksdb.compaction.key.drop.obsolete",
		"rocksdb.compaction.key.drop.range.del",
		// File operations
		"rocksdb.no.file.opens",
		"rocksdb.no.file.errors",
		// MultiGet statistics
		"rocksdb.number.multiget.calls",
		"rocksdb.number.multiget.keys.read",
		"rocksdb.number.multiget.keys.found",
		"rocksdb.number.multiget.bytes.read",
		// Merge failures
		"rocksdb.number.merge.failures",
	}
	if int(t) < len(names) {
		return names[t]
	}
	return "unknown"
}

// HistogramType represents different types of histograms.
type HistogramType int

const (
	// HistogramDBGet is the histogram for db.Get() latency.
	HistogramDBGet HistogramType = iota
	// HistogramDBWrite is the histogram for db.Write() latency.
	HistogramDBWrite
	// HistogramCompactionTime is the histogram for compaction time.
	HistogramCompactionTime
	// HistogramFlushTime is the histogram for flush time.
	HistogramFlushTime
	// HistogramTableSyncMicros is the histogram for table sync time.
	HistogramTableSyncMicros
	// HistogramWALFileSyncMicros is the histogram for WAL sync time.
	HistogramWALFileSyncMicros
	// HistogramManifestFileSyncMicros is the histogram for manifest sync time.
	HistogramManifestFileSyncMicros
	// HistogramTableOpenIOMicros is the histogram for table open I/O time.
	HistogramTableOpenIOMicros
	// HistogramMultiGetIO is the histogram for multiget I/O.
	HistogramMultiGetIO
	// HistogramReadBlockGetMicros is the histogram for block read time.
	HistogramReadBlockGetMicros
	// HistogramWriteStallDuration is the histogram for write stall duration.
	HistogramWriteStallDuration
	// HistogramBytesPerRead is the histogram for bytes per read.
	HistogramBytesPerRead
	// HistogramBytesPerWrite is the histogram for bytes per write.
	HistogramBytesPerWrite
	// HistogramBytesPerMultiGet is the histogram for bytes per multiget.
	HistogramBytesPerMultiGet
	// HistogramBytesCompressed is the histogram for compressed bytes.
	HistogramBytesCompressed
	// HistogramBytesDecompressed is the histogram for decompressed bytes.
	HistogramBytesDecompressed
	// HistogramCompressionTimesNanos is the histogram for compression time.
	HistogramCompressionTimesNanos
	// HistogramDecompressionTimesNanos is the histogram for decompression time.
	HistogramDecompressionTimesNanos
	// HistogramSSTableBatchOpMicros is the histogram for SST batch operations.
	HistogramSSTableBatchOpMicros

	// HistogramEnumMax is the maximum histogram type for sizing arrays.
	HistogramEnumMax
)

// String returns the name of the histogram type.
func (h HistogramType) String() string {
	names := []string{
		"rocksdb.db.get.micros",
		"rocksdb.db.write.micros",
		"rocksdb.compaction.times.micros",
		"rocksdb.flush.time.micros",
		"rocksdb.table.sync.micros",
		"rocksdb.wal.file.sync.micros",
		"rocksdb.manifest.file.sync.micros",
		"rocksdb.table.open.io.micros",
		"rocksdb.multiget.io.micros",
		"rocksdb.read.block.get.micros",
		"rocksdb.write.stall.duration",
		"rocksdb.bytes.per.read",
		"rocksdb.bytes.per.write",
		"rocksdb.bytes.per.multiget",
		"rocksdb.bytes.compressed",
		"rocksdb.bytes.decompressed",
		"rocksdb.compression.times.nanos",
		"rocksdb.decompression.times.nanos",
		"rocksdb.sst.batch.op.micros",
	}
	if int(h) < len(names) {
		return names[h]
	}
	return "unknown"
}

// HistogramData contains histogram statistics.
type HistogramData struct {
	Median  float64
	P95     float64
	P99     float64
	Average float64
	StdDev  float64
	Max     float64
	Min     float64
	Count   uint64
	Sum     uint64
}

// Statistics collects and reports database metrics.
type Statistics interface {
	// GetTickerCount returns the current value of a ticker.
	GetTickerCount(tickerType TickerType) uint64

	// RecordTick increments a ticker by count.
	RecordTick(tickerType TickerType, count uint64)

	// SetTickerCount sets the ticker to a specific value.
	SetTickerCount(tickerType TickerType, count uint64)

	// GetHistogramData returns histogram statistics.
	GetHistogramData(histogramType HistogramType) HistogramData

	// MeasureTime records a value to a histogram.
	MeasureTime(histogramType HistogramType, value uint64)

	// Reset clears all statistics.
	Reset()

	// String returns a formatted string of all statistics.
	String() string
}

// statisticsImpl is the default implementation of Statistics.
type statisticsImpl struct {
	tickers    [TickerEnumMax]uint64
	histograms [HistogramEnumMax]*histogramImpl
}

// histogramImpl is a simple histogram implementation.
type histogramImpl struct {
	min   uint64
	max   uint64
	sum   uint64
	count uint64
}

// NewStatistics creates a new Statistics instance.
func NewStatistics() Statistics {
	s := &statisticsImpl{}
	for i := range s.histograms {
		s.histograms[i] = &histogramImpl{min: ^uint64(0)}
	}
	return s
}

// GetTickerCount returns the current value of a ticker.
func (s *statisticsImpl) GetTickerCount(tickerType TickerType) uint64 {
	if tickerType < 0 || tickerType >= TickerEnumMax {
		return 0
	}
	return atomic.LoadUint64(&s.tickers[tickerType])
}

// RecordTick increments a ticker by count.
func (s *statisticsImpl) RecordTick(tickerType TickerType, count uint64) {
	if tickerType < 0 || tickerType >= TickerEnumMax {
		return
	}
	atomic.AddUint64(&s.tickers[tickerType], count)
}

// SetTickerCount sets the ticker to a specific value.
func (s *statisticsImpl) SetTickerCount(tickerType TickerType, count uint64) {
	if tickerType < 0 || tickerType >= TickerEnumMax {
		return
	}
	atomic.StoreUint64(&s.tickers[tickerType], count)
}

// GetHistogramData returns histogram statistics.
func (s *statisticsImpl) GetHistogramData(histogramType HistogramType) HistogramData {
	if histogramType < 0 || histogramType >= HistogramEnumMax {
		return HistogramData{}
	}

	h := s.histograms[histogramType]
	count := atomic.LoadUint64(&h.count)
	if count == 0 {
		return HistogramData{}
	}

	sum := atomic.LoadUint64(&h.sum)
	min := atomic.LoadUint64(&h.min)
	max := atomic.LoadUint64(&h.max)

	return HistogramData{
		Count:   count,
		Sum:     sum,
		Min:     float64(min),
		Max:     float64(max),
		Average: float64(sum) / float64(count),
		// Note: Median, P95, P99, StdDev would require storing all values
		// or using more sophisticated streaming algorithms
	}
}

// MeasureTime records a value to a histogram.
func (s *statisticsImpl) MeasureTime(histogramType HistogramType, value uint64) {
	if histogramType < 0 || histogramType >= HistogramEnumMax {
		return
	}

	h := s.histograms[histogramType]
	atomic.AddUint64(&h.count, 1)
	atomic.AddUint64(&h.sum, value)

	// Update min atomically
	for {
		old := atomic.LoadUint64(&h.min)
		if value >= old {
			break
		}
		if atomic.CompareAndSwapUint64(&h.min, old, value) {
			break
		}
	}

	// Update max atomically
	for {
		old := atomic.LoadUint64(&h.max)
		if value <= old {
			break
		}
		if atomic.CompareAndSwapUint64(&h.max, old, value) {
			break
		}
	}
}

// Reset clears all statistics.
func (s *statisticsImpl) Reset() {
	for i := range s.tickers {
		atomic.StoreUint64(&s.tickers[i], 0)
	}
	for i := range s.histograms {
		s.histograms[i] = &histogramImpl{min: ^uint64(0)}
	}
}

// String returns a formatted string of all statistics.
func (s *statisticsImpl) String() string {
	var result string

	// Tickers
	result = "TICKERS:\n"
	for i := range TickerEnumMax {
		count := s.GetTickerCount(i)
		if count > 0 {
			result += "  " + i.String() + " : " + formatCount(count) + "\n"
		}
	}

	// Histograms
	result += "\nHISTOGRAMS:\n"
	for i := range HistogramEnumMax {
		data := s.GetHistogramData(i)
		if data.Count > 0 {
			result += "  " + i.String() + " :\n"
			result += "    Count: " + formatCount(data.Count) + "\n"
			result += "    Avg: " + formatFloat(data.Average) + "\n"
			result += "    Min: " + formatFloat(data.Min) + "\n"
			result += "    Max: " + formatFloat(data.Max) + "\n"
		}
	}

	return result
}

func formatCount(n uint64) string {
	if n < 1000 {
		return string(rune('0'+n%10)) + string(rune('0'+n/10%10)) + string(rune('0'+n/100%10))
	}
	// Simple formatting for larger numbers
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}

func formatFloat(f float64) string {
	// Simple float formatting
	intPart := int64(f)
	fracPart := int64((f - float64(intPart)) * 100)
	if fracPart < 0 {
		fracPart = -fracPart
	}
	return formatInt(intPart) + "." + formatInt(fracPart)
}

func formatInt(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}
