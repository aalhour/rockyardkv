package rockyardkv

// options.go implements database configuration options.

import (
	"time"

	"github.com/aalhour/rockyardkv/internal/checksum"
	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/logging"
	"github.com/aalhour/rockyardkv/vfs"
)

// Logger is an alias for the logging.Logger interface.
// This allows users to pass their own logger implementation.
type Logger = logging.Logger

// CompressionType is an alias for the compression type.
type CompressionType = compression.Type

// Compression type constants.
const (
	CompressionNone   = compression.NoCompression
	CompressionSnappy = compression.SnappyCompression
	CompressionZstd   = compression.ZstdCompression
	CompressionLZ4    = compression.LZ4Compression
)

// Compression type constants
const (
	NoCompression     = compression.NoCompression
	SnappyCompression = compression.SnappyCompression
	ZlibCompression   = compression.ZlibCompression
	LZ4Compression    = compression.LZ4Compression
	LZ4HCCompression  = compression.LZ4HCCompression
	ZstdCompression   = compression.ZstdCompression
)

// ChecksumType is an alias for the checksum type.
type ChecksumType = checksum.Type

// Checksum type constants
const (
	ChecksumTypeNoChecksum = checksum.TypeNoChecksum
	ChecksumTypeCRC32C     = checksum.TypeCRC32C
	ChecksumTypeXXHash     = checksum.TypeXXHash
	ChecksumTypeXXHash64   = checksum.TypeXXHash64
	ChecksumTypeXXH3       = checksum.TypeXXH3
)

// CompactionStyle specifies the compaction strategy.
type CompactionStyle int

const (
	// CompactionStyleLevel is the default leveled compaction.
	// Files are organized into levels with each level having a size limit.
	// Optimized for read-heavy workloads.
	CompactionStyleLevel CompactionStyle = iota

	// CompactionStyleUniversal (size-tiered) is optimized for write-heavy workloads.
	// All files are kept in L0 and compacted together when size ratio is exceeded.
	// Lower write amplification but higher space amplification.
	CompactionStyleUniversal

	// CompactionStyleFIFO simply deletes the oldest files when the total size
	// exceeds the limit. Optimized for time-series data with no reads of old data.
	CompactionStyleFIFO
)

// String returns the string representation of the compaction style.
func (cs CompactionStyle) String() string {
	switch cs {
	case CompactionStyleLevel:
		return "Level"
	case CompactionStyleUniversal:
		return "Universal"
	case CompactionStyleFIFO:
		return "FIFO"
	default:
		return "Unknown"
	}
}

// UniversalCompactionOptions contains options for universal compaction.
type UniversalCompactionOptions struct {
	// SizeRatio is the percentage trigger for size ratio compaction.
	// Default: 1
	SizeRatio int

	// MinMergeWidth is the minimum number of files to merge.
	// Default: 2
	MinMergeWidth int

	// MaxMergeWidth is the maximum number of files to merge.
	// Default: unlimited
	MaxMergeWidth int

	// MaxSizeAmplificationPercent triggers full compaction when exceeded.
	// Default: 200
	MaxSizeAmplificationPercent int

	// AllowTrivialMove allows trivial moves when possible.
	// Default: false
	AllowTrivialMove bool
}

// DefaultUniversalCompactionOptions returns default options.
func DefaultUniversalCompactionOptions() *UniversalCompactionOptions {
	return &UniversalCompactionOptions{
		SizeRatio:                   1,
		MinMergeWidth:               2,
		MaxMergeWidth:               1<<31 - 1,
		MaxSizeAmplificationPercent: 200,
		AllowTrivialMove:            false,
	}
}

// FIFOCompactionOptions contains options for FIFO compaction.
type FIFOCompactionOptions struct {
	// MaxTableFilesSize is the maximum total size before deletion.
	// Default: 1GB
	MaxTableFilesSize uint64

	// TTL is the time-to-live for files before deletion.
	// Default: 0 (disabled)
	TTL time.Duration

	// AllowCompaction allows intra-L0 compaction.
	// Default: false
	AllowCompaction bool
}

// DefaultFIFOCompactionOptions returns default options.
func DefaultFIFOCompactionOptions() *FIFOCompactionOptions {
	return &FIFOCompactionOptions{
		MaxTableFilesSize: 1 << 30, // 1GB
		TTL:               0,
		AllowCompaction:   false,
	}
}

// Options contains all configuration options for opening a database.
type Options struct {
	// CreateIfMissing causes Open to create the database if it does not exist.
	CreateIfMissing bool

	// ErrorIfExists causes Open to return an error if the database already exists.
	ErrorIfExists bool

	// ParanoidChecks enables additional checks for data integrity.
	ParanoidChecks bool

	// FS is the filesystem implementation to use.
	// If nil, the OS filesystem is used.
	FS vfs.FS

	// Comparator defines the order of keys in the database.
	// If nil, a default bytewise comparator is used.
	Comparator Comparator

	// WriteBufferSize is the size of a single memtable.
	// Default: 64MB
	WriteBufferSize int

	// MaxWriteBufferNumber is the maximum number of memtables to keep in memory.
	// Default: 2
	MaxWriteBufferNumber int

	// MaxOpenFiles is the maximum number of SST files to keep open.
	// Default: 1000
	MaxOpenFiles int

	// BlockSize is the approximate size of data blocks within SST files.
	// Default: 4KB
	BlockSize int

	// BlockRestartInterval is how often to create restart points in blocks.
	// Default: 16
	BlockRestartInterval int

	// ChecksumType specifies the checksum algorithm for SST files.
	// Default: CRC32C
	ChecksumType ChecksumType

	// FormatVersion is the SST file format version.
	// Default: 3
	FormatVersion uint32

	// MergeOperator specifies the merge operator for merge operations.
	// If nil, Merge operations will return an error.
	MergeOperator MergeOperator

	// PrefixExtractor extracts prefixes from keys for prefix-based operations.
	// When set, bloom filters are built for prefixes instead of whole keys,
	// and prefix seek can be used for efficient iteration within a prefix.
	// If nil, no prefix optimization is used.
	PrefixExtractor PrefixExtractor

	// Level0FileNumCompactionTrigger is the number of files in level-0 that
	// triggers compaction to level-1.
	// Default: 4
	Level0FileNumCompactionTrigger int

	// MaxBytesForLevelBase is the maximum total data size for level-1.
	// Default: 256MB
	MaxBytesForLevelBase int64

	// BloomFilterBitsPerKey is the number of bits per key for bloom filters.
	// 0 disables bloom filters. Default: 10
	BloomFilterBitsPerKey int

	// Level0SlowdownWritesTrigger is the number of L0 files that triggers
	// write slowdown. When L0 file count exceeds this, writes are delayed.
	// Default: 20
	Level0SlowdownWritesTrigger int

	// Level0StopWritesTrigger is the number of L0 files that stops writes.
	// When L0 file count exceeds this, all writes are blocked until
	// compaction reduces the count.
	// Default: 36
	Level0StopWritesTrigger int

	// DisableAutoCompactions disables background compaction.
	// When true, no write stalling occurs based on L0 file count.
	// Default: false
	DisableAutoCompactions bool

	// CompactionFilter is called for each key-value pair during compaction.
	// It can filter out or modify entries during compaction.
	// If nil, no filtering is applied.
	CompactionFilter CompactionFilter

	// CompactionFilterFactory creates a new CompactionFilter for each compaction.
	// This takes precedence over CompactionFilter if both are set.
	// If nil, CompactionFilter is used directly.
	CompactionFilterFactory CompactionFilterFactory

	// CompactionStyle specifies the compaction strategy.
	// Default: CompactionStyleLevel
	CompactionStyle CompactionStyle

	// UniversalCompactionOptions contains options for universal compaction.
	// Only used when CompactionStyle is CompactionStyleUniversal.
	UniversalCompactionOptions *UniversalCompactionOptions

	// FIFOCompactionOptions contains options for FIFO compaction.
	// Only used when CompactionStyle is CompactionStyleFIFO.
	FIFOCompactionOptions *FIFOCompactionOptions

	// RateLimiter controls the rate of I/O operations.
	// If nil, no rate limiting is applied.
	RateLimiter RateLimiter

	// Compression specifies the compression algorithm for SST blocks.
	// Default: NoCompression
	Compression CompressionType

	// MaxSubcompactions is the maximum number of subcompactions per compaction job.
	// Subcompactions allow parallel compaction within a single job by dividing
	// the key range. Higher values can improve compaction throughput on multi-core
	// systems but increase memory usage.
	// Default: 1 (no parallel subcompaction)
	MaxSubcompactions int

	// UseDirectReads enables O_DIRECT for reading data.
	// This bypasses the OS page cache and reads directly from disk.
	// Beneficial for reducing memory pressure and cache pollution.
	// Requires aligned buffers and may not be supported on all platforms.
	// Reference: RocksDB v10.7.5 include/rocksdb/options.h line 1022-1024
	// Default: false
	UseDirectReads bool

	// UseDirectIOForFlushAndCompaction enables O_DIRECT for background
	// flush and compaction writes. This bypasses the OS page cache.
	// Reference: RocksDB v10.7.5 include/rocksdb/options.h line 1026-1028
	// Default: false
	UseDirectIOForFlushAndCompaction bool

	// Logger is the logger for database operations.
	// If nil, a default logger writing to stderr is used.
	Logger Logger
}

// DefaultOptions returns a new Options with default values.
func DefaultOptions() *Options {
	return &Options{
		CreateIfMissing:                  false,
		ErrorIfExists:                    false,
		ParanoidChecks:                   false,
		FS:                               nil,              // Will use vfs.Default()
		Comparator:                       nil,              // Will use BytewiseComparator
		WriteBufferSize:                  64 * 1024 * 1024, // 64MB
		MaxWriteBufferNumber:             2,
		MaxOpenFiles:                     1000,
		BlockSize:                        4096,
		BlockRestartInterval:             16,
		ChecksumType:                     ChecksumTypeCRC32C,
		FormatVersion:                    3,
		Level0FileNumCompactionTrigger:   4,
		MaxBytesForLevelBase:             256 * 1024 * 1024, // 256MB
		BloomFilterBitsPerKey:            10,
		Level0SlowdownWritesTrigger:      20,
		Level0StopWritesTrigger:          36,
		DisableAutoCompactions:           false,
		CompactionStyle:                  CompactionStyleLevel,
		MaxSubcompactions:                1,     // Default: no parallel subcompaction
		UseDirectReads:                   false, // Direct I/O disabled by default
		UseDirectIOForFlushAndCompaction: false,
		Logger:                           nil, // Will use defaultLogger
	}
}

// ReadOptions contains options for read operations.
type ReadOptions struct {
	// VerifyChecksums enables checksum verification when reading.
	VerifyChecksums bool

	// FillCache indicates whether to fill the block cache on reads.
	FillCache bool

	// Snapshot provides a consistent view of the database.
	// If nil, the most recent state is used.
	Snapshot *Snapshot

	// Timestamp specifies the timestamp for reading.
	// Read will return the latest data visible to the specified timestamp.
	// All timestamps of the same database must be of the same length.
	// For iterators, IterStartTimestamp is the lower bound (older) and
	// Timestamp serves as the upper bound.
	// If nil, timestamps are not used.
	//
	// Reference: RocksDB v10.7.5 include/rocksdb/options.h (ReadOptions::timestamp)
	Timestamp []byte

	// IterStartTimestamp is the lower bound (older) timestamp for iterators.
	// Versions of the same record that fall in the timestamp range
	// [IterStartTimestamp, Timestamp] will be returned.
	// If nil, only the most recent version visible to Timestamp is returned.
	//
	// Reference: RocksDB v10.7.5 include/rocksdb/options.h (ReadOptions::iter_start_ts)
	IterStartTimestamp []byte

	// TotalOrderSeek enables total order seek.
	// When true, prefix bloom filters are bypassed and all keys are considered.
	// When false (default), prefix seek optimization is used if a prefix extractor
	// is configured.
	TotalOrderSeek bool

	// PrefixSameAsStart optimizes iteration when the user knows the iteration
	// will stay within the same prefix.
	// When true, the iterator may skip to the next data block if it determines
	// all keys in the current block have a different prefix.
	PrefixSameAsStart bool

	// IterateUpperBound sets an upper bound for iteration.
	// The iterator will stop before any key >= this bound.
	// This can be used with prefix seek to efficiently limit iteration.
	IterateUpperBound []byte

	// IterateLowerBound sets a lower bound for iteration.
	// The iterator will skip any key < this bound.
	IterateLowerBound []byte
}

// DefaultReadOptions returns ReadOptions with default values.
func DefaultReadOptions() *ReadOptions {
	return &ReadOptions{
		VerifyChecksums: true,
		FillCache:       true,
		Snapshot:        nil,
	}
}

// WriteOptions contains options for write operations.
type WriteOptions struct {
	// Sync causes writes to be flushed to the WAL and fsynced before returning.
	// This provides the strongest durability guarantee but reduces throughput.
	Sync bool

	// DisableWAL disables the write-ahead log for this write.
	//
	// WARNING: With DisableWAL=true, writes go directly to the memtable.
	// If the process crashes before Flush() is called, data will be lost.
	// This matches C++ RocksDB behavior exactly.
	//
	// Use only when you can tolerate data loss in exchange for higher throughput.
	// Call Flush() explicitly before shutdown to persist unflushed data.
	DisableWAL bool
}

// DefaultWriteOptions returns WriteOptions with default values.
func DefaultWriteOptions() *WriteOptions {
	return &WriteOptions{
		Sync:       false,
		DisableWAL: false,
	}
}

// FlushOptions contains options for flush operations.
type FlushOptions struct {
	// Wait indicates whether to wait for the flush to complete.
	Wait bool

	// AllowWriteStall indicates whether to allow write stalls.
	AllowWriteStall bool
}

// DefaultFlushOptions returns FlushOptions with default values.
func DefaultFlushOptions() *FlushOptions {
	return &FlushOptions{
		Wait:            true,
		AllowWriteStall: false,
	}
}
