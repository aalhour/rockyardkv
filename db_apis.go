package rockyardkv

// db_apis.go implements extended DB APIs.
//
// Reference: RocksDB v10.7.5:
//   - include/rocksdb/db.h (DB interface)
//   - db/db_impl/db_impl.cc (implementation)

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/memtable"
	"github.com/aalhour/rockyardkv/internal/version"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// Range represents a key range for size approximation.
// Reference: RocksDB v10.7.5 include/rocksdb/db.h
type Range struct {
	Start []byte
	Limit []byte
}

// SizeApproximationFlags controls what is included in size estimates.
// Reference: RocksDB v10.7.5 include/rocksdb/db.h
type SizeApproximationFlags uint8

const (
	// SizeApproximationNone includes nothing.
	SizeApproximationNone SizeApproximationFlags = 0
	// SizeApproximationIncludeMemtables includes memtable sizes.
	SizeApproximationIncludeMemtables SizeApproximationFlags = 1 << 0
	// SizeApproximationIncludeFiles includes SST file sizes.
	SizeApproximationIncludeFiles SizeApproximationFlags = 1 << 1
)

// SizeApproximationOptions controls size approximation behavior.
// Reference: RocksDB v10.7.5 include/rocksdb/db.h
type SizeApproximationOptions struct {
	IncludeMemtables bool
	IncludeFiles     bool
}

// WaitForCompactOptions controls WaitForCompact behavior.
// Reference: RocksDB v10.7.5 include/rocksdb/options.h
type WaitForCompactOptions struct {
	// AbortOnPause makes WaitForCompact abort if compaction is paused.
	AbortOnPause bool
	// FlushFirst flushes memtable before waiting for compaction.
	FlushFirst bool
	// CloseDB closes the database after waiting (for graceful shutdown).
	CloseDB bool
	// Timeout is the maximum time to wait. Zero means wait forever.
	Timeout time.Duration
}

// KeyMayExist checks if a key may exist using bloom filters.
// Returns true if the key may exist, false if it definitely doesn't exist.
// If valueFound is not nil, it indicates whether the value was found in cache.
// This is an optimization hint - true doesn't guarantee existence.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1022-1050
//   - db/db_impl/db_impl.cc dbImpl::KeyMayExist
func (db *dbImpl) KeyMayExist(opts *ReadOptions, key []byte, value *[]byte) (mayExist bool, valueFound bool) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return true, false // Conservative: may exist
	}
	db.mu.RUnlock()

	// Check bloom filter in memtable
	db.mu.RLock()
	mem := db.mem
	imm := db.imm
	v := db.versions.Current()
	if v != nil {
		v.Ref()
	}
	db.mu.RUnlock()

	// Check memtable
	if mem != nil {
		val, found, deleted := mem.Get(key, dbformat.MaxSequenceNumber)
		if found && !deleted {
			if value != nil {
				*value = val
			}
			return true, true
		}
		if deleted {
			return false, false // Key is deleted
		}
	}

	// Check immutable memtable
	if imm != nil {
		val, found, deleted := imm.Get(key, dbformat.MaxSequenceNumber)
		if found && !deleted {
			if value != nil {
				*value = val
			}
			return true, true
		}
		if deleted {
			return false, false
		}
	}

	// Check SST files using bloom filters
	if v != nil {
		defer v.Unref()

		// Use bloom filter to check if key may exist in any level
		for level := range v.NumLevels() {
			files := v.Files(level)
			for _, f := range files {
				// Check if key is in range
				if db.comparator.Compare(key, f.Smallest) < 0 ||
					db.comparator.Compare(key, f.Largest) > 0 {
					continue
				}

				// If we have a bloom filter, check it
				// For now, we conservatively say key may exist if it's in range
				return true, false
			}
		}
	}

	// Key not in any range - definitely doesn't exist
	return false, false
}

// KeyMayExistCF checks if a key may exist in the specified column family.
func (db *dbImpl) KeyMayExistCF(opts *ReadOptions, cf ColumnFamilyHandle, key []byte, value *[]byte) (mayExist bool, valueFound bool) {
	// For now, delegate to default implementation
	return db.KeyMayExist(opts, key, value)
}

// WaitForCompact waits for all compactions to complete.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1705-1708
//   - db/db_impl/db_impl_compaction_flush.cc
func (db *dbImpl) WaitForCompact(opts *WaitForCompactOptions) error {
	if opts == nil {
		opts = &WaitForCompactOptions{}
	}

	// Flush first if requested
	if opts.FlushFirst {
		if err := db.Flush(nil); err != nil && !errors.Is(err, ErrDBClosed) {
			// Ignore "immutable memtable already exists" error
			if !strings.Contains(err.Error(), "immutable memtable already exists") {
				return err
			}
		}
	}

	// Wait for background compaction to complete
	start := time.Now()
	for {
		db.mu.RLock()
		closed := db.closed
		db.mu.RUnlock()

		if closed {
			return ErrDBClosed
		}

		// Check background work state
		var isRunning, isPaused bool
		if db.bgWork != nil {
			db.bgWork.mu.Lock()
			isRunning = db.bgWork.compactionRunning || db.bgWork.flushRunning
			isPaused = db.bgWork.paused
			db.bgWork.mu.Unlock()
		}

		if !isRunning {
			break
		}

		if opts.AbortOnPause && isPaused {
			return errors.New("db: compaction is paused")
		}

		if opts.Timeout > 0 && time.Since(start) > opts.Timeout {
			return errors.New("db: timeout waiting for compaction")
		}

		time.Sleep(10 * time.Millisecond)
	}

	if opts.CloseDB {
		return db.Close()
	}

	return nil
}

// GetApproximateSizes returns the approximate sizes of key ranges.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1533-1565
//   - db/db_impl/db_impl.cc dbImpl::GetApproximateSizes
func (db *dbImpl) GetApproximateSizes(ranges []Range, flags SizeApproximationFlags) ([]uint64, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrDBClosed
	}
	db.mu.RUnlock()

	includeMemtables := (flags & SizeApproximationIncludeMemtables) != 0
	includeFiles := (flags & SizeApproximationIncludeFiles) != 0

	// Default to including files if nothing specified
	if !includeMemtables && !includeFiles {
		includeFiles = true
	}

	sizes := make([]uint64, len(ranges))

	db.mu.RLock()
	v := db.versions.Current()
	if v != nil {
		v.Ref()
	}
	mem := db.mem
	imm := db.imm
	db.mu.RUnlock()

	if v != nil {
		defer v.Unref()
	}

	for i, r := range ranges {
		var size uint64

		// Estimate memtable size
		if includeMemtables {
			size += estimateMemtableRangeSizeFromMem(mem, r.Start, r.Limit)
			size += estimateMemtableRangeSizeFromMem(imm, r.Start, r.Limit)
		}

		// Estimate SST file sizes
		if includeFiles && v != nil {
			for level := range v.NumLevels() {
				files := v.Files(level)
				for _, f := range files {
					if rangesOverlap(r.Start, r.Limit, f.Smallest, f.Largest, db.comparator) {
						// Estimate portion of file in range
						size += f.FD.FileSize
					}
				}
			}
		}

		sizes[i] = size
	}

	return sizes, nil
}

// GetApproximateMemTableStats returns approximate memtable statistics for a range.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1556-1564
func (db *dbImpl) GetApproximateMemTableStats(r Range) (count, size uint64) {
	db.mu.RLock()
	mem := db.mem
	imm := db.imm
	db.mu.RUnlock()

	if mem != nil {
		count += uint64(mem.Count())
		size += estimateMemtableRangeSizeFromMem(mem, r.Start, r.Limit)
	}
	if imm != nil {
		count += uint64(imm.Count())
		size += estimateMemtableRangeSizeFromMem(imm, r.Start, r.Limit)
	}

	return count, size
}

// NumberLevels returns the number of levels in the LSM tree.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1710-1712
func (db *dbImpl) NumberLevels() int {
	return version.MaxNumLevels
}

// Level0StopWriteTrigger returns the number of L0 files that triggers write stop.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1725-1727
func (db *dbImpl) Level0StopWriteTrigger() int {
	return db.options.Level0StopWritesTrigger
}

// GetName returns the name/path of the database.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h line 1733
func (db *dbImpl) GetName() string {
	return db.name
}

// GetEnv returns the Env/VFS used by the database.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h line 1735
func (db *dbImpl) GetEnv() vfs.FS {
	return db.fs
}

// GetOptions returns a copy of the current database options.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1741-1748
func (db *dbImpl) GetOptions() Options {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return *db.options
}

// GetDBOptions returns a copy of the current database-wide options.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h line 1750
func (db *dbImpl) GetDBOptions() Options {
	return db.GetOptions()
}

// SetOptions dynamically changes database options.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1807-1809
func (db *dbImpl) SetOptions(newOptions map[string]string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for k, v := range newOptions {
		switch k {
		case "write_buffer_size":
			size, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid write_buffer_size: %w", err)
			}
			db.options.WriteBufferSize = int(size)
		case "max_write_buffer_number":
			num, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("invalid max_write_buffer_number: %w", err)
			}
			db.options.MaxWriteBufferNumber = num
		case "disable_auto_compactions":
			disabled := v == "true" || v == "1"
			db.options.DisableAutoCompactions = disabled
		default:
			// Unknown option - ignore for flexibility
		}
	}

	return nil
}

// SetDBOptions dynamically changes database-wide options.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1810-1812
func (db *dbImpl) SetDBOptions(newOptions map[string]string) error {
	return db.SetOptions(newOptions)
}

// GetIntProperty returns an integer property value.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1366-1368
func (db *dbImpl) GetIntProperty(name string) (uint64, bool) {
	strVal, ok := db.GetProperty(name)
	if !ok {
		return 0, false
	}
	val, err := strconv.ParseUint(strVal, 10, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

// GetMapProperty returns a map property value.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1370-1372
func (db *dbImpl) GetMapProperty(name string) (map[string]string, bool) {
	// For now, return basic properties as a map
	result := make(map[string]string)

	switch name {
	case "rocksdb.cfstats":
		result["num-immutable-mem-table"] = "0"
		result["num-entries-active-mem-table"] = fmt.Sprintf("%d", db.mem.Count())
		return result, true
	case "rocksdb.dbstats":
		result["uptime"] = "0"
		result["cumulative.writes"] = "0"
		return result, true
	default:
		return nil, false
	}
}

// NewIterators creates iterators for multiple column families.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1066-1069
func (db *dbImpl) NewIterators(opts *ReadOptions, cfs []ColumnFamilyHandle) ([]Iterator, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrDBClosed
	}
	db.mu.RUnlock()

	iters := make([]Iterator, len(cfs))
	for i, cf := range cfs {
		iters[i] = db.NewIteratorCF(opts, cf)
	}
	return iters, nil
}

// Resume resumes the database after an error.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 476-482
func (db *dbImpl) Resume() error {
	// No-op for now - database auto-resumes
	return nil
}

// walLockState tracks WAL lock state
var walLockMu sync.Mutex

// LockWAL locks the WAL, preventing new writes.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1791-1800
func (db *dbImpl) LockWAL() error {
	walLockMu.Lock()
	return nil
}

// UnlockWAL unlocks the WAL.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1801-1806
func (db *dbImpl) UnlockWAL() error {
	walLockMu.Unlock()
	return nil
}

// ResetStats resets database statistics.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h
func (db *dbImpl) ResetStats() error {
	// Statistics are reset internally if a stats object exists
	// This is a no-op if no statistics are configured
	return nil
}

// CompactFiles compacts specific files.
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h lines 1633-1653
func (db *dbImpl) CompactFiles(opts *CompactionOptions, inputFileNames []string, outputLevel int) error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrDBClosed
	}
	db.mu.RUnlock()

	// For now, trigger a regular compaction
	// Full implementation would select specific files
	return db.CompactRange(nil, nil, nil)
}

// CompactionOptions for CompactFiles.
// Reference: RocksDB v10.7.5 include/rocksdb/options.h
type CompactionOptions struct {
	OutputLevel           int
	TargetLevel           int
	MaxSubcompactions     uint32
	OutputFilePathID      uint32
	CompressionType       CompressionType
	OutputFileSizeLimit   uint64
	MaxCompactionBytes    uint64
	PenultimateOutputPath bool
}

// Helper to check if ranges overlap
func rangesOverlap(start1, limit1, start2, limit2 []byte, cmp Comparator) bool {
	// Check if [start1, limit1) overlaps with [start2, limit2)
	if limit1 != nil && cmp.Compare(limit1, start2) <= 0 {
		return false
	}
	if start1 != nil && limit2 != nil && cmp.Compare(start1, limit2) >= 0 {
		return false
	}
	return true
}

// Helper to estimate memtable size for a range using concrete type
func estimateMemtableRangeSizeFromMem(mem *memtable.MemTable, start, limit []byte) uint64 {
	if mem == nil {
		return 0
	}
	// Simple estimate based on total size and key range
	// A more accurate implementation would iterate the memtable
	totalSize := max(mem.ApproximateMemoryUsage(), 0)

	// If range covers everything, return total
	if start == nil && limit == nil {
		return uint64(totalSize)
	}

	// Rough estimate: assume uniform distribution
	// Return a portion based on the range
	return uint64(totalSize) / 2
}
