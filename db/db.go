// Package db provides the main database interface and implementation.
//
// Reference: RocksDB v10.7.5 include/rocksdb/db.h
//
// # Whitebox Testing Hooks
//
// This file contains sync points (requires -tags synctest) and kill points
// (requires -tags crashtest) for whitebox testing. In production builds,
// these compile to no-ops with zero overhead. See docs/testing.md for usage.
package db

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/compaction"
	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/memtable"
	"github.com/aalhour/rockyardkv/internal/rangedel"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/testutil"
	"github.com/aalhour/rockyardkv/internal/version"
	"github.com/aalhour/rockyardkv/internal/vfs"
	"github.com/aalhour/rockyardkv/internal/wal"
)

// Common errors returned by DB operations.
var (
	ErrDBClosed            = errors.New("db: database is closed")
	ErrNotFound            = errors.New("db: key not found")
	ErrMergeOperatorNotSet = errors.New("db: merge operator not set in options")
	ErrDBExists            = errors.New("db: database already exists")
	ErrDBNotFound          = errors.New("db: database not found")
	ErrCorruption          = errors.New("db: corruption detected")
	ErrInvalidOptions      = errors.New("db: invalid options")
	ErrBackgroundError     = errors.New("db: unrecoverable background error")
)

// DB is the main interface for interacting with the database.
type DB interface {
	// Put sets the value for the given key in the default column family.
	Put(opts *WriteOptions, key, value []byte) error

	// PutCF sets the value for the given key in the specified column family.
	PutCF(opts *WriteOptions, cf ColumnFamilyHandle, key, value []byte) error

	// Get retrieves the value for the given key from the default column family.
	// Returns ErrNotFound if the key does not exist.
	Get(opts *ReadOptions, key []byte) ([]byte, error)

	// GetCF retrieves the value for the given key from the specified column family.
	GetCF(opts *ReadOptions, cf ColumnFamilyHandle, key []byte) ([]byte, error)

	// MultiGet retrieves multiple values for the given keys.
	// Returns a slice of values in the same order as keys.
	// If a key doesn't exist, the corresponding value is nil and error is ErrNotFound.
	MultiGet(opts *ReadOptions, keys [][]byte) ([][]byte, []error)

	// Delete removes the given key from the default column family.
	Delete(opts *WriteOptions, key []byte) error

	// SingleDelete removes the given key from the default column family.
	// Unlike Delete, SingleDelete is only valid for keys that have been Put exactly once
	// without any Merge operations. If there are multiple Put operations for a key,
	// SingleDelete may not work correctly.
	SingleDelete(opts *WriteOptions, key []byte) error

	// DeleteCF removes the given key from the specified column family.
	DeleteCF(opts *WriteOptions, cf ColumnFamilyHandle, key []byte) error

	// DeleteRange removes all keys in the range [startKey, endKey) from the default column family.
	DeleteRange(opts *WriteOptions, startKey, endKey []byte) error

	// DeleteRangeCF removes all keys in the range [startKey, endKey) from the specified column family.
	DeleteRangeCF(opts *WriteOptions, cf ColumnFamilyHandle, startKey, endKey []byte) error

	// Merge applies a merge operation for the given key in the default column family.
	// The merge operator specified in Options will be used to combine the operand
	// with any existing value during reads and compaction.
	Merge(opts *WriteOptions, key, value []byte) error

	// MergeCF applies a merge operation for the given key in the specified column family.
	MergeCF(opts *WriteOptions, cf ColumnFamilyHandle, key, value []byte) error

	// Write applies a batch of operations atomically.
	Write(opts *WriteOptions, batch *batch.WriteBatch) error

	// NewIterator creates an iterator over the default column family.
	NewIterator(opts *ReadOptions) Iterator

	// NewIteratorCF creates an iterator over the specified column family.
	NewIteratorCF(opts *ReadOptions, cf ColumnFamilyHandle) Iterator

	// GetSnapshot creates a new snapshot of the database.
	GetSnapshot() *Snapshot

	// ReleaseSnapshot releases a previously acquired snapshot.
	ReleaseSnapshot(s *Snapshot)

	// Flush flushes the memtable to disk.
	Flush(opts *FlushOptions) error

	// Close closes the database, releasing all resources.
	Close() error

	// GetProperty returns the value of a database property.
	GetProperty(name string) (string, bool)

	// CreateColumnFamily creates a new column family.
	CreateColumnFamily(opts ColumnFamilyOptions, name string) (ColumnFamilyHandle, error)

	// DropColumnFamily drops the specified column family.
	DropColumnFamily(cf ColumnFamilyHandle) error

	// ListColumnFamilies returns the names of all column families.
	ListColumnFamilies() []string

	// DefaultColumnFamily returns a handle to the default column family.
	DefaultColumnFamily() ColumnFamilyHandle

	// GetColumnFamily returns a handle to the named column family, or nil if not found.
	GetColumnFamily(name string) ColumnFamilyHandle

	// CompactRange manually triggers compaction for the specified key range.
	// If start and end are nil, the entire database is compacted.
	CompactRange(opts *CompactRangeOptions, start, end []byte) error

	// BeginTransaction begins a new optimistic transaction.
	BeginTransaction(opts TransactionOptions, writeOpts *WriteOptions) Transaction

	// IngestExternalFile loads external SST files into the database.
	IngestExternalFile(paths []string, opts IngestExternalFileOptions) error

	// SyncWAL syncs the current WAL to disk, ensuring all data is durable.
	// This is more expensive than FlushWAL(false) but provides stronger durability.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1782-1789
	SyncWAL() error

	// FlushWAL flushes the WAL buffer to the file system.
	// If sync is true, it also syncs the WAL to disk (equivalent to SyncWAL).
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1775-1780
	FlushWAL(sync bool) error

	// GetLatestSequenceNumber returns the sequence number of the most recent transaction.
	// This is useful for tracking database state and replication.
	GetLatestSequenceNumber() uint64

	// GetLiveFiles returns a list of all files in the database except WAL files.
	// The files are relative to the dbname. The manifest file size is returned.
	// If flushMemtable is true, the memtable is flushed before getting files.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1929-1947
	GetLiveFiles(flushMemtable bool) (files []string, manifestFileSize uint64, err error)

	// GetLiveFilesMetaData returns metadata about all live SST files in the database.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1892-1897
	GetLiveFilesMetaData() []LiveFileMetaData

	// DisableFileDeletions prevents file deletions. Call EnableFileDeletions when done.
	// This is useful for making consistent backups.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h
	DisableFileDeletions() error

	// EnableFileDeletions re-enables file deletions after DisableFileDeletions.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h
	EnableFileDeletions() error

	// PauseBackgroundWork pauses all background work (compaction, flush).
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h
	PauseBackgroundWork() error

	// ContinueBackgroundWork resumes background work after PauseBackgroundWork.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h
	ContinueBackgroundWork() error

	// KeyMayExist checks if a key may exist using bloom filters.
	// Returns true if the key may exist, false if it definitely doesn't exist.
	// If value pointer is not nil, the value may be set if found in cache.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1022-1050
	KeyMayExist(opts *ReadOptions, key []byte, value *[]byte) (mayExist bool, valueFound bool)

	// NewIterators creates iterators for multiple column families.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1066-1069
	NewIterators(opts *ReadOptions, cfs []ColumnFamilyHandle) ([]Iterator, error)

	// GetApproximateSizes returns the approximate sizes of key ranges.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1533-1565
	GetApproximateSizes(ranges []Range, flags SizeApproximationFlags) ([]uint64, error)

	// GetOptions returns a copy of the current database options.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1741-1748
	GetOptions() Options

	// GetDBOptions returns a copy of the current database-wide options.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h line 1750
	GetDBOptions() Options

	// SetOptions dynamically changes database options.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1807-1809
	SetOptions(newOptions map[string]string) error

	// SetDBOptions dynamically changes database-wide options.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1810-1812
	SetDBOptions(newOptions map[string]string) error

	// GetIntProperty returns an integer property value.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1366-1368
	GetIntProperty(name string) (uint64, bool)

	// GetMapProperty returns a map property value.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1370-1372
	GetMapProperty(name string) (map[string]string, bool)

	// WaitForCompact waits for all compactions to complete.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1705-1708
	WaitForCompact(opts *WaitForCompactOptions) error

	// LockWAL locks the WAL, preventing new writes.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1791-1800
	LockWAL() error

	// UnlockWAL unlocks the WAL.
	// Reference: RocksDB v10.7.5 include/rocksdb/db.h lines 1801-1806
	UnlockWAL() error
}

// Open opens the database at the specified path.
func Open(path string, opts *Options) (DB, error) {
	// Whitebox [synctest]: barrier at DB open start
	_ = testutil.SP(testutil.SPDBOpen)

	if opts == nil {
		opts = DefaultOptions()
	}

	// Use default filesystem if not specified
	fs := opts.FS
	if fs == nil {
		fs = vfs.Default()
	}

	// Use default comparator if not specified
	comparator := opts.Comparator
	if comparator == nil {
		comparator = DefaultComparator()
	}

	// Check if database exists
	exists := fs.Exists(filepath.Join(path, "CURRENT"))

	if exists && opts.ErrorIfExists {
		return nil, ErrDBExists
	}

	if !exists && !opts.CreateIfMissing {
		return nil, ErrDBNotFound
	}

	// Create directory if needed
	if !exists {
		if err := fs.MkdirAll(path, 0755); err != nil {
			return nil, err
		}
	}

	// Use default logger if not specified
	logger := opts.Logger
	if logger == nil {
		logger = newDefaultLogger()
	}

	// Create the DB implementation
	db := &DBImpl{
		name:            path,
		options:         opts,
		fs:              fs,
		comparator:      comparator,
		cmp:             comparator,
		shutdownCh:      make(chan struct{}),
		tableCache:      table.NewTableCache(fs, table.DefaultTableCacheOptions()),
		writeController: NewWriteController(),
		logger:          logger,
	}
	// Initialize condition variable for immutable memtable waiting
	db.immCond = sync.NewCond(&db.mu)

	// Initialize column family set
	db.columnFamilies = newColumnFamilySet(db)

	// Initialize version set
	vsOpts := version.VersionSetOptions{
		DBName:              path,
		FS:                  fs,
		MaxManifestFileSize: 1024 * 1024 * 1024, // 1GB
		NumLevels:           version.MaxNumLevels,
	}
	db.versions = version.NewVersionSet(vsOpts)

	// Open or create the database
	if exists {
		// Recover from existing database
		if err := db.recover(); err != nil {
			return nil, err
		}
	} else {
		// Create new database
		if err := db.create(); err != nil {
			return nil, err
		}
	}

	// Start background workers
	db.bgWork = newBackgroundWork(db, opts)
	db.bgWork.Start()

	// Check if compaction is needed after recovery
	db.bgWork.MaybeScheduleCompaction()

	// Whitebox [synctest]: barrier at DB open complete
	_ = testutil.SP(testutil.SPDBOpenComplete)

	return db, nil
}

// DBImpl is the concrete implementation of the DB interface.
type DBImpl struct {
	// Database path
	name string

	// Configuration
	options    *Options
	fs         vfs.FS
	comparator Comparator
	cmp        Comparator // Alias for comparator

	// Mutex for protecting internal state
	mu sync.RWMutex

	// Version management
	versions *version.VersionSet

	// WAL (write-ahead log)
	logFile       vfs.WritableFile
	logFileNumber uint64
	logWriter     *wal.Writer

	// MemTable (for default column family - kept for backward compatibility)
	mem *memtable.MemTable
	imm *memtable.MemTable // Immutable memtable being flushed
	seq uint64             // Current sequence number

	// Column Families
	columnFamilies *columnFamilySet

	// Table cache for SST files
	tableCache *table.TableCache

	// Snapshots (linked list)
	snapshots    *Snapshot
	snapshotLock sync.Mutex

	// Background work (compaction, flush)
	bgWork *BackgroundWork

	// Write controller for stalling
	writeController *WriteController

	// Background error state
	// When a fatal I/O error occurs (e.g., EPERM, EROFS), this is set
	// to prevent further writes while still allowing reads.
	backgroundError error

	// Condition variable for waiting on immutable memtable flush
	immCond *sync.Cond

	// Logger for warnings and info
	logger Logger

	// Track if WAL-disabled warning has been logged (to avoid spam)
	walDisabledWarned bool

	// Shutdown
	closed     bool
	shutdownCh chan struct{}
}

// create initializes a new database.
func (db *DBImpl) create() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Create the version set
	if err := db.versions.Create(); err != nil {
		return err
	}

	// Create WAL
	logNumber := db.versions.NextFileNumber()
	logPath := db.logFilePath(logNumber)

	logFile, err := db.fs.Create(logPath)
	if err != nil {
		return err
	}

	db.logFile = logFile
	db.logFileNumber = logNumber
	db.logWriter = wal.NewWriter(logFile, logNumber, false /* not recyclable */)

	// Create memtable with the configured comparator
	var memCmp memtable.Comparator
	if db.comparator != nil {
		memCmp = db.comparator.Compare
	}
	db.mem = memtable.NewMemTable(memCmp)
	db.seq = 0

	// Log the WAL creation in MANIFEST
	edit := &manifest.VersionEdit{
		HasLogNumber: true,
		LogNumber:    logNumber,
	}
	if err := db.versions.LogAndApply(edit); err != nil {
		return err
	}

	return nil
}

// recover recovers the database from an existing state.
func (db *DBImpl) recover() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Recover version set (reads MANIFEST)
	if err := db.versions.Recover(); err != nil {
		return err
	}

	// Get the sequence number from the recovered state
	db.seq = db.versions.LastSequence()

	// Restore column families from MANIFEST
	recoveredCFs := db.versions.RecoveredColumnFamilies()
	maxCF := db.versions.MaxColumnFamily()
	for _, cf := range recoveredCFs {
		_, err := db.columnFamilies.CreateWithID(cf.ID, cf.Name, DefaultColumnFamilyOptions())
		if err != nil && !errors.Is(err, ErrColumnFamilyExists) {
			return fmt.Errorf("failed to restore column family %s: %w", cf.Name, err)
		}
	}
	// Update the next CF ID based on what was in the MANIFEST
	db.columnFamilies.SetNextID(maxCF + 1)

	// Replay WAL files to recover unflushed writes
	if err := db.replayWAL(); err != nil {
		return fmt.Errorf("WAL replay failed: %w", err)
	}

	// Create a new WAL for new writes
	logNumber := db.versions.NextFileNumber()
	logPath := db.logFilePath(logNumber)

	logFile, err := db.fs.Create(logPath)
	if err != nil {
		return err
	}

	db.logFile = logFile
	db.logFileNumber = logNumber
	db.logWriter = wal.NewWriter(logFile, logNumber, false /* not recyclable */)

	// Record NextFileNumber to prevent file number reuse, but do NOT update
	// LogNumber. The LogNumber determines which logs are replayed during
	// recovery - it should only be updated after a flush completes.
	// This ensures all unflushed data from older WALs is preserved.
	// Reference: RocksDB db/db_impl/db_impl_open.cc RecoverLogFiles
	edit := &manifest.VersionEdit{
		// Only update NextFileNumber, NOT LogNumber
		// LogNumber stays at the old value so older logs are replayed
	}
	if err := db.versions.LogAndApply(edit); err != nil {
		return err
	}

	return nil
}

// Put sets the value for the given key in the default column family.
func (db *DBImpl) Put(opts *WriteOptions, key, value []byte) error {
	return db.PutCF(opts, nil, key, value)
}

// PutCF sets the value for the given key in the specified column family.
func (db *DBImpl) PutCF(opts *WriteOptions, cf ColumnFamilyHandle, key, value []byte) error {
	cfd, err := db.getColumnFamilyData(cf)
	if err != nil {
		return err
	}

	wb := batch.New()
	if cfd.id == DefaultColumnFamilyID {
		wb.Put(key, value)
	} else {
		wb.PutCF(cfd.id, key, value)
	}
	return db.Write(opts, wb)
}

// Get retrieves the value for the given key from the default column family.
func (db *DBImpl) Get(opts *ReadOptions, key []byte) ([]byte, error) {
	return db.GetCF(opts, nil, key)
}

// GetCF retrieves the value for the given key from the specified column family.
func (db *DBImpl) GetCF(opts *ReadOptions, cf ColumnFamilyHandle, key []byte) ([]byte, error) {
	// Whitebox [synctest]: barrier at Get start
	_ = testutil.SP(testutil.SPDBGet)

	cfd, err := db.getColumnFamilyData(cf)
	if err != nil {
		return nil, err
	}

	if opts == nil {
		opts = DefaultReadOptions()
	}

	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrDBClosed
	}

	// Determine the snapshot sequence to use
	var snapshot uint64
	if opts.Snapshot != nil {
		snapshot = opts.Snapshot.Sequence()
	} else {
		snapshot = db.seq
	}

	// Check memtable first (use column family's memtable if available)
	var mem, imm *memtable.MemTable
	if cfd.id == DefaultColumnFamilyID {
		mem = db.mem
		imm = db.imm
	} else {
		cfd.memMu.RLock()
		mem = cfd.mem
		if len(cfd.imm) > 0 {
			imm = cfd.imm[0] // Check first immutable memtable
		}
		cfd.memMu.RUnlock()
	}
	db.mu.RUnlock()

	// Collect merge operands if we encounter them
	var mergeOperands [][]byte

	// Lookup in memtable (with merge support)
	if mem != nil {
		baseValue, memOperands, foundBase, deleted := mem.CollectMergeOperands(key, dbformat.SequenceNumber(snapshot))
		if deleted {
			// Key was deleted - if we have merge operands, apply them with nil base
			if len(memOperands) > 0 {
				return db.applyMerge(key, nil, memOperands)
			}
			return nil, ErrNotFound
		}
		if foundBase {
			// Found a value - if we have merge operands, apply them
			if len(memOperands) > 0 {
				return db.applyMerge(key, baseValue, memOperands)
			}
			// IMPORTANT: Copy the value to prevent aliasing with memtable internal data.
			// Users may modify the returned slice, and we must not corrupt internal state.
			// Reference: RocksDB uses PinnableSlice::PinSelf() which copies the data.
			return copySlice(baseValue), nil
		}
		// Collect any merge operands found
		mergeOperands = append(mergeOperands, memOperands...)
	}

	// Lookup in immutable memtable (with merge support)
	if imm != nil {
		baseValue, immOperands, foundBase, deleted := imm.CollectMergeOperands(key, dbformat.SequenceNumber(snapshot))
		if deleted {
			if len(mergeOperands) > 0 || len(immOperands) > 0 {
				allOperands := append(mergeOperands, immOperands...)
				return db.applyMerge(key, nil, allOperands)
			}
			return nil, ErrNotFound
		}
		if foundBase {
			allOperands := append(mergeOperands, immOperands...)
			if len(allOperands) > 0 {
				return db.applyMerge(key, baseValue, allOperands)
			}
			// IMPORTANT: Copy the value to prevent aliasing with memtable internal data.
			return copySlice(baseValue), nil
		}
		// Collect any merge operands found
		mergeOperands = append(mergeOperands, immOperands...)
	}

	// Lookup in SST files via VersionSet/TableCache
	db.mu.RLock()
	current := db.versions.Current()
	if current != nil {
		current.Ref() // Keep version alive while searching
	}
	db.mu.RUnlock()

	if current != nil {
		defer current.Unref()
		value, err := db.getFromVersionWithMerge(current, key, dbformat.SequenceNumber(snapshot), mergeOperands, cfd.id)
		if err == nil {
			return value, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}

	// If we only have merge operands but no base value was found, apply merge with nil base
	if len(mergeOperands) > 0 {
		return db.applyMerge(key, nil, mergeOperands)
	}

	return nil, ErrNotFound
}

// MultiGet retrieves multiple values for the given keys.
// Returns a slice of values in the same order as keys.
// If a key doesn't exist, the corresponding value is nil and error is ErrNotFound.
func (db *DBImpl) MultiGet(opts *ReadOptions, keys [][]byte) ([][]byte, []error) {
	if len(keys) == 0 {
		return nil, nil
	}

	values := make([][]byte, len(keys))
	errors := make([]error, len(keys))

	// For now, use simple sequential get - can be optimized later
	// C++ RocksDB uses batched I/O and sorted key ordering for optimization
	for i, key := range keys {
		value, err := db.Get(opts, key)
		values[i] = value
		errors[i] = err
	}

	return values, errors
}

// getFromVersion searches for a key in the SST files of a version.
// It also handles merge operands by collecting them and applying the merge operator.
// Reserved for future use - currently getFromVersionWithMerge is used directly.
func (db *DBImpl) getFromVersion(v *version.Version, key []byte, seq dbformat.SequenceNumber, cfID uint32) ([]byte, error) { //nolint:unused // reserved for future use
	return db.getFromVersionWithMerge(v, key, seq, nil, cfID)
}

// getFromVersionWithMerge searches for a key in SST files and handles merge operands.
// mergeOperands contains any merge operands already collected from memtable.
// cfID specifies which column family to search in (for CF isolation).
func (db *DBImpl) getFromVersionWithMerge(v *version.Version, key []byte, seq dbformat.SequenceNumber, mergeOperands [][]byte, cfID uint32) ([]byte, error) {
	// Create a range deletion aggregator to track tombstones across files.
	// The upperBound is the snapshot sequence - tombstones with seq > upperBound are invisible.
	rangeDelAgg := rangedel.NewRangeDelAggregator(seq)

	// Search each level starting from L0
	// L0 files may overlap, so we must search all of them in reverse order (newest first)
	// For L1+, files are sorted and non-overlapping, so we can binary search

	var existingValue []byte
	foundBase := false

	// Search L0 files (newest first)
	l0Files := v.Files(0)
	for i := len(l0Files) - 1; i >= 0; i-- {
		f := l0Files[i]
		// Skip files that don't belong to this column family.
		// This is critical for CF isolation: each CF's data is stored in separate files.
		if f.ColumnFamilyID != cfID {
			continue
		}
		// Check if key is in this file's range
		if db.cmp.Compare(key, extractUserKey(f.Smallest)) < 0 {
			continue
		}
		if db.cmp.Compare(key, extractUserKey(f.Largest)) > 0 {
			continue
		}

		// Key might be in this file, search it
		value, found, deleted, isMerge, foundSeq, err := db.getFromFile(f, key, seq, rangeDelAgg)
		if err != nil {
			return nil, err
		}
		if found {
			// Check if the found value is covered by a range tombstone
			if deleted || rangeDelAgg.ShouldDelete(key, foundSeq) {
				// Base is deleted - apply merge with nil base
				if len(mergeOperands) > 0 {
					return db.applyMerge(key, nil, mergeOperands)
				}
				return nil, ErrNotFound
			}
			if isMerge {
				// Collect this merge operand and continue searching
				mergeOperands = append(mergeOperands, value)
				continue
			}
			// Found a value - this is the base
			foundBase = true
			existingValue = value
			break
		}
	}

	// Search L1+ files if we haven't found the base yet
	// NOTE: We search ALL files in each level because files may overlap due to
	// trivial moves or pending compactions. The binary search optimization is
	// only safe when we can guarantee non-overlapping files, which we can't
	// currently guarantee. This is a correctness fix at the cost of performance.
	// TODO: Add non-overlap invariant enforcement to compaction and re-enable binary search.
	if !foundBase {
		for level := 1; level < v.NumLevels(); level++ {
			files := v.Files(level)
			if len(files) == 0 {
				continue
			}

			// Search all files in reverse order (newest first) since we can't
			// guarantee non-overlapping files at L1+
			for i := len(files) - 1; i >= 0; i-- {
				f := files[i]

				// Skip files that don't belong to this column family.
				if f.ColumnFamilyID != cfID {
					continue
				}
				// Check if key is in this file's range
				if db.cmp.Compare(key, extractUserKey(f.Smallest)) < 0 {
					continue
				}
				if db.cmp.Compare(key, extractUserKey(f.Largest)) > 0 {
					continue
				}

				// Key might be in this file
				value, found, deleted, isMerge, foundSeq, err := db.getFromFile(f, key, seq, rangeDelAgg)
				if err != nil {
					return nil, err
				}
				if found {
					// Check if the found value is covered by a range tombstone
					if deleted || rangeDelAgg.ShouldDelete(key, foundSeq) {
						// Base is deleted - apply merge with nil base
						if len(mergeOperands) > 0 {
							return db.applyMerge(key, nil, mergeOperands)
						}
						return nil, ErrNotFound
					}
					if isMerge {
						// Collect this merge operand and continue searching
						mergeOperands = append(mergeOperands, value)
						continue
					}
					// Found a value - this is the base
					foundBase = true
					existingValue = value
					break
				}
			}
			if foundBase {
				break
			}
		}
	}

	// Apply merge if we have operands
	if len(mergeOperands) > 0 {
		return db.applyMerge(key, existingValue, mergeOperands)
	}

	if foundBase {
		// IMPORTANT: Copy the value to prevent aliasing with cached block data.
		// SST block data is cached and shared; users must not modify returned values.
		return copySlice(existingValue), nil
	}

	return nil, ErrNotFound
}

// applyMerge applies the merge operator to resolve merge operands.
// operands are in newest-first order, so we reverse them for FullMerge.
func (db *DBImpl) applyMerge(key []byte, existingValue []byte, operands [][]byte) ([]byte, error) {
	if db.options.MergeOperator == nil {
		return nil, ErrMergeOperatorNotSet
	}

	// Reverse operands to get oldest-first order for FullMerge
	reversed := make([][]byte, len(operands))
	for i, op := range operands {
		reversed[len(operands)-1-i] = op
	}

	result, ok := db.options.MergeOperator.FullMerge(key, existingValue, reversed)
	if !ok {
		return nil, fmt.Errorf("merge operator failed for key %q", key)
	}

	return result, nil
}

// copySlice creates a copy of a byte slice to prevent aliasing with internal buffers.
// This is critical for safety: returned values must not share memory with internal state.
// Reference: RocksDB v10.7.5 uses PinnableSlice::PinSelf() which copies data.
func copySlice(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// extractUserKey extracts the user key from an internal key.
func extractUserKey(internalKey []byte) []byte {
	if len(internalKey) < 8 {
		return internalKey
	}
	return internalKey[:len(internalKey)-8]
}

// getFromFile searches for a key in a single SST file.
// It also loads range tombstones from the file and adds them to the aggregator.
// Returns: value, found, deleted, isMerge, foundSeqNum, error
func (db *DBImpl) getFromFile(f *manifest.FileMetaData, key []byte, seq dbformat.SequenceNumber, rangeDelAgg *rangedel.RangeDelAggregator) ([]byte, bool, bool, bool, dbformat.SequenceNumber, error) {
	fileNum := f.FD.GetNumber()
	path := db.sstFilePath(fileNum)

	reader, err := db.tableCache.Get(fileNum, path)
	if err != nil {
		return nil, false, false, false, 0, err
	}
	defer db.tableCache.Release(fileNum)

	// Load range tombstones from this file and add to the aggregator.
	// This must be done before checking for the key to ensure we catch
	// range deletions that might cover keys in older files.
	if rangeDelAgg != nil {
		tombstoneList, err := reader.GetRangeTombstoneList()
		if err == nil && !tombstoneList.IsEmpty() {
			// Use level 0 for all files since we're doing a point lookup.
			// The aggregator will still correctly apply sequence number visibility.
			rangeDelAgg.AddTombstoneList(0, tombstoneList)
		}
	}

	// Create seek key: userKey + seq for this lookup
	seekKey := makeInternalKey(key, uint64(seq), dbformat.ValueTypeForSeek)

	iter := reader.NewIterator()
	iter.Seek(seekKey)

	if !iter.Valid() {
		return nil, false, false, false, 0, nil
	}

	// Check if we found the right key
	foundKey := iter.Key()
	foundUserKey := extractUserKey(foundKey)
	if db.cmp.Compare(foundUserKey, key) != 0 {
		return nil, false, false, false, 0, nil
	}

	// Extract sequence number and value type from internal key
	foundSeq := extractSequenceNumber(foundKey)
	valueType := extractValueType(foundKey)

	if valueType == dbformat.TypeDeletion || valueType == dbformat.TypeSingleDeletion {
		return nil, true, true, false, foundSeq, nil
	}

	if valueType == dbformat.TypeMerge {
		return iter.Value(), true, false, true, foundSeq, nil
	}

	return iter.Value(), true, false, false, foundSeq, nil
}

// makeInternalKey constructs an internal key from user key, sequence, and type.
func makeInternalKey(userKey []byte, seq uint64, typ dbformat.ValueType) []byte {
	key := make([]byte, len(userKey)+8)
	copy(key, userKey)
	trailer := (seq << 8) | uint64(typ)
	key[len(userKey)] = byte(trailer)
	key[len(userKey)+1] = byte(trailer >> 8)
	key[len(userKey)+2] = byte(trailer >> 16)
	key[len(userKey)+3] = byte(trailer >> 24)
	key[len(userKey)+4] = byte(trailer >> 32)
	key[len(userKey)+5] = byte(trailer >> 40)
	key[len(userKey)+6] = byte(trailer >> 48)
	key[len(userKey)+7] = byte(trailer >> 56)
	return key
}

// extractValueType extracts the value type from an internal key.
func extractValueType(internalKey []byte) dbformat.ValueType {
	if len(internalKey) < 8 {
		return dbformat.TypeValue
	}
	// Type is in the lowest byte of the trailer
	return dbformat.ValueType(internalKey[len(internalKey)-8])
}

// extractSequenceNumber extracts the sequence number from an internal key.
func extractSequenceNumber(internalKey []byte) dbformat.SequenceNumber {
	if len(internalKey) < 8 {
		return 0
	}
	// Sequence number is in the upper 56 bits of the 8-byte trailer
	trailer := uint64(0)
	for i := range 8 {
		trailer |= uint64(internalKey[len(internalKey)-8+i]) << (i * 8)
	}
	return dbformat.SequenceNumber(trailer >> 8)
}

// findFile finds the file in a sorted level that might contain the key.
// Returns the index of the first file whose largest key >= key.
//
// NOTE: This function is currently unused because Get() iterates through
// all files at L1+ to handle cases where overlapping files exist at higher
// levels (which shouldn't happen but can due to compaction bugs).
// Once the compaction invariant (non-overlapping files at L1+) is fixed,
// this function should be reinstated for O(log n) file lookup.
//
//nolint:unused // reinstated once compaction guarantees non-overlapping files at L1+
func (db *DBImpl) findFile(files []*manifest.FileMetaData, key []byte) int {
	lo := 0
	hi := len(files)
	for lo < hi {
		mid := (lo + hi) / 2
		if db.cmp.Compare(extractUserKey(files[mid].Largest), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Delete removes the given key from the database.
// Delete removes the given key from the default column family.
func (db *DBImpl) Delete(opts *WriteOptions, key []byte) error {
	return db.DeleteCF(opts, nil, key)
}

// SingleDelete removes the given key from the default column family.
// Unlike Delete, SingleDelete is only valid for keys that have been Put exactly once
// without any Merge operations. If there are multiple Put operations for a key,
// SingleDelete may not work correctly.
func (db *DBImpl) SingleDelete(opts *WriteOptions, key []byte) error {
	wb := batch.New()
	wb.SingleDelete(key)
	return db.Write(opts, wb)
}

// DeleteCF removes the given key from the specified column family.
func (db *DBImpl) DeleteCF(opts *WriteOptions, cf ColumnFamilyHandle, key []byte) error {
	cfd, err := db.getColumnFamilyData(cf)
	if err != nil {
		return err
	}

	wb := batch.New()
	if cfd.id == DefaultColumnFamilyID {
		wb.Delete(key)
	} else {
		wb.DeleteCF(cfd.id, key)
	}
	return db.Write(opts, wb)
}

// DeleteRange removes all keys in the range [startKey, endKey) from the default column family.
func (db *DBImpl) DeleteRange(opts *WriteOptions, startKey, endKey []byte) error {
	return db.DeleteRangeCF(opts, nil, startKey, endKey)
}

// DeleteRangeCF removes all keys in the range [startKey, endKey) from the specified column family.
func (db *DBImpl) DeleteRangeCF(opts *WriteOptions, cf ColumnFamilyHandle, startKey, endKey []byte) error {
	cfd, err := db.getColumnFamilyData(cf)
	if err != nil {
		return err
	}

	wb := batch.New()
	if cfd.id == DefaultColumnFamilyID {
		wb.DeleteRange(startKey, endKey)
	} else {
		wb.DeleteRangeCF(cfd.id, startKey, endKey)
	}
	return db.Write(opts, wb)
}

// Merge applies a merge operation for the given key in the default column family.
func (db *DBImpl) Merge(opts *WriteOptions, key, value []byte) error {
	return db.MergeCF(opts, nil, key, value)
}

// MergeCF applies a merge operation for the given key in the specified column family.
func (db *DBImpl) MergeCF(opts *WriteOptions, cf ColumnFamilyHandle, key, value []byte) error {
	if db.options.MergeOperator == nil {
		return ErrMergeOperatorNotSet
	}

	cfd, err := db.getColumnFamilyData(cf)
	if err != nil {
		return err
	}

	wb := batch.New()
	if cfd.id == DefaultColumnFamilyID {
		wb.Merge(key, value)
	} else {
		wb.MergeCF(cfd.id, key, value)
	}
	return db.Write(opts, wb)
}

// Write applies a batch of operations atomically.
func (db *DBImpl) Write(opts *WriteOptions, wb *batch.WriteBatch) error {
	// Whitebox [synctest]: barrier at Write start
	_ = testutil.SP(testutil.SPDBWrite)

	if opts == nil {
		opts = DefaultWriteOptions()
	}

	// Check write stall condition and wait if needed
	writeSize := len(wb.Data())
	db.writeController.MaybeStallWrite(writeSize)

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDBClosed
	}
	// Check for unrecoverable background error
	if db.backgroundError != nil {
		err := fmt.Errorf("%w: %w", ErrBackgroundError, db.backgroundError)
		db.mu.Unlock()
		return err
	}

	// Assign sequence numbers
	count := wb.Count()
	firstSeq := db.seq + 1
	wb.SetSequence(firstSeq)
	db.seq += uint64(count)

	// Write to WAL (unless disabled)
	if opts.DisableWAL {
		// Warn once about data loss risk
		if !db.walDisabledWarned {
			db.walDisabledWarned = true
			if db.logger != nil {
				db.logger.Warn("DisableWAL=true: writes will be lost if process crashes before Flush()")
			}
		}
	} else if db.logWriter != nil {
		// Whitebox [synctest]: barrier before WAL write
		_ = testutil.SP(testutil.SPDBWriteWAL)

		data := wb.Data()
		if _, err := db.logWriter.AddRecord(data); err != nil {
			db.mu.Unlock()
			return err
		}

		// Sync if requested
		if opts.Sync && db.logWriter != nil {
			if err := db.logWriter.Sync(); err != nil {
				db.mu.Unlock()
				return err
			}
		}

		// Whitebox [synctest]: barrier after WAL write
		_ = testutil.SP(testutil.SPDBWriteWALComplete)
	}

	// Whitebox [synctest]: barrier before memtable insert
	_ = testutil.SP(testutil.SPDBWriteMemtable)

	// Capture memtable reference while holding lock to avoid race with Flush
	seq := firstSeq
	mem := db.mem
	handler := &memtableInserter{
		db:         db,
		sequence:   seq,
		defaultMem: mem,
	}
	db.mu.Unlock()

	// Iterate through the batch and apply to memtables
	if err := wb.Iterate(handler); err != nil {
		return err
	}

	// Whitebox [synctest]: barrier after memtable insert
	_ = testutil.SP(testutil.SPDBWriteMemtableComplete)

	// Whitebox [synctest]: barrier at Write complete
	_ = testutil.SP(testutil.SPDBWriteComplete)

	return nil
}

// memtableInserter applies batch operations to a memtable.
type memtableInserter struct {
	db         *DBImpl
	sequence   uint64
	defaultMem *memtable.MemTable // Captured at write time to avoid race with flush
	lockHeld   bool               // True if caller already holds db.mu (e.g., during recovery)
}

func (m *memtableInserter) getMemtable(cfID uint32) *memtable.MemTable {
	if cfID == DefaultColumnFamilyID {
		return m.defaultMem
	}
	// For non-default CFs, we need to look up the CF data
	// If the lock is already held by caller, don't try to acquire it
	if !m.lockHeld {
		m.db.mu.RLock()
		defer m.db.mu.RUnlock()
	}
	cfd := m.db.columnFamilies.GetByID(cfID)
	if cfd == nil {
		return m.defaultMem // Fallback to default
	}
	return cfd.mem
}

func (m *memtableInserter) Put(key, value []byte) error {
	return m.PutCF(DefaultColumnFamilyID, key, value)
}

func (m *memtableInserter) PutCF(cfID uint32, key, value []byte) error {
	mem := m.getMemtable(cfID)
	mem.Add(dbformat.SequenceNumber(m.sequence), dbformat.TypeValue, key, value)
	m.sequence++
	return nil
}

func (m *memtableInserter) Delete(key []byte) error {
	return m.DeleteCF(DefaultColumnFamilyID, key)
}

func (m *memtableInserter) DeleteCF(cfID uint32, key []byte) error {
	mem := m.getMemtable(cfID)
	mem.Add(dbformat.SequenceNumber(m.sequence), dbformat.TypeDeletion, key, nil)
	m.sequence++
	return nil
}

func (m *memtableInserter) SingleDelete(key []byte) error {
	return m.SingleDeleteCF(DefaultColumnFamilyID, key)
}

func (m *memtableInserter) SingleDeleteCF(cfID uint32, key []byte) error {
	mem := m.getMemtable(cfID)
	mem.Add(dbformat.SequenceNumber(m.sequence), dbformat.TypeSingleDeletion, key, nil)
	m.sequence++
	return nil
}

func (m *memtableInserter) Merge(key, value []byte) error {
	return m.MergeCF(DefaultColumnFamilyID, key, value)
}

func (m *memtableInserter) MergeCF(cfID uint32, key, value []byte) error {
	mem := m.getMemtable(cfID)
	mem.Add(dbformat.SequenceNumber(m.sequence), dbformat.TypeMerge, key, value)
	m.sequence++
	return nil
}

func (m *memtableInserter) DeleteRange(startKey, endKey []byte) error {
	return m.DeleteRangeCF(DefaultColumnFamilyID, startKey, endKey)
}

func (m *memtableInserter) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error {
	mem := m.getMemtable(cfID)
	mem.AddRangeTombstone(dbformat.SequenceNumber(m.sequence), startKey, endKey)
	m.sequence++
	return nil
}

func (m *memtableInserter) LogData(blob []byte) {
	// Log data is ignored
}

// NewIterator creates an iterator over the default column family.
func (db *DBImpl) NewIterator(opts *ReadOptions) Iterator {
	return db.NewIteratorCF(opts, nil)
}

// NewIteratorCF creates an iterator over the specified column family.
func (db *DBImpl) NewIteratorCF(opts *ReadOptions, cf ColumnFamilyHandle) Iterator {
	cfd, err := db.getColumnFamilyData(cf)
	if err != nil {
		return &errorIterator{err: err}
	}

	if opts == nil {
		opts = DefaultReadOptions()
	}

	var snapshot *Snapshot
	if opts.Snapshot != nil {
		snapshot = opts.Snapshot
	} else {
		snapshot = db.GetSnapshot()
		// Note: The iterator owns this snapshot and should release it on Close
	}

	iter := newDBIteratorCF(db, cfd, snapshot)

	// Set up prefix seek options
	iter.prefixExtractor = db.options.PrefixExtractor
	iter.iterateUpperBound = opts.IterateUpperBound
	iter.iterateLowerBound = opts.IterateLowerBound
	iter.prefixSameAsStart = opts.PrefixSameAsStart
	iter.totalOrderSeek = opts.TotalOrderSeek

	return iter
}

// GetSnapshot creates a new snapshot of the database.
func (db *DBImpl) GetSnapshot() *Snapshot {
	db.mu.RLock()
	seq := db.seq
	db.mu.RUnlock()

	s := newSnapshot(db, seq)

	db.snapshotLock.Lock()
	// Add to linked list
	s.next = db.snapshots
	if db.snapshots != nil {
		db.snapshots.prev = s
	}
	db.snapshots = s
	db.snapshotLock.Unlock()

	return s
}

// ReleaseSnapshot releases a previously acquired snapshot.
func (db *DBImpl) ReleaseSnapshot(s *Snapshot) {
	s.Release()
}

// releaseSnapshot is called when a snapshot's reference count reaches zero.
func (db *DBImpl) releaseSnapshot(s *Snapshot) {
	db.snapshotLock.Lock()
	defer db.snapshotLock.Unlock()

	// Remove from linked list
	if s.prev != nil {
		s.prev.next = s.next
	} else {
		db.snapshots = s.next
	}
	if s.next != nil {
		s.next.prev = s.prev
	}
}

// Flush flushes the memtable to disk.
func (db *DBImpl) Flush(opts *FlushOptions) error {
	if opts == nil {
		opts = DefaultFlushOptions()
	}

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDBClosed
	}
	// Check for unrecoverable background error
	if db.backgroundError != nil {
		err := fmt.Errorf("%w: %w", ErrBackgroundError, db.backgroundError)
		db.mu.Unlock()
		return err
	}

	// Wait for any existing immutable memtable to be flushed
	// This prevents "immutable memtable already exists" spam during stress tests
	for db.imm != nil {
		// Check for shutdown or background error while waiting
		if db.closed {
			db.mu.Unlock()
			return ErrDBClosed
		}
		if db.backgroundError != nil {
			err := fmt.Errorf("%w: %w", ErrBackgroundError, db.backgroundError)
			db.mu.Unlock()
			return err
		}
		// Wait for the background flush to complete
		db.immCond.Wait()
	}

	// Skip if memtable is empty
	if db.mem.Empty() {
		db.mu.Unlock()
		return nil
	}

	// Switch memtable: current becomes immutable, create new active memtable.
	// NOTE: We do NOT create a new WAL here (unlike RocksDB which rotates WALs).
	// This means the current WAL continues to receive writes from the new memtable.
	// Therefore, we do NOT set nextLogNumber - we can't advance LogNumber until
	// we actually create a new WAL (on DB open/recovery).
	// Reference: RocksDB v10.7.5 db/db_impl/db_impl_write.cc:2722 (for WAL rotation)
	db.imm = db.mem
	// Don't set nextLogNumber - same WAL is used for new memtable
	var memCmp memtable.Comparator
	if db.comparator != nil {
		memCmp = db.comparator.Compare
	}
	db.mem = memtable.NewMemTable(memCmp)

	// Recalculate write stall condition (may now be stalled due to imm)
	db.recalculateWriteStall()
	db.mu.Unlock()

	// Perform the flush synchronously
	if err := db.doFlush(); err != nil {
		return err
	}

	// Wait for completion if requested
	if opts.Wait {
		// Already done synchronously above
	}

	// Trigger compaction check after flush
	if db.bgWork != nil {
		db.bgWork.MaybeScheduleCompaction()
	}

	return nil
}

// SyncWAL syncs the current WAL to disk, ensuring all data is durable.
// Reference: RocksDB v10.7.5
//
//	db/db_impl/db_impl.cc - SyncWAL() implementation (lines 1533-1550)
//	include/rocksdb/db.h - SyncWAL() interface (lines 1782-1789)
func (db *DBImpl) SyncWAL() error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrDBClosed
	}
	logWriter := db.logWriter
	db.mu.RUnlock()

	if logWriter == nil {
		return nil
	}

	// Sync the WAL file to disk (uses logWriter.Sync for kill point support)
	return logWriter.Sync()
}

// FlushWAL flushes the WAL buffer to the file system.
// If sync is true, it also syncs the WAL to disk (equivalent to SyncWAL).
// Reference: RocksDB v10.7.5
//
//	db/db_impl/db_impl.cc - FlushWAL() implementation (lines 1483-1512)
//	include/rocksdb/db.h - FlushWAL() interface (lines 1775-1780)
func (db *DBImpl) FlushWAL(sync bool) error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrDBClosed
	}
	logFile := db.logFile
	db.mu.RUnlock()

	if logFile == nil {
		return nil
	}

	// In RocksDB, FlushWAL with sync=false just writes buffered data
	// to the OS (no fsync). With sync=true, it also calls SyncWAL.
	//
	// Our implementation always syncs when writing to the WAL (no buffering),
	// so FlushWAL(false) is a no-op and FlushWAL(true) syncs.
	if sync {
		return db.SyncWAL()
	}

	return nil
}

// GetLatestSequenceNumber returns the sequence number of the most recent transaction.
// Reference: RocksDB v10.7.5 include/rocksdb/db.h GetLatestSequenceNumber()
func (db *DBImpl) GetLatestSequenceNumber() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.seq
}

// Close closes the database, releasing all resources.
func (db *DBImpl) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	// Stop background workers first (outside mutex to avoid deadlock)
	if db.bgWork != nil {
		db.bgWork.Stop()
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Signal shutdown
	close(db.shutdownCh)

	// Close WAL
	if db.logFile != nil {
		_ = db.logFile.Close()
		db.logFile = nil
		db.logWriter = nil
	}

	// Close table cache
	if db.tableCache != nil {
		_ = db.tableCache.Close()
	}

	// Close version set
	if db.versions != nil {
		_ = db.versions.Close()
	}

	return nil
}

// SetBackgroundError sets an unrecoverable background error.
// This is called when I/O errors occur in background operations (flush, compaction).
// Once set, new write operations will fail with this error.
// The error is sticky - it can only be cleared by reopening the database.
func (db *DBImpl) SetBackgroundError(err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	// Only set if not already set (first error wins)
	if db.backgroundError == nil && err != nil {
		db.backgroundError = err
	}
}

// GetBackgroundError returns the current background error, if any.
func (db *DBImpl) GetBackgroundError() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.backgroundError
}

// Property name constants for GetProperty.
// Reference: RocksDB include/rocksdb/db.h
const (
	// Memtable properties
	PropertyNumImmutableMemTable        = "rocksdb.num-immutable-mem-table"
	PropertyNumImmutableMemTableFlushed = "rocksdb.num-immutable-mem-table-flushed"
	PropertyMemTableFlushPending        = "rocksdb.mem-table-flush-pending"
	PropertyCurSizeActiveMemTable       = "rocksdb.cur-size-active-mem-table"
	PropertyCurSizeAllMemTables         = "rocksdb.cur-size-all-mem-tables"
	PropertyNumEntriesActiveMemTable    = "rocksdb.num-entries-active-mem-table"
	PropertyNumDeletesActiveMemTable    = "rocksdb.num-deletes-active-mem-table"

	// Compaction properties
	PropertyCompactionPending     = "rocksdb.compaction-pending"
	PropertyNumRunningFlushes     = "rocksdb.num-running-flushes"
	PropertyNumRunningCompactions = "rocksdb.num-running-compactions"

	// Level properties (use PropertyNumFilesAtLevelPrefix + "N")
	PropertyNumFilesAtLevelPrefix = "rocksdb.num-files-at-level"
	PropertyLevelStats            = "rocksdb.levelstats"

	// Snapshot properties
	PropertyNumSnapshots       = "rocksdb.num-snapshots"
	PropertyOldestSnapshotTime = "rocksdb.oldest-snapshot-time"

	// Key estimates
	PropertyEstimateNumKeys = "rocksdb.estimate-num-keys"

	// Live data size
	PropertyEstimateLiveDataSize = "rocksdb.estimate-live-data-size"
	PropertyTotalSstFilesSize    = "rocksdb.total-sst-files-size"
	PropertyLiveSstFilesSize     = "rocksdb.live-sst-files-size"

	// Background errors
	PropertyBackgroundErrors = "rocksdb.background-errors"

	// CF and version info
	PropertyNumLiveVersions           = "rocksdb.num-live-versions"
	PropertyCurrentSuperVersionNumber = "rocksdb.current-super-version-number"
	PropertyNumColumnFamilies         = "rocksdb.num-column-families"
)

// GetProperty returns the value of a database property.
// Returns the property value and true if the property exists, otherwise ("", false).
func (db *DBImpl) GetProperty(name string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return "", false
	}

	// Handle level-specific properties (rocksdb.num-files-at-level<N>)
	if after, ok := strings.CutPrefix(name, PropertyNumFilesAtLevelPrefix); ok {
		levelStr := after
		level, err := strconv.Atoi(levelStr)
		if err != nil || level < 0 || level >= 7 {
			return "", false
		}
		v := db.versions.Current()
		if v == nil {
			return "0", true
		}
		files := v.Files(level)
		return strconv.Itoa(len(files)), true
	}

	switch name {
	// Memtable properties
	case PropertyNumImmutableMemTable:
		count := 0
		if db.imm != nil {
			count = 1
		}
		return strconv.Itoa(count), true

	case PropertyNumImmutableMemTableFlushed:
		// We don't track this separately; return 0
		return "0", true

	case PropertyMemTableFlushPending:
		pending := 0
		if db.imm != nil {
			pending = 1
		}
		return strconv.Itoa(pending), true

	case PropertyCurSizeActiveMemTable:
		if db.mem != nil {
			return strconv.FormatUint(uint64(db.mem.ApproximateMemoryUsage()), 10), true
		}
		return "0", true

	case PropertyCurSizeAllMemTables:
		size := uint64(0)
		if db.mem != nil {
			size += uint64(db.mem.ApproximateMemoryUsage())
		}
		if db.imm != nil {
			size += uint64(db.imm.ApproximateMemoryUsage())
		}
		return strconv.FormatUint(size, 10), true

	case PropertyNumEntriesActiveMemTable:
		if db.mem != nil {
			return strconv.FormatInt(db.mem.Count(), 10), true
		}
		return "0", true

	case PropertyNumDeletesActiveMemTable:
		// We don't track deletes separately in memtable
		return "0", true

	// Compaction properties
	case PropertyCompactionPending:
		if db.bgWork != nil && db.bgWork.IsCompactionPending() {
			return "1", true
		}
		return "0", true

	case PropertyNumRunningFlushes:
		if db.bgWork != nil {
			return strconv.Itoa(db.bgWork.NumRunningFlushes()), true
		}
		return "0", true

	case PropertyNumRunningCompactions:
		if db.bgWork != nil {
			return strconv.Itoa(db.bgWork.NumRunningCompactions()), true
		}
		return "0", true

	// Level stats
	case PropertyLevelStats:
		return db.getLevelStats(), true

	// Snapshot properties
	case PropertyNumSnapshots:
		return strconv.Itoa(db.countSnapshots()), true

	case PropertyOldestSnapshotTime:
		oldest := db.getOldestSnapshotTime()
		if oldest == 0 {
			return "0", true
		}
		return strconv.FormatInt(oldest, 10), true

	// Key estimates
	case PropertyEstimateNumKeys:
		estimate := db.estimateNumKeys()
		return strconv.FormatUint(estimate, 10), true

	// File size properties
	case PropertyTotalSstFilesSize, PropertyLiveSstFilesSize:
		size := db.getTotalSstFilesSize()
		return strconv.FormatUint(size, 10), true

	case PropertyEstimateLiveDataSize:
		size := db.getTotalSstFilesSize()
		return strconv.FormatUint(size, 10), true

	// Background errors
	case PropertyBackgroundErrors:
		if db.bgWork != nil {
			return strconv.Itoa(db.bgWork.NumBackgroundErrors()), true
		}
		return "0", true

	// Version info
	case PropertyNumLiveVersions:
		if db.versions != nil {
			return strconv.Itoa(db.versions.NumLiveVersions()), true
		}
		return "1", true

	case PropertyCurrentSuperVersionNumber:
		if db.versions != nil {
			return strconv.FormatUint(db.versions.CurrentVersionNumber(), 10), true
		}
		return "0", true

	case PropertyNumColumnFamilies:
		return strconv.Itoa(db.columnFamilies.Count()), true

	default:
		return "", false
	}
}

// getLevelStats returns a formatted string with level statistics.
func (db *DBImpl) getLevelStats() string {
	v := db.versions.Current()
	if v == nil {
		return "Level Files Size(MB)\n"
	}

	var sb strings.Builder
	sb.WriteString("Level Files Size(MB)\n")
	for level := range 7 {
		files := v.Files(level)
		var totalSize uint64
		for _, f := range files {
			totalSize += f.FD.FileSize
		}
		sizeMB := float64(totalSize) / (1024 * 1024)
		sb.WriteString(fmt.Sprintf("  %d   %5d %8.2f\n", level, len(files), sizeMB))
	}
	return sb.String()
}

// countSnapshots counts the number of active snapshots.
func (db *DBImpl) countSnapshots() int {
	db.snapshotLock.Lock()
	defer db.snapshotLock.Unlock()

	count := 0
	for s := db.snapshots; s != nil; s = s.next {
		count++
	}
	return count
}

// getOldestSnapshotTime returns the creation time of the oldest snapshot (Unix timestamp).
func (db *DBImpl) getOldestSnapshotTime() int64 {
	db.snapshotLock.Lock()
	defer db.snapshotLock.Unlock()

	if db.snapshots == nil {
		return 0
	}

	// Find the oldest (smallest sequence number = oldest)
	oldest := db.snapshots
	for s := db.snapshots.next; s != nil; s = s.next {
		if s.sequence < oldest.sequence {
			oldest = s
		}
	}
	return oldest.createdAt
}

// estimateNumKeys estimates the total number of keys in the database.
func (db *DBImpl) estimateNumKeys() uint64 {
	var estimate uint64

	// Count keys in memtables
	if db.mem != nil {
		estimate += uint64(db.mem.Count())
	}
	if db.imm != nil {
		estimate += uint64(db.imm.Count())
	}

	// Estimate keys from SST files based on file size
	// Assume average key-value pair is ~100 bytes
	v := db.versions.Current()
	if v != nil {
		for level := range 7 {
			for _, f := range v.Files(level) {
				// Rough estimate: 1 entry per 100 bytes
				estimate += f.FD.FileSize / 100
			}
		}
	}

	return estimate
}

// getTotalSstFilesSize returns the total size of all SST files.
func (db *DBImpl) getTotalSstFilesSize() uint64 {
	v := db.versions.Current()
	if v == nil {
		return 0
	}

	var totalSize uint64
	for level := range 7 {
		for _, f := range v.Files(level) {
			totalSize += f.FD.FileSize
		}
	}
	return totalSize
}

// CreateColumnFamily creates a new column family.
func (db *DBImpl) CreateColumnFamily(opts ColumnFamilyOptions, name string) (ColumnFamilyHandle, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrDBClosed
	}

	cfd, err := db.columnFamilies.Create(name, opts)
	if err != nil {
		return nil, err
	}

	// Persist CF creation to MANIFEST
	edit := &manifest.VersionEdit{}
	edit.SetColumnFamily(cfd.id)
	edit.AddColumnFamily(name)
	edit.SetMaxColumnFamily(db.columnFamilies.NextID())
	if err := db.versions.LogAndApply(edit); err != nil {
		// Rollback: remove from in-memory set
		_ = db.columnFamilies.Drop(cfd) // Ignore error during rollback
		return nil, fmt.Errorf("failed to persist column family: %w", err)
	}

	return &columnFamilyHandle{cfd: cfd}, nil
}

// DropColumnFamily drops the specified column family.
func (db *DBImpl) DropColumnFamily(cf ColumnFamilyHandle) error {
	if cf == nil {
		return ErrInvalidColumnFamilyHandle
	}

	handle, ok := cf.(*columnFamilyHandle)
	if !ok || handle == nil || handle.cfd == nil {
		return ErrInvalidColumnFamilyHandle
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	cfID := handle.cfd.id

	// Persist CF drop to MANIFEST first
	edit := &manifest.VersionEdit{}
	edit.SetColumnFamily(cfID)
	edit.DropColumnFamily()
	if err := db.versions.LogAndApply(edit); err != nil {
		return fmt.Errorf("failed to persist column family drop: %w", err)
	}

	return db.columnFamilies.Drop(handle.cfd)
}

// ListColumnFamilies returns the names of all column families.
func (db *DBImpl) ListColumnFamilies() []string {
	return db.columnFamilies.ListNames()
}

// DefaultColumnFamily returns a handle to the default column family.
func (db *DBImpl) DefaultColumnFamily() ColumnFamilyHandle {
	return &columnFamilyHandle{cfd: db.columnFamilies.GetDefault()}
}

// GetColumnFamily returns a handle to the named column family, or nil if not found.
func (db *DBImpl) GetColumnFamily(name string) ColumnFamilyHandle {
	db.mu.RLock()
	defer db.mu.RUnlock()

	cfd := db.columnFamilies.GetByName(name)
	if cfd == nil {
		return nil
	}
	return &columnFamilyHandle{cfd: cfd}
}

// CompactRangeOptions specifies options for manual compaction.
type CompactRangeOptions struct {
	// ChangeLevel when true, will move compacted files to the minimum level
	// capable of holding the data.
	ChangeLevel bool
	// TargetLevel specifies the target level for the compacted files.
	TargetLevel int
	// ExclusiveManualCompaction when true, only one manual compaction runs at a time.
	ExclusiveManualCompaction bool
}

// CompactRange manually triggers compaction for the specified key range.
// If start and end are nil, the entire database is compacted.
func (db *DBImpl) CompactRange(opts *CompactRangeOptions, start, end []byte) error {
	if opts == nil {
		opts = &CompactRangeOptions{}
	}

	// Flush memtable first to ensure all data is in SSTs
	if err := db.Flush(nil); err != nil {
		return err
	}

	// Get current version
	db.mu.RLock()
	v := db.versions.Current()
	if v != nil {
		v.Ref()
	}
	db.mu.RUnlock()

	if v == nil {
		return nil
	}
	defer v.Unref()

	// Compact each level from L0 down to the bottommost level
	for level := range 6 {
		if err := db.compactLevel(v, level, start, end, opts); err != nil {
			return err
		}

		// Re-get version after each level since it may have changed
		db.mu.RLock()
		v.Unref()
		v = db.versions.Current()
		if v != nil {
			v.Ref()
		}
		db.mu.RUnlock()

		if v == nil {
			return nil
		}
	}

	return nil
}

// compactLevel compacts files in a specific level that overlap the given range.
func (db *DBImpl) compactLevel(v *version.Version, level int, start, end []byte, opts *CompactRangeOptions) error {
	files := v.Files(level)
	if len(files) == 0 {
		return nil
	}

	// Find files that overlap [start, end)
	var overlappingFiles []*manifest.FileMetaData
	for _, f := range files {
		if f.BeingCompacted {
			continue
		}
		// Check overlap
		if len(start) > 0 && bytes.Compare(f.Largest, start) < 0 {
			continue // File is entirely before start
		}
		if len(end) > 0 && bytes.Compare(f.Smallest, end) >= 0 {
			continue // File is entirely after or at end
		}
		overlappingFiles = append(overlappingFiles, f)
	}

	if len(overlappingFiles) == 0 {
		return nil
	}

	// Create a manual compaction
	outputLevel := level + 1
	if opts.ChangeLevel && opts.TargetLevel > outputLevel {
		outputLevel = opts.TargetLevel
	}

	input := &compaction.CompactionInputFiles{
		Level: level,
		Files: overlappingFiles,
	}

	// Find overlapping files in the output level
	var smallest, largest []byte
	for _, f := range overlappingFiles {
		if smallest == nil || bytes.Compare(f.Smallest, smallest) < 0 {
			smallest = f.Smallest
		}
		if largest == nil || bytes.Compare(f.Largest, largest) > 0 {
			largest = f.Largest
		}
	}

	outputFiles := v.OverlappingInputs(outputLevel, smallest, largest)
	var outputAvailable []*manifest.FileMetaData
	for _, f := range outputFiles {
		if !f.BeingCompacted {
			outputAvailable = append(outputAvailable, f)
		}
	}

	inputs := []*compaction.CompactionInputFiles{input}
	if len(outputAvailable) > 0 {
		inputs = append(inputs, &compaction.CompactionInputFiles{
			Level: outputLevel,
			Files: outputAvailable,
		})
	}

	c := compaction.NewCompaction(inputs, outputLevel)
	c.Reason = compaction.CompactionReasonManualCompaction

	// Mark files as being compacted
	db.mu.Lock()
	c.MarkFilesBeingCompacted(true)
	db.mu.Unlock()

	defer func() {
		db.mu.Lock()
		c.MarkFilesBeingCompacted(false)
		db.mu.Unlock()
	}()

	// Execute the compaction using the background work handler
	return db.bgWork.executeCompaction(c)
}

// BeginTransaction begins a new optimistic transaction.
func (db *DBImpl) BeginTransaction(opts TransactionOptions, writeOpts *WriteOptions) Transaction {
	if writeOpts == nil {
		writeOpts = DefaultWriteOptions()
	}
	return newOptimisticTransaction(db, opts, writeOpts)
}

// logFilePath returns the path to a log file.
func (db *DBImpl) logFilePath(number uint64) string {
	return filepath.Join(db.name, logFileName(number))
}

// logFileName returns the filename for a log file.
func logFileName(number uint64) string {
	return fmt.Sprintf("%06d.log", number)
}

// recalculateWriteStall recalculates and updates the write stall condition.
// REQUIRES: db.mu is held.
func (db *DBImpl) recalculateWriteStall() {
	// Count unflushed memtables
	numUnflushed := 1 // Current memtable
	if db.imm != nil {
		numUnflushed++
	}

	// Count L0 files
	numL0Files := 0
	if v := db.versions.Current(); v != nil {
		numL0Files = len(v.Files(0))
	}

	// Recalculate condition
	condition, cause := RecalculateWriteStallCondition(
		numUnflushed,
		numL0Files,
		db.options.MaxWriteBufferNumber,
		db.options.Level0SlowdownWritesTrigger,
		db.options.Level0StopWritesTrigger,
		db.options.DisableAutoCompactions,
	)

	// Update write controller
	db.writeController.SetStallCondition(condition, cause)
}
