package rockyardkv

// db_secondary.go implements Secondary Instance mode.
// A secondary instance can read data from a primary but cannot write.
// It can periodically catch up with the primary by tailing the MANIFEST.
//
// Reference: RocksDB v10.7.5
//   - db/db_impl/db_impl_secondary.cc
//   - include/rocksdb/db.h (OpenAsSecondary)

import (
	"fmt"
	"strings"
	"sync"

	"github.com/aalhour/rockyardkv/internal/logging"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/version"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// dbImplSecondary is a secondary instance that can read from a primary.
// It periodically catches up with the primary by tailing the MANIFEST.
type dbImplSecondary struct {
	*dbImpl

	// Path to the primary database
	primaryPath string

	// Path to the secondary instance's local directory (for caching)
	secondaryPath string

	// Mutex for catching up
	catchupMu sync.Mutex
}

// OpenAsSecondary opens a database as a secondary instance.
// The secondary can read data from the primary but cannot write.
// primaryPath is the path to the primary database directory.
// secondaryPath is an optional path for the secondary's local state.
func OpenAsSecondary(primaryPath, secondaryPath string, opts *Options) (SecondaryDB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	fs := opts.FS
	if fs == nil {
		fs = vfs.Default()
	}

	// Verify the primary database exists
	if !fs.Exists(primaryPath) {
		return nil, fmt.Errorf("db: primary database at %q does not exist", primaryPath)
	}

	// Setup comparator
	cmp := opts.Comparator
	if cmp == nil {
		cmp = BytewiseComparator{}
	}

	// Logger configuration: db.logger is NEVER nil.
	// If opts.Logger is nil or typed-nil, we use a default WARN logger.
	logger := logging.OrDefault(opts.Logger)

	// Create the base DB implementation (read-only)
	db := &dbImpl{
		name:            primaryPath,
		options:         opts,
		fs:              fs,
		comparator:      cmp,
		cmp:             cmp,
		shutdownCh:      make(chan struct{}),
		tableCache:      table.NewTableCache(fs, table.DefaultTableCacheOptions()),
		writeController: newWriteController(),
		logger:          logger,
	}

	// Wire FatalHandler: when Fatalf is called, set background error.
	// For secondary DB this is less critical but maintains consistency.
	if dl, ok := logger.(*logging.DefaultLogger); ok {
		dl.SetFatalHandler(func(msg string) {
			db.SetBackgroundError(fmt.Errorf("%w: %s", logging.ErrFatal, msg))
		})
	}

	// Initialize column family set
	db.columnFamilies = newColumnFamilySet(db)

	// Initialize version set pointing to primary
	vsOpts := version.VersionSetOptions{
		DBName:              primaryPath,
		FS:                  fs,
		MaxManifestFileSize: 1024 * 1024 * 1024,
		NumLevels:           version.MaxNumLevels,
		Logger:              db.logger, // Pass through for MANIFEST logging
	}
	db.versions = version.NewVersionSet(vsOpts)

	// Recover from primary's MANIFEST
	if err := db.versions.Recover(); err != nil {
		_ = db.tableCache.Close()
		return nil, fmt.Errorf("db: failed to recover: %w", err)
	}

	// Set the sequence number from the recovered version
	db.seq = db.versions.LastSequence()

	// Initialize default column family data from the recovered version
	defaultCF := &columnFamilyData{
		id:      DefaultColumnFamilyID,
		name:    DefaultColumnFamilyName,
		options: DefaultColumnFamilyOptions(),
		// No memtable for secondary - reads go directly to SST files
	}
	db.columnFamilies.byID[DefaultColumnFamilyID] = defaultCF
	db.columnFamilies.byName[DefaultColumnFamilyName] = defaultCF

	secondary := &dbImplSecondary{
		dbImpl:        db,
		primaryPath:   primaryPath,
		secondaryPath: secondaryPath,
	}

	return secondary, nil
}

// TryCatchUpWithPrimary attempts to catch up with the primary database.
// It reads new records from the MANIFEST and applies them.
func (db *dbImplSecondary) TryCatchUpWithPrimary() error {
	db.catchupMu.Lock()
	defer db.catchupMu.Unlock()

	// Re-recover from MANIFEST to get new changes
	// This is a simplified implementation - a full implementation would
	// tail the MANIFEST from the last read position
	if err := db.versions.Recover(); err != nil {
		return fmt.Errorf("db: failed to catch up: %w", err)
	}

	// Update the sequence number from the recovered version
	db.seq = db.versions.LastSequence()

	return nil
}

// Put is not supported in secondary mode.
func (db *dbImplSecondary) Put(opts *WriteOptions, key, value []byte) error {
	return ErrReadOnly
}

// Delete is not supported in secondary mode.
func (db *dbImplSecondary) Delete(opts *WriteOptions, key []byte) error {
	return ErrReadOnly
}

// SingleDelete is not supported in secondary mode.
func (db *dbImplSecondary) SingleDelete(opts *WriteOptions, key []byte) error {
	return ErrReadOnly
}

// DeleteRange is not supported in secondary mode.
func (db *dbImplSecondary) DeleteRange(opts *WriteOptions, start, end []byte) error {
	return ErrReadOnly
}

// Merge is not supported in secondary mode.
func (db *dbImplSecondary) Merge(opts *WriteOptions, key, operand []byte) error {
	return ErrReadOnly
}

// Write is not supported in secondary mode.
func (db *dbImplSecondary) Write(opts *WriteOptions, b *WriteBatch) error {
	return ErrReadOnly
}

// Flush is not supported in secondary mode.
func (db *dbImplSecondary) Flush(opts *FlushOptions) error {
	return ErrReadOnly
}

// CompactRange is not supported in secondary mode.
func (db *dbImplSecondary) CompactRange(opts *CompactRangeOptions, start, end []byte) error {
	return ErrReadOnly
}

// CreateColumnFamily is not supported in secondary mode.
func (db *dbImplSecondary) CreateColumnFamily(opts ColumnFamilyOptions, name string) (ColumnFamilyHandle, error) {
	return nil, ErrReadOnly
}

// DropColumnFamily is not supported in secondary mode.
func (db *dbImplSecondary) DropColumnFamily(handle ColumnFamilyHandle) error {
	return ErrReadOnly
}

// PutCF is not supported in secondary mode.
func (db *dbImplSecondary) PutCF(opts *WriteOptions, cf ColumnFamilyHandle, key, value []byte) error {
	return ErrReadOnly
}

// DeleteCF is not supported in secondary mode.
func (db *dbImplSecondary) DeleteCF(opts *WriteOptions, cf ColumnFamilyHandle, key []byte) error {
	return ErrReadOnly
}

// DeleteRangeCF is not supported in secondary mode.
func (db *dbImplSecondary) DeleteRangeCF(opts *WriteOptions, cf ColumnFamilyHandle, start, end []byte) error {
	return ErrReadOnly
}

// MergeCF is not supported in secondary mode.
func (db *dbImplSecondary) MergeCF(opts *WriteOptions, cf ColumnFamilyHandle, key, operand []byte) error {
	return ErrReadOnly
}

// IngestExternalFile is not supported in secondary mode.
func (db *dbImplSecondary) IngestExternalFile(paths []string, opts IngestExternalFileOptions) error {
	return ErrReadOnly
}

// SyncWAL is not supported in secondary mode.
func (db *dbImplSecondary) SyncWAL() error {
	return ErrReadOnly
}

// FlushWAL is not supported in secondary mode.
func (db *dbImplSecondary) FlushWAL(sync bool) error {
	return ErrReadOnly
}

// GetLatestSequenceNumber returns the sequence number of the most recent transaction.
func (db *dbImplSecondary) GetLatestSequenceNumber() uint64 {
	if db.versions == nil {
		return 0
	}
	return db.versions.LastSequence()
}

// GetLiveFiles returns a list of all files in the database.
// flushMemtable is ignored in secondary mode (no memtable to flush).
func (db *dbImplSecondary) GetLiveFiles(flushMemtable bool) ([]string, uint64, error) {
	if db.closed {
		return nil, 0, ErrDBClosed
	}
	// Delegate to embedded dbImpl, but ignore flushMemtable since we're secondary
	return db.dbImpl.GetLiveFiles(false)
}

// GetLiveFilesMetaData returns metadata about all live SST files.
func (db *dbImplSecondary) GetLiveFilesMetaData() []LiveFileMetaData {
	if db.closed {
		return nil
	}
	// Delegate to embedded dbImpl
	return db.dbImpl.GetLiveFilesMetaData()
}

// DisableFileDeletions is a no-op in secondary mode.
func (db *dbImplSecondary) DisableFileDeletions() error {
	return nil
}

// EnableFileDeletions is a no-op in secondary mode.
func (db *dbImplSecondary) EnableFileDeletions() error {
	return nil
}

// PauseBackgroundWork is a no-op in secondary mode.
func (db *dbImplSecondary) PauseBackgroundWork() error {
	return nil
}

// ContinueBackgroundWork is a no-op in secondary mode.
func (db *dbImplSecondary) ContinueBackgroundWork() error {
	return nil
}

// BeginTransaction is not supported in secondary mode.
func (db *dbImplSecondary) BeginTransaction(opts TransactionOptions, writeOpts *WriteOptions) Transaction {
	return nil
}

// NewCheckpoint is not supported in secondary mode.
func (db *dbImplSecondary) NewCheckpoint() *Checkpoint {
	return nil
}

// Close closes the secondary database.
func (db *dbImplSecondary) Close() error {
	if db.closed {
		return ErrDBClosed
	}
	db.closed = true

	if db.tableCache != nil {
		_ = db.tableCache.Close()
	}

	return nil
}

// GetProperty returns a property value, with additional secondary-specific properties.
func (db *dbImplSecondary) GetProperty(name string) (string, bool) {
	if strings.HasPrefix(name, "rocksdb.secondary.") {
		switch name {
		case "rocksdb.secondary.primary-path":
			return db.primaryPath, true
		case "rocksdb.secondary.secondary-path":
			return db.secondaryPath, true
		default:
			return "", false
		}
	}
	return db.dbImpl.GetProperty(name)
}
