package rockyardkv

// db_readonly.go implements Read-Only database mode.
//
// Reference: RocksDB v10.7.5
//   - db/db_impl/db_impl_readonly.cc
//   - include/rocksdb/db.h (OpenForReadOnly)


import (
	"errors"
	"fmt"
	"strings"

	"github.com/aalhour/rockyardkv/internal/logging"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/version"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// ErrReadOnly is returned when attempting a write operation on a read-only database.
var ErrReadOnly = errors.New("db: database is opened in read-only mode")

// DBImplReadOnly is a read-only view of the database.
// It wraps DBImpl and disables all write operations.
type DBImplReadOnly struct {
	*DBImpl
}

// OpenForReadOnly opens a database in read-only mode.
// If errorIfWALExists is true, an error is returned if there are WAL files
// that would need to be replayed (indicating unclean shutdown).
func OpenForReadOnly(path string, opts *Options, errorIfWALExists bool) (DB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	fs := opts.FS
	if fs == nil {
		fs = vfs.Default()
	}

	// Verify the database directory exists
	if !fs.Exists(path) {
		return nil, fmt.Errorf("db: database at %q does not exist", path)
	}

	// Check for WAL files if requested
	if errorIfWALExists {
		// List files and check for .log files
		files, err := fs.ListDir(path)
		if err != nil {
			return nil, fmt.Errorf("db: failed to list directory: %w", err)
		}
		for _, f := range files {
			if strings.HasSuffix(f, ".log") {
				return nil, fmt.Errorf("db: WAL files exist, database was not cleanly shut down")
			}
		}
	}

	// Setup comparator
	cmp := opts.Comparator
	if cmp == nil {
		cmp = BytewiseComparator{}
	}

	// Logger configuration: db.logger is NEVER nil.
	// If opts.Logger is nil or typed-nil, we use a default WARN logger.
	logger := logging.OrDefault(opts.Logger)

	// Create the base DB implementation
	db := &DBImpl{
		name:            path,
		options:         opts,
		fs:              fs,
		comparator:      cmp,
		cmp:             cmp,
		shutdownCh:      make(chan struct{}),
		tableCache:      table.NewTableCache(fs, table.DefaultTableCacheOptions()),
		writeController: NewWriteController(),
		logger:          logger,
	}

	// Wire FatalHandler: when Fatalf is called, set background error to stop writes.
	// For read-only DB this is less critical but maintains consistency.
	if dl, ok := logger.(*logging.DefaultLogger); ok {
		dl.SetFatalHandler(func(msg string) {
			db.SetBackgroundError(fmt.Errorf("%w: %s", logging.ErrFatal, msg))
		})
	}

	// Initialize column family set
	db.columnFamilies = newColumnFamilySet(db)

	// Initialize version set
	vsOpts := version.VersionSetOptions{
		DBName:              path,
		FS:                  fs,
		MaxManifestFileSize: 1024 * 1024 * 1024, // 1GB
		NumLevels:           version.MaxNumLevels,
		Logger:              db.logger, // Pass through for MANIFEST logging
	}
	db.versions = version.NewVersionSet(vsOpts)

	// Recover from existing database (read-only - no WAL replay)
	if err := db.versions.Recover(); err != nil {
		_ = db.tableCache.Close()
		return nil, fmt.Errorf("db: failed to recover: %w", err)
	}

	// Set sequence number to max for reads
	db.seq = ^uint64(0) >> 1 // MaxSequenceNumber

	// Return the read-only wrapper
	return &DBImplReadOnly{DBImpl: db}, nil
}

// Put is not supported in read-only mode.
func (db *DBImplReadOnly) Put(opts *WriteOptions, key, value []byte) error {
	return ErrReadOnly
}

// Delete is not supported in read-only mode.
func (db *DBImplReadOnly) Delete(opts *WriteOptions, key []byte) error {
	return ErrReadOnly
}

// SingleDelete is not supported in read-only mode.
func (db *DBImplReadOnly) SingleDelete(opts *WriteOptions, key []byte) error {
	return ErrReadOnly
}

// DeleteRange is not supported in read-only mode.
func (db *DBImplReadOnly) DeleteRange(opts *WriteOptions, start, end []byte) error {
	return ErrReadOnly
}

// Merge is not supported in read-only mode.
func (db *DBImplReadOnly) Merge(opts *WriteOptions, key, operand []byte) error {
	return ErrReadOnly
}

// Write is not supported in read-only mode.
func (db *DBImplReadOnly) Write(opts *WriteOptions, b *WriteBatch) error {
	return ErrReadOnly
}

// Flush is not supported in read-only mode.
func (db *DBImplReadOnly) Flush(opts *FlushOptions) error {
	return ErrReadOnly
}

// CompactRange is not supported in read-only mode.
func (db *DBImplReadOnly) CompactRange(opts *CompactRangeOptions, start, end []byte) error {
	return ErrReadOnly
}

// CreateColumnFamily is not supported in read-only mode.
func (db *DBImplReadOnly) CreateColumnFamily(opts ColumnFamilyOptions, name string) (ColumnFamilyHandle, error) {
	return nil, ErrReadOnly
}

// DropColumnFamily is not supported in read-only mode.
func (db *DBImplReadOnly) DropColumnFamily(handle ColumnFamilyHandle) error {
	return ErrReadOnly
}

// PutCF is not supported in read-only mode.
func (db *DBImplReadOnly) PutCF(opts *WriteOptions, cf ColumnFamilyHandle, key, value []byte) error {
	return ErrReadOnly
}

// DeleteCF is not supported in read-only mode.
func (db *DBImplReadOnly) DeleteCF(opts *WriteOptions, cf ColumnFamilyHandle, key []byte) error {
	return ErrReadOnly
}

// DeleteRangeCF is not supported in read-only mode.
func (db *DBImplReadOnly) DeleteRangeCF(opts *WriteOptions, cf ColumnFamilyHandle, start, end []byte) error {
	return ErrReadOnly
}

// MergeCF is not supported in read-only mode.
func (db *DBImplReadOnly) MergeCF(opts *WriteOptions, cf ColumnFamilyHandle, key, operand []byte) error {
	return ErrReadOnly
}

// IngestExternalFile is not supported in read-only mode.
func (db *DBImplReadOnly) IngestExternalFile(paths []string, opts IngestExternalFileOptions) error {
	return ErrReadOnly
}

// SyncWAL is not supported in read-only mode.
func (db *DBImplReadOnly) SyncWAL() error {
	return ErrReadOnly
}

// FlushWAL is not supported in read-only mode.
func (db *DBImplReadOnly) FlushWAL(sync bool) error {
	return ErrReadOnly
}

// GetLatestSequenceNumber returns the sequence number of the most recent transaction.
func (db *DBImplReadOnly) GetLatestSequenceNumber() uint64 {
	if db.versions == nil {
		return 0
	}
	return db.versions.LastSequence()
}

// GetLiveFiles returns a list of all files in the database.
// flushMemtable is ignored in read-only mode (no memtable to flush).
func (db *DBImplReadOnly) GetLiveFiles(flushMemtable bool) ([]string, uint64, error) {
	if db.closed {
		return nil, 0, ErrDBClosed
	}
	// Delegate to embedded DBImpl, but ignore flushMemtable since we're read-only
	return db.DBImpl.GetLiveFiles(false)
}

// GetLiveFilesMetaData returns metadata about all live SST files.
func (db *DBImplReadOnly) GetLiveFilesMetaData() []LiveFileMetaData {
	if db.closed {
		return nil
	}
	// Delegate to embedded DBImpl
	return db.DBImpl.GetLiveFilesMetaData()
}

// DisableFileDeletions is a no-op in read-only mode.
func (db *DBImplReadOnly) DisableFileDeletions() error {
	return nil
}

// EnableFileDeletions is a no-op in read-only mode.
func (db *DBImplReadOnly) EnableFileDeletions() error {
	return nil
}

// PauseBackgroundWork is a no-op in read-only mode.
func (db *DBImplReadOnly) PauseBackgroundWork() error {
	return nil
}

// ContinueBackgroundWork is a no-op in read-only mode.
func (db *DBImplReadOnly) ContinueBackgroundWork() error {
	return nil
}

// BeginTransaction is not supported in read-only mode.
func (db *DBImplReadOnly) BeginTransaction(opts TransactionOptions, writeOpts *WriteOptions) Transaction {
	return nil
}

// NewCheckpoint is not supported in read-only mode.
func (db *DBImplReadOnly) NewCheckpoint() *Checkpoint {
	return nil
}

// Close closes the read-only database.
func (db *DBImplReadOnly) Close() error {
	if db.closed {
		return ErrDBClosed
	}
	db.closed = true

	if db.tableCache != nil {
		_ = db.tableCache.Close()
	}

	return nil
}

// ListColumnFamilies returns the list of column family names in the database.
func ListColumnFamilies(path string, opts *Options) ([]string, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	fs := opts.FS
	if fs == nil {
		fs = vfs.Default()
	}

	// Verify database exists
	if !fs.Exists(path) {
		return nil, fmt.Errorf("db: database at %q does not exist", path)
	}

	// Use version set to recover and get column families
	// Note: nil logger is acceptable here - this is a short-lived VersionSet
	vsOpts := version.VersionSetOptions{
		DBName:              path,
		FS:                  fs,
		MaxManifestFileSize: 1 << 30,
		NumLevels:           version.MaxNumLevels,
		Logger:              nil, // No logging for temporary VersionSet
	}
	vs := version.NewVersionSet(vsOpts)

	if err := vs.Recover(); err != nil {
		return nil, fmt.Errorf("db: failed to recover: %w", err)
	}

	// Get recovered column families
	recovered := vs.RecoveredColumnFamilies()
	names := make([]string, 0, len(recovered)+1)
	names = append(names, "default") // Always include default

	for _, cf := range recovered {
		if cf.Name != "default" {
			names = append(names, cf.Name)
		}
	}

	return names, nil
}
