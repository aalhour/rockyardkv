package rockyardkv

// transaction_log.go implements transaction log iteration for replication.
//
// TransactionLogIterator allows iterating over WAL records starting from
// a specific sequence number. This is used for replication and backup
// scenarios where a replica needs to catch up with the primary.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/transaction_log.h
//   - db/transaction_log_impl.h
//   - db/transaction_log_impl.cc


import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/vfs"
	"github.com/aalhour/rockyardkv/internal/wal"
)

var (
	// ErrWALNotAvailable is returned when the requested WAL is no longer available.
	ErrWALNotAvailable = errors.New("db: WAL file not available")

	// ErrIteratorNotValid is returned when accessing an invalid iterator.
	ErrIteratorNotValid = errors.New("db: transaction log iterator is not valid")
)

// WalFileType indicates whether a WAL file is live or archived.
type WalFileType int

const (
	// WalFileTypeLive indicates the WAL file is still being written to.
	WalFileTypeLive WalFileType = iota

	// WalFileTypeArchived indicates the WAL file has been archived.
	WalFileTypeArchived
)

// WalFile represents a WAL file.
type WalFile struct {
	// PathName returns the full path to the WAL file.
	PathName string

	// LogNumber is the WAL file number.
	LogNumber uint64

	// Type indicates whether the file is live or archived.
	Type WalFileType

	// StartSequence is the starting sequence number of the first write batch.
	StartSequence uint64

	// SizeBytes is the size of the file in bytes.
	SizeBytes uint64
}

// BatchResult contains a write batch and its sequence number.
type BatchResult struct {
	// Sequence is the sequence number of the first operation in the batch.
	Sequence uint64

	// WriteBatch is the batch of operations.
	WriteBatch *batch.WriteBatch
}

// TransactionLogIteratorReadOptions controls transaction log iterator behavior.
type TransactionLogIteratorReadOptions struct {
	// VerifyChecksums enables checksum verification when reading WAL records.
	VerifyChecksums bool
}

// DefaultTransactionLogIteratorReadOptions returns default read options.
func DefaultTransactionLogIteratorReadOptions() TransactionLogIteratorReadOptions {
	return TransactionLogIteratorReadOptions{
		VerifyChecksums: true,
	}
}

// TransactionLogIterator iterates over WAL records.
type TransactionLogIterator struct {
	db            *DBImpl
	fs            vfs.FS
	readOpts      TransactionLogIteratorReadOptions
	startSeq      uint64
	walFiles      []WalFile
	currentWalIdx int
	reader        *wal.Reader
	currentFile   vfs.SequentialFile
	currentBatch  *BatchResult
	valid         bool
	err           error
}

// GetUpdatesSince returns an iterator positioned at the first write batch
// whose sequence number is >= seq_number.
//
// Reference: RocksDB v10.7.5 db/db_impl/db_impl.cc (GetUpdatesSince)
func (db *DBImpl) GetUpdatesSince(seqNumber uint64, readOpts TransactionLogIteratorReadOptions) (*TransactionLogIterator, error) {
	// Get list of WAL files
	walFiles, err := db.getSortedWalFiles()
	if err != nil {
		return nil, err
	}

	if len(walFiles) == 0 {
		return nil, ErrWALNotAvailable
	}

	// Find the first WAL file that might contain seqNumber
	startIdx := -1
	for i, wf := range walFiles {
		// A WAL file might contain seqNumber if StartSequence <= seqNumber
		// (but we don't know the end sequence without reading the file)
		if i == len(walFiles)-1 || walFiles[i+1].StartSequence > seqNumber {
			if wf.StartSequence <= seqNumber {
				startIdx = i
				break
			}
		}
	}

	if startIdx == -1 {
		// The requested sequence number is too old
		startIdx = 0
	}

	iter := &TransactionLogIterator{
		db:            db,
		fs:            db.fs,
		readOpts:      readOpts,
		startSeq:      seqNumber,
		walFiles:      walFiles[startIdx:],
		currentWalIdx: 0,
		valid:         false,
	}

	// Position at the first record
	iter.seekToSeq(seqNumber)

	return iter, nil
}

// Valid returns true if the iterator is positioned at a valid write batch.
func (iter *TransactionLogIterator) Valid() bool {
	return iter.valid && iter.err == nil
}

// Next moves the iterator to the next write batch.
func (iter *TransactionLogIterator) Next() {
	if !iter.valid {
		return
	}

	iter.readNextBatch()
}

// Status returns any error encountered by the iterator.
func (iter *TransactionLogIterator) Status() error {
	return iter.err
}

// GetBatch returns the current write batch and its sequence number.
// REQUIRES: Valid() returns true.
func (iter *TransactionLogIterator) GetBatch() (*BatchResult, error) {
	if !iter.valid {
		return nil, ErrIteratorNotValid
	}
	return iter.currentBatch, nil
}

// Close releases resources associated with the iterator.
func (iter *TransactionLogIterator) Close() error {
	var closeErr error
	if iter.currentFile != nil {
		closeErr = iter.currentFile.Close()
		iter.currentFile = nil
	}
	iter.reader = nil
	iter.valid = false
	return closeErr
}

// seekToSeq positions the iterator at the first batch with seq >= target.
func (iter *TransactionLogIterator) seekToSeq(target uint64) {
	for iter.currentWalIdx < len(iter.walFiles) {
		if err := iter.openCurrentWal(); err != nil {
			iter.err = err
			iter.valid = false
			return
		}

		// Read batches until we find one >= target
		for {
			batchResult, err := iter.readBatchFromReader()
			if err != nil {
				// End of this WAL file
				iter.closeCurrentWal()
				iter.currentWalIdx++
				break
			}

			if batchResult.Sequence >= target {
				iter.currentBatch = batchResult
				iter.valid = true
				return
			}
		}
	}

	// No more records
	iter.valid = false
}

// readNextBatch reads the next batch from the current or subsequent WAL files.
func (iter *TransactionLogIterator) readNextBatch() {
	for iter.currentWalIdx < len(iter.walFiles) {
		if iter.reader == nil {
			if err := iter.openCurrentWal(); err != nil {
				iter.err = err
				iter.valid = false
				return
			}
		}

		batchResult, err := iter.readBatchFromReader()
		if err != nil {
			// End of this WAL file, move to next
			iter.closeCurrentWal()
			iter.currentWalIdx++
			continue
		}

		iter.currentBatch = batchResult
		iter.valid = true
		return
	}

	// No more records
	iter.valid = false
}

// openCurrentWal opens the current WAL file for reading.
func (iter *TransactionLogIterator) openCurrentWal() error {
	if iter.currentWalIdx >= len(iter.walFiles) {
		return ErrWALNotAvailable
	}

	walFile := iter.walFiles[iter.currentWalIdx]
	file, err := iter.fs.Open(walFile.PathName)
	if err != nil {
		return fmt.Errorf("failed to open WAL %s: %w", walFile.PathName, err)
	}

	iter.currentFile = file
	iter.reader = wal.NewReader(file, nil, iter.readOpts.VerifyChecksums, walFile.LogNumber)
	return nil
}

// closeCurrentWal closes the current WAL file.
func (iter *TransactionLogIterator) closeCurrentWal() {
	if iter.currentFile != nil {
		_ = iter.currentFile.Close() // Error ignored - moving to next file
		iter.currentFile = nil
	}
	iter.reader = nil
}

// readBatchFromReader reads the next write batch from the current reader.
func (iter *TransactionLogIterator) readBatchFromReader() (*BatchResult, error) {
	if iter.reader == nil {
		return nil, ErrWALNotAvailable
	}

	record, err := iter.reader.ReadRecord()
	if err != nil {
		return nil, err
	}

	// Parse the write batch
	wb, err := batch.NewFromData(record)
	if err != nil {
		return nil, fmt.Errorf("failed to decode write batch: %w", err)
	}

	return &BatchResult{
		Sequence:   wb.Sequence(),
		WriteBatch: wb,
	}, nil
}

// getSortedWalFiles returns a list of WAL files sorted by log number.
func (db *DBImpl) getSortedWalFiles() ([]WalFile, error) {
	entries, err := db.fs.ListDir(db.name)
	if err != nil {
		return nil, fmt.Errorf("failed to list database directory: %w", err)
	}

	var walFiles []WalFile
	for _, entry := range entries {
		if !strings.HasSuffix(entry, ".log") {
			continue
		}

		var logNum uint64
		_, err := fmt.Sscanf(entry, "%d.log", &logNum)
		if err != nil {
			continue
		}

		fullPath := db.name + "/" + entry
		info, err := db.fs.Stat(fullPath)
		if err != nil {
			continue
		}

		// Determine if this is the current WAL or archived
		fileType := WalFileTypeArchived
		if logNum == db.logFileNumber {
			fileType = WalFileTypeLive
		}

		walFiles = append(walFiles, WalFile{
			PathName:      fullPath,
			LogNumber:     logNum,
			Type:          fileType,
			StartSequence: 0, // Will be filled when reading
			SizeBytes:     uint64(info.Size()),
		})
	}

	// Sort by log number
	sort.Slice(walFiles, func(i, j int) bool {
		return walFiles[i].LogNumber < walFiles[j].LogNumber
	})

	return walFiles, nil
}

// GetSortedWalFiles returns a list of all WAL files sorted by log number.
// This is useful for backup and replication scenarios.
func (db *DBImpl) GetSortedWalFiles() ([]WalFile, error) {
	return db.getSortedWalFiles()
}
