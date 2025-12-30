// transaction.go implements optimistic transaction support.
//
// Transaction provides atomic, consistent, isolated database operations.
// This file implements optimistic concurrency control where conflicts
// are detected at commit time.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/utilities/transaction.h
//   - utilities/transactions/optimistic_transaction.h
package db

import (
	"errors"
	"sync"

	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/memtable"
)

// Transaction errors
var (
	// ErrTransactionConflict is returned when a transaction commit fails due to conflicts.
	ErrTransactionConflict = errors.New("db: transaction conflict")

	// ErrTransactionNotFound is returned when a transaction is not found.
	ErrTransactionNotFound = errors.New("db: transaction not found")

	// ErrTransactionClosed is returned when operating on a closed transaction.
	ErrTransactionClosed = errors.New("db: transaction is closed")
)

// TransactionOptions configures a transaction.
type TransactionOptions struct {
	// SetSnapshot determines if the transaction should set a snapshot at creation.
	SetSnapshot bool
}

// DefaultTransactionOptions returns default transaction options.
func DefaultTransactionOptions() TransactionOptions {
	return TransactionOptions{
		SetSnapshot: true,
	}
}

// Transaction represents an optimistic transaction.
// Changes made within a transaction are isolated from other transactions
// until the transaction is committed. At commit time, the transaction
// validates that no conflicts have occurred with other concurrent transactions.
type Transaction interface {
	// Put sets the value for the given key within the transaction.
	Put(key, value []byte) error

	// PutCF sets the value for the given key in the specified column family.
	PutCF(cf ColumnFamilyHandle, key, value []byte) error

	// Get retrieves the value for the given key.
	// It first checks the transaction's write batch, then falls back to the database.
	Get(key []byte) ([]byte, error)

	// GetCF retrieves the value from the specified column family.
	GetCF(cf ColumnFamilyHandle, key []byte) ([]byte, error)

	// GetForUpdate retrieves the value and marks it for conflict detection.
	// For optimistic transactions, this tracks the key for validation at commit.
	// For pessimistic transactions, this acquires a lock.
	GetForUpdate(key []byte, exclusive bool) ([]byte, error)

	// Delete removes the key from the transaction.
	Delete(key []byte) error

	// DeleteCF removes the key from the specified column family.
	DeleteCF(cf ColumnFamilyHandle, key []byte) error

	// Commit validates and applies the transaction.
	// Returns ErrTransactionConflict if there are write conflicts.
	Commit() error

	// Rollback discards the transaction.
	Rollback() error

	// SetSnapshot sets the transaction's snapshot to the current database state.
	SetSnapshot()

	// GetSnapshot returns the transaction's snapshot.
	GetSnapshot() *Snapshot
}

// trackedKey represents a key tracked for conflict detection.
type trackedKey struct {
	cfID     uint32
	key      string
	seqNum   dbformat.SequenceNumber
	readOnly bool
}

// optimisticTransaction implements Transaction using optimistic concurrency control.
type optimisticTransaction struct {
	mu sync.Mutex

	// The database
	db *DBImpl

	// Write batch for transaction writes
	writeBatch *batch.WriteBatch

	// Snapshot for consistent reads
	snapshot *Snapshot

	// Tracked keys for conflict detection
	trackedKeys map[string]trackedKey

	// Write options
	writeOpts *WriteOptions

	// Whether the transaction is closed
	closed bool
}

// newOptimisticTransaction creates a new optimistic transaction.
func newOptimisticTransaction(db *DBImpl, opts TransactionOptions, writeOpts *WriteOptions) *optimisticTransaction {
	txn := &optimisticTransaction{
		db:          db,
		writeBatch:  batch.New(),
		trackedKeys: make(map[string]trackedKey),
		writeOpts:   writeOpts,
	}

	if opts.SetSnapshot {
		txn.SetSnapshot()
	}

	return txn
}

// Put sets the value for the given key.
func (txn *optimisticTransaction) Put(key, value []byte) error {
	return txn.PutCF(nil, key, value)
}

// PutCF sets the value for the given key in the specified column family.
func (txn *optimisticTransaction) PutCF(cf ColumnFamilyHandle, key, value []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return ErrTransactionClosed
	}

	cfID := uint32(0)
	if cf != nil {
		cfID = cf.ID()
	}

	// Track the key for conflict detection
	txn.trackKey(cfID, key, false /* not read-only */)

	// Add to write batch
	if cfID == 0 {
		txn.writeBatch.Put(key, value)
	} else {
		txn.writeBatch.PutCF(cfID, key, value)
	}

	return nil
}

// Get retrieves the value for the given key.
func (txn *optimisticTransaction) Get(key []byte) ([]byte, error) {
	return txn.GetCF(nil, key)
}

// GetForUpdate retrieves the value and marks it for conflict detection.
// For optimistic transactions, this is equivalent to Get but tracks for validation.
func (txn *optimisticTransaction) GetForUpdate(key []byte, exclusive bool) ([]byte, error) {
	// For optimistic transactions, GetForUpdate is just Get with tracking
	// The 'exclusive' flag is ignored since we use optimistic concurrency
	return txn.Get(key)
}

// GetCF retrieves the value from the specified column family.
func (txn *optimisticTransaction) GetCF(cf ColumnFamilyHandle, key []byte) ([]byte, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return nil, ErrTransactionClosed
	}

	cfID := uint32(0)
	if cf != nil {
		cfID = cf.ID()
	}

	// Track the key for conflict detection (read)
	txn.trackKey(cfID, key, true /* read-only */)

	// First, check if we have a pending write for this key in our batch
	val, found, deleted := txn.getFromWriteBatch(cfID, key)
	if found {
		if deleted {
			return nil, ErrNotFound
		}
		return val, nil
	}

	// Read from database using snapshot
	readOpts := DefaultReadOptions()
	if txn.snapshot != nil {
		readOpts.Snapshot = txn.snapshot
	}

	if cf == nil {
		return txn.db.Get(readOpts, key)
	}
	return txn.db.GetCF(readOpts, cf, key)
}

// getFromWriteBatch checks if we have a pending write for this key.
// Returns (value, found, deleted).
func (txn *optimisticTransaction) getFromWriteBatch(cfID uint32, key []byte) ([]byte, bool, bool) {
	// Iterate through the write batch to find the most recent write for this key
	handler := &txnBatchReader{
		targetCFID: cfID,
		targetKey:  key,
	}
	_ = txn.writeBatch.Iterate(handler)
	return handler.value, handler.found, handler.deleted
}

// Delete removes the key.
func (txn *optimisticTransaction) Delete(key []byte) error {
	return txn.DeleteCF(nil, key)
}

// DeleteCF removes the key from the specified column family.
func (txn *optimisticTransaction) DeleteCF(cf ColumnFamilyHandle, key []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return ErrTransactionClosed
	}

	cfID := uint32(0)
	if cf != nil {
		cfID = cf.ID()
	}

	// Track the key for conflict detection
	txn.trackKey(cfID, key, false /* not read-only */)

	// Add to write batch
	if cfID == 0 {
		txn.writeBatch.Delete(key)
	} else {
		txn.writeBatch.DeleteCF(cfID, key)
	}

	return nil
}

// Commit validates and applies the transaction.
func (txn *optimisticTransaction) Commit() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return ErrTransactionClosed
	}

	// Check for conflicts
	if err := txn.checkForConflicts(); err != nil {
		return err
	}

	// Apply the write batch
	writeCount := txn.writeBatch.Count()
	if err := txn.db.Write(txn.writeOpts, newWriteBatchFromInternal(txn.writeBatch)); err != nil {
		return err
	}

	// Release snapshot and mark as closed
	txn.close()

	txn.db.logger.Debugf("[txn] committed optimistic txn (%d writes)", writeCount)

	return nil
}

// Rollback discards the transaction.
func (txn *optimisticTransaction) Rollback() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return ErrTransactionClosed
	}

	txn.close()
	return nil
}

// SetSnapshot sets the transaction's snapshot.
func (txn *optimisticTransaction) SetSnapshot() {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.snapshot != nil {
		txn.db.ReleaseSnapshot(txn.snapshot)
	}
	txn.snapshot = txn.db.GetSnapshot()
}

// GetSnapshot returns the transaction's snapshot.
func (txn *optimisticTransaction) GetSnapshot() *Snapshot {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.snapshot
}

// trackKey adds a key to the set of tracked keys for conflict detection.
func (txn *optimisticTransaction) trackKey(cfID uint32, key []byte, readOnly bool) {
	keyStr := string(key)
	trackKey := makeTrackKey(cfID, keyStr)

	// Get the current sequence number
	var seq dbformat.SequenceNumber
	if txn.snapshot != nil {
		seq = dbformat.SequenceNumber(txn.snapshot.Sequence())
	} else {
		txn.db.mu.RLock()
		seq = dbformat.SequenceNumber(txn.db.seq)
		txn.db.mu.RUnlock()
	}

	// Only track if not already tracked, or if we're upgrading from read to write
	if existing, ok := txn.trackedKeys[trackKey]; ok {
		// If we're writing to a previously read key, update to non-read-only
		if existing.readOnly && !readOnly {
			existing.readOnly = false
			txn.trackedKeys[trackKey] = existing
		}
	} else {
		txn.trackedKeys[trackKey] = trackedKey{
			cfID:     cfID,
			key:      keyStr,
			seqNum:   seq,
			readOnly: readOnly,
		}
	}
}

// makeTrackKey creates a unique key for the trackedKeys map.
func makeTrackKey(cfID uint32, key string) string {
	// Simple encoding: cfID as 4 bytes + key
	b := make([]byte, 4+len(key))
	b[0] = byte(cfID)
	b[1] = byte(cfID >> 8)
	b[2] = byte(cfID >> 16)
	b[3] = byte(cfID >> 24)
	copy(b[4:], key)
	return string(b)
}

// checkForConflicts checks if any tracked keys have been modified since the snapshot.
func (txn *optimisticTransaction) checkForConflicts() error {
	for _, tracked := range txn.trackedKeys {
		// Get the latest sequence number for this key
		cfd, err := txn.db.getColumnFamilyData(nil) // Use default CF for now
		if err != nil {
			return err
		}
		if tracked.cfID != 0 {
			cfd = txn.db.columnFamilies.GetByID(tracked.cfID)
			if cfd == nil {
				return ErrColumnFamilyNotFound
			}
		}

		// Check if the key has been modified after our tracked sequence number
		// This is a simplified check - we look for the key's latest version
		latestSeq := txn.getLatestSeqForKey(cfd, []byte(tracked.key))
		if latestSeq > tracked.seqNum {
			// Key was modified after our read/write
			return ErrTransactionConflict
		}
	}
	return nil
}

// getLatestSeqForKey gets the latest sequence number for a key.
func (txn *optimisticTransaction) getLatestSeqForKey(cfd *columnFamilyData, key []byte) dbformat.SequenceNumber {
	// Check memtable
	var mem *memtable.MemTable
	txn.db.mu.RLock()
	if cfd.id == DefaultColumnFamilyID {
		mem = txn.db.mem
	} else {
		cfd.memMu.RLock()
		mem = cfd.mem
		cfd.memMu.RUnlock()
	}
	txn.db.mu.RUnlock()

	if mem != nil {
		// Search the memtable for the latest version of this key
		iter := mem.NewIterator()
		iter.Seek(key)
		if iter.Valid() {
			// Parse the internal key to get the sequence number
			internalKey := iter.Key()
			if len(internalKey) >= 8 {
				// Internal key format: user_key + 8-byte trailer
				// Trailer: (seq << 8) | type
				userKey := internalKey[:len(internalKey)-8]
				if string(userKey) == string(key) {
					trailer := internalKey[len(internalKey)-8:]
					seqAndType := uint64(trailer[0]) |
						uint64(trailer[1])<<8 |
						uint64(trailer[2])<<16 |
						uint64(trailer[3])<<24 |
						uint64(trailer[4])<<32 |
						uint64(trailer[5])<<40 |
						uint64(trailer[6])<<48 |
						uint64(trailer[7])<<56
					seq := dbformat.SequenceNumber(seqAndType >> 8)
					return seq
				}
			}
		}
	}

	// If not in memtable, the key's sequence is older than our snapshot
	// (or doesn't exist), so no conflict
	return 0
}

// close releases resources and marks the transaction as closed.
func (txn *optimisticTransaction) close() {
	if txn.snapshot != nil {
		txn.db.ReleaseSnapshot(txn.snapshot)
		txn.snapshot = nil
	}
	txn.writeBatch = nil
	txn.trackedKeys = nil
	txn.closed = true
}

// txnBatchReader reads values from a write batch.
type txnBatchReader struct {
	targetCFID uint32
	targetKey  []byte
	found      bool
	deleted    bool
	value      []byte
}

func (r *txnBatchReader) Put(key, value []byte) error {
	if r.targetCFID == 0 && bytesEqual(key, r.targetKey) {
		r.found = true
		r.deleted = false
		r.value = append([]byte{}, value...)
	}
	return nil
}

func (r *txnBatchReader) PutCF(cfID uint32, key, value []byte) error {
	if cfID == r.targetCFID && bytesEqual(key, r.targetKey) {
		r.found = true
		r.deleted = false
		r.value = append([]byte{}, value...)
	}
	return nil
}

func (r *txnBatchReader) Delete(key []byte) error {
	if r.targetCFID == 0 && bytesEqual(key, r.targetKey) {
		r.found = true
		r.deleted = true
		r.value = nil
	}
	return nil
}

func (r *txnBatchReader) DeleteCF(cfID uint32, key []byte) error {
	if cfID == r.targetCFID && bytesEqual(key, r.targetKey) {
		r.found = true
		r.deleted = true
		r.value = nil
	}
	return nil
}

func (r *txnBatchReader) SingleDelete(key []byte) error {
	return r.Delete(key)
}

func (r *txnBatchReader) SingleDeleteCF(cfID uint32, key []byte) error {
	return r.DeleteCF(cfID, key)
}

func (r *txnBatchReader) Merge(key, value []byte) error                      { return nil }
func (r *txnBatchReader) MergeCF(cfID uint32, key, value []byte) error       { return nil }
func (r *txnBatchReader) DeleteRange(start, end []byte) error                { return nil }
func (r *txnBatchReader) DeleteRangeCF(cfID uint32, start, end []byte) error { return nil }
func (r *txnBatchReader) LogData(blob []byte)                                {}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
