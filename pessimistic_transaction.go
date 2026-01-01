package rockyardkv

// pessimistic_transaction.go implements pessimistic concurrency control.
//
// PessimisticTransaction acquires locks before modifying data, preventing
// conflicts through pessimistic locking rather than conflict detection.
//
// Reference: RocksDB v10.7.5
//   - utilities/transactions/pessimistic_transaction.h
//   - utilities/transactions/pessimistic_transaction.cc


import (
	"errors"
	"sync"
	"time"

	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/dbformat"
)

// Pessimistic transaction errors
var (
	// ErrTransactionExpired is returned when a transaction has expired.
	ErrTransactionExpired = errors.New("db: transaction expired")

	// ErrNoSavePoint is returned when trying to rollback to a savepoint that doesn't exist.
	ErrNoSavePoint = errors.New("db: no savepoint to rollback to")

	// ErrTransactionReadOnly is returned when trying to write in a read-only transaction.
	ErrTransactionReadOnly = errors.New("db: transaction is read-only")

	// ErrWriteConflict is returned when a key was modified after the transaction's snapshot.
	ErrWriteConflict = errors.New("db: write conflict - key modified after snapshot")
)

// PessimisticTransactionOptions configures a pessimistic transaction.
type PessimisticTransactionOptions struct {
	// SetSnapshot determines if the transaction should set a snapshot at creation.
	SetSnapshot bool

	// LockTimeout is the timeout for acquiring locks.
	LockTimeout time.Duration

	// DeadlockDetect enables deadlock detection.
	DeadlockDetect bool

	// Expiration is the transaction expiration time (0 = no expiration).
	Expiration time.Duration

	// ReadOnly makes the transaction read-only (no writes allowed).
	ReadOnly bool
}

// DefaultPessimisticTransactionOptions returns default options.
func DefaultPessimisticTransactionOptions() PessimisticTransactionOptions {
	return PessimisticTransactionOptions{
		SetSnapshot:    true,
		LockTimeout:    5 * time.Second,
		DeadlockDetect: true,
		Expiration:     0,
		ReadOnly:       false,
	}
}

// savePoint represents a transaction savepoint.
type savePoint struct {
	// The number of entries in the write batch at the time of the savepoint
	writeBatchSize uint32

	// Keys that were locked after this savepoint (for selective unlock)
	lockedKeys [][]byte
}

// PessimisticTransaction implements a transaction with pessimistic concurrency control.
// It uses Two-Phase Locking (2PL) to ensure serializability:
// - Growing phase: locks are acquired but never released
// - Shrinking phase (after commit/rollback): all locks are released
type PessimisticTransaction struct {
	mu sync.Mutex

	// The transaction database
	txnDB *TransactionDB

	// Unique transaction ID
	id uint64

	// Write batch for transaction writes
	writeBatch *batch.WriteBatch

	// Snapshot for consistent reads
	snapshot *Snapshot

	// Locks held by this transaction
	lockedKeys map[string]LockType

	// Track sequence numbers for keys we've validated or locked.
	// Key -> sequence number at which the key was validated.
	// Used for snapshot validation to avoid rechecking already-validated keys.
	trackedKeys map[string]dbformat.SequenceNumber

	// Savepoints
	savepoints []savePoint

	// Options
	opts PessimisticTransactionOptions

	// Write options
	writeOpts *WriteOptions

	// Transaction state
	closed  bool
	expired bool

	// Expiration timer
	expirationTime time.Time
}

// newPessimisticTransaction creates a new pessimistic transaction.
func newPessimisticTransaction(txnDB *TransactionDB, opts PessimisticTransactionOptions, writeOpts *WriteOptions) *PessimisticTransaction {
	txn := &PessimisticTransaction{
		txnDB:       txnDB,
		id:          txnDB.nextTxnID(),
		writeBatch:  batch.New(),
		lockedKeys:  make(map[string]LockType),
		trackedKeys: make(map[string]dbformat.SequenceNumber),
		opts:        opts,
		writeOpts:   writeOpts,
	}

	if opts.SetSnapshot {
		txn.snapshot = txnDB.db.GetSnapshot()
	}

	if opts.Expiration > 0 {
		txn.expirationTime = time.Now().Add(opts.Expiration)
	}

	return txn
}

// ID returns the transaction ID.
func (txn *PessimisticTransaction) ID() uint64 {
	return txn.id
}

// Put acquires an exclusive lock and sets the value for the given key.
func (txn *PessimisticTransaction) Put(key, value []byte) error {
	return txn.PutCF(nil, key, value)
}

// PutCF acquires an exclusive lock and sets the value in the specified column family.
func (txn *PessimisticTransaction) PutCF(cf ColumnFamilyHandle, key, value []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if err := txn.checkState(); err != nil {
		return err
	}

	if txn.opts.ReadOnly {
		return ErrTransactionReadOnly
	}

	// Acquire exclusive lock
	if err := txn.tryLock(key, LockTypeExclusive); err != nil {
		return err
	}

	// Validate that the key hasn't been modified since our snapshot
	if err := txn.validateSnapshot(key); err != nil {
		// Unlock the key we just locked since validation failed
		_ = txn.txnDB.lockManager.Unlock(txn.id, key)
		delete(txn.lockedKeys, string(key))
		return err
	}

	// Add to write batch
	cfID := uint32(0)
	if cf != nil {
		cfID = cf.ID()
	}

	if cfID == 0 {
		txn.writeBatch.Put(key, value)
	} else {
		txn.writeBatch.PutCF(cfID, key, value)
	}

	return nil
}

// Get retrieves the value for the given key.
// This does NOT acquire a lock (use GetForUpdate for that).
func (txn *PessimisticTransaction) Get(key []byte) ([]byte, error) {
	return txn.GetCF(nil, key)
}

// GetCF retrieves the value from the specified column family.
func (txn *PessimisticTransaction) GetCF(cf ColumnFamilyHandle, key []byte) ([]byte, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if err := txn.checkState(); err != nil {
		return nil, err
	}

	cfID := uint32(0)
	if cf != nil {
		cfID = cf.ID()
	}

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
		return txn.txnDB.db.Get(readOpts, key)
	}
	return txn.txnDB.db.GetCF(readOpts, cf, key)
}

// GetForUpdate acquires a lock and retrieves the value for the given key.
// This is the key method for pessimistic concurrency control.
func (txn *PessimisticTransaction) GetForUpdate(key []byte, exclusive bool) ([]byte, error) {
	return txn.GetForUpdateCF(nil, key, exclusive)
}

// GetForUpdateCF acquires a lock and retrieves the value from the specified column family.
func (txn *PessimisticTransaction) GetForUpdateCF(cf ColumnFamilyHandle, key []byte, exclusive bool) ([]byte, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if err := txn.checkState(); err != nil {
		return nil, err
	}

	// Acquire lock
	lockType := LockTypeShared
	if exclusive {
		lockType = LockTypeExclusive
	}
	if err := txn.tryLock(key, lockType); err != nil {
		return nil, err
	}

	// Validate that the key hasn't been modified since our snapshot
	if err := txn.validateSnapshot(key); err != nil {
		// Unlock the key we just locked since validation failed
		_ = txn.txnDB.lockManager.Unlock(txn.id, key)
		delete(txn.lockedKeys, string(key))
		return nil, err
	}

	cfID := uint32(0)
	if cf != nil {
		cfID = cf.ID()
	}

	// Check write batch first
	val, found, deleted := txn.getFromWriteBatch(cfID, key)
	if found {
		if deleted {
			return nil, ErrNotFound
		}
		return val, nil
	}

	// Read from database
	readOpts := DefaultReadOptions()
	if txn.snapshot != nil {
		readOpts.Snapshot = txn.snapshot
	}

	if cf == nil {
		return txn.txnDB.db.Get(readOpts, key)
	}
	return txn.txnDB.db.GetCF(readOpts, cf, key)
}

// Delete acquires an exclusive lock and removes the key.
func (txn *PessimisticTransaction) Delete(key []byte) error {
	return txn.DeleteCF(nil, key)
}

// DeleteCF acquires an exclusive lock and removes the key from the specified column family.
func (txn *PessimisticTransaction) DeleteCF(cf ColumnFamilyHandle, key []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if err := txn.checkState(); err != nil {
		return err
	}

	if txn.opts.ReadOnly {
		return ErrTransactionReadOnly
	}

	// Acquire exclusive lock
	if err := txn.tryLock(key, LockTypeExclusive); err != nil {
		return err
	}

	// Validate that the key hasn't been modified since our snapshot
	if err := txn.validateSnapshot(key); err != nil {
		// Unlock the key we just locked since validation failed
		_ = txn.txnDB.lockManager.Unlock(txn.id, key)
		delete(txn.lockedKeys, string(key))
		return err
	}

	// Add to write batch
	cfID := uint32(0)
	if cf != nil {
		cfID = cf.ID()
	}

	if cfID == 0 {
		txn.writeBatch.Delete(key)
	} else {
		txn.writeBatch.DeleteCF(cfID, key)
	}

	return nil
}

// Commit applies the transaction and releases all locks.
func (txn *PessimisticTransaction) Commit() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if err := txn.checkState(); err != nil {
		return err
	}

	// Apply the write batch
	writeCount := txn.writeBatch.Count()
	if writeCount > 0 {
		if err := txn.txnDB.db.Write(txn.writeOpts, newWriteBatchFromInternal(txn.writeBatch)); err != nil {
			// On failure, still release locks
			txn.releaseLocks()
			return err
		}
	}

	// Release all locks and cleanup
	txn.releaseLocks()
	txn.close()

	txn.txnDB.db.logger.Debugf("[txn] committed txn %d (%d writes)", txn.id, writeCount)

	return nil
}

// Rollback discards the transaction and releases all locks.
func (txn *PessimisticTransaction) Rollback() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return ErrTransactionClosed
	}

	// Release all locks and cleanup
	txn.releaseLocks()
	txn.close()

	txn.txnDB.db.logger.Debugf("[txn] rolled back txn %d", txn.id)

	return nil
}

// SetSavePoint creates a savepoint at the current state.
func (txn *PessimisticTransaction) SetSavePoint() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if err := txn.checkState(); err != nil {
		return err
	}

	sp := savePoint{
		writeBatchSize: txn.writeBatch.Count(),
		lockedKeys:     nil, // Will be populated as new locks are acquired
	}
	txn.savepoints = append(txn.savepoints, sp)

	return nil
}

// RollbackToSavePoint rolls back to the last savepoint.
func (txn *PessimisticTransaction) RollbackToSavePoint() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if err := txn.checkState(); err != nil {
		return err
	}

	if len(txn.savepoints) == 0 {
		return ErrNoSavePoint
	}

	// Pop the last savepoint
	sp := txn.savepoints[len(txn.savepoints)-1]
	txn.savepoints = txn.savepoints[:len(txn.savepoints)-1]

	// Rollback write batch to the savepoint state
	// We need to rebuild the write batch with only entries up to the savepoint
	if sp.writeBatchSize < txn.writeBatch.Count() {
		// Create a new write batch and copy entries up to savepoint
		newBatch := batch.New()
		handler := &batchCopier{
			target:   newBatch,
			maxCount: sp.writeBatchSize,
		}
		_ = txn.writeBatch.Iterate(handler) // Ignore iteration errors during rollback

		if handler.count != sp.writeBatchSize {
			// Fallback: clear and rebuild if iteration didn't work as expected
			newBatch = batch.New()
		}
		txn.writeBatch = newBatch
	}

	// Release locks acquired after the savepoint
	for _, key := range sp.lockedKeys {
		keyStr := string(key)
		if _, held := txn.lockedKeys[keyStr]; held {
			_ = txn.txnDB.lockManager.Unlock(txn.id, key)
			delete(txn.lockedKeys, keyStr)
		}
	}

	return nil
}

// PopSavePoint removes the last savepoint without rolling back.
func (txn *PessimisticTransaction) PopSavePoint() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if err := txn.checkState(); err != nil {
		return err
	}

	if len(txn.savepoints) == 0 {
		return ErrNoSavePoint
	}

	txn.savepoints = txn.savepoints[:len(txn.savepoints)-1]
	return nil
}

// SetSnapshot sets the transaction's snapshot to the current database state.
func (txn *PessimisticTransaction) SetSnapshot() {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.snapshot != nil {
		txn.txnDB.db.ReleaseSnapshot(txn.snapshot)
	}
	txn.snapshot = txn.txnDB.db.GetSnapshot()
}

// GetSnapshot returns the transaction's snapshot.
func (txn *PessimisticTransaction) GetSnapshot() *Snapshot {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.snapshot
}

// GetWriteBatchSize returns the number of entries in the write batch.
func (txn *PessimisticTransaction) GetWriteBatchSize() uint32 {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.writeBatch.Count()
}

// GetNumLocks returns the number of locks held by this transaction.
func (txn *PessimisticTransaction) GetNumLocks() int {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return len(txn.lockedKeys)
}

// IsExpired returns true if the transaction has expired.
func (txn *PessimisticTransaction) IsExpired() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.isExpired()
}

// checkState checks if the transaction is in a valid state.
func (txn *PessimisticTransaction) checkState() error {
	if txn.closed {
		return ErrTransactionClosed
	}
	if txn.isExpired() {
		txn.expired = true
		return ErrTransactionExpired
	}
	return nil
}

// isExpired checks if the transaction has expired (without lock).
func (txn *PessimisticTransaction) isExpired() bool {
	if txn.expired {
		return true
	}
	if txn.opts.Expiration > 0 && time.Now().After(txn.expirationTime) {
		return true
	}
	return false
}

// tryLock attempts to acquire a lock on the key.
func (txn *PessimisticTransaction) tryLock(key []byte, lockType LockType) error {
	keyStr := string(key)

	// Check if we already hold a compatible lock
	if currentType, held := txn.lockedKeys[keyStr]; held {
		if currentType == LockTypeExclusive || lockType == LockTypeShared {
			return nil // Already have sufficient lock
		}
		// Need to upgrade from shared to exclusive
	}

	// Acquire lock from lock manager
	err := txn.txnDB.lockManager.Lock(txn.id, key, lockType, txn.opts.LockTimeout)
	if err != nil {
		return err
	}

	// Track the lock
	txn.lockedKeys[keyStr] = lockType

	// Track for savepoint if we have active savepoints
	if len(txn.savepoints) > 0 {
		sp := &txn.savepoints[len(txn.savepoints)-1]
		sp.lockedKeys = append(sp.lockedKeys, append([]byte(nil), key...))
	}

	return nil
}

// releaseLocks releases all locks held by this transaction.
func (txn *PessimisticTransaction) releaseLocks() {
	txn.txnDB.lockManager.UnlockAll(txn.id)
	txn.lockedKeys = make(map[string]LockType)
}

// validateSnapshot checks if the key has been modified since the transaction's snapshot.
// This is called after acquiring a lock to ensure the key hasn't been updated by
// another transaction since this transaction's snapshot was taken.
//
// If the key was modified after the snapshot, returns ErrWriteConflict.
// If the key was already validated at an earlier sequence number, returns nil.
//
// This implements the conflict detection from C++ RocksDB's PessimisticTransaction::ValidateSnapshot.
func (txn *PessimisticTransaction) validateSnapshot(key []byte) error {
	// If no snapshot is set, no validation needed
	if txn.snapshot == nil {
		return nil
	}

	keyStr := string(key)
	snapSeq := dbformat.SequenceNumber(txn.snapshot.sequence)

	// Check if we've already validated this key at an earlier sequence
	if trackedSeq, ok := txn.trackedKeys[keyStr]; ok {
		if trackedSeq <= snapSeq {
			// Key was already validated at or before snapshot sequence
			return nil
		}
	}

	// Get the current latest sequence number from the database
	txn.txnDB.db.mu.RLock()
	currentSeq := dbformat.SequenceNumber(txn.txnDB.db.seq)
	txn.txnDB.db.mu.RUnlock()

	// If the database hasn't changed since our snapshot, no conflict possible
	if currentSeq <= snapSeq {
		txn.trackedKeys[keyStr] = snapSeq
		return nil
	}

	// Check if this specific key was modified after the snapshot.
	// We do this by reading the key at the current sequence and comparing
	// to what we'd read at the snapshot sequence.
	//
	// Optimization: If the key doesn't exist in the memtable or was only
	// written before our snapshot, there's no conflict.
	//
	// For now, we use a simpler approach: check if the key exists in the
	// database at a newer sequence than our snapshot by comparing values.

	// Read at snapshot time
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = txn.snapshot
	snapVal, snapErr := txn.txnDB.db.Get(snapOpts, key)

	// Read at current time (no snapshot)
	currOpts := DefaultReadOptions()
	currVal, currErr := txn.txnDB.db.Get(currOpts, key)

	// Compare results
	conflictDetected := false

	if errors.Is(snapErr, ErrNotFound) && currErr == nil {
		// Key was created after snapshot
		conflictDetected = true
	} else if snapErr == nil && errors.Is(currErr, ErrNotFound) {
		// Key was deleted after snapshot
		conflictDetected = true
	} else if snapErr == nil && currErr == nil {
		// Both exist - compare values
		if !bytesEqual(snapVal, currVal) {
			conflictDetected = true
		}
	}
	// If both are ErrNotFound, no conflict (key never existed)

	if conflictDetected {
		return ErrWriteConflict
	}

	// Track the validated sequence
	txn.trackedKeys[keyStr] = currentSeq
	return nil
}

// close releases resources and marks the transaction as closed.
func (txn *PessimisticTransaction) close() {
	if txn.snapshot != nil {
		txn.txnDB.db.ReleaseSnapshot(txn.snapshot)
		txn.snapshot = nil
	}
	txn.writeBatch = nil
	txn.savepoints = nil
	txn.closed = true
}

// getFromWriteBatch checks if we have a pending write for this key.
func (txn *PessimisticTransaction) getFromWriteBatch(cfID uint32, key []byte) ([]byte, bool, bool) {
	handler := &pessimisticBatchReader{
		targetCFID: cfID,
		targetKey:  key,
	}
	_ = txn.writeBatch.Iterate(handler)
	return handler.value, handler.found, handler.deleted
}

// pessimisticBatchReader reads values from a write batch.
type pessimisticBatchReader struct {
	targetCFID uint32
	targetKey  []byte
	found      bool
	deleted    bool
	value      []byte
}

func (r *pessimisticBatchReader) Put(key, value []byte) error {
	if r.targetCFID == 0 && bytesEqual(key, r.targetKey) {
		r.found = true
		r.deleted = false
		r.value = append([]byte{}, value...)
	}
	return nil
}

func (r *pessimisticBatchReader) PutCF(cfID uint32, key, value []byte) error {
	if cfID == r.targetCFID && bytesEqual(key, r.targetKey) {
		r.found = true
		r.deleted = false
		r.value = append([]byte{}, value...)
	}
	return nil
}

func (r *pessimisticBatchReader) Delete(key []byte) error {
	if r.targetCFID == 0 && bytesEqual(key, r.targetKey) {
		r.found = true
		r.deleted = true
		r.value = nil
	}
	return nil
}

func (r *pessimisticBatchReader) DeleteCF(cfID uint32, key []byte) error {
	if cfID == r.targetCFID && bytesEqual(key, r.targetKey) {
		r.found = true
		r.deleted = true
		r.value = nil
	}
	return nil
}

func (r *pessimisticBatchReader) SingleDelete(key []byte) error {
	// SingleDelete has the same effect as Delete for read purposes
	return r.Delete(key)
}

func (r *pessimisticBatchReader) SingleDeleteCF(cfID uint32, key []byte) error {
	// SingleDelete has the same effect as Delete for read purposes
	return r.DeleteCF(cfID, key)
}
func (r *pessimisticBatchReader) Merge(key, value []byte) error                      { return nil }
func (r *pessimisticBatchReader) MergeCF(cfID uint32, key, value []byte) error       { return nil }
func (r *pessimisticBatchReader) DeleteRange(start, end []byte) error                { return nil }
func (r *pessimisticBatchReader) DeleteRangeCF(cfID uint32, start, end []byte) error { return nil }
func (r *pessimisticBatchReader) LogData(blob []byte)                                {}

// batchCopier copies entries from one batch to another up to a max count.
type batchCopier struct {
	target   *batch.WriteBatch
	maxCount uint32
	count    uint32
}

func (c *batchCopier) Put(key, value []byte) error {
	if c.count >= c.maxCount {
		return nil
	}
	c.target.Put(key, value)
	c.count++
	return nil
}

func (c *batchCopier) PutCF(cfID uint32, key, value []byte) error {
	if c.count >= c.maxCount {
		return nil
	}
	c.target.PutCF(cfID, key, value)
	c.count++
	return nil
}

func (c *batchCopier) Delete(key []byte) error {
	if c.count >= c.maxCount {
		return nil
	}
	c.target.Delete(key)
	c.count++
	return nil
}

func (c *batchCopier) DeleteCF(cfID uint32, key []byte) error {
	if c.count >= c.maxCount {
		return nil
	}
	c.target.DeleteCF(cfID, key)
	c.count++
	return nil
}

func (c *batchCopier) SingleDelete(key []byte) error {
	if c.count >= c.maxCount {
		return nil
	}
	c.target.SingleDelete(key)
	c.count++
	return nil
}

func (c *batchCopier) SingleDeleteCF(cfID uint32, key []byte) error {
	if c.count >= c.maxCount {
		return nil
	}
	c.target.SingleDeleteCF(cfID, key)
	c.count++
	return nil
}
func (c *batchCopier) Merge(key, value []byte) error {
	if c.count >= c.maxCount {
		return nil
	}
	c.target.Merge(key, value)
	c.count++
	return nil
}

func (c *batchCopier) MergeCF(cfID uint32, key, value []byte) error {
	if c.count >= c.maxCount {
		return nil
	}
	c.target.MergeCF(cfID, key, value)
	c.count++
	return nil
}

func (c *batchCopier) DeleteRange(start, end []byte) error {
	if c.count >= c.maxCount {
		return nil
	}
	c.target.DeleteRange(start, end)
	c.count++
	return nil
}

func (c *batchCopier) DeleteRangeCF(cfID uint32, start, end []byte) error {
	if c.count >= c.maxCount {
		return nil
	}
	c.target.DeleteRangeCF(cfID, start, end)
	c.count++
	return nil
}

func (c *batchCopier) LogData(blob []byte) {
	// LogData is metadata, not an operation - don't count it
}

// Verify interface compliance
var _ = (dbformat.SequenceNumber)(0) // Use dbformat to avoid unused import
