package rockyardkv

// write_prepared_txn.go implements Write-Prepared Transactions for RocksDB compatibility.
//
// Write-Prepared transactions use a two-phase commit protocol:
// 1. PREPARE: Writes go to memtable with a prepare sequence number
// 2. COMMIT: A commit entry is added to a commit cache
//
// Benefits:
// - Writes are visible in memtable before commit (for 2PC recovery)
// - Better performance than pessimistic for high-contention workloads
// - Supports external coordinators (XA transactions)
//
// Reference: RocksDB v10.7.5
//   - utilities/transactions/write_prepared_txn.cc
//   - utilities/transactions/write_prepared_txn_db.cc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/wal"
)

// Write-prepared transaction errors
var (
	// ErrTxnNotPrepared is returned when trying to commit an unprepared transaction.
	ErrTxnNotPrepared = errors.New("db: transaction not prepared")

	// ErrTxnAlreadyPrepared is returned when trying to prepare an already prepared transaction.
	ErrTxnAlreadyPrepared = errors.New("db: transaction already prepared")

	// ErrTxnPrepareConflict is returned when prepare fails due to conflict.
	ErrTxnPrepareConflict = errors.New("db: prepare conflict")
)

// TransactionState represents the state of a write-prepared transaction.
type TransactionState int

const (
	// TxnStateStarted is the initial state.
	TxnStateStarted TransactionState = iota
	// TxnStatePrepared means the transaction has been prepared but not committed.
	TxnStatePrepared
	// TxnStateCommitted means the transaction has been committed.
	TxnStateCommitted
	// TxnStateRolledBack means the transaction was rolled back.
	TxnStateRolledBack
)

// commitCache tracks prepared->committed sequence mappings.
// It's used to determine if data written by a prepared transaction
// should be visible to a reader at a given sequence number.
type commitCache struct {
	mu sync.RWMutex

	// Maps prepareSeq -> commitSeq
	cache map[uint64]uint64

	// Maximum evicted prepare sequence (for compaction)
	maxEvicted uint64

	// Capacity before eviction
	capacity int
}

// newCommitCache creates a new commit cache.
func newCommitCache(capacity int) *commitCache {
	if capacity <= 0 {
		capacity = 8 * 1024 // 8K entries default
	}
	return &commitCache{
		cache:    make(map[uint64]uint64, capacity),
		capacity: capacity,
	}
}

// Add records a prepare->commit mapping.
func (c *commitCache) Add(prepareSeq, commitSeq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[prepareSeq] = commitSeq

	// Evict old entries if over capacity
	if len(c.cache) > c.capacity {
		c.evictOldest()
	}
}

// Get returns the commit sequence for a prepare sequence.
// Returns (commitSeq, true) if found, (0, false) otherwise.
func (c *commitCache) Get(prepareSeq uint64) (uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	commitSeq, ok := c.cache[prepareSeq]
	return commitSeq, ok
}

// IsCommitted checks if a prepare sequence has been committed.
// Also returns true if the sequence is old enough to have been evicted.
func (c *commitCache) IsCommitted(prepareSeq uint64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// If evicted, it was committed (we only evict committed entries)
	if prepareSeq <= c.maxEvicted {
		return true
	}

	_, ok := c.cache[prepareSeq]
	return ok
}

// evictOldest removes the oldest entries from the cache.
// Must be called with mu held.
func (c *commitCache) evictOldest() {
	// Find and remove the oldest entries (lowest prepareSeq)
	evictCount := len(c.cache) - c.capacity + 100 // Evict 100 extra

	if evictCount <= 0 {
		return
	}

	// Find the minimum sequence numbers to evict
	for prepareSeq := range c.cache {
		if evictCount <= 0 {
			break
		}
		if prepareSeq > c.maxEvicted {
			c.maxEvicted = prepareSeq
		}
		delete(c.cache, prepareSeq)
		evictCount--
	}
}

// prepareHeap tracks in-flight prepared transactions.
type prepareHeap struct {
	mu sync.RWMutex

	// Set of prepare sequence numbers that are still in-flight
	prepared map[uint64]struct{}

	// Minimum prepare sequence (optimization for IsInPrepareHeap)
	minPrepared uint64
}

// newPrepareHeap creates a new prepare heap.
func newPrepareHeap() *prepareHeap {
	return &prepareHeap{
		prepared:    make(map[uint64]struct{}),
		minPrepared: ^uint64(0), // Max value
	}
}

// Add adds a prepare sequence to the heap.
func (h *prepareHeap) Add(prepareSeq uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.prepared[prepareSeq] = struct{}{}
	if prepareSeq < h.minPrepared {
		h.minPrepared = prepareSeq
	}
}

// Remove removes a prepare sequence from the heap.
func (h *prepareHeap) Remove(prepareSeq uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.prepared, prepareSeq)

	// Update min if needed
	if prepareSeq == h.minPrepared && len(h.prepared) > 0 {
		h.minPrepared = ^uint64(0)
		for seq := range h.prepared {
			if seq < h.minPrepared {
				h.minPrepared = seq
			}
		}
	}
}

// Contains checks if a prepare sequence is in the heap.
func (h *prepareHeap) Contains(prepareSeq uint64) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	_, ok := h.prepared[prepareSeq]
	return ok
}

// WritePreparedTxnDB extends TransactionDB with write-prepared transaction support.
type WritePreparedTxnDB struct {
	*TransactionDB

	// Commit cache for tracking prepared->committed mappings
	commitCache *commitCache

	// Prepare heap for tracking in-flight prepared transactions
	prepareHeap *prepareHeap

	// Sequence number for the next prepare
	prepareSeq atomic.Uint64

	// Recovered prepared transactions that need resolution
	recovered recoveredPreparedTxns
}

// RecoveredPreparedTxn represents a prepared transaction that was found
// during recovery but was neither committed nor rolled back.
type RecoveredPreparedTxn struct {
	Name       string
	PrepareSeq uint64
	Keys       [][]byte // Keys that were written in this transaction
}

// recoveredPreparedTxns stores prepared transactions that survived a crash
// and need to be resolved (committed or rolled back) by the application.
type recoveredPreparedTxns struct {
	mu   sync.RWMutex
	txns map[string]*RecoveredPreparedTxn
}

// OpenWritePreparedTxnDB opens a database with write-prepared transaction support.
func OpenWritePreparedTxnDB(path string, opts *Options, txnOpts TransactionDBOptions) (*WritePreparedTxnDB, error) {
	txnDB, err := OpenTransactionDB(path, opts, txnOpts)
	if err != nil {
		return nil, err
	}

	wpDB := &WritePreparedTxnDB{
		TransactionDB: txnDB,
		commitCache:   newCommitCache(8 * 1024),
		prepareHeap:   newPrepareHeap(),
		recovered: recoveredPreparedTxns{
			txns: make(map[string]*RecoveredPreparedTxn),
		},
	}

	// Initialize prepare sequence from current DB sequence
	wpDB.prepareSeq.Store(txnDB.db.seq)

	// Perform 2PC recovery: scan WAL for prepared/committed/rolled-back transactions
	if err := wpDB.recover2PC(); err != nil {
		_ = txnDB.Close()
		return nil, fmt.Errorf("2PC recovery failed: %w", err)
	}

	return wpDB, nil
}

// recover2PC scans the WAL to identify 2PC transaction state and clean up
// rolled-back transactions.
func (db *WritePreparedTxnDB) recover2PC() error {
	// Scan the WAL with 2PC-aware handler
	preparedTxns, committedTxns, rolledBackTxns, err := db.scanWALFor2PC()
	if err != nil {
		return err
	}

	// For rolled-back transactions, we need to remove their data from memtable
	// This is done by writing delete tombstones for all keys in rolled-back transactions
	for name := range rolledBackTxns {
		if txn, ok := preparedTxns[name]; ok {
			// Delete all keys that were written by this rolled-back transaction
			for _, key := range txn.Keys {
				// Write a deletion to cancel out the rolled-back put
				if err := db.db.Delete(nil, key); err != nil {
					return fmt.Errorf("failed to cleanup rolled-back key: %w", err)
				}
			}
		}
	}

	// For committed transactions, update the commit cache
	for name := range committedTxns {
		if txn, ok := preparedTxns[name]; ok {
			// Add to commit cache so reads work correctly
			db.commitCache.Add(txn.PrepareSeq, txn.PrepareSeq+1)
		}
	}

	// For prepared-but-not-committed/rolled-back transactions, add to prepare heap
	// and store for application resolution
	for name, txn := range preparedTxns {
		if !committedTxns[name] && !rolledBackTxns[name] {
			db.prepareHeap.Add(txn.PrepareSeq)
			// Store for application to resolve
			db.recovered.mu.Lock()
			db.recovered.txns[name] = txn
			db.recovered.mu.Unlock()
		}
	}

	return nil
}

// GetAllPreparedTransactions returns all prepared transactions that were
// recovered after a crash and need to be resolved (committed or rolled back).
func (db *WritePreparedTxnDB) GetAllPreparedTransactions() []*RecoveredPreparedTxn {
	db.recovered.mu.RLock()
	defer db.recovered.mu.RUnlock()

	result := make([]*RecoveredPreparedTxn, 0, len(db.recovered.txns))
	for _, txn := range db.recovered.txns {
		result = append(result, txn)
	}
	return result
}

// CommitPreparedTransaction commits a recovered prepared transaction by name.
func (db *WritePreparedTxnDB) CommitPreparedTransaction(name string) error {
	db.recovered.mu.Lock()
	txn, ok := db.recovered.txns[name]
	if !ok {
		db.recovered.mu.Unlock()
		return fmt.Errorf("db: prepared transaction %q not found", name)
	}
	delete(db.recovered.txns, name)
	db.recovered.mu.Unlock()

	// Write commit marker to WAL
	commitBatch := batch.New()
	commitBatch.MarkCommit([]byte(name))
	if err := db.db.Write(nil, newWriteBatchFromInternal(commitBatch)); err != nil {
		return err
	}

	// Add to commit cache
	db.commitCache.Add(txn.PrepareSeq, txn.PrepareSeq+1)

	// Remove from prepare heap
	db.prepareHeap.Remove(txn.PrepareSeq)

	return nil
}

// RollbackPreparedTransaction rolls back a recovered prepared transaction by name.
func (db *WritePreparedTxnDB) RollbackPreparedTransaction(name string) error {
	db.recovered.mu.Lock()
	txn, ok := db.recovered.txns[name]
	if !ok {
		db.recovered.mu.Unlock()
		return fmt.Errorf("db: prepared transaction %q not found", name)
	}
	delete(db.recovered.txns, name)
	db.recovered.mu.Unlock()

	// Write rollback marker to WAL
	rollbackBatch := batch.New()
	rollbackBatch.MarkRollback([]byte(name))
	if err := db.db.Write(nil, newWriteBatchFromInternal(rollbackBatch)); err != nil {
		return err
	}

	// Delete all keys that were written by this transaction
	for _, key := range txn.Keys {
		if err := db.db.Delete(nil, key); err != nil {
			return fmt.Errorf("failed to cleanup rolled-back key: %w", err)
		}
	}

	// Remove from prepare heap
	db.prepareHeap.Remove(txn.PrepareSeq)

	return nil
}

// scanWALFor2PC scans the WAL files to extract 2PC transaction information.
func (db *WritePreparedTxnDB) scanWALFor2PC() (
	preparedTxns map[string]*RecoveredPreparedTxn,
	committedTxns map[string]bool,
	rolledBackTxns map[string]bool,
	err error,
) {
	preparedTxns = make(map[string]*RecoveredPreparedTxn)
	committedTxns = make(map[string]bool)
	rolledBackTxns = make(map[string]bool)

	// Find log files to scan
	logFiles, err := db.db.findLogFiles()
	if err != nil {
		return nil, nil, nil, err
	}

	// Get minimum log number to replay
	minLogNumber := db.db.versions.LogNumber()

	// Scan each log file
	for _, logNum := range logFiles {
		if logNum < minLogNumber {
			continue
		}

		if err := db.scanLogFileFor2PC(logNum, preparedTxns, committedTxns, rolledBackTxns); err != nil {
			// Non-fatal - log might be empty or partially written
			continue
		}
	}

	return preparedTxns, committedTxns, rolledBackTxns, nil
}

// scanLogFileFor2PC scans a single log file for 2PC markers.
func (db *WritePreparedTxnDB) scanLogFileFor2PC(
	logNum uint64,
	preparedTxns map[string]*RecoveredPreparedTxn,
	committedTxns map[string]bool,
	rolledBackTxns map[string]bool,
) error {
	logPath := db.db.logFilePath(logNum)

	file, err := db.db.fs.Open(logPath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	reader := wal.NewReader(file, nil, true, logNum)
	handler := &wal2PCScanner{
		preparedTxns:   preparedTxns,
		committedTxns:  committedTxns,
		rolledBackTxns: rolledBackTxns,
	}

	for {
		record, err := reader.ReadRecord()
		if err != nil {
			break // EOF or error
		}
		if record == nil {
			break
		}

		wb, err := batch.NewFromData(record)
		if err != nil {
			continue
		}

		handler.currentSeq = wb.Sequence()
		if err := wb.Iterate(handler); err != nil {
			continue
		}
	}

	return nil
}

// wal2PCScanner scans WAL batches for 2PC markers.
type wal2PCScanner struct {
	preparedTxns   map[string]*RecoveredPreparedTxn
	committedTxns  map[string]bool
	rolledBackTxns map[string]bool
	currentSeq     uint64
	inPrepare      bool
	currentPrepare *RecoveredPreparedTxn
}

// Compile-time check
var _ batch.Handler2PC = (*wal2PCScanner)(nil)

func (h *wal2PCScanner) Put(key, value []byte) error {
	if h.inPrepare && h.currentPrepare != nil {
		// Track this key as part of the prepared transaction
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		h.currentPrepare.Keys = append(h.currentPrepare.Keys, keyCopy)
	}
	return nil
}

func (h *wal2PCScanner) Delete(key []byte) error                                  { return nil }
func (h *wal2PCScanner) SingleDelete(key []byte) error                            { return nil }
func (h *wal2PCScanner) Merge(key, value []byte) error                            { return nil }
func (h *wal2PCScanner) DeleteRange(startKey, endKey []byte) error                { return nil }
func (h *wal2PCScanner) LogData(blob []byte)                                      {}
func (h *wal2PCScanner) PutCF(cfID uint32, key, value []byte) error               { return h.Put(key, value) }
func (h *wal2PCScanner) DeleteCF(cfID uint32, key []byte) error                   { return nil }
func (h *wal2PCScanner) SingleDeleteCF(cfID uint32, key []byte) error             { return nil }
func (h *wal2PCScanner) MergeCF(cfID uint32, key, value []byte) error             { return nil }
func (h *wal2PCScanner) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error { return nil }

func (h *wal2PCScanner) MarkBeginPrepare(unprepared bool) error {
	h.inPrepare = true
	h.currentPrepare = &RecoveredPreparedTxn{
		PrepareSeq: h.currentSeq,
		Keys:       make([][]byte, 0),
	}
	return nil
}

func (h *wal2PCScanner) MarkEndPrepare(xid []byte) error {
	if h.inPrepare && h.currentPrepare != nil {
		name := string(xid)
		h.currentPrepare.Name = name
		h.preparedTxns[name] = h.currentPrepare
	}
	h.inPrepare = false
	h.currentPrepare = nil
	return nil
}

func (h *wal2PCScanner) MarkCommit(xid []byte) error {
	h.committedTxns[string(xid)] = true
	return nil
}

func (h *wal2PCScanner) MarkRollback(xid []byte) error {
	h.rolledBackTxns[string(xid)] = true
	return nil
}

// BeginWritePreparedTransaction starts a new write-prepared transaction.
func (db *WritePreparedTxnDB) BeginWritePreparedTransaction(opts PessimisticTransactionOptions, writeOpts *WriteOptions) *WritePreparedTxn {
	if writeOpts == nil {
		writeOpts = DefaultWriteOptions()
	}

	return newWritePreparedTxn(db, opts, writeOpts)
}

// nextPrepareSeq returns the next prepare sequence number.
func (db *WritePreparedTxnDB) nextPrepareSeq() uint64 {
	return db.prepareSeq.Add(1)
}

// WritePreparedTxn implements a write-prepared transaction.
type WritePreparedTxn struct {
	*PessimisticTransaction

	// The write-prepared transaction DB
	wpDB *WritePreparedTxnDB

	// Prepare sequence number (assigned during Prepare)
	prepareSeq uint64

	// Commit sequence number (assigned during Commit)
	commitSeq uint64

	// Transaction state
	state TransactionState

	// Name for external coordination (e.g., XA)
	name string
}

// newWritePreparedTxn creates a new write-prepared transaction.
func newWritePreparedTxn(wpDB *WritePreparedTxnDB, opts PessimisticTransactionOptions, writeOpts *WriteOptions) *WritePreparedTxn {
	return &WritePreparedTxn{
		PessimisticTransaction: newPessimisticTransaction(wpDB.TransactionDB, opts, writeOpts),
		wpDB:                   wpDB,
		state:                  TxnStateStarted,
	}
}

// SetName sets the transaction name for external coordination.
func (txn *WritePreparedTxn) SetName(name string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != TxnStateStarted {
		return errors.New("db: can only set name on started transaction")
	}
	txn.name = name
	return nil
}

// GetName returns the transaction name.
func (txn *WritePreparedTxn) GetName() string {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.name
}

// Prepare prepares the transaction for commit.
// After Prepare, the writes are in the WAL/memtable but not yet visible.
// This implements the first phase of two-phase commit.
func (txn *WritePreparedTxn) Prepare() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if err := txn.checkState(); err != nil {
		return err
	}

	if txn.state == TxnStatePrepared {
		return ErrTxnAlreadyPrepared
	}

	if txn.state != TxnStateStarted {
		return errors.New("db: invalid transaction state for prepare")
	}

	// Get a prepare sequence number
	txn.prepareSeq = txn.wpDB.nextPrepareSeq()

	// Write to WAL with prepare markers
	if txn.writeBatch.Count() > 0 {
		// Create a new batch with prepare markers wrapping the data
		// Order must be: BeginPrepare, <data>, EndPrepare
		prepareBatch := batch.New()

		// Add BeginPrepare marker first
		prepareBatch.MarkBeginPrepare()

		// Append the transaction's data
		prepareBatch.Append(txn.writeBatch)

		// Add EndPrepare marker with transaction name at the end
		xid := []byte(txn.name)
		if len(xid) == 0 {
			// Generate a default name if none set
			xid = fmt.Appendf(nil, "txn_%d", txn.prepareSeq)
			txn.name = string(xid)
		}
		prepareBatch.MarkEndPrepare(xid)

		// Write the prepared batch to WAL
		if err := txn.wpDB.db.Write(txn.writeOpts, newWriteBatchFromInternal(prepareBatch)); err != nil {
			return err
		}
	}

	// Add to prepare heap
	txn.wpDB.prepareHeap.Add(txn.prepareSeq)

	txn.state = TxnStatePrepared
	return nil
}

// Commit commits a prepared transaction.
// This implements the second phase of two-phase commit.
func (txn *WritePreparedTxn) Commit() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if err := txn.checkState(); err != nil {
		return err
	}

	// For write-prepared, we require Prepare to be called first
	// unless there are no writes
	if txn.state == TxnStateStarted {
		if txn.writeBatch.Count() == 0 {
			// Empty transaction, just cleanup
			txn.releaseLocks()
			txn.close()
			txn.state = TxnStateCommitted
			return nil
		}
		// Auto-prepare if not already prepared
		txn.mu.Unlock()
		if err := txn.Prepare(); err != nil {
			txn.mu.Lock()
			return err
		}
		txn.mu.Lock()
	}

	if txn.state != TxnStatePrepared {
		return ErrTxnNotPrepared
	}

	// Get commit sequence
	txn.wpDB.db.mu.Lock()
	txn.commitSeq = txn.wpDB.db.seq
	txn.wpDB.db.mu.Unlock()

	// Write commit marker to WAL
	commitBatch := batch.New()
	commitBatch.MarkCommit([]byte(txn.name))
	if err := txn.wpDB.db.Write(txn.writeOpts, newWriteBatchFromInternal(commitBatch)); err != nil {
		return err
	}

	// Add to commit cache
	txn.wpDB.commitCache.Add(txn.prepareSeq, txn.commitSeq)

	// Remove from prepare heap
	txn.wpDB.prepareHeap.Remove(txn.prepareSeq)

	// Release locks and cleanup
	txn.releaseLocks()
	txn.close()

	txn.state = TxnStateCommitted
	return nil
}

// Rollback rolls back the transaction.
func (txn *WritePreparedTxn) Rollback() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return ErrTransactionClosed
	}

	// If prepared, write rollback marker to WAL and remove from prepare heap
	if txn.state == TxnStatePrepared {
		// Write rollback marker to WAL
		rollbackBatch := batch.New()
		rollbackBatch.MarkRollback([]byte(txn.name))
		if err := txn.wpDB.db.Write(txn.writeOpts, newWriteBatchFromInternal(rollbackBatch)); err != nil {
			return err
		}

		txn.wpDB.prepareHeap.Remove(txn.prepareSeq)
	}

	// Release locks and cleanup
	txn.releaseLocks()
	txn.close()

	txn.state = TxnStateRolledBack
	return nil
}

// GetState returns the current transaction state.
func (txn *WritePreparedTxn) GetState() TransactionState {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.state
}

// GetPrepareSeq returns the prepare sequence number.
func (txn *WritePreparedTxn) GetPrepareSeq() uint64 {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.prepareSeq
}

// GetCommitSeq returns the commit sequence number.
func (txn *WritePreparedTxn) GetCommitSeq() uint64 {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.commitSeq
}

// WaitAfterPrepare waits for the specified duration after prepare.
// This is useful for testing 2PC recovery scenarios.
func (txn *WritePreparedTxn) WaitAfterPrepare(d time.Duration) {
	txn.mu.Lock()
	state := txn.state
	txn.mu.Unlock()

	if state == TxnStatePrepared {
		time.Sleep(d)
	}
}
