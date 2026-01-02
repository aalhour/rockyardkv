package rockyardkv

// transaction_db.go implements TransactionDB wrapper for transactional operations.
//
// TransactionDB wraps a regular DB and provides pessimistic transactions
// with configurable isolation levels and lock management.
//
// Reference: RocksDB v10.7.5
//   - utilities/transactions/pessimistic_transaction_db.h
//   - utilities/transactions/pessimistic_transaction_db.cc

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aalhour/rockyardkv/internal/txn"
)

// TransactionDB wraps a regular DB and provides transactional operations.
// It supports both optimistic and pessimistic concurrency control.
type TransactionDB struct {
	// The underlying database
	db *dbImpl

	// Lock manager for pessimistic transactions
	lockManager *txn.LockManager

	// Transaction ID counter
	txnIDCounter uint64

	// Active transactions (for tracking/cleanup)
	mu         sync.RWMutex
	activeTxns map[uint64]*PessimisticTransaction

	// Configuration
	opts TransactionDBOptions
}

// TransactionDBOptions configures a TransactionDB.
type TransactionDBOptions struct {
	// LockManagerOptions configures the lock manager.
	LockManagerOptions LockManagerOptions

	// MaxNumLocks is the maximum number of locks to track (0 = unlimited).
	MaxNumLocks uint64

	// NumStripes is the number of lock stripes for lock manager striping (for future use).
	NumStripes int

	// TransactionLockTimeout is the default lock timeout for transactions.
	TransactionLockTimeout int64 // in milliseconds
}

// DefaultTransactionDBOptions returns default options.
func DefaultTransactionDBOptions() TransactionDBOptions {
	return TransactionDBOptions{
		LockManagerOptions:     DefaultLockManagerOptions(),
		MaxNumLocks:            0,
		NumStripes:             16,
		TransactionLockTimeout: 5000,
	}
}

// OpenTransactionDB opens or creates a TransactionDB.
func OpenTransactionDB(path string, dbOpts *Options, txnDBOpts TransactionDBOptions) (*TransactionDB, error) {
	opts := dbOpts
	if opts == nil {
		opts = DefaultOptions()
	}

	// Open the underlying database
	database, err := Open(path, opts)
	if err != nil {
		return nil, err
	}

	// Cast to *dbImpl
	dbImpl, ok := database.(*dbImpl)
	if !ok {
		_ = database.Close()
		return nil, fmt.Errorf("transactiondb: requires rockyardkv DB implementation, got %T", database)
	}

	txnDB := &TransactionDB{
		db:          dbImpl,
		lockManager: txn.NewLockManager(txnDBOpts.LockManagerOptions),
		activeTxns:  make(map[uint64]*PessimisticTransaction),
		opts:        txnDBOpts,
	}

	return txnDB, nil
}

// WrapDB wraps an existing database as a TransactionDB.
func WrapDB(database DB, txnDBOpts TransactionDBOptions) (*TransactionDB, error) {
	dbImpl, ok := database.(*dbImpl)
	if !ok {
		return nil, fmt.Errorf("transactiondb: requires rockyardkv DB implementation, got %T", database)
	}
	return &TransactionDB{
		db:          dbImpl,
		lockManager: txn.NewLockManager(txnDBOpts.LockManagerOptions),
		activeTxns:  make(map[uint64]*PessimisticTransaction),
		opts:        txnDBOpts,
	}, nil
}

// Close closes the TransactionDB and the underlying database.
func (txnDB *TransactionDB) Close() error {
	// Rollback all active transactions
	txnDB.mu.Lock()
	for _, txn := range txnDB.activeTxns {
		_ = txn.Rollback() // Best-effort rollback during close
	}
	txnDB.activeTxns = nil
	txnDB.mu.Unlock()

	return txnDB.db.Close()
}

// GetDB returns the underlying database.
func (txnDB *TransactionDB) GetDB() DB {
	return txnDB.db
}

// BeginTransaction begins a new pessimistic transaction.
func (txnDB *TransactionDB) BeginTransaction(opts PessimisticTransactionOptions, writeOpts *WriteOptions) *PessimisticTransaction {
	if writeOpts == nil {
		writeOpts = DefaultWriteOptions()
	}

	txn := newPessimisticTransaction(txnDB, opts, writeOpts)

	txnDB.mu.Lock()
	txnDB.activeTxns[txn.id] = txn
	txnDB.mu.Unlock()

	return txn
}

// GetTransactionByID returns the transaction with the given ID, or nil if not found.
func (txnDB *TransactionDB) GetTransactionByID(txnID uint64) *PessimisticTransaction {
	txnDB.mu.RLock()
	defer txnDB.mu.RUnlock()
	return txnDB.activeTxns[txnID]
}

// GetAllPreparedTransactions returns all transactions (for recovery).
func (txnDB *TransactionDB) GetAllPreparedTransactions() []*PessimisticTransaction {
	txnDB.mu.RLock()
	defer txnDB.mu.RUnlock()

	txns := make([]*PessimisticTransaction, 0, len(txnDB.activeTxns))
	for _, txn := range txnDB.activeTxns {
		txns = append(txns, txn)
	}
	return txns
}

// nextTxnID generates the next transaction ID.
func (txnDB *TransactionDB) nextTxnID() uint64 {
	return atomic.AddUint64(&txnDB.txnIDCounter, 1)
}

// unregisterTransaction removes a transaction from the active set.
// Called when a transaction commits or rolls back.
// Reserved for future cleanup implementation.
func (txnDB *TransactionDB) unregisterTransaction(txnID uint64) { //nolint:unused // reserved for future use
	txnDB.mu.Lock()
	delete(txnDB.activeTxns, txnID)
	txnDB.mu.Unlock()
}

// NumActiveTransactions returns the number of active transactions.
func (txnDB *TransactionDB) NumActiveTransactions() int {
	txnDB.mu.RLock()
	defer txnDB.mu.RUnlock()
	return len(txnDB.activeTxns)
}

// getLockManager returns the lock manager (for testing/debugging).
// Not exported - internal method.
func (txnDB *TransactionDB) getLockManager() *txn.LockManager {
	return txnDB.lockManager
}

// ----- Pass-through methods for convenience -----

// Get retrieves a value from the database.
func (txnDB *TransactionDB) Get(key []byte) ([]byte, error) {
	return txnDB.db.Get(nil, key)
}

// GetWithOptions retrieves a value with read options.
func (txnDB *TransactionDB) GetWithOptions(readOpts *ReadOptions, key []byte) ([]byte, error) {
	return txnDB.db.Get(readOpts, key)
}

// Put sets a value in the database (outside of a transaction).
func (txnDB *TransactionDB) Put(key, value []byte) error {
	return txnDB.db.Put(nil, key, value)
}

// Delete removes a key from the database (outside of a transaction).
func (txnDB *TransactionDB) Delete(key []byte) error {
	return txnDB.db.Delete(nil, key)
}

// NewIterator creates an iterator over the database.
func (txnDB *TransactionDB) NewIterator(opts *ReadOptions) Iterator {
	return txnDB.db.NewIterator(opts)
}

// GetSnapshot creates a snapshot of the database.
func (txnDB *TransactionDB) GetSnapshot() *Snapshot {
	return txnDB.db.GetSnapshot()
}

// ReleaseSnapshot releases a snapshot.
func (txnDB *TransactionDB) ReleaseSnapshot(snapshot *Snapshot) {
	txnDB.db.ReleaseSnapshot(snapshot)
}

// Flush flushes all memtables to disk.
func (txnDB *TransactionDB) Flush(opts *FlushOptions) error {
	return txnDB.db.Flush(opts)
}

// GetProperty returns a database property.
func (txnDB *TransactionDB) GetProperty(property string) (string, bool) {
	return txnDB.db.GetProperty(property)
}
