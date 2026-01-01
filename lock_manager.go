package rockyardkv

// lock_manager.go implements the lock manager for pessimistic transactions.
//
// LockManager handles point and range locks for transaction isolation.
// It supports deadlock detection and configurable lock timeouts.
//
// Reference: RocksDB v10.7.5
//   - utilities/transactions/lock/lock_manager.h
//   - utilities/transactions/lock/point/point_lock_manager.cc


import (
	"errors"
	"maps"
	"sync"
	"time"
)

// Lock Manager errors
var (
	// ErrLockTimeout is returned when a lock request times out.
	ErrLockTimeout = errors.New("db: lock request timed out")

	// ErrDeadlock is returned when a deadlock is detected.
	ErrDeadlock = errors.New("db: deadlock detected")

	// ErrLockNotHeld is returned when trying to unlock a key not held by the transaction.
	ErrLockNotHeld = errors.New("db: lock not held by transaction")
)

// LockType represents the type of lock.
type LockType int

const (
	// LockTypeShared allows multiple readers but no writers.
	LockTypeShared LockType = iota
	// LockTypeExclusive allows only one holder (reader or writer).
	LockTypeExclusive
)

// String returns a string representation of the lock type.
func (lt LockType) String() string {
	switch lt {
	case LockTypeShared:
		return "Shared"
	case LockTypeExclusive:
		return "Exclusive"
	default:
		return "Unknown"
	}
}

// LockRequest represents a pending or granted lock request.
type LockRequest struct {
	TxnID    uint64
	LockType LockType
	Granted  bool
	Waiting  chan struct{} // Closed when the lock is granted
}

// LockInfo holds information about locks on a single key.
type LockInfo struct {
	// Holders are transactions that currently hold a lock on this key.
	// For shared locks, there can be multiple holders.
	// For exclusive locks, there is at most one holder.
	Holders map[uint64]LockType

	// WaitQueue is an ordered list of pending lock requests.
	WaitQueue []*LockRequest
}

// NewLockInfo creates a new LockInfo.
func NewLockInfo() *LockInfo {
	return &LockInfo{
		Holders:   make(map[uint64]LockType),
		WaitQueue: nil,
	}
}

// IsHeldBy returns true if the key is locked by the given transaction.
func (li *LockInfo) IsHeldBy(txnID uint64) bool {
	_, held := li.Holders[txnID]
	return held
}

// GetLockType returns the lock type held by the transaction, or -1 if not held.
func (li *LockInfo) GetLockType(txnID uint64) LockType {
	if lockType, ok := li.Holders[txnID]; ok {
		return lockType
	}
	return -1
}

// HasExclusiveHolder returns true if there's an exclusive lock holder.
func (li *LockInfo) HasExclusiveHolder() bool {
	for _, lt := range li.Holders {
		if lt == LockTypeExclusive {
			return true
		}
	}
	return false
}

// NumHolders returns the number of lock holders.
func (li *LockInfo) NumHolders() int {
	return len(li.Holders)
}

// LockManager manages locks for pessimistic transactions.
// It supports shared and exclusive locks with deadlock detection.
type LockManager struct {
	mu sync.Mutex

	// locks maps key -> lock info
	locks map[string]*LockInfo

	// waitFor maps txnID -> set of txnIDs it's waiting for (for deadlock detection)
	waitFor map[uint64]map[uint64]struct{}

	// txnLocks maps txnID -> set of keys it holds locks on (for bulk unlock)
	txnLocks map[uint64]map[string]struct{}

	// Configuration
	defaultTimeout time.Duration
}

// LockManagerOptions configures the lock manager.
type LockManagerOptions struct {
	DefaultTimeout time.Duration
}

// DefaultLockManagerOptions returns default options.
func DefaultLockManagerOptions() LockManagerOptions {
	return LockManagerOptions{
		DefaultTimeout: 5 * time.Second,
	}
}

// NewLockManager creates a new lock manager.
func NewLockManager(opts LockManagerOptions) *LockManager {
	if opts.DefaultTimeout == 0 {
		opts.DefaultTimeout = 5 * time.Second
	}
	return &LockManager{
		locks:          make(map[string]*LockInfo),
		waitFor:        make(map[uint64]map[uint64]struct{}),
		txnLocks:       make(map[uint64]map[string]struct{}),
		defaultTimeout: opts.DefaultTimeout,
	}
}

// Lock attempts to acquire a lock on the given key for the transaction.
// If the lock cannot be immediately granted, it waits up to the timeout.
// Returns ErrDeadlock if acquiring the lock would cause a deadlock.
// Returns ErrLockTimeout if the timeout expires.
func (lm *LockManager) Lock(txnID uint64, key []byte, lockType LockType, timeout time.Duration) error {
	if timeout == 0 {
		timeout = lm.defaultTimeout
	}

	keyStr := string(key)

	lm.mu.Lock()

	// Get or create lock info for this key
	lockInfo, exists := lm.locks[keyStr]
	if !exists {
		lockInfo = NewLockInfo()
		lm.locks[keyStr] = lockInfo
	}

	// Check if we already hold a compatible lock
	if currentType, held := lockInfo.Holders[txnID]; held {
		if currentType == LockTypeExclusive || lockType == LockTypeShared {
			// We already hold an exclusive lock, or we're requesting shared and already have something
			lm.mu.Unlock()
			return nil
		}
		// We hold a shared lock but want exclusive - need to upgrade
		// For simplicity, we'll treat this as a new exclusive lock request
		// In a production system, we'd need lock upgrading logic
	}

	// Check if we can grant the lock immediately
	if lm.canGrantLock(lockInfo, txnID, lockType) {
		lm.grantLock(lockInfo, txnID, keyStr, lockType)
		lm.mu.Unlock()
		return nil
	}

	// Need to wait - check for deadlock first
	waitingFor := lm.collectBlockingTxns(lockInfo, txnID)
	if lm.wouldCauseDeadlock(txnID, waitingFor) {
		lm.mu.Unlock()
		return ErrDeadlock
	}

	// Add to wait-for graph
	lm.addToWaitFor(txnID, waitingFor)

	// Create wait request
	req := &LockRequest{
		TxnID:    txnID,
		LockType: lockType,
		Granted:  false,
		Waiting:  make(chan struct{}),
	}
	lockInfo.WaitQueue = append(lockInfo.WaitQueue, req)

	lm.mu.Unlock()

	// Wait for lock or timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-req.Waiting:
		// Lock granted
		return nil
	case <-timer.C:
		// Timeout - remove from wait queue
		lm.removeFromWaitQueue(keyStr, txnID)
		return ErrLockTimeout
	}
}

// TryLock attempts to acquire a lock without waiting.
// Returns true if the lock was acquired, false otherwise.
func (lm *LockManager) TryLock(txnID uint64, key []byte, lockType LockType) bool {
	keyStr := string(key)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	lockInfo, exists := lm.locks[keyStr]
	if !exists {
		lockInfo = NewLockInfo()
		lm.locks[keyStr] = lockInfo
	}

	// Check if we already hold a compatible lock
	if currentType, held := lockInfo.Holders[txnID]; held {
		if currentType == LockTypeExclusive || lockType == LockTypeShared {
			return true
		}
	}

	if lm.canGrantLock(lockInfo, txnID, lockType) {
		lm.grantLock(lockInfo, txnID, keyStr, lockType)
		return true
	}

	return false
}

// Unlock releases the lock held by the transaction on the given key.
func (lm *LockManager) Unlock(txnID uint64, key []byte) error {
	keyStr := string(key)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	return lm.unlockInternal(txnID, keyStr)
}

// unlockInternal releases a lock (caller must hold lm.mu).
func (lm *LockManager) unlockInternal(txnID uint64, keyStr string) error {
	lockInfo, exists := lm.locks[keyStr]
	if !exists {
		return ErrLockNotHeld
	}

	if _, held := lockInfo.Holders[txnID]; !held {
		return ErrLockNotHeld
	}

	// Remove from holders
	delete(lockInfo.Holders, txnID)

	// Remove from txn's lock set
	if txnKeys, ok := lm.txnLocks[txnID]; ok {
		delete(txnKeys, keyStr)
		if len(txnKeys) == 0 {
			delete(lm.txnLocks, txnID)
		}
	}

	// Remove from wait-for graph
	delete(lm.waitFor, txnID)
	for _, waitingFor := range lm.waitFor {
		delete(waitingFor, txnID)
	}

	// Try to grant locks to waiting requests
	lm.processWaitQueue(keyStr, lockInfo)

	// Clean up empty lock info
	if len(lockInfo.Holders) == 0 && len(lockInfo.WaitQueue) == 0 {
		delete(lm.locks, keyStr)
	}

	return nil
}

// UnlockAll releases all locks held by the transaction.
func (lm *LockManager) UnlockAll(txnID uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Get all keys held by this transaction
	txnKeys, ok := lm.txnLocks[txnID]
	if !ok {
		return
	}

	// Make a copy since unlockInternal modifies txnLocks
	keys := make([]string, 0, len(txnKeys))
	for key := range txnKeys {
		keys = append(keys, key)
	}

	// Unlock each key
	for _, key := range keys {
		_ = lm.unlockInternal(txnID, key)
	}

	// Clean up wait-for graph
	delete(lm.waitFor, txnID)
	for _, waitingFor := range lm.waitFor {
		delete(waitingFor, txnID)
	}
}

// canGrantLock checks if a lock can be immediately granted.
func (lm *LockManager) canGrantLock(lockInfo *LockInfo, txnID uint64, lockType LockType) bool {
	// If no holders, we can always grant
	if len(lockInfo.Holders) == 0 {
		return true
	}

	// If we already hold the lock, check compatibility
	if currentType, held := lockInfo.Holders[txnID]; held {
		if currentType == LockTypeExclusive {
			return true // Already have exclusive
		}
		if lockType == LockTypeShared {
			return true // Already have shared, want shared
		}
		// Have shared, want exclusive - can only upgrade if we're the only holder
		return len(lockInfo.Holders) == 1
	}

	// We don't hold the lock - check if compatible with current holders
	if lockType == LockTypeExclusive {
		return false // Exclusive needs no other holders
	}

	// Shared lock - can grant if all holders are shared
	return !lockInfo.HasExclusiveHolder()
}

// grantLock grants the lock to the transaction.
func (lm *LockManager) grantLock(lockInfo *LockInfo, txnID uint64, keyStr string, lockType LockType) {
	lockInfo.Holders[txnID] = lockType

	// Track in txn's lock set
	if _, ok := lm.txnLocks[txnID]; !ok {
		lm.txnLocks[txnID] = make(map[string]struct{})
	}
	lm.txnLocks[txnID][keyStr] = struct{}{}
}

// collectBlockingTxns returns the set of transactions blocking the given transaction.
func (lm *LockManager) collectBlockingTxns(lockInfo *LockInfo, txnID uint64) map[uint64]struct{} {
	blocking := make(map[uint64]struct{})
	for holderID := range lockInfo.Holders {
		if holderID != txnID {
			blocking[holderID] = struct{}{}
		}
	}
	return blocking
}

// addToWaitFor adds wait-for edges in the graph.
func (lm *LockManager) addToWaitFor(txnID uint64, waitingFor map[uint64]struct{}) {
	if _, ok := lm.waitFor[txnID]; !ok {
		lm.waitFor[txnID] = make(map[uint64]struct{})
	}
	for targetID := range waitingFor {
		lm.waitFor[txnID][targetID] = struct{}{}
	}
}

// wouldCauseDeadlock checks if adding wait edges would cause a cycle.
func (lm *LockManager) wouldCauseDeadlock(txnID uint64, waitingFor map[uint64]struct{}) bool {
	// DFS to detect cycles in wait-for graph
	visited := make(map[uint64]bool)
	inStack := make(map[uint64]bool)

	var dfs func(node uint64) bool
	dfs = func(node uint64) bool {
		visited[node] = true
		inStack[node] = true

		// Check existing wait-for edges
		if edges, ok := lm.waitFor[node]; ok {
			for target := range edges {
				if target == txnID {
					return true // Cycle found!
				}
				if !visited[target] {
					if dfs(target) {
						return true
					}
				} else if inStack[target] {
					return true
				}
			}
		}

		inStack[node] = false
		return false
	}

	// Check each transaction we would wait for
	for targetID := range waitingFor {
		if targetID == txnID {
			continue
		}
		visited = make(map[uint64]bool)
		inStack = make(map[uint64]bool)
		if dfs(targetID) {
			return true
		}
	}

	return false
}

// processWaitQueue tries to grant locks to waiting requests.
func (lm *LockManager) processWaitQueue(keyStr string, lockInfo *LockInfo) {
	// Process wait queue in order (FIFO)
	newQueue := make([]*LockRequest, 0, len(lockInfo.WaitQueue))

	for _, req := range lockInfo.WaitQueue {
		if req.Granted {
			continue // Already granted
		}

		if lm.canGrantLock(lockInfo, req.TxnID, req.LockType) {
			// Grant the lock
			lm.grantLock(lockInfo, req.TxnID, keyStr, req.LockType)
			req.Granted = true

			// Remove from wait-for graph
			delete(lm.waitFor, req.TxnID)

			// Signal the waiting goroutine
			close(req.Waiting)
		} else {
			// Can't grant yet, keep in queue
			newQueue = append(newQueue, req)
		}
	}

	lockInfo.WaitQueue = newQueue
}

// removeFromWaitQueue removes a transaction from the wait queue for a key.
func (lm *LockManager) removeFromWaitQueue(keyStr string, txnID uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lockInfo, exists := lm.locks[keyStr]
	if !exists {
		return
	}

	newQueue := make([]*LockRequest, 0, len(lockInfo.WaitQueue))
	for _, req := range lockInfo.WaitQueue {
		if req.TxnID != txnID {
			newQueue = append(newQueue, req)
		}
	}
	lockInfo.WaitQueue = newQueue

	// Remove from wait-for graph
	delete(lm.waitFor, txnID)
}

// GetLockInfo returns information about a lock (for debugging/testing).
func (lm *LockManager) GetLockInfo(key []byte) *LockInfo {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lockInfo, exists := lm.locks[string(key)]; exists {
		// Return a copy to avoid races
		copy := &LockInfo{
			Holders:   make(map[uint64]LockType),
			WaitQueue: make([]*LockRequest, len(lockInfo.WaitQueue)),
		}
		maps.Copy(copy.Holders, lockInfo.Holders)
		for i, req := range lockInfo.WaitQueue {
			copy.WaitQueue[i] = &LockRequest{
				TxnID:    req.TxnID,
				LockType: req.LockType,
				Granted:  req.Granted,
			}
		}
		return copy
	}
	return nil
}

// NumLocks returns the number of keys with active locks.
func (lm *LockManager) NumLocks() int {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return len(lm.locks)
}

// NumTxnLocks returns the number of locks held by a transaction.
func (lm *LockManager) NumTxnLocks(txnID uint64) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if keys, ok := lm.txnLocks[txnID]; ok {
		return len(keys)
	}
	return 0
}
