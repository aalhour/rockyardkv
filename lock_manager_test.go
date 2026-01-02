package rockyardkv

// lock_manager_test.go implements tests for lock manager.

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLockManagerBasic(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	// Test acquiring an exclusive lock
	err := lm.Lock(1, []byte("key1"), LockTypeExclusive, time.Second)
	if err != nil {
		t.Fatalf("Failed to acquire exclusive lock: %v", err)
	}

	// Verify lock is held
	info := lm.GetLockInfo([]byte("key1"))
	if info == nil {
		t.Fatal("Expected lock info to exist")
	}
	if !info.IsHeldBy(1) {
		t.Error("Expected txn 1 to hold the lock")
	}
	if info.NumHolders() != 1 {
		t.Errorf("Expected 1 holder, got %d", info.NumHolders())
	}

	// Unlock
	err = lm.Unlock(1, []byte("key1"))
	if err != nil {
		t.Fatalf("Failed to unlock: %v", err)
	}

	// Verify lock is released
	info = lm.GetLockInfo([]byte("key1"))
	if info != nil {
		t.Error("Expected lock info to be cleaned up")
	}
}

func TestLockManagerSharedLocks(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	// Multiple transactions can hold shared locks
	err := lm.Lock(1, []byte("key1"), LockTypeShared, time.Second)
	if err != nil {
		t.Fatalf("Txn 1 failed to acquire shared lock: %v", err)
	}

	err = lm.Lock(2, []byte("key1"), LockTypeShared, time.Second)
	if err != nil {
		t.Fatalf("Txn 2 failed to acquire shared lock: %v", err)
	}

	err = lm.Lock(3, []byte("key1"), LockTypeShared, time.Second)
	if err != nil {
		t.Fatalf("Txn 3 failed to acquire shared lock: %v", err)
	}

	info := lm.GetLockInfo([]byte("key1"))
	if info.NumHolders() != 3 {
		t.Errorf("Expected 3 holders, got %d", info.NumHolders())
	}

	// Unlock all
	lm.Unlock(1, []byte("key1"))
	lm.Unlock(2, []byte("key1"))
	lm.Unlock(3, []byte("key1"))

	if lm.NumLocks() != 0 {
		t.Errorf("Expected 0 locks after unlocking, got %d", lm.NumLocks())
	}
}

func TestLockManagerExclusiveBlocksShared(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	// Txn 1 acquires exclusive lock
	err := lm.Lock(1, []byte("key1"), LockTypeExclusive, time.Second)
	if err != nil {
		t.Fatalf("Txn 1 failed to acquire exclusive lock: %v", err)
	}

	// Txn 2 tries to acquire shared lock - should block and timeout
	err = lm.Lock(2, []byte("key1"), LockTypeShared, 100*time.Millisecond)
	if !errors.Is(err, ErrLockTimeout) {
		t.Errorf("Expected ErrLockTimeout, got %v", err)
	}

	// Unlock txn 1
	lm.Unlock(1, []byte("key1"))

	// Now txn 2 should be able to acquire
	err = lm.Lock(2, []byte("key1"), LockTypeShared, time.Second)
	if err != nil {
		t.Fatalf("Txn 2 should acquire shared lock after unlock: %v", err)
	}
}

func TestLockManagerSharedBlocksExclusive(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	// Txn 1 acquires shared lock
	err := lm.Lock(1, []byte("key1"), LockTypeShared, time.Second)
	if err != nil {
		t.Fatalf("Txn 1 failed to acquire shared lock: %v", err)
	}

	// Txn 2 tries to acquire exclusive lock - should block and timeout
	err = lm.Lock(2, []byte("key1"), LockTypeExclusive, 100*time.Millisecond)
	if !errors.Is(err, ErrLockTimeout) {
		t.Errorf("Expected ErrLockTimeout, got %v", err)
	}

	// Unlock txn 1
	lm.Unlock(1, []byte("key1"))

	// Now txn 2 should be able to acquire
	err = lm.Lock(2, []byte("key1"), LockTypeExclusive, time.Second)
	if err != nil {
		t.Fatalf("Txn 2 should acquire exclusive lock after unlock: %v", err)
	}
}

func TestLockManagerTryLock(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	// TryLock should succeed when no lock exists
	if !lm.TryLock(1, []byte("key1"), LockTypeExclusive) {
		t.Error("TryLock should succeed on uncontested key")
	}

	// TryLock should fail when exclusive lock is held
	if lm.TryLock(2, []byte("key1"), LockTypeShared) {
		t.Error("TryLock should fail when exclusive lock is held")
	}

	// Same transaction should succeed
	if !lm.TryLock(1, []byte("key1"), LockTypeShared) {
		t.Error("TryLock should succeed for same transaction")
	}
}

func TestLockManagerUnlockAll(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	// Txn 1 acquires multiple locks
	lm.Lock(1, []byte("key1"), LockTypeExclusive, time.Second)
	lm.Lock(1, []byte("key2"), LockTypeExclusive, time.Second)
	lm.Lock(1, []byte("key3"), LockTypeShared, time.Second)

	if lm.NumTxnLocks(1) != 3 {
		t.Errorf("Expected 3 locks for txn 1, got %d", lm.NumTxnLocks(1))
	}

	// Unlock all
	lm.UnlockAll(1)

	if lm.NumTxnLocks(1) != 0 {
		t.Errorf("Expected 0 locks after UnlockAll, got %d", lm.NumTxnLocks(1))
	}
	if lm.NumLocks() != 0 {
		t.Errorf("Expected 0 total locks, got %d", lm.NumLocks())
	}
}

func TestLockManagerDeadlockDetection(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	// Txn 1 holds key1
	err := lm.Lock(1, []byte("key1"), LockTypeExclusive, time.Second)
	if err != nil {
		t.Fatalf("Txn 1 failed to acquire key1: %v", err)
	}

	// Txn 2 holds key2
	err = lm.Lock(2, []byte("key2"), LockTypeExclusive, time.Second)
	if err != nil {
		t.Fatalf("Txn 2 failed to acquire key2: %v", err)
	}

	// Start txn 1 waiting for key2 in background
	var txn1Err error
	var wg sync.WaitGroup
	wg.Go(func() {
		txn1Err = lm.Lock(1, []byte("key2"), LockTypeExclusive, time.Second)
	})

	// Give txn 1 time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Now txn 2 tries to get key1 - this should detect deadlock
	err = lm.Lock(2, []byte("key1"), LockTypeExclusive, time.Second)
	if !errors.Is(err, ErrDeadlock) {
		t.Errorf("Expected ErrDeadlock, got %v", err)
	}

	// Unlock txn 2's key2 so txn 1 can proceed
	lm.Unlock(2, []byte("key2"))

	// Wait for txn 1
	wg.Wait()

	if txn1Err != nil {
		t.Errorf("Txn 1 should have acquired key2 after txn 2 released: %v", txn1Err)
	}
}

func TestLockManagerWaitQueue(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	// Txn 1 holds exclusive lock
	err := lm.Lock(1, []byte("key1"), LockTypeExclusive, time.Second)
	if err != nil {
		t.Fatalf("Txn 1 failed to acquire: %v", err)
	}

	// Start multiple transactions waiting
	var wg sync.WaitGroup
	var acquired int32

	for i := uint64(2); i <= 4; i++ {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			err := lm.Lock(txnID, []byte("key1"), LockTypeShared, 2*time.Second)
			if err == nil {
				atomic.AddInt32(&acquired, 1)
			}
		}(i)
	}

	// Give waiters time to queue
	time.Sleep(50 * time.Millisecond)

	info := lm.GetLockInfo([]byte("key1"))
	if len(info.WaitQueue) != 3 {
		t.Errorf("Expected 3 waiters, got %d", len(info.WaitQueue))
	}

	// Release the lock - all shared waiters should be granted
	lm.Unlock(1, []byte("key1"))

	// Wait for all
	wg.Wait()

	if atomic.LoadInt32(&acquired) != 3 {
		t.Errorf("Expected 3 transactions to acquire, got %d", acquired)
	}
}

func TestLockManagerReentrant(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	// Same transaction can acquire the same lock multiple times
	err := lm.Lock(1, []byte("key1"), LockTypeExclusive, time.Second)
	if err != nil {
		t.Fatalf("First lock failed: %v", err)
	}

	err = lm.Lock(1, []byte("key1"), LockTypeExclusive, time.Second)
	if err != nil {
		t.Fatalf("Reentrant lock should succeed: %v", err)
	}

	err = lm.Lock(1, []byte("key1"), LockTypeShared, time.Second)
	if err != nil {
		t.Fatalf("Downgrade should succeed: %v", err)
	}

	// Unlock once should release
	lm.Unlock(1, []byte("key1"))

	// Lock should be released
	if lm.TryLock(2, []byte("key1"), LockTypeExclusive) == false {
		t.Error("Another transaction should be able to acquire after unlock")
	}
}

func TestLockManagerUnlockNotHeld(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	// Trying to unlock a key we don't hold should return error
	err := lm.Unlock(1, []byte("key1"))
	if !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("Expected ErrLockNotHeld, got %v", err)
	}

	// Acquire and unlock by wrong txn
	lm.Lock(1, []byte("key1"), LockTypeExclusive, time.Second)
	err = lm.Unlock(2, []byte("key1"))
	if !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("Expected ErrLockNotHeld for wrong txn, got %v", err)
	}
}

func TestLockManagerLockTypeString(t *testing.T) {
	tests := []struct {
		lt       LockType
		expected string
	}{
		{LockTypeShared, "Shared"},
		{LockTypeExclusive, "Exclusive"},
		{LockType(99), "Unknown"},
	}

	for _, tc := range tests {
		if tc.lt.String() != tc.expected {
			t.Errorf("LockType(%d).String() = %q, want %q", tc.lt, tc.lt.String(), tc.expected)
		}
	}
}

// TestLockManagerRaceCondition tests for race conditions with concurrent access.
func TestLockManagerRaceCondition(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	var wg sync.WaitGroup
	numGoroutines := 50
	numOps := 100

	for i := range numGoroutines {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			for range numOps {
				key := []byte("shared-key")
				if lm.TryLock(txnID, key, LockTypeShared) {
					// Do some work
					time.Sleep(time.Microsecond)
					lm.Unlock(txnID, key)
				}
			}
		}(uint64(i))
	}

	wg.Wait()

	// All locks should be released
	if lm.NumLocks() != 0 {
		t.Errorf("Expected 0 locks after test, got %d", lm.NumLocks())
	}
}

// TestLockManagerStress tests the lock manager under stress.
func TestLockManagerStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	lm := NewLockManager(DefaultLockManagerOptions())

	var wg sync.WaitGroup
	numGoroutines := 20
	numKeys := 10
	duration := 2 * time.Second

	stop := make(chan struct{})
	time.AfterFunc(duration, func() { close(stop) })

	var lockCount, timeoutCount, deadlockCount int64

	for i := range numGoroutines {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				keyNum := txnID % uint64(numKeys)
				key := []byte{byte(keyNum)}

				lockType := LockTypeShared
				if txnID%3 == 0 {
					lockType = LockTypeExclusive
				}

				err := lm.Lock(txnID, key, lockType, 10*time.Millisecond)
				switch {
				case err == nil:
					atomic.AddInt64(&lockCount, 1)
					time.Sleep(time.Microsecond)
					lm.Unlock(txnID, key)
				case errors.Is(err, ErrLockTimeout):
					atomic.AddInt64(&timeoutCount, 1)
				case errors.Is(err, ErrDeadlock):
					atomic.AddInt64(&deadlockCount, 1)
				}
			}
		}(uint64(i))
	}

	wg.Wait()

	t.Logf("Stress test results: locks=%d, timeouts=%d, deadlocks=%d",
		lockCount, timeoutCount, deadlockCount)

	if lm.NumLocks() != 0 {
		t.Errorf("Expected 0 locks after test, got %d", lm.NumLocks())
	}
}

// TestLockManagerDeadlockChain tests deadlock detection with longer chains.
func TestLockManagerDeadlockChain(t *testing.T) {
	lm := NewLockManager(DefaultLockManagerOptions())

	// Create a chain: T1 holds K1, T2 holds K2, T3 holds K3
	lm.Lock(1, []byte("key1"), LockTypeExclusive, time.Second)
	lm.Lock(2, []byte("key2"), LockTypeExclusive, time.Second)
	lm.Lock(3, []byte("key3"), LockTypeExclusive, time.Second)

	var wg sync.WaitGroup

	// T1 wants K2
	wg.Go(func() {
		lm.Lock(1, []byte("key2"), LockTypeExclusive, 2*time.Second)
	})

	time.Sleep(20 * time.Millisecond)

	// T2 wants K3
	wg.Go(func() {
		lm.Lock(2, []byte("key3"), LockTypeExclusive, 2*time.Second)
	})

	time.Sleep(20 * time.Millisecond)

	// T3 wants K1 - this should detect deadlock (T3->T1->T2->T3)
	err := lm.Lock(3, []byte("key1"), LockTypeExclusive, 2*time.Second)
	if !errors.Is(err, ErrDeadlock) {
		t.Errorf("Expected ErrDeadlock for chain deadlock, got %v", err)
	}

	// Clean up - release all locks to let waiting goroutines finish
	lm.UnlockAll(1)
	lm.UnlockAll(2)
	lm.UnlockAll(3)

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Error("Timed out waiting for goroutines to finish")
	}
}
