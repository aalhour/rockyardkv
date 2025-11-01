package testutil

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// TestExpectedValueBasic tests basic ExpectedValue operations.
func TestExpectedValueBasic(t *testing.T) {
	ev := NewExpectedValue()

	// Should start as deleted
	if !ev.IsDeleted() {
		t.Error("New ExpectedValue should be deleted")
	}
	if ev.Exists() {
		t.Error("New ExpectedValue should not exist")
	}

	// Value base should be 0
	if ev.GetValueBase() != 0 {
		t.Errorf("Initial value base: got %d, want 0", ev.GetValueBase())
	}

	// Del counter should be 0
	if ev.GetDelCounter() != 0 {
		t.Errorf("Initial del counter: got %d, want 0", ev.GetDelCounter())
	}
}

// TestExpectedValuePut tests put operations.
func TestExpectedValuePut(t *testing.T) {
	ev := NewExpectedValue()

	// Put (non-pending)
	ev.Put(false)

	// Should exist now
	if !ev.Exists() {
		t.Error("After put, should exist")
	}
	if ev.IsDeleted() {
		t.Error("After put, should not be deleted")
	}

	// Value base should have incremented
	if ev.GetValueBase() != 1 {
		t.Errorf("After first put, value base: got %d, want 1", ev.GetValueBase())
	}

	// Another put
	ev.Put(false)
	if ev.GetValueBase() != 2 {
		t.Errorf("After second put, value base: got %d, want 2", ev.GetValueBase())
	}
}

// TestExpectedValuePendingPut tests pending put operations.
func TestExpectedValuePendingPut(t *testing.T) {
	ev := NewExpectedValue()

	// Put with pending flag
	ev.Put(true)

	// Should have pending write flag set
	if !ev.PendingWrite() {
		t.Error("After pending put, should have pending write flag")
	}

	// Value base should NOT have changed yet
	if ev.GetValueBase() != 0 {
		t.Errorf("After pending put, value base should be 0, got %d", ev.GetValueBase())
	}

	// Final value base should be 1
	if ev.GetFinalValueBase() != 1 {
		t.Errorf("After pending put, final value base: got %d, want 1", ev.GetFinalValueBase())
	}

	// Complete the pending put
	ev.SyncPendingPut()

	// Pending flag should be cleared
	if ev.PendingWrite() {
		t.Error("After sync, pending write flag should be cleared")
	}

	// Value base should now be 1
	if ev.GetValueBase() != 1 {
		t.Errorf("After sync, value base: got %d, want 1", ev.GetValueBase())
	}
}

// TestExpectedValueDelete tests delete operations.
func TestExpectedValueDelete(t *testing.T) {
	ev := NewExpectedValue()
	ev.Put(false) // First make it exist

	if !ev.Exists() {
		t.Error("Should exist after put")
	}

	// Delete (non-pending)
	existed := ev.Delete(false)

	if !existed {
		t.Error("Delete should return true for existing key")
	}
	if !ev.IsDeleted() {
		t.Error("After delete, should be deleted")
	}
	if ev.Exists() {
		t.Error("After delete, should not exist")
	}
	if ev.GetDelCounter() != 1 {
		t.Errorf("After first delete, del counter: got %d, want 1", ev.GetDelCounter())
	}
}

// TestExpectedValuePendingDelete tests pending delete operations.
func TestExpectedValuePendingDelete(t *testing.T) {
	ev := NewExpectedValue()
	ev.Put(false) // Make it exist

	// Delete with pending flag
	ev.Delete(true)

	// Should have pending delete flag set
	if !ev.PendingDelete() {
		t.Error("After pending delete, should have pending delete flag")
	}

	// Del counter should NOT have changed yet
	if ev.GetDelCounter() != 0 {
		t.Errorf("After pending delete, del counter should be 0, got %d", ev.GetDelCounter())
	}

	// Final del counter should be 1
	if ev.GetFinalDelCounter() != 1 {
		t.Errorf("After pending delete, final del counter: got %d, want 1", ev.GetFinalDelCounter())
	}

	// Complete the pending delete
	ev.SyncDelete()

	// Pending flag should be cleared
	if ev.PendingDelete() {
		t.Error("After sync, pending delete flag should be cleared")
	}

	// Del counter should now be 1
	if ev.GetDelCounter() != 1 {
		t.Errorf("After sync, del counter: got %d, want 1", ev.GetDelCounter())
	}
}

// TestExpectedValueWraparound tests value base wraparound.
func TestExpectedValueWraparound(t *testing.T) {
	ev := NewExpectedValue()
	ev.SetValueBase(valueBaseMask) // Max value

	// Next value should wrap to 0
	if ev.NextValueBase() != 0 {
		t.Errorf("Next value after max: got %d, want 0", ev.NextValueBase())
	}

	ev.Put(false)
	if ev.GetValueBase() != 0 {
		t.Errorf("After wraparound put, value base: got %d, want 0", ev.GetValueBase())
	}
}

// TestExpectedValueDelCounterWraparound tests deletion counter wraparound.
func TestExpectedValueDelCounterWraparound(t *testing.T) {
	ev := NewExpectedValue()
	ev.SetDelCounter(delCounterMask >> 16) // Max counter

	// Next counter should wrap to 0
	if ev.NextDelCounter() != 0 {
		t.Errorf("Next del counter after max: got %d, want 0", ev.NextDelCounter())
	}
}

// TestPendingExpectedValueV2Commit tests pending value commit.
func TestPendingExpectedValueV2Commit(t *testing.T) {
	var value atomic.Uint32
	value.Store(uint32(deletedMask))

	origValue := NewExpectedValueFromRaw(value.Load())
	finalValue := origValue
	finalValue.Put(false)

	pev := NewPendingExpectedValueV2(&value, origValue, finalValue)

	// Commit
	pev.Commit()

	// Value should be final value
	result := NewExpectedValueFromRaw(value.Load())
	if result.GetValueBase() != 1 {
		t.Errorf("After commit, value base: got %d, want 1", result.GetValueBase())
	}
	if result.IsDeleted() {
		t.Error("After commit, should not be deleted")
	}

	// Should be closed
	if !pev.IsClosed() {
		t.Error("After commit, should be closed")
	}
}

// TestPendingExpectedValueV2Rollback tests pending value rollback.
func TestPendingExpectedValueV2Rollback(t *testing.T) {
	var value atomic.Uint32
	value.Store(uint32(deletedMask))

	origValue := NewExpectedValueFromRaw(value.Load())
	finalValue := origValue
	finalValue.Put(false)

	pev := NewPendingExpectedValueV2(&value, origValue, finalValue)

	// Rollback
	pev.Rollback()

	// Value should be original value
	result := NewExpectedValueFromRaw(value.Load())
	if !result.IsDeleted() {
		t.Error("After rollback, should still be deleted")
	}
	if result.GetValueBase() != 0 {
		t.Errorf("After rollback, value base: got %d, want 0", result.GetValueBase())
	}
}

// TestMustHaveNotExisted tests the MustHaveNotExisted helper.
func TestMustHaveNotExisted(t *testing.T) {
	tests := []struct {
		name     string
		preRead  ExpectedValue
		postRead ExpectedValue
		want     bool
	}{
		{
			name:     "deleted and no write",
			preRead:  NewExpectedValue(), // Deleted
			postRead: NewExpectedValue(), // Still deleted
			want:     true,
		},
		{
			name:    "deleted but write happened",
			preRead: NewExpectedValue(),
			postRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.SetPendingWrite() // Write happened
				return ev
			}(),
			want: false,
		},
		{
			name: "not deleted",
			preRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.Put(false)
				return ev
			}(),
			postRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.Put(false)
				return ev
			}(),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MustHaveNotExisted(tc.preRead, tc.postRead)
			if got != tc.want {
				t.Errorf("MustHaveNotExisted: got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestMustHaveExisted tests the MustHaveExisted helper.
func TestMustHaveExisted(t *testing.T) {
	tests := []struct {
		name     string
		preRead  ExpectedValue
		postRead ExpectedValue
		want     bool
	}{
		{
			name:     "deleted",
			preRead:  NewExpectedValue(), // Deleted
			postRead: NewExpectedValue(), // Still deleted
			want:     false,
		},
		{
			name: "existed and no delete",
			preRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.Put(false)
				return ev
			}(),
			postRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.Put(false)
				return ev
			}(),
			want: true,
		},
		{
			name: "existed but delete happened",
			preRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.Put(false)
				return ev
			}(),
			postRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.Put(false)
				ev.SetPendingDelete() // Delete happened
				return ev
			}(),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MustHaveExisted(tc.preRead, tc.postRead)
			if got != tc.want {
				t.Errorf("MustHaveExisted: got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestInExpectedValueBaseRange tests the InExpectedValueBaseRange helper.
func TestInExpectedValueBaseRange(t *testing.T) {
	tests := []struct {
		name      string
		valueBase uint32
		preRead   ExpectedValue
		postRead  ExpectedValue
		want      bool
	}{
		{
			name:      "value in range",
			valueBase: 5,
			preRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.SetValueBase(3)
				return ev
			}(),
			postRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.SetValueBase(7)
				return ev
			}(),
			want: true,
		},
		{
			name:      "value below range",
			valueBase: 2,
			preRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.SetValueBase(3)
				return ev
			}(),
			postRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.SetValueBase(7)
				return ev
			}(),
			want: false,
		},
		{
			name:      "value above range",
			valueBase: 8,
			preRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.SetValueBase(3)
				return ev
			}(),
			postRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.SetValueBase(7)
				return ev
			}(),
			want: false,
		},
		{
			name:      "wraparound - in lower part",
			valueBase: 2,
			preRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.SetValueBase(valueBaseMask - 2) // Near max
				return ev
			}(),
			postRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.SetValueBase(5) // Wrapped to low
				return ev
			}(),
			want: true,
		},
		{
			name:      "wraparound - in upper part",
			valueBase: valueBaseMask - 1,
			preRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.SetValueBase(valueBaseMask - 2) // Near max
				return ev
			}(),
			postRead: func() ExpectedValue {
				ev := NewExpectedValue()
				ev.SetValueBase(5) // Wrapped to low
				return ev
			}(),
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := InExpectedValueBaseRange(tc.valueBase, tc.preRead, tc.postRead)
			if got != tc.want {
				t.Errorf("InExpectedValueBaseRange(%d): got %v, want %v", tc.valueBase, got, tc.want)
			}
		})
	}
}

// TestExpectedStateV2Basic tests basic ExpectedStateV2 operations.
func TestExpectedStateV2Basic(t *testing.T) {
	es := NewExpectedStateV2(100, 2, 2)

	if es.MaxKey() != 100 {
		t.Errorf("MaxKey: got %d, want 100", es.MaxKey())
	}
	if es.NumColumnFamilies() != 2 {
		t.Errorf("NumColumnFamilies: got %d, want 2", es.NumColumnFamilies())
	}

	// All keys should start deleted
	for key := range int64(10) {
		if es.Exists(0, key) {
			t.Errorf("Key %d should not exist initially", key)
		}
	}
}

// TestExpectedStateV2PutDelete tests put and delete with locks.
func TestExpectedStateV2PutDelete(t *testing.T) {
	es := NewExpectedStateV2(100, 1, 2)

	key := int64(42)

	// Acquire lock
	mu := es.GetMutexForKey(0, key)
	mu.Lock()

	// Prepare put
	pev := es.PreparePut(0, key)
	if pev == nil {
		t.Fatal("PreparePut returned nil")
	}

	// Check pending flag is set
	ev := es.Get(0, key)
	if !ev.PendingWrite() {
		t.Error("After PreparePut, pending write flag should be set")
	}

	// Commit
	pev.Commit()
	mu.Unlock()

	// Should exist now
	if !es.Exists(0, key) {
		t.Error("After commit, key should exist")
	}

	// Acquire lock again for delete
	mu.Lock()

	// Prepare delete
	pev = es.PrepareDelete(0, key)
	if pev == nil {
		t.Fatal("PrepareDelete returned nil")
	}

	// Check pending flag
	ev = es.Get(0, key)
	if !ev.PendingDelete() {
		t.Error("After PrepareDelete, pending delete flag should be set")
	}

	// Commit
	pev.Commit()
	mu.Unlock()

	// Should not exist now
	if es.Exists(0, key) {
		t.Error("After delete commit, key should not exist")
	}
}

// TestExpectedStateV2Rollback tests rollback.
func TestExpectedStateV2Rollback(t *testing.T) {
	es := NewExpectedStateV2(100, 1, 2)

	key := int64(42)

	// Acquire lock and prepare put
	mu := es.GetMutexForKey(0, key)
	mu.Lock()

	pev := es.PreparePut(0, key)

	// Rollback
	pev.Rollback()
	mu.Unlock()

	// Should still not exist
	if es.Exists(0, key) {
		t.Error("After rollback, key should not exist")
	}
}

// TestExpectedStateV2Concurrent tests concurrent access with locks.
func TestExpectedStateV2Concurrent(t *testing.T) {
	es := NewExpectedStateV2(1000, 1, 2)

	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100

	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := range opsPerGoroutine {
				key := int64((goroutineID*opsPerGoroutine + i) % 100)

				// Acquire lock
				mu := es.GetMutexForKey(0, key)
				mu.Lock()

				if i%2 == 0 {
					// Put
					pev := es.PreparePut(0, key)
					pev.Commit()
				} else {
					// Delete
					pev := es.PrepareDelete(0, key)
					pev.Commit()
				}

				mu.Unlock()
			}
		}(g)
	}

	wg.Wait()

	// No panic = success for concurrent test
	t.Log("Concurrent test completed without panic")
}

// TestExpectedStateV2ColumnFamilyIsolation tests CF isolation.
func TestExpectedStateV2ColumnFamilyIsolation(t *testing.T) {
	es := NewExpectedStateV2(100, 3, 2)

	key := int64(50)

	// Put in CF 0
	mu := es.GetMutexForKey(0, key)
	mu.Lock()
	pev := es.PreparePut(0, key)
	pev.Commit()
	mu.Unlock()

	// CF 0 should have it, CF 1 and 2 should not
	if !es.Exists(0, key) {
		t.Error("Key should exist in CF 0")
	}
	if es.Exists(1, key) {
		t.Error("Key should not exist in CF 1")
	}
	if es.Exists(2, key) {
		t.Error("Key should not exist in CF 2")
	}

	// Clear CF 0
	es.ClearColumnFamily(0)

	if es.Exists(0, key) {
		t.Error("After clear, key should not exist in CF 0")
	}
}

// TestExpectedStateV2LockCoverage tests that locks cover the right keys.
func TestExpectedStateV2LockCoverage(t *testing.T) {
	es := NewExpectedStateV2(100, 1, 2) // 4 keys per lock

	// Keys 0-3 should share a lock
	mu0 := es.GetMutexForKey(0, 0)
	mu1 := es.GetMutexForKey(0, 1)
	mu2 := es.GetMutexForKey(0, 2)
	mu3 := es.GetMutexForKey(0, 3)

	if mu0 != mu1 || mu0 != mu2 || mu0 != mu3 {
		t.Error("Keys 0-3 should share the same lock")
	}

	// Key 4 should have a different lock
	mu4 := es.GetMutexForKey(0, 4)
	if mu0 == mu4 {
		t.Error("Key 4 should have a different lock than key 0")
	}
}

// ============================================================================
// Persistent ExpectedStateV2 Tests
// ============================================================================

func TestExpectedStateV2SaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/expected_state_v2.bin"

	// Create and populate
	es := NewExpectedStateV2(100, 2, 2)

	// Make some changes - each Put increments the value base
	mu := es.GetMutexForKey(0, 10)
	mu.Lock()
	for range 5 {
		pending := es.PreparePut(0, 10)
		pending.Commit()
	}
	mu.Unlock()

	mu = es.GetMutexForKey(0, 20)
	mu.Lock()
	for range 10 {
		pending := es.PreparePut(0, 20)
		pending.Commit()
	}
	mu.Unlock()

	mu = es.GetMutexForKey(1, 30)
	mu.Lock()
	for range 3 {
		pending := es.PreparePut(1, 30)
		pending.Commit()
	}
	mu.Unlock()

	es.SetPersistedSeqno(42)

	// Record expected values before save
	origVal10 := es.Get(0, 10)
	origVal20 := es.Get(0, 20)
	origVal30 := es.Get(1, 30)

	// Save
	if err := es.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Load
	loaded, err := LoadExpectedStateV2FromFile(path)
	if err != nil {
		t.Fatalf("LoadExpectedStateV2FromFile failed: %v", err)
	}

	// Verify
	if loaded.maxKey != es.maxKey {
		t.Errorf("maxKey = %d, want %d", loaded.maxKey, es.maxKey)
	}
	if loaded.numColumnFamilies != es.numColumnFamilies {
		t.Errorf("numCFs = %d, want %d", loaded.numColumnFamilies, es.numColumnFamilies)
	}
	if loaded.log2KeysPerLock != es.log2KeysPerLock {
		t.Errorf("log2KeysPerLock = %d, want %d", loaded.log2KeysPerLock, es.log2KeysPerLock)
	}
	if loaded.GetPersistedSeqno() != 42 {
		t.Errorf("seqno = %d, want 42", loaded.GetPersistedSeqno())
	}

	// Check values match original
	val := loaded.Get(0, 10)
	if uint32(val) != uint32(origVal10) {
		t.Errorf("key 0:10 = %d, want %d", uint32(val), uint32(origVal10))
	}

	val = loaded.Get(0, 20)
	if uint32(val) != uint32(origVal20) {
		t.Errorf("key 0:20 = %d, want %d", uint32(val), uint32(origVal20))
	}

	val = loaded.Get(1, 30)
	if uint32(val) != uint32(origVal30) {
		t.Errorf("key 1:30 = %d, want %d", uint32(val), uint32(origVal30))
	}

	// Verify existence
	if !loaded.Get(0, 10).Exists() {
		t.Error("key 0:10 should exist")
	}
	if !loaded.Get(0, 20).Exists() {
		t.Error("key 0:20 should exist")
	}
	if !loaded.Get(1, 30).Exists() {
		t.Error("key 1:30 should exist")
	}
}

func TestExpectedStateV2SaveLoadLarge(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/expected_state_v2_large.bin"

	// Create large state
	es := NewExpectedStateV2(1000, 2, 3)
	for cf := range 2 {
		for key := range int64(1000) {
			mu := es.GetMutexForKey(cf, key)
			mu.Lock()
			if key%3 == 0 {
				// Put a few times based on key to vary the value
				numPuts := int((key % 10) + 1)
				for range numPuts {
					pending := es.PreparePut(cf, key)
					pending.Commit()
				}
			}
			// Otherwise leave as deleted (default)
			mu.Unlock()
		}
	}

	// Save
	if err := es.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Load
	loaded, err := LoadExpectedStateV2FromFile(path)
	if err != nil {
		t.Fatalf("LoadExpectedStateV2FromFile failed: %v", err)
	}

	// Verify all values match
	for cf := range 2 {
		for key := range int64(1000) {
			expected := es.Get(cf, key)
			actual := loaded.Get(cf, key)
			if uint32(expected) != uint32(actual) {
				t.Errorf("cf=%d key=%d: got %d, want %d", cf, key, uint32(actual), uint32(expected))
			}
		}
	}
}

func TestExpectedStateV2LoadInvalid(t *testing.T) {
	dir := t.TempDir()

	// Wrong magic
	wrongPath := dir + "/wrong.bin"
	data := make([]byte, 100)
	copy(data, "WRONGMAG")
	if err := writeFileV2(wrongPath, data); err != nil {
		t.Fatal(err)
	}
	_, err := LoadExpectedStateV2FromFile(wrongPath)
	if !errors.Is(err, errInvalidMagicV2) {
		t.Errorf("Expected errInvalidMagicV2, got %v", err)
	}
}
