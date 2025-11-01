package testutil

import (
	"errors"
	"sync"
	"testing"
)

func TestNewExpectedState(t *testing.T) {
	es := NewExpectedState(1000, 3)
	if es == nil {
		t.Fatal("expected non-nil ExpectedState")
	}
	if es.maxKey != 1000 {
		t.Errorf("maxKey = %d, want 1000", es.maxKey)
	}
	if es.numColumnFamilies != 3 {
		t.Errorf("numColumnFamilies = %d, want 3", es.numColumnFamilies)
	}
}

func TestNewExpectedStateDefaults(t *testing.T) {
	// Test with zero/negative values
	es := NewExpectedState(0, 0)
	if es.maxKey != 1 {
		t.Errorf("maxKey = %d, want 1 (default)", es.maxKey)
	}
	if es.numColumnFamilies != 1 {
		t.Errorf("numColumnFamilies = %d, want 1 (default)", es.numColumnFamilies)
	}
}

func TestExpectedStateInitiallyUnknown(t *testing.T) {
	es := NewExpectedState(100, 1)
	for key := range int64(100) {
		state := es.Get(0, key)
		if state != ValueStateUnknown {
			t.Errorf("key %d: state = %d, want %d (Unknown)", key, state, ValueStateUnknown)
		}
	}
}

func TestExpectedStatePutGet(t *testing.T) {
	es := NewExpectedState(100, 1)

	es.Put(0, 42, 123)

	state := es.Get(0, 42)
	if state != ValueStateExists+123 {
		t.Errorf("Get(0, 42) = %d, want %d", state, ValueStateExists+123)
	}

	if !es.Exists(0, 42) {
		t.Error("Exists(0, 42) = false, want true")
	}

	valueID, ok := es.GetValueID(0, 42)
	if !ok {
		t.Error("GetValueID(0, 42) returned false, want true")
	}
	if valueID != 123 {
		t.Errorf("GetValueID(0, 42) = %d, want 123", valueID)
	}
}

func TestExpectedStateDelete(t *testing.T) {
	es := NewExpectedState(100, 1)

	// Put then delete
	es.Put(0, 42, 123)
	es.Delete(0, 42)

	state := es.Get(0, 42)
	if state != ValueStateDeleted {
		t.Errorf("Get(0, 42) = %d, want %d (Deleted)", state, ValueStateDeleted)
	}

	if !es.IsDeleted(0, 42) {
		t.Error("IsDeleted(0, 42) = false, want true")
	}

	if es.Exists(0, 42) {
		t.Error("Exists(0, 42) = true, want false")
	}
}

func TestExpectedStateMultipleCFs(t *testing.T) {
	es := NewExpectedState(100, 3)

	// Write different values to same key in different CFs
	es.Put(0, 10, 100)
	es.Put(1, 10, 200)
	es.Put(2, 10, 300)

	for cf := range 3 {
		valueID, ok := es.GetValueID(cf, 10)
		if !ok {
			t.Errorf("CF %d: GetValueID failed", cf)
		}
		expected := uint32((cf + 1) * 100)
		if valueID != expected {
			t.Errorf("CF %d: valueID = %d, want %d", cf, valueID, expected)
		}
	}
}

func TestExpectedStateSeqno(t *testing.T) {
	es := NewExpectedState(100, 1)

	if es.Seqno() != 0 {
		t.Errorf("initial Seqno = %d, want 0", es.Seqno())
	}

	es.Put(0, 1, 1)
	if es.Seqno() != 1 {
		t.Errorf("after Put, Seqno = %d, want 1", es.Seqno())
	}

	es.Delete(0, 2)
	if es.Seqno() != 2 {
		t.Errorf("after Delete, Seqno = %d, want 2", es.Seqno())
	}
}

func TestExpectedStateClear(t *testing.T) {
	es := NewExpectedState(100, 1)

	es.Put(0, 1, 100)
	es.Put(0, 2, 200)
	es.Delete(0, 3)

	es.Clear()

	for key := range int64(100) {
		if es.Get(0, key) != ValueStateUnknown {
			t.Errorf("after Clear, key %d not Unknown", key)
		}
	}

	if es.Seqno() != 0 {
		t.Errorf("after Clear, Seqno = %d, want 0", es.Seqno())
	}
}

func TestExpectedStateOutOfBounds(t *testing.T) {
	es := NewExpectedState(100, 2)

	// Invalid CF
	state := es.Get(-1, 50)
	if state != ValueStateUnknown {
		t.Errorf("invalid CF: state = %d, want Unknown", state)
	}

	state = es.Get(2, 50) // CF 2 is out of bounds (0, 1 valid)
	if state != ValueStateUnknown {
		t.Errorf("CF out of bounds: state = %d, want Unknown", state)
	}

	// Invalid key
	state = es.Get(0, -1)
	if state != ValueStateUnknown {
		t.Errorf("negative key: state = %d, want Unknown", state)
	}

	state = es.Get(0, 100) // Key 100 is out of bounds (0-99 valid)
	if state != ValueStateUnknown {
		t.Errorf("key out of bounds: state = %d, want Unknown", state)
	}
}

func TestExpectedStateConcurrentAccess(t *testing.T) {
	es := NewExpectedState(1000, 1)
	var wg sync.WaitGroup

	// Concurrent writes
	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				key := int64(id*100 + j)
				es.Put(0, key, uint32(id*1000+j))
			}
		}(i)
	}

	wg.Wait()

	// Verify all writes succeeded
	for key := range int64(1000) {
		if !es.Exists(0, key) {
			t.Errorf("key %d does not exist after concurrent writes", key)
		}
	}
}

func TestPendingExpectedValueCommit(t *testing.T) {
	es := NewExpectedState(100, 1)

	// Initial state
	es.Put(0, 42, 100)

	// Create pending operation
	pev := es.PreparePut(0, 42, 200)
	if pev == nil {
		t.Fatal("PreparePut returned nil")
	}

	// State should still be the original
	valueID, _ := es.GetValueID(0, 42)
	if valueID != 100 {
		t.Errorf("before commit: valueID = %d, want 100", valueID)
	}

	// Commit
	pev.Commit(200, false)

	// State should now be updated
	valueID, _ = es.GetValueID(0, 42)
	if valueID != 200 {
		t.Errorf("after commit: valueID = %d, want 200", valueID)
	}
}

func TestPendingExpectedValueRollback(t *testing.T) {
	es := NewExpectedState(100, 1)

	// Initial state
	es.Put(0, 42, 100)

	// Create pending operation
	pev := es.PreparePut(0, 42, 200)

	// Rollback
	pev.Rollback()

	// State should still be the original
	valueID, _ := es.GetValueID(0, 42)
	if valueID != 100 {
		t.Errorf("after rollback: valueID = %d, want 100", valueID)
	}
}

func TestPendingExpectedValueDeleteCommit(t *testing.T) {
	es := NewExpectedState(100, 1)

	es.Put(0, 42, 100)
	pev := es.PrepareDelete(0, 42)
	pev.Commit(0, true)

	if !es.IsDeleted(0, 42) {
		t.Error("key should be deleted after commit")
	}
}

func TestPendingExpectedValueDoubleCommit(t *testing.T) {
	es := NewExpectedState(100, 1)

	pev := es.PreparePut(0, 42, 100)
	pev.Commit(100, false)
	pev.Commit(200, false) // Should be ignored

	valueID, _ := es.GetValueID(0, 42)
	if valueID != 100 {
		t.Errorf("valueID = %d, want 100 (second commit should be ignored)", valueID)
	}
}

func TestGenerateValue(t *testing.T) {
	value := GenerateValue(42, 100, 64)

	if len(value) != 64 {
		t.Errorf("len(value) = %d, want 64", len(value))
	}

	if !VerifyValue(42, 100, value) {
		t.Error("VerifyValue returned false for generated value")
	}
}

func TestGenerateValueMinSize(t *testing.T) {
	// Request size smaller than minimum
	value := GenerateValue(1, 1, 5)

	if len(value) < 12 {
		t.Errorf("len(value) = %d, want >= 12", len(value))
	}
}

func TestVerifyValueWrongKey(t *testing.T) {
	value := GenerateValue(42, 100, 64)

	if VerifyValue(43, 100, value) {
		t.Error("VerifyValue should return false for wrong key")
	}
}

func TestVerifyValueWrongID(t *testing.T) {
	value := GenerateValue(42, 100, 64)

	if VerifyValue(42, 101, value) {
		t.Error("VerifyValue should return false for wrong valueID")
	}
}

func TestVerifyValueTooShort(t *testing.T) {
	value := []byte{1, 2, 3, 4, 5}

	if VerifyValue(1, 1, value) {
		t.Error("VerifyValue should return false for too-short value")
	}
}

func TestExpectedStateManager(t *testing.T) {
	es := NewExpectedState(100, 1)
	mgr := NewExpectedStateManager(es)

	// Initial state
	es.Put(0, 10, 100)
	es.Put(0, 20, 200)

	// Take snapshot
	mgr.TakeSnapshot()

	if mgr.NumSnapshots() != 1 {
		t.Errorf("NumSnapshots = %d, want 1", mgr.NumSnapshots())
	}

	// Modify state
	es.Put(0, 10, 999)
	es.Delete(0, 20)
	es.Put(0, 30, 300)

	// Verify modifications
	valueID, _ := es.GetValueID(0, 10)
	if valueID != 999 {
		t.Errorf("valueID = %d, want 999", valueID)
	}

	// Restore snapshot
	if !mgr.RestoreLatestSnapshot() {
		t.Error("RestoreLatestSnapshot returned false")
	}

	// Verify restored state
	valueID, _ = es.GetValueID(0, 10)
	if valueID != 100 {
		t.Errorf("after restore: valueID = %d, want 100", valueID)
	}

	valueID, _ = es.GetValueID(0, 20)
	if valueID != 200 {
		t.Errorf("after restore: key 20 valueID = %d, want 200", valueID)
	}

	if es.Exists(0, 30) {
		t.Error("key 30 should not exist after restore")
	}
}

func TestExpectedStateManagerMultipleSnapshots(t *testing.T) {
	es := NewExpectedState(100, 1)
	mgr := NewExpectedStateManager(es)

	es.Put(0, 1, 1)
	mgr.TakeSnapshot()

	es.Put(0, 1, 2)
	mgr.TakeSnapshot()

	es.Put(0, 1, 3)
	mgr.TakeSnapshot()

	if mgr.NumSnapshots() != 3 {
		t.Errorf("NumSnapshots = %d, want 3", mgr.NumSnapshots())
	}

	// Restore restores the latest (value 3 -> 2 should restore to snapshot 3's state)
	mgr.RestoreLatestSnapshot()
	valueID, _ := es.GetValueID(0, 1)
	if valueID != 3 {
		t.Errorf("valueID = %d, want 3", valueID)
	}
}

func TestExpectedStateManagerRestoreEmpty(t *testing.T) {
	es := NewExpectedState(100, 1)
	mgr := NewExpectedStateManager(es)

	if mgr.RestoreLatestSnapshot() {
		t.Error("RestoreLatestSnapshot should return false when no snapshots")
	}
}

func TestExpectedStateManagerClearSnapshots(t *testing.T) {
	es := NewExpectedState(100, 1)
	mgr := NewExpectedStateManager(es)

	mgr.TakeSnapshot()
	mgr.TakeSnapshot()
	mgr.ClearSnapshots()

	if mgr.NumSnapshots() != 0 {
		t.Errorf("NumSnapshots = %d, want 0", mgr.NumSnapshots())
	}
}

func TestExpectedStateValueStateConstants(t *testing.T) {
	// Verify value encoding
	if ValueStateUnknown != 0 {
		t.Errorf("ValueStateUnknown = %d, want 0", ValueStateUnknown)
	}
	if ValueStateDeleted != 1 {
		t.Errorf("ValueStateDeleted = %d, want 1", ValueStateDeleted)
	}
	if ValueStateExists != 2 {
		t.Errorf("ValueStateExists = %d, want 2", ValueStateExists)
	}
}

func TestExpectedStateLargeValueID(t *testing.T) {
	es := NewExpectedState(100, 1)

	// Large value ID near max uint32
	largeID := uint32(0xFFFFFF00) // Leave room for ValueStateExists offset
	es.Put(0, 1, largeID)

	valueID, ok := es.GetValueID(0, 1)
	if !ok {
		t.Error("GetValueID returned false")
	}
	if valueID != largeID {
		t.Errorf("valueID = %d, want %d", valueID, largeID)
	}
}

func TestExpectedStateOverwrite(t *testing.T) {
	es := NewExpectedState(100, 1)

	// Write multiple times
	for i := range uint32(100) {
		es.Put(0, 42, i)
	}

	// Final value should be the last one
	valueID, _ := es.GetValueID(0, 42)
	if valueID != 99 {
		t.Errorf("valueID = %d, want 99", valueID)
	}
}

// Benchmark concurrent operations
func BenchmarkExpectedStatePut(b *testing.B) {
	es := NewExpectedState(int64(b.N), 1)
	b.ResetTimer()

	for i := range b.N {
		es.Put(0, int64(i), uint32(i))
	}
}

func BenchmarkExpectedStateGet(b *testing.B) {
	es := NewExpectedState(int64(b.N), 1)
	for i := range b.N {
		es.Put(0, int64(i), uint32(i))
	}
	b.ResetTimer()

	for i := range b.N {
		es.Get(0, int64(i))
	}
}

func BenchmarkExpectedStateConcurrent(b *testing.B) {
	es := NewExpectedState(1000000, 1)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		key := int64(0)
		for pb.Next() {
			es.Put(0, key%1000000, uint32(key))
			key++
		}
	})
}

// ============================================================================
// Persistent ExpectedState Tests
// ============================================================================

func TestExpectedStateSaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/expected_state.bin"

	// Create and populate
	es := NewExpectedState(100, 2)
	es.Put(0, 10, 100)
	es.Put(0, 20, 200)
	es.Put(1, 30, 300)
	es.Delete(0, 50)

	// Save
	if err := es.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Load
	loaded, err := LoadExpectedStateFromFile(path)
	if err != nil {
		t.Fatalf("LoadExpectedStateFromFile failed: %v", err)
	}

	// Verify
	if loaded.maxKey != es.maxKey {
		t.Errorf("maxKey = %d, want %d", loaded.maxKey, es.maxKey)
	}
	if loaded.numColumnFamilies != es.numColumnFamilies {
		t.Errorf("numCFs = %d, want %d", loaded.numColumnFamilies, es.numColumnFamilies)
	}

	// Check values
	valueID, exists := loaded.GetValueID(0, 10)
	if !exists || valueID != 100 {
		t.Errorf("key 0:10 = (%d, %v), want (100, true)", valueID, exists)
	}

	valueID, exists = loaded.GetValueID(0, 20)
	if !exists || valueID != 200 {
		t.Errorf("key 0:20 = (%d, %v), want (200, true)", valueID, exists)
	}

	valueID, exists = loaded.GetValueID(1, 30)
	if !exists || valueID != 300 {
		t.Errorf("key 1:30 = (%d, %v), want (300, true)", valueID, exists)
	}

	if !loaded.IsDeleted(0, 50) {
		t.Error("key 0:50 should be deleted")
	}
}

func TestExpectedStateSaveLoadLarge(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/expected_state_large.bin"

	// Create large state
	es := NewExpectedState(10000, 3)
	for cf := range 3 {
		for key := range int64(10000) {
			switch key % 3 {
			case 0:
				es.Put(cf, key, uint32(key*10+int64(cf)))
			case 1:
				es.Delete(cf, key)
			}
			// key%3 == 2 left as unknown
		}
	}

	// Save
	if err := es.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Load
	loaded, err := LoadExpectedStateFromFile(path)
	if err != nil {
		t.Fatalf("LoadExpectedStateFromFile failed: %v", err)
	}

	// Verify all values match
	for cf := range 3 {
		for key := range int64(10000) {
			expected := es.Get(cf, key)
			actual := loaded.Get(cf, key)
			if expected != actual {
				t.Errorf("cf=%d key=%d: got %d, want %d", cf, key, actual, expected)
			}
		}
	}
}

func TestExpectedStateLoadInvalidFile(t *testing.T) {
	dir := t.TempDir()

	// Empty file
	emptyPath := dir + "/empty.bin"
	if err := writeFile(emptyPath, []byte{}); err != nil {
		t.Fatal(err)
	}
	_, err := LoadExpectedStateFromFile(emptyPath)
	if err == nil {
		t.Error("Expected error for empty file")
	}

	// Wrong magic
	wrongMagicPath := dir + "/wrong_magic.bin"
	data := make([]byte, 100)
	copy(data, "WRONGMAG")
	if err := writeFile(wrongMagicPath, data); err != nil {
		t.Fatal(err)
	}
	_, err = LoadExpectedStateFromFile(wrongMagicPath)
	if !errors.Is(err, errInvalidMagic) {
		t.Errorf("Expected errInvalidMagic, got %v", err)
	}
}

func TestExpectedStateLoadNonExistent(t *testing.T) {
	_, err := LoadExpectedStateFromFile("/nonexistent/path/state.bin")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}
