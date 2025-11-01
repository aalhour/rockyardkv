package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileExpectedState_CreateAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "expected_state.bin")

	// Create new file expected state
	fes, err := NewFileExpectedState(path, 1000, 1)
	if err != nil {
		t.Fatalf("Failed to create file expected state: %v", err)
	}

	// Put some values
	fes.Put(0, 100, 42)
	fes.Put(0, 200, 99)
	fes.Delete(0, 300)

	// Verify in-memory state
	if id, ok := fes.GetValueID(0, 100); !ok || id != 42 {
		t.Errorf("Expected valueID 42, got %d (ok=%v)", id, ok)
	}
	if !fes.IsDeleted(0, 300) {
		t.Error("Expected key 300 to be deleted")
	}

	// Save and close
	if err := fes.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Reload from file
	fes2, err := NewFileExpectedState(path, 1000, 1)
	if err != nil {
		t.Fatalf("Failed to load file expected state: %v", err)
	}
	defer fes2.Close()

	// Verify loaded state
	if id, ok := fes2.GetValueID(0, 100); !ok || id != 42 {
		t.Errorf("After reload: Expected valueID 42, got %d (ok=%v)", id, ok)
	}
	if id, ok := fes2.GetValueID(0, 200); !ok || id != 99 {
		t.Errorf("After reload: Expected valueID 99, got %d (ok=%v)", id, ok)
	}
	if !fes2.IsDeleted(0, 300) {
		t.Error("After reload: Expected key 300 to be deleted")
	}
	if fes2.Exists(0, 400) {
		t.Error("After reload: Expected key 400 to not exist")
	}
}

func TestFileExpectedState_MultipleColumnFamilies(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "expected_state.bin")

	fes, err := NewFileExpectedState(path, 100, 3)
	if err != nil {
		t.Fatalf("Failed to create: %v", err)
	}

	// Put values in different column families
	fes.Put(0, 10, 100)
	fes.Put(1, 10, 200)
	fes.Put(2, 10, 300)

	// Verify they're separate
	if id, ok := fes.GetValueID(0, 10); !ok || id != 100 {
		t.Errorf("CF0: Expected 100, got %d", id)
	}
	if id, ok := fes.GetValueID(1, 10); !ok || id != 200 {
		t.Errorf("CF1: Expected 200, got %d", id)
	}
	if id, ok := fes.GetValueID(2, 10); !ok || id != 300 {
		t.Errorf("CF2: Expected 300, got %d", id)
	}

	fes.Close()
}

func TestFileExpectedState_Seqno(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "expected_state.bin")

	fes, err := NewFileExpectedState(path, 100, 1)
	if err != nil {
		t.Fatalf("Failed to create: %v", err)
	}

	initialSeqno := fes.Seqno()
	fes.Put(0, 1, 1)
	fes.Put(0, 2, 2)
	fes.Delete(0, 3)

	expectedSeqno := initialSeqno + 3
	if fes.Seqno() != expectedSeqno {
		t.Errorf("Expected seqno %d, got %d", expectedSeqno, fes.Seqno())
	}

	fes.Close()

	// Reload and check seqno persists
	fes2, _ := NewFileExpectedState(path, 100, 1)
	if fes2.Seqno() != expectedSeqno {
		t.Errorf("After reload: Expected seqno %d, got %d", expectedSeqno, fes2.Seqno())
	}
	fes2.Close()
}

func TestFileExpectedState_Clear(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "expected_state.bin")

	fes, err := NewFileExpectedState(path, 100, 1)
	if err != nil {
		t.Fatalf("Failed to create: %v", err)
	}

	fes.Put(0, 1, 100)
	fes.Put(0, 2, 200)

	fes.Clear()

	if fes.Exists(0, 1) {
		t.Error("Expected key 1 to not exist after clear")
	}
	if fes.Seqno() != 0 {
		t.Errorf("Expected seqno 0 after clear, got %d", fes.Seqno())
	}

	fes.Close()
}

func TestFileExpectedState_InvalidFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "invalid.bin")

	// Create invalid file
	if err := os.WriteFile(path, []byte("invalid"), 0644); err != nil {
		t.Fatalf("Failed to create invalid file: %v", err)
	}

	_, err := NewFileExpectedState(path, 100, 1)
	if err == nil {
		t.Error("Expected error for invalid file")
	}
}

func TestFileExpectedState_ConfigMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "expected_state.bin")

	// Create with one config
	fes, err := NewFileExpectedState(path, 1000, 1)
	if err != nil {
		t.Fatalf("Failed to create: %v", err)
	}
	fes.Close()

	// Try to load with different config
	_, err = NewFileExpectedState(path, 2000, 1) // Different maxKey
	if err == nil {
		t.Error("Expected error for config mismatch")
	}
}

func TestFileExpectedState_SyncOnDirty(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "expected_state.bin")

	fes, err := NewFileExpectedState(path, 100, 1)
	if err != nil {
		t.Fatalf("Failed to create: %v", err)
	}

	// Sync when not dirty should be fast
	if err := fes.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}

	// Make dirty
	fes.Put(0, 1, 100)

	// Sync should write
	if err := fes.Sync(); err != nil {
		t.Errorf("Sync after put failed: %v", err)
	}

	fes.Close()
}

func TestFileExpectedState_LargeKeyspace(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "expected_state.bin")

	maxKey := int64(100000)
	fes, err := NewFileExpectedState(path, maxKey, 1)
	if err != nil {
		t.Fatalf("Failed to create: %v", err)
	}

	// Put values at various positions
	fes.Put(0, 0, 1)
	fes.Put(0, maxKey/2, 2)
	fes.Put(0, maxKey-1, 3)

	fes.Close()

	// Reload and verify
	fes2, _ := NewFileExpectedState(path, maxKey, 1)
	defer fes2.Close()

	if id, ok := fes2.GetValueID(0, 0); !ok || id != 1 {
		t.Errorf("Key 0: Expected 1, got %d", id)
	}
	if id, ok := fes2.GetValueID(0, maxKey/2); !ok || id != 2 {
		t.Errorf("Key %d: Expected 2, got %d", maxKey/2, id)
	}
	if id, ok := fes2.GetValueID(0, maxKey-1); !ok || id != 3 {
		t.Errorf("Key %d: Expected 3, got %d", maxKey-1, id)
	}
}
