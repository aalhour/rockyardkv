package campaign

import (
	"os"
	"path/filepath"
	"testing"
)

func TestKnownFailures_InMemory(t *testing.T) {
	kf := NewKnownFailures("")

	// Initially empty
	if kf.Count() != 0 {
		t.Errorf("Count() = %d, want 0", kf.Count())
	}

	// Record first failure
	isNew := kf.Record("fp1", "instance1", "2024-01-01T00:00:00Z")
	if !isNew {
		t.Error("first Record() should return true (new)")
	}
	if kf.Count() != 1 {
		t.Errorf("Count() = %d, want 1", kf.Count())
	}

	// Check duplicate
	if !kf.IsDuplicate("fp1") {
		t.Error("IsDuplicate() should return true for recorded fingerprint")
	}
	if kf.IsDuplicate("fp2") {
		t.Error("IsDuplicate() should return false for unknown fingerprint")
	}

	// Record duplicate
	isNew = kf.Record("fp1", "instance1", "2024-01-01T00:00:01Z")
	if isNew {
		t.Error("duplicate Record() should return false")
	}
	if kf.Count() != 1 {
		t.Errorf("Count() = %d, want 1 (duplicate shouldn't increase count)", kf.Count())
	}

	// Record new
	isNew = kf.Record("fp2", "instance2", "2024-01-01T00:00:02Z")
	if !isNew {
		t.Error("new Record() should return true")
	}
	if kf.Count() != 2 {
		t.Errorf("Count() = %d, want 2", kf.Count())
	}
}

func TestKnownFailures_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "known-failures.json")

	// Create and populate
	kf1 := NewKnownFailures(path)
	kf1.Record("fp1", "instance1", "2024-01-01T00:00:00Z")
	kf1.Record("fp2", "instance2", "2024-01-01T00:00:01Z")

	// Load from disk
	kf2 := NewKnownFailures(path)
	if kf2.Count() != 2 {
		t.Errorf("Count() after reload = %d, want 2", kf2.Count())
	}
	if !kf2.IsDuplicate("fp1") {
		t.Error("fp1 should be loaded from disk")
	}
	if !kf2.IsDuplicate("fp2") {
		t.Error("fp2 should be loaded from disk")
	}
}

func TestKnownFailures_All(t *testing.T) {
	kf := NewKnownFailures("")
	kf.Record("fp1", "instance1", "2024-01-01T00:00:00Z")
	kf.Record("fp2", "instance2", "2024-01-01T00:00:01Z")

	all := kf.All()
	if len(all) != 2 {
		t.Errorf("All() length = %d, want 2", len(all))
	}

	// Check contents
	fingerprints := make(map[string]bool)
	for _, f := range all {
		fingerprints[f.Fingerprint] = true
	}
	if !fingerprints["fp1"] || !fingerprints["fp2"] {
		t.Error("All() should contain both fingerprints")
	}
}

func TestKnownFailures_NonexistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nonexistent", "known-failures.json")

	// Should not panic on non-existent file
	kf := NewKnownFailures(path)
	if kf.Count() != 0 {
		t.Errorf("Count() = %d, want 0 for non-existent file", kf.Count())
	}

	// Should create directory on save
	kf.Record("fp1", "instance1", "2024-01-01T00:00:00Z")

	// Verify file exists
	if _, err := os.Stat(path); err != nil {
		t.Errorf("file should exist after Record(): %v", err)
	}
}
