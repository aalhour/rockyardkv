package campaign

import (
	"os"
	"path/filepath"
	"testing"
)

// Contract: A fingerprint is not a duplicate until it has been recorded.
func TestKnownFailures_RecordAndIsDuplicate(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "known.json")

	kf := NewKnownFailures(path)

	fingerprint := "abc123def456"

	if kf.IsDuplicate(fingerprint) {
		t.Error("new fingerprint should not be duplicate")
	}

	kf.Record(fingerprint, "test.instance", "2025-01-01T00:00:00Z")

	if !kf.IsDuplicate(fingerprint) {
		t.Error("recorded fingerprint should be duplicate")
	}
}

// Contract: Count returns the number of unique fingerprints recorded.
func TestKnownFailures_Count(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "known.json")

	kf := NewKnownFailures(path)

	if kf.Count() != 0 {
		t.Errorf("Count() = %d, want 0", kf.Count())
	}

	kf.Record("fp1", "instance1", "2025-01-01T00:00:00Z")
	kf.Record("fp2", "instance2", "2025-01-01T00:00:00Z")

	if kf.Count() != 2 {
		t.Errorf("Count() = %d, want 2", kf.Count())
	}
}

// Contract: Recording the same fingerprint twice does not increase the count.
func TestKnownFailures_RecordIgnoresDuplicates(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "known.json")

	kf := NewKnownFailures(path)

	kf.Record("same-fp", "instance1", "2025-01-01T00:00:00Z")
	kf.Record("same-fp", "instance2", "2025-01-02T00:00:00Z")

	if kf.Count() != 1 {
		t.Errorf("Count() = %d, want 1 (duplicate should be ignored)", kf.Count())
	}
}

// Contract: Fingerprints persist across process restarts via JSON file.
func TestKnownFailures_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "known.json")

	kf1 := NewKnownFailures(path)
	kf1.Record("persistent-fp", "test.instance", "2025-01-01T00:00:00Z")

	kf2 := NewKnownFailures(path)

	if !kf2.IsDuplicate("persistent-fp") {
		t.Error("fingerprint should persist across instances")
	}

	if kf2.Count() != 1 {
		t.Errorf("Count() = %d, want 1 after reload", kf2.Count())
	}
}

// Contract: Loading from a nonexistent file initializes an empty tracker.
func TestKnownFailures_LoadNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nonexistent.json")

	kf := NewKnownFailures(path)

	if kf.Count() != 0 {
		t.Errorf("Count() = %d, want 0 for nonexistent file", kf.Count())
	}
}

// Contract: Loading from an invalid JSON file initializes an empty tracker.
func TestKnownFailures_LoadInvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "invalid.json")

	if err := os.WriteFile(path, []byte("not valid json"), 0o644); err != nil {
		t.Fatal(err)
	}

	kf := NewKnownFailures(path)

	if kf.Count() != 0 {
		t.Errorf("Count() = %d, want 0 for invalid JSON", kf.Count())
	}
}

// Contract: All returns all recorded failures.
func TestKnownFailures_All(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "known.json")

	kf := NewKnownFailures(path)

	// Empty tracker returns empty slice
	all := kf.All()
	if len(all) != 0 {
		t.Errorf("All() length = %d, want 0", len(all))
	}

	// Record some failures
	kf.Record("fp1", "instance1", "2025-01-01T00:00:00Z")
	kf.Record("fp2", "instance2", "2025-01-02T00:00:00Z")

	all = kf.All()
	if len(all) != 2 {
		t.Errorf("All() length = %d, want 2", len(all))
	}

	// Verify the returned failures contain expected data
	fpMap := make(map[string]bool)
	for _, f := range all {
		fpMap[f.Fingerprint] = true
	}

	if !fpMap["fp1"] || !fpMap["fp2"] {
		t.Error("All() should contain both recorded fingerprints")
	}
}
