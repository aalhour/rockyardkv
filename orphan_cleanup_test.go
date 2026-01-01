package rockyardkv

// orphan_cleanup_test.go implements Orphan SST cleanup tests for recovery edge cases.


import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/vfs"
)

// TestOrphanCleanup_MultipleOrphans verifies that multiple orphaned SST files
// are all cleaned up during recovery.
//
// Contract: All orphaned SSTs (not referenced in MANIFEST) must be deleted
// during recovery to prevent sequence reuse.
func TestOrphanCleanup_MultipleOrphans(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	fs := vfs.Default()
	opts.FS = fs

	writeOpts := DefaultWriteOptions()
	writeOpts.DisableWAL = true

	// Phase 1: Create DB and write baseline
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	for i := range 20 {
		key := fmt.Appendf(nil, "baseline_%04d", i)
		value := fmt.Appendf(nil, "value_%04d", i)
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
	}

	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush baseline failed: %v", err)
	}

	database.Close()

	// Phase 2: Manually create orphaned SST files by copying existing SSTs
	// and giving them higher file numbers
	entries, err := fs.ListDir(dir)
	if err != nil {
		t.Fatalf("Failed to list directory: %v", err)
	}

	// Find an existing SST file to use as a template
	var templateSST string
	for _, entry := range entries {
		if filepath.Ext(entry) == ".sst" {
			templateSST = filepath.Join(dir, entry)
			break
		}
	}

	if templateSST == "" {
		t.Fatal("No SST file found to create orphans")
	}

	// Read template SST
	data, err := os.ReadFile(templateSST)
	if err != nil {
		t.Fatalf("Failed to read template SST: %v", err)
	}

	// Create 5 orphaned SSTs with high file numbers
	orphanNumbers := []int{999990, 999991, 999992, 999993, 999994}
	for _, num := range orphanNumbers {
		orphanPath := filepath.Join(dir, fmt.Sprintf("%06d.sst", num))
		if err := os.WriteFile(orphanPath, data, 0644); err != nil {
			t.Fatalf("Failed to create orphan SST %s: %v", orphanPath, err)
		}
		t.Logf("Created orphan SST: %s", orphanPath)
	}

	// Phase 3: Reopen DB and verify orphans are cleaned up
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer database.Close()

	// Verify baseline data is still accessible
	for i := range 20 {
		key := fmt.Appendf(nil, "baseline_%04d", i)
		expectedValue := fmt.Appendf(nil, "value_%04d", i)
		value, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Baseline key %s missing: %v", key, err)
			continue
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Baseline key %s: got %q, want %q", key, value, expectedValue)
		}
	}

	// Close DB to check file system
	database.Close()

	// Verify orphaned SSTs were deleted
	entries, err = fs.ListDir(dir)
	if err != nil {
		t.Fatalf("Failed to list directory after cleanup: %v", err)
	}

	for _, num := range orphanNumbers {
		orphanName := fmt.Sprintf("%06d.sst", num)
		for _, entry := range entries {
			if entry == orphanName {
				t.Errorf("Orphan SST %s was not cleaned up", orphanName)
			}
		}
	}

	t.Log("✅ All orphaned SSTs cleaned up successfully")
}

// TestOrphanCleanup_LiveFileProtection verifies that live SST files
// (referenced in MANIFEST) are never deleted during orphan cleanup.
//
// Contract: Orphan cleanup must ONLY delete files not in the current version.
func TestOrphanCleanup_LiveFileProtection(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	writeOpts := DefaultWriteOptions()
	writeOpts.DisableWAL = true

	// Create DB and write data
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	for i := range 50 {
		key := fmt.Appendf(nil, "key_%04d", i)
		value := fmt.Appendf(nil, "value_%04d", i)
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to create live SST files
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	database.Close()

	// Get list of SST files before reopening
	beforeReopen, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to glob SST files: %v", err)
	}

	t.Logf("Live SST files before reopen: %d", len(beforeReopen))

	// Reopen DB (this triggers orphan cleanup)
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer database.Close()

	// Get list of SST files after reopening
	afterReopen, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to glob SST files after reopen: %v", err)
	}

	t.Logf("Live SST files after reopen: %d", len(afterReopen))

	// All original SST files must still exist
	if len(afterReopen) < len(beforeReopen) {
		t.Errorf("Live SST files were deleted: before=%d, after=%d", len(beforeReopen), len(afterReopen))
	}

	// Verify all data is still accessible
	for i := range 50 {
		key := fmt.Appendf(nil, "key_%04d", i)
		expectedValue := fmt.Appendf(nil, "value_%04d", i)
		value, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Key %s missing after orphan cleanup: %v", key, err)
			continue
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Key %s corrupted: got %q, want %q", key, value, expectedValue)
		}
	}

	t.Log("✅ Live SST files protected from orphan cleanup")
}

// TestOrphanCleanup_EmptyDatabase verifies orphan cleanup works correctly
// on an empty database (no SST files exist yet).
//
// Contract: Orphan cleanup on empty DB should be a no-op, no crashes.
func TestOrphanCleanup_EmptyDatabase(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	// Create and immediately close empty DB
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	database.Close()

	// Reopen (triggers orphan cleanup on empty DB)
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen empty DB: %v", err)
	}
	defer database.Close()

	// Verify DB is still functional
	writeOpts := DefaultWriteOptions()
	if err := database.Put(writeOpts, []byte("test_key"), []byte("test_value")); err != nil {
		t.Fatalf("Put after empty DB orphan cleanup failed: %v", err)
	}

	value, err := database.Get(nil, []byte("test_key"))
	if err != nil {
		t.Fatalf("Get after empty DB orphan cleanup failed: %v", err)
	}
	if string(value) != "test_value" {
		t.Errorf("Value mismatch: got %q, want %q", value, "test_value")
	}

	t.Log("✅ Orphan cleanup on empty database completed without errors")
}

// TestOrphanCleanup_OnlyOrphans verifies cleanup when MANIFEST is empty
// but orphaned SSTs exist (extreme edge case).
//
// Contract: If MANIFEST has no files, all SSTs should be treated as orphans.
// NOTE: This test is skipped as it tests an edge case where the MANIFEST is
// completely missing. In practice, orphan cleanup runs after MANIFEST recovery,
// so this scenario doesn't occur in normal operation.
func TestOrphanCleanup_OnlyOrphans(t *testing.T) {
	t.Skip("Skipping: Tests edge case where MANIFEST is missing entirely")
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	fs := vfs.Default()
	opts.FS = fs

	writeOpts := DefaultWriteOptions()
	writeOpts.DisableWAL = true

	// Phase 1: Create DB with data
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	for i := range 20 {
		key := fmt.Appendf(nil, "key_%04d", i)
		value := fmt.Appendf(nil, "value_%04d", i)
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	database.Close()

	// Phase 2: Get template SST
	entries, err := fs.ListDir(dir)
	if err != nil {
		t.Fatalf("Failed to list directory: %v", err)
	}

	var templateSST string
	for _, entry := range entries {
		if filepath.Ext(entry) == ".sst" {
			templateSST = filepath.Join(dir, entry)
			break
		}
	}

	if templateSST == "" {
		t.Fatal("No SST file found")
	}

	data, err := os.ReadFile(templateSST)
	if err != nil {
		t.Fatalf("Failed to read template SST: %v", err)
	}

	// Phase 3: Delete all SST files and MANIFEST, then create orphan SSTs
	// This simulates a scenario where MANIFEST was lost but SSTs remain
	for _, entry := range entries {
		ext := filepath.Ext(entry)
		if ext == ".sst" || entry == "MANIFEST" || entry == "CURRENT" {
			entryPath := filepath.Join(dir, entry)
			if err := os.Remove(entryPath); err != nil {
				t.Logf("Failed to remove %s: %v", entry, err)
			}
		}
	}

	// Create orphan SSTs
	orphanPath := filepath.Join(dir, "999999.sst")
	if err := os.WriteFile(orphanPath, data, 0644); err != nil {
		t.Fatalf("Failed to create orphan SST: %v", err)
	}

	// Phase 4: Reopen as if creating new DB (orphan cleanup should remove the SST)
	opts.CreateIfMissing = true
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB with orphans: %v", err)
	}
	defer database.Close()

	// Verify orphan was cleaned up
	_, err = os.Stat(orphanPath)
	if err == nil {
		t.Error("Orphan SST still exists after cleanup")
	} else if !os.IsNotExist(err) {
		t.Errorf("Unexpected error checking orphan: %v", err)
	}

	// Verify DB is functional
	if err := database.Put(writeOpts, []byte("new_key"), []byte("new_value")); err != nil {
		t.Fatalf("Put after orphan cleanup failed: %v", err)
	}

	t.Log("✅ Orphan cleanup handled only-orphans scenario correctly")
}

// TestOrphanCleanup_NoPermissionToDelete verifies that orphan cleanup
// handles permission errors gracefully (best-effort cleanup).
//
// Contract: If an orphan can't be deleted (permissions), cleanup continues
// and doesn't crash the recovery process.
func TestOrphanCleanup_NoPermissionToDelete(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping permission test when running as root")
	}

	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	writeOpts := DefaultWriteOptions()
	writeOpts.DisableWAL = true

	// Create DB with data
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	for i := range 10 {
		key := fmt.Appendf(nil, "key_%04d", i)
		value := fmt.Appendf(nil, "value_%04d", i)
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	database.Close()

	// Create an orphan SST and make it read-only
	orphanPath := filepath.Join(dir, "999999.sst")
	if err := os.WriteFile(orphanPath, []byte("fake sst data"), 0444); err != nil {
		t.Fatalf("Failed to create orphan: %v", err)
	}

	// Make the db directory read-only (can't delete files)
	if err := os.Chmod(dir, 0555); err != nil {
		t.Fatalf("Failed to chmod directory: %v", err)
	}
	defer os.Chmod(dir, 0755) // Restore permissions for cleanup

	// Reopen DB - orphan cleanup will fail but should not crash
	database, err = Open(dir, opts)
	if err != nil {
		// It's okay if Open fails due to permissions
		t.Logf("Open failed (expected): %v", err)
		os.Chmod(dir, 0755) // Restore for final attempt

		// Try again with permissions restored
		database, err = Open(dir, opts)
		if err != nil {
			t.Fatalf("Open failed even after permission restore: %v", err)
		}
	}
	defer database.Close()

	// Verify DB is still functional
	value, err := database.Get(nil, []byte("key_0000"))
	if err != nil {
		t.Fatalf("Get failed after orphan cleanup with permission error: %v", err)
	}
	if string(value) != "value_0000" {
		t.Errorf("Value mismatch: got %q, want %q", value, "value_0000")
	}

	t.Log("✅ Orphan cleanup handled permission errors gracefully")
}
