// Tests for the collision detection tool itself.
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/db"
)

// TestCollisionCheck_NoFalsePositives verifies the tool doesn't report
// collisions in a valid database with no actual collisions.
//
// Contract: Large valid DB with many keys should report 0 collisions.
func TestCollisionCheck_NoFalsePositives(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer database.Close()

	writeOpts := db.DefaultWriteOptions()

	// Write 1000 keys across multiple flushes
	for flush := range 5 {
		for i := range 200 {
			key := fmt.Appendf(nil, "key_%03d_%04d", flush, i)
			value := fmt.Appendf(nil, "value_%03d_%04d", flush, i)
			if err := database.Put(writeOpts, key, value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	}

	database.Close()

	// Run collision checker
	cmd := exec.Command("go", "run", ".", dir)
	cmd.Dir = "."
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Collision check failed: %v\nOutput:\n%s", err, output)
	}

	// Verify no collisions reported
	if bytes.Contains(output, []byte("SMOKING GUN")) {
		t.Errorf("False positive: collision reported in valid DB:\n%s", output)
	}

	if !bytes.Contains(output, []byte("No internal key collisions")) {
		t.Errorf("Unexpected output (should say no collisions):\n%s", output)
	}

	t.Log("✅ No false positives on valid database")
}

// TestCollisionCheck_EmptyDatabase verifies the tool handles empty databases.
//
// Contract: Empty DB should report 0 collisions without errors.
func TestCollisionCheck_EmptyDatabase(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	database.Close()

	// Run collision checker on empty DB
	dbPath := filepath.Join(dir, "db")
	cmd := exec.Command("go", "run", ".", dbPath)
	cmd.Dir = "."
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Collision check on empty DB failed: %v\nOutput:\n%s", err, output)
	}

	// Should report no collisions
	if !bytes.Contains(output, []byte("No internal key collisions")) {
		t.Errorf("Unexpected output on empty DB:\n%s", output)
	}

	t.Log("✅ Empty database handled correctly")
}

// TestCollisionCheck_SingleSST verifies the tool works with just one SST file.
//
// Contract: Single SST can't have collisions (within same file), should report 0.
func TestCollisionCheck_SingleSST(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer database.Close()

	writeOpts := db.DefaultWriteOptions()

	// Write data and flush (creates one SST)
	for i := range 100 {
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

	// Run collision checker
	cmd := exec.Command("go", "run", ".", dir)
	cmd.Dir = "."
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Collision check failed: %v\nOutput:\n%s", err, output)
	}

	// Should report no collisions
	if bytes.Contains(output, []byte("SMOKING GUN")) {
		t.Errorf("False positive on single SST:\n%s", output)
	}

	if !bytes.Contains(output, []byte("No internal key collisions")) {
		t.Errorf("Unexpected output:\n%s", output)
	}

	t.Log("✅ Single SST handled correctly")
}

// TestCollisionCheck_NonexistentPath verifies the tool handles invalid paths.
//
// Contract: Invalid path should produce a clear error, not crash.
func TestCollisionCheck_NonexistentPath(t *testing.T) {
	cmd := exec.Command("go", "run", ".", "/nonexistent/path/to/db")
	cmd.Dir = "."
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Errorf("Expected error for nonexistent path, got success:\n%s", output)
	}

	// Should have an error message
	if len(output) == 0 {
		t.Error("No error message for nonexistent path")
	}

	t.Log("✅ Nonexistent path handled gracefully")
}

// TestCollisionCheck_CorruptedSST verifies the tool handles corrupted SST files.
//
// Contract: Corrupted SST should be reported, tool doesn't crash.
func TestCollisionCheck_CorruptedSST(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	writeOpts := db.DefaultWriteOptions()

	// Write and flush to create an SST
	for i := range 50 {
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

	// Corrupt an SST file
	dbPath := filepath.Join(dir, "db")
	sstFiles, err := filepath.Glob(filepath.Join(dbPath, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to find SST files: %v", err)
	}

	if len(sstFiles) == 0 {
		t.Fatal("No SST files found")
	}

	// Truncate the SST file to corrupt it
	if err := os.Truncate(sstFiles[0], 100); err != nil {
		t.Fatalf("Failed to corrupt SST: %v", err)
	}

	// Run collision checker (should handle corruption gracefully)
	cmd := exec.Command("go", "run", ".", dbPath)
	cmd.Dir = "."
	output, _ := cmd.CombinedOutput() // Error expected for corrupted file

	// It's okay if it fails (corrupted file), but shouldn't crash
	t.Logf("Output from corrupted SST check:\n%s", output)

	// Main requirement: tool doesn't panic/crash
	if bytes.Contains(output, []byte("panic")) {
		t.Errorf("Tool panicked on corrupted SST:\n%s", output)
	}

	t.Log("✅ Corrupted SST handled without panic")
}

// TestCollisionCheck_MultipleSSTs verifies the tool scans all SST files.
//
// Contract: Keys spread across multiple SSTs should all be checked.
func TestCollisionCheck_MultipleSSTs(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer database.Close()

	writeOpts := db.DefaultWriteOptions()

	// Create multiple SST files by flushing separately
	for flush := range 10 {
		for i := range 100 {
			key := fmt.Appendf(nil, "key_%03d_%04d", flush, i)
			value := fmt.Appendf(nil, "value_%03d_%04d", flush, i)
			if err := database.Put(writeOpts, key, value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	}

	database.Close()

	// Count SST files
	dbPath := filepath.Join(dir, "db")
	sstFiles, err := filepath.Glob(filepath.Join(dbPath, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to glob SST files: %v", err)
	}

	t.Logf("Created %d SST files", len(sstFiles))

	if len(sstFiles) < 5 {
		t.Errorf("Expected multiple SST files, got %d", len(sstFiles))
	}

	// Run collision checker
	cmd := exec.Command("go", "run", ".", dbPath)
	cmd.Dir = "."
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Collision check failed: %v\nOutput:\n%s", err, output)
	}

	// Verify no collisions (all keys are unique)
	if !bytes.Contains(output, []byte("No internal key collisions")) {
		t.Errorf("Unexpected output:\n%s", output)
	}

	t.Logf("✅ Multiple SSTs scanned successfully")
}

// TestCollisionCheck_UpdatedKeys verifies the tool correctly handles the
// same user key with different sequence numbers (normal updates).
//
// Contract: Same user key with different sequences is NOT a collision.
func TestCollisionCheck_UpdatedKeys(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer database.Close()

	writeOpts := db.DefaultWriteOptions()

	// Write the same keys multiple times (creates different sequences)
	for update := range 5 {
		for i := range 20 {
			key := fmt.Appendf(nil, "key_%04d", i)
			value := fmt.Appendf(nil, "value_update%d_%04d", update, i)
			if err := database.Put(writeOpts, key, value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	}

	database.Close()

	// Run collision checker
	cmd := exec.Command("go", "run", ".", dir)
	cmd.Dir = "."
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Collision check failed: %v\nOutput:\n%s", err, output)
	}

	// Should report no collisions (different sequences = not a collision)
	if bytes.Contains(output, []byte("SMOKING GUN")) {
		t.Errorf("False positive: normal key updates reported as collision:\n%s", output)
	}

	if !bytes.Contains(output, []byte("No internal key collisions")) {
		t.Errorf("Unexpected output:\n%s", output)
	}

	t.Log("✅ Updated keys (different sequences) not reported as collisions")
}
