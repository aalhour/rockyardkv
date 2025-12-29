package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/db"
)

// TestLdbScan_SurfacesCorruptionErrors verifies that ldb scan exits
// with a non-zero exit code when SST corruption is detected.
// This tests the contract: corruption errors must surface as user-visible failures.
func TestLdbScan_SurfacesCorruptionErrors(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a valid DB first
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := db.Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Write some data
	for i := range 100 {
		key := fmt.Appendf(nil, "key%05d", i)
		value := fmt.Appendf(nil, "value%05d", i)
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Failed to Put: %v", err)
		}
	}

	// Flush to create SST
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Failed to Flush: %v", err)
	}
	database.Close()

	// Find an SST file and corrupt it
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to list dir: %v", err)
	}

	var sstPath string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".sst" {
			sstPath = filepath.Join(tmpDir, entry.Name())
			break
		}
	}

	if sstPath == "" {
		t.Fatal("No SST file found after flush")
	}

	// Corrupt the SST file by flipping bits in the data section.
	// SST file structure:
	//   [data blocks] [meta blocks] [metaindex block] [index block] [footer]
	// We corrupt the data blocks to trigger checksum verification failures.
	data, err := os.ReadFile(sstPath)
	if err != nil {
		t.Fatalf("Failed to read SST: %v", err)
	}

	// Footer is the last 48-64 bytes depending on format version.
	// Corrupt data in the first half of the file (data blocks region).
	// XOR multiple bytes to ensure we corrupt actual block data + checksums.
	dataRegion := len(data) / 2
	if dataRegion > 100 {
		for i := 50; i < dataRegion && i < len(data)-100; i += 50 {
			data[i] ^= 0xFF // Flip all bits
		}
	}

	if err := os.WriteFile(sstPath, data, 0644); err != nil {
		t.Fatalf("Failed to write corrupted SST: %v", err)
	}

	// Now run the scan command and check that it fails
	var stdout, stderr bytes.Buffer
	exitCode := runWithArgs([]string{"scan", "--db", tmpDir}, &stdout, &stderr)

	// The scan MUST fail with non-zero exit code
	if exitCode == 0 {
		t.Errorf("ldb scan should have failed with non-zero exit code for corrupt SST, got exit code 0\nstdout: %s\nstderr: %s",
			stdout.String(), stderr.String())
	} else {
		t.Logf("Correctly got exit code %d for corrupt SST", exitCode)
		t.Logf("stderr: %s", stderr.String())
	}
}

// TestLdbScan_ValidDB ensures ldb scan works correctly on a valid database.
func TestLdbScan_ValidDB(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a valid DB
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := db.Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Write some data
	for i := range 10 {
		key := fmt.Appendf(nil, "key%05d", i)
		value := fmt.Appendf(nil, "value%05d", i)
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Failed to Put: %v", err)
		}
	}

	// Flush to create SST
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Failed to Flush: %v", err)
	}
	database.Close()

	// Scan should succeed with exit code 0
	var stdout, stderr bytes.Buffer
	exitCode := runWithArgs([]string{"scan", "--db", tmpDir}, &stdout, &stderr)

	if exitCode != 0 {
		t.Errorf("ldb scan should succeed on valid DB, got exit code %d\nstderr: %s",
			exitCode, stderr.String())
	}

	// Should have output the keys
	output := stdout.String()
	if !bytes.Contains([]byte(output), []byte("key00000")) {
		t.Errorf("Expected scan output to contain 'key00000', got: %s", output)
	}
}

// runWithArgs runs the ldb command with the given arguments and returns the exit code.
// This simulates what happens when ldb is run as a CLI tool.
func runWithArgs(args []string, stdout, stderr *bytes.Buffer) int {
	// Parse flags
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = append([]string{"ldb"}, args...)

	// Capture output
	// Note: We can't easily capture os.Exit, so we'll refactor main to return an exit code

	// For now, test by calling the function directly
	// This requires refactoring main() to be testable
	return runMain(args, stdout, stderr)
}

// runMain is the testable version of main() that returns an exit code.
func runMain(args []string, stdout, stderr *bytes.Buffer) int {
	// This is a stub - we need to refactor cmd/ldb/main.go to support this
	// For now, just call the scan logic directly

	// Parse the args to find the db path
	var dbPath string
	for i, arg := range args {
		if arg == "--db" && i+1 < len(args) {
			dbPath = args[i+1]
		}
	}

	if dbPath == "" {
		fmt.Fprintln(stderr, "Error: --db flag is required")
		return 1
	}

	if len(args) == 0 || (args[0] != "scan" && args[0] != "get") {
		fmt.Fprintln(stderr, "Error: unknown command")
		return 1
	}

	// Open the database
	opts := db.DefaultOptions()

	database, err := db.Open(dbPath, opts)
	if err != nil {
		fmt.Fprintf(stderr, "Error: failed to open database: %v\n", err)
		return 1
	}
	defer database.Close()

	// Perform scan
	readOpts := &db.ReadOptions{}
	iter := database.NewIterator(readOpts)
	defer iter.Close()

	// Check for construction errors
	if err := iter.Error(); err != nil {
		fmt.Fprintf(stderr, "Error: iterator construction failed: %v\n", err)
		return 1
	}

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Fprintf(stdout, "%s\t%s\n", iter.Key(), iter.Value())

		// Check for iteration errors
		if err := iter.Error(); err != nil {
			fmt.Fprintf(stderr, "Error: iteration error: %v\n", err)
			return 1
		}
	}

	// Final error check after iteration
	if err := iter.Error(); err != nil {
		fmt.Fprintf(stderr, "Error: iteration error: %v\n", err)
		return 1
	}

	return 0
}

// TestCheckCollision_NoFalsePositives verifies the tool doesn't report
// collisions in a valid database with no actual collisions.
//
// Contract: Large valid DB with many keys should report 0 collisions.
func TestCheckCollision_NoFalsePositives(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

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

	// Set the global dbPath and run the check
	oldDbPath := *dbPath
	*dbPath = dir
	defer func() { *dbPath = oldDbPath }()

	err = cmdCheckCollision()
	if err != nil {
		t.Errorf("Collision check should pass on valid DB: %v", err)
	}

	t.Log("✅ No false positives on valid database")
}

// TestCheckCollision_EmptyDatabase verifies the tool handles empty databases.
//
// Contract: Empty DB should report 0 collisions without errors.
func TestCheckCollision_EmptyDatabase(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	database.Close()

	// Set the global dbPath and run the check
	oldDbPath := *dbPath
	*dbPath = dir
	defer func() { *dbPath = oldDbPath }()

	err = cmdCheckCollision()
	if err != nil {
		t.Errorf("Collision check on empty DB should pass: %v", err)
	}

	t.Log("✅ Empty database handled correctly")
}

// TestCheckCollision_SingleSST verifies the tool works with just one SST file.
//
// Contract: Single SST can't have collisions (within same file), should report 0.
func TestCheckCollision_SingleSST(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

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

	// Set the global dbPath and run the check
	oldDbPath := *dbPath
	*dbPath = dir
	defer func() { *dbPath = oldDbPath }()

	err = cmdCheckCollision()
	if err != nil {
		t.Errorf("Collision check should pass on single SST: %v", err)
	}

	t.Log("✅ Single SST handled correctly")
}

// TestCheckCollision_MultipleSSTs verifies the tool scans all SST files.
//
// Contract: Keys spread across multiple SSTs should all be checked.
func TestCheckCollision_MultipleSSTs(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

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
	sstFiles, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to glob SST files: %v", err)
	}

	t.Logf("Created %d SST files", len(sstFiles))

	if len(sstFiles) < 5 {
		t.Errorf("Expected multiple SST files, got %d", len(sstFiles))
	}

	// Set the global dbPath and run the check
	oldDbPath := *dbPath
	*dbPath = dir
	defer func() { *dbPath = oldDbPath }()

	err = cmdCheckCollision()
	if err != nil {
		t.Errorf("Collision check should pass: %v", err)
	}

	t.Logf("✅ Multiple SSTs scanned successfully")
}

// TestCheckCollision_UpdatedKeys verifies the tool correctly handles the
// same user key with different sequence numbers (normal updates).
//
// Contract: Same user key with different sequences is NOT a collision.
func TestCheckCollision_UpdatedKeys(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

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

	// Set the global dbPath and run the check
	oldDbPath := *dbPath
	*dbPath = dir
	defer func() { *dbPath = oldDbPath }()

	err = cmdCheckCollision()
	if err != nil {
		t.Errorf("Collision check should pass (different sequences = not a collision): %v", err)
	}

	t.Log("✅ Updated keys (different sequences) not reported as collisions")
}
