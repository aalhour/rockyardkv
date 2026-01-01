// checkpoint_test.go implements tests for checkpoint.
package rockyardkv

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestCheckpointBasic(t *testing.T) {
	// Create temp directory for source DB
	srcDir, err := os.MkdirTemp("", "rockyard-checkpoint-src-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(srcDir)

	// Create temp directory for checkpoint
	checkpointDir, err := os.MkdirTemp("", "rockyard-checkpoint-dst-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	os.RemoveAll(checkpointDir) // Checkpoint will create it
	defer os.RemoveAll(checkpointDir)

	// Open source database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	database, err := Open(srcDir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	// Write some data
	for i := range 100 {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to create SST files
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Create checkpoint
	cp, err := NewCheckpoint(database)
	if err != nil {
		t.Fatalf("NewCheckpoint failed: %v", err)
	}

	if err := cp.CreateCheckpoint(checkpointDir, 0); err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	// Close source database
	database.Close()

	// Verify checkpoint directory exists
	if _, err := os.Stat(checkpointDir); os.IsNotExist(err) {
		t.Fatal("Checkpoint directory should exist")
	}

	// Verify CURRENT file exists
	if _, err := os.Stat(filepath.Join(checkpointDir, "CURRENT")); os.IsNotExist(err) {
		t.Fatal("CURRENT file should exist in checkpoint")
	}

	// Open the checkpoint as a database
	checkpointDB, err := Open(checkpointDir, opts)
	if err != nil {
		t.Fatalf("Failed to open checkpoint: %v", err)
	}
	defer checkpointDB.Close()

	// Verify all data is present
	for i := range 100 {
		key := []byte("key" + strconv.Itoa(i))
		expectedValue := []byte("value" + strconv.Itoa(i))

		value, err := checkpointDB.Get(nil, key)
		if err != nil {
			t.Errorf("Get failed for key%d: %v", i, err)
			continue
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Value mismatch for key%d: got %s, want %s", i, value, expectedValue)
		}
	}
}

func TestCheckpointWithOngoingWrites(t *testing.T) {
	// Create temp directory for source DB
	srcDir, err := os.MkdirTemp("", "rockyard-checkpoint-writes-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(srcDir)

	checkpointDir := srcDir + "-checkpoint"
	defer os.RemoveAll(checkpointDir)

	// Open source database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	database, err := Open(srcDir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer database.Close()

	// Write initial data
	for i := range 50 {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		database.Put(nil, key, value)
	}

	// Flush to create SST files
	database.Flush(nil)

	// Create checkpoint
	cp, _ := NewCheckpoint(database)
	if err := cp.CreateCheckpoint(checkpointDir, 0); err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	// Write more data AFTER checkpoint
	for i := 50; i < 100; i++ {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		database.Put(nil, key, value)
	}

	// Open the checkpoint
	checkpointDB, err := Open(checkpointDir, opts)
	if err != nil {
		t.Fatalf("Failed to open checkpoint: %v", err)
	}
	defer checkpointDB.Close()

	// Checkpoint should have only the first 50 keys
	for i := range 50 {
		key := []byte("key" + strconv.Itoa(i))
		_, err := checkpointDB.Get(nil, key)
		if err != nil {
			t.Errorf("Key%d should exist in checkpoint: %v", i, err)
		}
	}

	// Keys written after checkpoint should NOT exist
	for i := 50; i < 100; i++ {
		key := []byte("key" + strconv.Itoa(i))
		_, err := checkpointDB.Get(nil, key)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Key%d should NOT exist in checkpoint (written after)", i)
		}
	}
}

func TestCheckpointExistingDirectory(t *testing.T) {
	// Create temp directory for source DB
	srcDir, err := os.MkdirTemp("", "rockyard-checkpoint-exist-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(srcDir)

	// Create the checkpoint directory ahead of time
	checkpointDir, err := os.MkdirTemp("", "rockyard-checkpoint-dst-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(checkpointDir)

	// Open source database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	database, err := Open(srcDir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer database.Close()

	// Create checkpoint - should fail because directory exists
	cp, _ := NewCheckpoint(database)
	err = cp.CreateCheckpoint(checkpointDir, 0)
	if err == nil {
		t.Fatal("Checkpoint should fail when directory exists")
	}
}

func TestCheckpointEmptyDatabase(t *testing.T) {
	// Create temp directory for source DB
	srcDir, err := os.MkdirTemp("", "rockyard-checkpoint-empty-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(srcDir)

	checkpointDir := srcDir + "-checkpoint"
	defer os.RemoveAll(checkpointDir)

	// Open source database (empty)
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	database, err := Open(srcDir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer database.Close()

	// Create checkpoint of empty database
	cp, _ := NewCheckpoint(database)
	if err := cp.CreateCheckpoint(checkpointDir, 0); err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	// Open the checkpoint
	checkpointDB, err := Open(checkpointDir, opts)
	if err != nil {
		t.Fatalf("Failed to open checkpoint: %v", err)
	}
	defer checkpointDB.Close()

	// Should be empty
	iter := checkpointDB.NewIterator(nil)
	defer iter.Close()
	iter.SeekToFirst()
	if iter.Valid() {
		t.Error("Checkpoint should be empty")
	}
}

func TestCheckpointMultipleSSTs(t *testing.T) {
	// Create temp directory for source DB
	srcDir, err := os.MkdirTemp("", "rockyard-checkpoint-multi-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(srcDir)

	checkpointDir := srcDir + "-checkpoint"
	defer os.RemoveAll(checkpointDir)

	// Open source database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024 // Small buffer to trigger multiple flushes
	database, err := Open(srcDir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer database.Close()

	// Write data and flush multiple times to create multiple SST files
	for batch := range 5 {
		for i := range 20 {
			key := []byte("batch" + strconv.Itoa(batch) + "_key" + strconv.Itoa(i))
			value := make([]byte, 100) // Larger values to trigger flushes
			database.Put(nil, key, value)
		}
		database.Flush(nil)
	}

	// Check number of files at L0
	numFiles, _ := database.GetProperty(PropertyNumFilesAtLevelPrefix + "0")
	t.Logf("L0 files before checkpoint: %s", numFiles)

	// Create checkpoint
	cp, _ := NewCheckpoint(database)
	if err := cp.CreateCheckpoint(checkpointDir, 0); err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	// Count SST files in checkpoint
	files, _ := filepath.Glob(filepath.Join(checkpointDir, "*.sst"))
	t.Logf("SST files in checkpoint: %d", len(files))

	// Open the checkpoint
	checkpointDB, err := Open(checkpointDir, opts)
	if err != nil {
		t.Fatalf("Failed to open checkpoint: %v", err)
	}
	defer checkpointDB.Close()

	// Verify all data is present
	for batch := range 5 {
		for i := range 20 {
			key := []byte("batch" + strconv.Itoa(batch) + "_key" + strconv.Itoa(i))
			_, err := checkpointDB.Get(nil, key)
			if err != nil {
				t.Errorf("Key %s missing: %v", key, err)
			}
		}
	}
}
