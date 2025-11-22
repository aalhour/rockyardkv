package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestGetUpdatesSinceBasic(t *testing.T) {
	dir, err := os.MkdirTemp("", "transaction_log_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	dbPath := filepath.Join(dir, "db")
	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	impl := database.(*DBImpl)

	// Write some data
	for i := range 10 {
		key := []byte{byte('k'), byte('0' + i)}
		value := []byte{byte('v'), byte('0' + i)}
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Get updates since sequence 0
	readOpts := DefaultTransactionLogIteratorReadOptions()
	iter, err := impl.GetUpdatesSince(0, readOpts)
	if err != nil {
		t.Fatalf("GetUpdatesSince failed: %v", err)
	}
	defer iter.Close()

	// Count the batches
	count := 0
	for iter.Valid() {
		batch, err := iter.GetBatch()
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}
		if batch.Sequence == 0 {
			t.Error("Batch sequence should not be 0")
		}
		count++
		iter.Next()
	}

	if err := iter.Status(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	if count == 0 {
		t.Error("Expected at least one batch from WAL")
	}
	t.Logf("Found %d batches in WAL", count)
}

func TestGetUpdatesSinceFromSequence(t *testing.T) {
	dir, err := os.MkdirTemp("", "transaction_log_seq_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	dbPath := filepath.Join(dir, "db")
	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	impl := database.(*DBImpl)

	// Write first batch
	for i := range 5 {
		key := []byte{byte('a'), byte('0' + i)}
		value := []byte{byte('x'), byte('0' + i)}
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Get current sequence - need to use the DB's internal method
	midSeq := impl.seq
	t.Logf("Sequence after first batch: %d", midSeq)

	// Write second batch
	for i := range 5 {
		key := []byte{byte('b'), byte('0' + i)}
		value := []byte{byte('y'), byte('0' + i)}
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Get updates since midSeq
	readOpts := DefaultTransactionLogIteratorReadOptions()
	iter, err := impl.GetUpdatesSince(midSeq+1, readOpts)
	if err != nil {
		t.Fatalf("GetUpdatesSince failed: %v", err)
	}
	defer iter.Close()

	// All returned batches should have sequence > midSeq
	count := 0
	for iter.Valid() {
		batch, _ := iter.GetBatch()
		if batch.Sequence <= midSeq {
			t.Errorf("Batch sequence %d should be > %d", batch.Sequence, midSeq)
		}
		count++
		iter.Next()
	}

	if count == 0 {
		t.Error("Expected at least one batch from WAL after midSeq")
	}
	t.Logf("Found %d batches after sequence %d", count, midSeq)
}

func TestGetSortedWalFiles(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_files_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	dbPath := filepath.Join(dir, "db")
	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	impl := database.(*DBImpl)

	// Write some data to ensure WAL is created
	if err := database.Put(nil, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get WAL files
	walFiles, err := impl.GetSortedWalFiles()
	if err != nil {
		t.Fatalf("GetSortedWalFiles failed: %v", err)
	}

	if len(walFiles) == 0 {
		t.Error("Expected at least one WAL file")
	}

	// Verify files are sorted by log number
	for i := 1; i < len(walFiles); i++ {
		if walFiles[i].LogNumber <= walFiles[i-1].LogNumber {
			t.Errorf("WAL files not sorted: %d <= %d",
				walFiles[i].LogNumber, walFiles[i-1].LogNumber)
		}
	}

	t.Logf("Found %d WAL file(s)", len(walFiles))
	for _, wf := range walFiles {
		t.Logf("  %s (num=%d, type=%d, size=%d)",
			wf.PathName, wf.LogNumber, wf.Type, wf.SizeBytes)
	}
}

func TestTransactionLogIteratorAfterFlush(t *testing.T) {
	dir, err := os.MkdirTemp("", "transaction_log_flush_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	dbPath := filepath.Join(dir, "db")
	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	impl := database.(*DBImpl)

	// Write some data
	for i := range 5 {
		key := []byte{byte('k'), byte('0' + i)}
		value := []byte{byte('v'), byte('0' + i)}
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to create SST
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Write more data
	for i := 5; i < 10; i++ {
		key := []byte{byte('k'), byte('0' + i)}
		value := []byte{byte('v'), byte('0' + i)}
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Get all updates from beginning
	readOpts := DefaultTransactionLogIteratorReadOptions()
	iter, err := impl.GetUpdatesSince(0, readOpts)
	if err != nil {
		t.Fatalf("GetUpdatesSince failed: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}

	if err := iter.Status(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	// Should have records from both before and after flush
	if count == 0 {
		t.Error("Expected batches from WAL")
	}
	t.Logf("Found %d batches in WAL(s)", count)
}

func TestTransactionLogIteratorInvalidOperations(t *testing.T) {
	dir, err := os.MkdirTemp("", "transaction_log_invalid_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	dbPath := filepath.Join(dir, "db")
	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	impl := database.(*DBImpl)

	// Write one record to create WAL
	if err := database.Put(nil, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get iterator
	readOpts := DefaultTransactionLogIteratorReadOptions()
	iter, err := impl.GetUpdatesSince(0, readOpts)
	if err != nil {
		t.Fatalf("GetUpdatesSince failed: %v", err)
	}

	// Read all records
	for iter.Valid() {
		iter.Next()
	}

	// Now iterator should be invalid
	if iter.Valid() {
		t.Error("Iterator should be invalid after exhaustion")
	}

	// GetBatch on invalid iterator should return error
	_, err = iter.GetBatch()
	if !errors.Is(err, ErrIteratorNotValid) {
		t.Errorf("GetBatch on invalid iterator: expected ErrIteratorNotValid, got %v", err)
	}

	iter.Close()
}
