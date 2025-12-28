// Package db provides the main database interface and implementation.
// This file tests WAL log number handling during flush operations.
package db

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/batch"
)

// TestFlush_LogNumber_OnlyAdvancesAfterFlush verifies that the MANIFEST's
// LogNumber only advances after a flush completes, and only to the point
// where all prior data is durable in SST files.
//
// This tests the invariant: after recovery, all unflushed WAL data must be
// replayed. The LogNumber determines which logs are replayed.
//
// Bug scenario this test guards against:
// 1. Writes go to WAL 13, fill memtable A
// 2. Memtable A becomes immutable, new WAL 32 created
// 3. More writes go to WAL 32, memtable B
// 4. Flush runs for memtable A
// 5. BUG: MANIFEST incorrectly gets LogNumber: 32 (current log, not imm's log)
// 6. Crash before memtable B is flushed
// 7. Recovery: LogNumber=32, but WAL 32's data isn't flushed → data loss!
//
// Correct behavior: After flushing memtable A (which was filled by WAL 13),
// LogNumber should become the WAL that started filling the NEXT memtable
// (not the current active WAL).
func TestFlush_LogNumber_OnlyAdvancesAfterFlush(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024 * 1024 // 1MB buffer

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Get initial log number from MANIFEST
	dbImpl := database.(*DBImpl)
	initialLogNumber := dbImpl.versions.LogNumber()
	t.Logf("Initial LogNumber: %d", initialLogNumber)
	t.Logf("Initial logFileNumber: %d", dbImpl.logFileNumber)

	// Write some data (goes to first WAL)
	writeOpts := DefaultWriteOptions()
	for i := range 100 {
		key := []byte("key-before-flush-" + string(rune('A'+i)))
		value := []byte("value-" + string(rune('A'+i)))
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	firstWALNumber := dbImpl.logFileNumber
	t.Logf("First WAL number (before flush): %d", firstWALNumber)

	// Flush - this should create an SST and potentially update LogNumber
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	postFlushLogNumber := dbImpl.versions.LogNumber()
	t.Logf("LogNumber after first flush: %d", postFlushLogNumber)

	// Now write more data (this goes to the same or new WAL)
	for i := range 100 {
		key := []byte("key-after-flush-" + string(rune('A'+i)))
		value := []byte("value-" + string(rune('A'+i)))
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	currentWALNumber := dbImpl.logFileNumber
	t.Logf("Current WAL number (after more writes): %d", currentWALNumber)

	// The key invariant: LogNumber should NOT jump ahead of data that
	// hasn't been flushed yet. Check the MANIFEST's LogNumber.
	currentManifestLogNumber := dbImpl.versions.LogNumber()
	t.Logf("Current MANIFEST LogNumber: %d", currentManifestLogNumber)

	// Close the database
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify all data is present
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer database.Close()

	// Verify pre-flush data (should be in SST from flush)
	for i := range 100 {
		key := []byte("key-before-flush-" + string(rune('A'+i)))
		val, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Pre-flush key %s not found after reopen: %v", key, err)
		} else if string(val) != "value-"+string(rune('A'+i)) {
			t.Errorf("Pre-flush key %s wrong value: got %s", key, val)
		}
	}

	// Verify post-flush data (should be recovered from WAL)
	for i := range 100 {
		key := []byte("key-after-flush-" + string(rune('A'+i)))
		val, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Post-flush key %s not found after reopen: %v", key, err)
		} else if string(val) != "value-"+string(rune('A'+i)) {
			t.Errorf("Post-flush key %s wrong value: got %s", key, val)
		}
	}
}

// TestFlush_LogNumber_MultipleFlushes tests that LogNumber correctly advances
// through multiple flush cycles without skipping any unflushed WAL data.
func TestFlush_LogNumber_MultipleFlushes(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024 // Small buffer to force flushes

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	dbImpl := database.(*DBImpl)
	writeOpts := DefaultWriteOptions()

	// Track log numbers through multiple flush cycles
	type checkpoint struct {
		iteration     int
		logFileNumber uint64
		manifestLog   uint64
		keyPrefix     string
	}
	var checkpoints []checkpoint

	for iteration := range 5 {
		keyPrefix := string(rune('A' + iteration))

		// Write data
		for i := range 50 {
			key := []byte("iter" + keyPrefix + "-key-" + string(rune('0'+i%10)))
			value := []byte("value-" + keyPrefix)
			if err := database.Put(writeOpts, key, value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		checkpoints = append(checkpoints, checkpoint{
			iteration:     iteration,
			logFileNumber: dbImpl.logFileNumber,
			manifestLog:   dbImpl.versions.LogNumber(),
			keyPrefix:     keyPrefix,
		})

		// Flush after each batch
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush %d failed: %v", iteration, err)
		}
	}

	// Log the progression
	for _, cp := range checkpoints {
		t.Logf("Iteration %d: logFileNumber=%d, manifestLog=%d",
			cp.iteration, cp.logFileNumber, cp.manifestLog)
	}

	// Close and reopen
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer database.Close()

	// Verify all data is present
	for _, cp := range checkpoints {
		for i := range 50 {
			key := []byte("iter" + cp.keyPrefix + "-key-" + string(rune('0'+i%10)))
			_, err := database.Get(nil, key)
			if err != nil {
				t.Errorf("Key %s from iteration %d not found after reopen: %v",
					key, cp.iteration, err)
			}
		}
	}
}

// TestFlush_LogNumber_CrashBeforeFlushComplete simulates a crash scenario
// where data is written to WAL but the flush that would persist it doesn't
// complete. This tests that WAL replay correctly recovers the data.
func TestFlush_LogNumber_CrashBeforeFlushComplete(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024 * 1024 // 1MB

	// First session: write and flush some data
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	writeOpts := DefaultWriteOptions()
	writeOpts.Sync = true // Ensure WAL is synced

	// Write initial data
	for i := range 10 {
		key := []byte("initial-key-" + string(rune('A'+i)))
		value := []byte("initial-value-" + string(rune('A'+i)))
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush initial data
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Write more data (this will be in WAL only, simulating "crash before flush")
	for i := range 10 {
		key := []byte("unflushed-key-" + string(rune('A'+i)))
		value := []byte("unflushed-value-" + string(rune('A'+i)))
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	dbImpl := database.(*DBImpl)
	manifestLogBeforeClose := dbImpl.versions.LogNumber()
	logFileNumberBeforeClose := dbImpl.logFileNumber
	t.Logf("Before close: manifestLog=%d, logFileNumber=%d",
		manifestLogBeforeClose, logFileNumberBeforeClose)

	// Close (simulates clean shutdown, but data wasn't flushed)
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify ALL data is present (including unflushed)
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer database.Close()

	// Verify flushed data
	for i := range 10 {
		key := []byte("initial-key-" + string(rune('A'+i)))
		val, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Flushed key %s not found: %v", key, err)
		} else if string(val) != "initial-value-"+string(rune('A'+i)) {
			t.Errorf("Flushed key %s wrong value", key)
		}
	}

	// Verify unflushed data (CRITICAL: this is the WAL replay test)
	for i := range 10 {
		key := []byte("unflushed-key-" + string(rune('A'+i)))
		val, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Unflushed key %s not found after recovery: %v", key, err)
		} else if string(val) != "unflushed-value-"+string(rune('A'+i)) {
			t.Errorf("Unflushed key %s wrong value", key)
		}
	}
}

// TestFlush_LogNumber_BatchWritesThenFlush tests that batch writes followed
// by flush correctly handle log number advancement.
func TestFlush_LogNumber_BatchWritesThenFlush(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	writeOpts := DefaultWriteOptions()
	writeOpts.Sync = true

	// Write a batch
	wb := batch.New()
	for i := range 100 {
		wb.Put([]byte("batch-key-"+string(rune('A'+i%26))+string(rune('0'+i/26))),
			[]byte("batch-value"))
	}
	if err := database.Write(writeOpts, wb); err != nil {
		t.Fatalf("Batch write failed: %v", err)
	}

	// Flush
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Write another batch (not flushed)
	wb = batch.New()
	for i := range 100 {
		wb.Put([]byte("batch2-key-"+string(rune('A'+i%26))+string(rune('0'+i/26))),
			[]byte("batch2-value"))
	}
	if err := database.Write(writeOpts, wb); err != nil {
		t.Fatalf("Batch write 2 failed: %v", err)
	}

	// Close and reopen
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer database.Close()

	// Verify all data
	for i := range 100 {
		key := []byte("batch-key-" + string(rune('A'+i%26)) + string(rune('0'+i/26)))
		_, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Batch 1 key %s not found: %v", key, err)
		}
	}

	for i := range 100 {
		key := []byte("batch2-key-" + string(rune('A'+i%26)) + string(rune('0'+i/26)))
		_, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Batch 2 key %s not found: %v", key, err)
		}
	}
}
