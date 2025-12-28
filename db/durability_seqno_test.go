// Durability tests for sequence number management across flush and recovery cycles.
package db

import (
	"bytes"
	"testing"

	"github.com/aalhour/rockyardkv/internal/vfs"
)

// TestFlushPreservesSequenceInvariant verifies that sequence numbers assigned
// to writes are never reused across flush and recovery cycles.
//
// Contract: After flush+crash+reopen, new writes must receive sequence numbers
// strictly greater than any sequence in the recovered database, preventing
// internal key collisions (same key+seq+type with different values).
func TestFlushPreservesSequenceInvariant(t *testing.T) {
	dir := t.TempDir()

	// Use FaultInjectionFS to simulate crashes
	faultFS := vfs.NewFaultInjectionFS(vfs.Default())

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	// Critical: DisableWAL exposes the bug
	writeOpts := DefaultWriteOptions()
	writeOpts.DisableWAL = true

	// Phase 1: Create DB and write some data
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	// Write keys with sequences 1-10
	for i := range 10 {
		key := []byte("key")
		key = append(key, byte(i))
		value := []byte("value_phase1_")
		value = append(value, byte(i))
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to make these durable (sequences 1-10 now in SST)
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Phase 2: Write more data WITHOUT flushing
	// These get sequences 11-20 but will be lost on crash
	for i := range 10 {
		key := []byte("newkey")
		key = append(key, byte(i))
		value := []byte("value_phase2_")
		value = append(value, byte(i))
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Close normally (simulating clean shutdown)
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate crash by dropping unsynced data
	if err := faultFS.DropUnsyncedData(); err != nil {
		t.Logf("DropUnsyncedData: %v", err)
	}

	// Phase 3: Reopen the DB
	// Contract: db.seq must be restored from the highest sequence in flushed SSTs,
	// never from the transient db.seq value (which may include unflushed writes)
	opts.CreateIfMissing = false
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer database.Close()

	// Phase 4: Write more data after recovery
	// These MUST get fresh sequence numbers > 10 (not reusing 11-20)
	for i := range 10 {
		key := []byte("key")
		key = append(key, byte(i))
		value := []byte("value_phase3_")
		value = append(value, byte(i))
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put after recovery failed: %v", err)
		}
	}

	// Flush the new writes
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush after recovery failed: %v", err)
	}

	// Phase 5: Verify values
	// All keys should have phase3 values (latest write wins)
	for i := range 10 {
		key := []byte("key")
		key = append(key, byte(i))
		expectedValue := []byte("value_phase3_")
		expectedValue = append(expectedValue, byte(i))

		value, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Get key%d failed: %v", i, err)
			continue
		}

		if !bytes.Equal(value, expectedValue) {
			t.Errorf("key%d: got value %q, want %q (contract violation: sequence reuse)",
				i, value, expectedValue)
		}
	}
}

// TestFlushSequenceMonotonicity verifies that LastSequence in the MANIFEST
// reflects only the sequences actually present in flushed SSTs, not sequences
// from unflushed writes still in the memtable.
//
// Contract: After flush, LastSequence must equal the largest sequence number
// in the flushed SST. It must never include sequences from concurrent writes
// to the active memtable that occur during or after the flush.
func TestFlushSequenceMonotonicity(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	writeOpts := DefaultWriteOptions()
	writeOpts.DisableWAL = true

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer database.Close()

	// Write and flush
	if err := database.Put(writeOpts, []byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	lastSeqAfterFlush := database.GetLatestSequenceNumber()

	// Write more without flushing
	if err := database.Put(writeOpts, []byte("key2"), []byte("val2")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := database.Put(writeOpts, []byte("key3"), []byte("val3")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	currentSeq := database.GetLatestSequenceNumber()

	// CurrentSeq should be > lastSeqAfterFlush (unflushed writes happened)
	if currentSeq <= lastSeqAfterFlush {
		t.Errorf("Sequence didn't advance: lastSeqAfterFlush=%d, currentSeq=%d",
			lastSeqAfterFlush, currentSeq)
	}

	// Now check what LastSequence is in MANIFEST after reopening
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen
	opts.CreateIfMissing = false
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer database.Close()

	recoveredSeq := database.GetLatestSequenceNumber()

	// With DisableWAL, recovered sequence should be lastSeqAfterFlush (only flushed data)
	// NOT currentSeq (which included unflushed data)
	if recoveredSeq != lastSeqAfterFlush {
		t.Errorf("Recovered sequence incorrect: got %d, want %d (lastSeqAfterFlush)",
			recoveredSeq, lastSeqAfterFlush)
		t.Error("Contract violation: LastSequence included unflushed sequences, enabling reuse after crash")
	}
}
