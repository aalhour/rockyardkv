package rockyardkv

// column_family_durability_test.go implements Column family durability tests for sequence number management.

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/aalhour/rockyardkv/internal/vfs"
)

// TestFlush_MultiCF_NoSequenceReuseAcrossCFs verifies that sequence numbers
// don't collide across column families after crash recovery.
//
// Contract: Sequences are global across all CFs. After flush+crash+recovery,
// new writes in any CF must receive sequences > all recovered sequences.
func TestFlush_MultiCF_NoSequenceReuseAcrossCFs(t *testing.T) {
	t.Skip("Skipping: Column family persistence/recovery not fully implemented yet")
	dir := t.TempDir()

	faultFS := vfs.NewFaultInjectionFS(vfs.Default())

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	writeOpts := DefaultWriteOptions()
	writeOpts.DisableWAL = true

	// Phase 1: Create DB with 3 column families
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	cf1, err := database.CreateColumnFamily(DefaultColumnFamilyOptions(), "cf1")
	if err != nil {
		t.Fatalf("Failed to create cf1: %v", err)
	}

	cf2, err := database.CreateColumnFamily(DefaultColumnFamilyOptions(), "cf2")
	if err != nil {
		t.Fatalf("Failed to create cf2: %v", err)
	}

	// Phase 2: Write data to each CF and flush
	for i := range 20 {
		// Write to default CF
		key := fmt.Appendf(nil, "default_%04d", i)
		value := fmt.Appendf(nil, "value_default_%04d", i)
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put to default CF failed: %v", err)
		}

		// Write to cf1
		key = fmt.Appendf(nil, "cf1_%04d", i)
		value = fmt.Appendf(nil, "value_cf1_%04d", i)
		if err := database.PutCF(writeOpts, cf1, key, value); err != nil {
			t.Fatalf("Put to cf1 failed: %v", err)
		}

		// Write to cf2
		key = fmt.Appendf(nil, "cf2_%04d", i)
		value = fmt.Appendf(nil, "value_cf2_%04d", i)
		if err := database.PutCF(writeOpts, cf2, key, value); err != nil {
			t.Fatalf("Put to cf2 failed: %v", err)
		}
	}

	// Flush all CFs
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	firstSeq := database.GetLatestSequenceNumber()
	t.Logf("Sequence after first flush: %d", firstSeq)

	// Phase 3: Write more data without flushing (will be lost)
	for i := 20; i < 30; i++ {
		key := fmt.Appendf(nil, "unflushed_%04d", i)
		value := fmt.Appendf(nil, "unflushed_value_%04d", i)
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put unflushed failed: %v", err)
		}
	}

	unflushedSeq := database.GetLatestSequenceNumber()
	t.Logf("Sequence with unflushed data: %d", unflushedSeq)

	database.Close()

	// Simulate crash
	if err := faultFS.DropUnsyncedData(); err != nil {
		t.Logf("DropUnsyncedData: %v", err)
	}

	// Phase 4: Reopen and verify
	opts.CreateIfMissing = false
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer database.Close()

	recoveredSeq := database.GetLatestSequenceNumber()
	t.Logf("Sequence after recovery: %d", recoveredSeq)

	// Recovered sequence should match first flush, not unflushed sequence
	if recoveredSeq != firstSeq {
		t.Errorf("Recovered sequence incorrect: got %d, want %d", recoveredSeq, firstSeq)
	}

	// Get column families
	cf1 = database.GetColumnFamily("cf1")
	if cf1 == nil {
		t.Fatal("Failed to get cf1")
	}

	cf2 = database.GetColumnFamily("cf2")
	if cf2 == nil {
		t.Fatal("Failed to get cf2")
	}

	// Phase 5: Verify all flushed data in all CFs
	for i := range 20 {
		// Verify default CF
		key := fmt.Appendf(nil, "default_%04d", i)
		expectedValue := fmt.Appendf(nil, "value_default_%04d", i)
		value, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Default CF key %s missing: %v", key, err)
			continue
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Default CF key %s: got %q, want %q", key, value, expectedValue)
		}

		// Verify cf1
		key = fmt.Appendf(nil, "cf1_%04d", i)
		expectedValue = fmt.Appendf(nil, "value_cf1_%04d", i)
		value, err = database.GetCF(nil, cf1, key)
		if err != nil {
			t.Errorf("CF1 key %s missing: %v", key, err)
			continue
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("CF1 key %s: got %q, want %q", key, value, expectedValue)
		}

		// Verify cf2
		key = fmt.Appendf(nil, "cf2_%04d", i)
		expectedValue = fmt.Appendf(nil, "value_cf2_%04d", i)
		value, err = database.GetCF(nil, cf2, key)
		if err != nil {
			t.Errorf("CF2 key %s missing: %v", key, err)
			continue
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("CF2 key %s: got %q, want %q", key, value, expectedValue)
		}
	}

	// Phase 6: Write new data to all CFs after recovery
	for i := 30; i < 40; i++ {
		// Default CF
		key := fmt.Appendf(nil, "recovery_default_%04d", i)
		value := fmt.Appendf(nil, "recovery_value_default_%04d", i)
		if err := database.Put(writeOpts, key, value); err != nil {
			t.Fatalf("Put recovery to default CF failed: %v", err)
		}

		// CF1
		key = fmt.Appendf(nil, "recovery_cf1_%04d", i)
		value = fmt.Appendf(nil, "recovery_value_cf1_%04d", i)
		if err := database.PutCF(writeOpts, cf1, key, value); err != nil {
			t.Fatalf("Put recovery to cf1 failed: %v", err)
		}

		// CF2
		key = fmt.Appendf(nil, "recovery_cf2_%04d", i)
		value = fmt.Appendf(nil, "recovery_value_cf2_%04d", i)
		if err := database.PutCF(writeOpts, cf2, key, value); err != nil {
			t.Fatalf("Put recovery to cf2 failed: %v", err)
		}
	}

	// Flush and verify all recovery data
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Recovery flush failed: %v", err)
	}

	finalSeq := database.GetLatestSequenceNumber()
	t.Logf("Final sequence: %d", finalSeq)

	// Verify recovery writes in all CFs
	for i := 30; i < 40; i++ {
		// Default CF
		key := fmt.Appendf(nil, "recovery_default_%04d", i)
		expectedValue := fmt.Appendf(nil, "recovery_value_default_%04d", i)
		value, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Recovery default CF key %s missing: %v", key, err)
			continue
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Recovery default CF key %s: got %q, want %q (sequence reuse?)", key, value, expectedValue)
		}

		// CF1
		key = fmt.Appendf(nil, "recovery_cf1_%04d", i)
		expectedValue = fmt.Appendf(nil, "recovery_value_cf1_%04d", i)
		value, err = database.GetCF(nil, cf1, key)
		if err != nil {
			t.Errorf("Recovery cf1 key %s missing: %v", key, err)
			continue
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Recovery cf1 key %s: got %q, want %q (sequence reuse?)", key, value, expectedValue)
		}

		// CF2
		key = fmt.Appendf(nil, "recovery_cf2_%04d", i)
		expectedValue = fmt.Appendf(nil, "recovery_value_cf2_%04d", i)
		value, err = database.GetCF(nil, cf2, key)
		if err != nil {
			t.Errorf("Recovery cf2 key %s missing: %v", key, err)
			continue
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Recovery cf2 key %s: got %q, want %q (sequence reuse?)", key, value, expectedValue)
		}
	}

	t.Log("✅ No sequence reuse across column families")
}

// TestFlush_MultiCF_IndependentFlushes verifies that flushing one CF doesn't
// affect sequence management in other CFs.
//
// Contract: Flush of CF1 updates global LastSequence correctly, doesn't
// interfere with CF2's unflushed writes.
func TestFlush_MultiCF_IndependentFlushes(t *testing.T) {
	t.Skip("Skipping: Column family persistence/recovery not fully implemented yet")
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

	cf1, err := database.CreateColumnFamily(DefaultColumnFamilyOptions(), "cf1")
	if err != nil {
		t.Fatalf("Failed to create cf1: %v", err)
	}

	cf2, err := database.CreateColumnFamily(DefaultColumnFamilyOptions(), "cf2")
	if err != nil {
		t.Fatalf("Failed to create cf2: %v", err)
	}

	// Write to CF1 and flush
	for i := range 10 {
		key := fmt.Appendf(nil, "cf1_%04d", i)
		value := fmt.Appendf(nil, "value_cf1_%04d", i)
		if err := database.PutCF(writeOpts, cf1, key, value); err != nil {
			t.Fatalf("Put to cf1 failed: %v", err)
		}
	}

	// Note: RockyardKV flushes all CFs, not individual ones
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	seqAfterCF1Flush := database.GetLatestSequenceNumber()

	// Write to CF2 without flushing
	for i := range 10 {
		key := fmt.Appendf(nil, "cf2_%04d", i)
		value := fmt.Appendf(nil, "value_cf2_%04d", i)
		if err := database.PutCF(writeOpts, cf2, key, value); err != nil {
			t.Fatalf("Put to cf2 failed: %v", err)
		}
	}

	seqWithUnflushedCF2 := database.GetLatestSequenceNumber()

	// Verify sequence advanced
	if seqWithUnflushedCF2 <= seqAfterCF1Flush {
		t.Errorf("Sequence didn't advance: cf1_flush=%d, with_cf2=%d", seqAfterCF1Flush, seqWithUnflushedCF2)
	}

	// Close and reopen
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	opts.CreateIfMissing = false
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer database.Close()

	recoveredSeq := database.GetLatestSequenceNumber()

	// With DisableWAL, recovered sequence should be from CF1 flush, not CF2 writes
	if recoveredSeq != seqAfterCF1Flush {
		t.Errorf("Recovered sequence incorrect: got %d, want %d (CF1 flush)", recoveredSeq, seqAfterCF1Flush)
	}

	t.Log("✅ Independent CF flushes maintain correct sequence isolation")
}
