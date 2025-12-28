package testutil

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestTraceWriterBasic tests basic trace writing functionality.
func TestTraceWriterBasic(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "test.trace")

	// Create trace writer
	tw, err := NewTraceWriter(tracePath, 100, 1, 10000)
	if err != nil {
		t.Fatalf("NewTraceWriter: %v", err)
	}

	// Write some records
	if err := tw.RecordPut(0, 42, 5, 101); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if err := tw.RecordDelete(0, 42, 102); err != nil {
		t.Fatalf("RecordDelete: %v", err)
	}
	if err := tw.RecordPut(0, 100, 1, 103); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	if tw.Count() != 3 {
		t.Errorf("Count = %d, want 3", tw.Count())
	}
	if tw.StartSeq() != 100 {
		t.Errorf("StartSeq = %d, want 100", tw.StartSeq())
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read back
	tr, err := OpenTraceReader(tracePath)
	if err != nil {
		t.Fatalf("OpenTraceReader: %v", err)
	}
	defer tr.Close()

	if tr.StartSeq() != 100 {
		t.Errorf("StartSeq = %d, want 100", tr.StartSeq())
	}
	if tr.NumCFs() != 1 {
		t.Errorf("NumCFs = %d, want 1", tr.NumCFs())
	}
	if tr.MaxKey() != 10000 {
		t.Errorf("MaxKey = %d, want 10000", tr.MaxKey())
	}

	// Record 1: Put key=42, valueBase=5, seqno=101
	rec, err := tr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rec.Op != TraceOpPut || rec.CF != 0 || rec.Key != 42 || rec.ValueBase != 5 || rec.SeqNo != 101 {
		t.Errorf("Record 1 = %+v, want Put(cf=0, key=42, valueBase=5, seqno=101)", rec)
	}

	// Record 2: Delete key=42, seqno=102
	rec, err = tr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rec.Op != TraceOpDelete || rec.CF != 0 || rec.Key != 42 || rec.SeqNo != 102 {
		t.Errorf("Record 2 = %+v, want Delete(cf=0, key=42, seqno=102)", rec)
	}

	// Record 3: Put key=100, valueBase=1, seqno=103
	rec, err = tr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rec.Op != TraceOpPut || rec.CF != 0 || rec.Key != 100 || rec.ValueBase != 1 || rec.SeqNo != 103 {
		t.Errorf("Record 3 = %+v, want Put(cf=0, key=100, valueBase=1, seqno=103)", rec)
	}

	// No more records
	_, err = tr.Next()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Next after last = %v, want io.EOF", err)
	}
}

// TestTraceWriterConcurrent tests concurrent trace writing.
func TestTraceWriterConcurrent(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "concurrent.trace")

	tw, err := NewTraceWriter(tracePath, 0, 1, 10000)
	if err != nil {
		t.Fatalf("NewTraceWriter: %v", err)
	}

	// Concurrent writes
	var wg sync.WaitGroup
	numGoroutines := 10
	recordsPerGoroutine := 100

	for g := range numGoroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range recordsPerGoroutine {
				seqno := uint64(gid*recordsPerGoroutine + i)
				key := int64(gid*1000 + i)
				if err := tw.RecordPut(0, key, uint32(i), seqno); err != nil {
					t.Errorf("RecordPut (g=%d, i=%d): %v", gid, i, err)
				}
			}
		}(g)
	}

	wg.Wait()

	if tw.Count() != uint64(numGoroutines*recordsPerGoroutine) {
		t.Errorf("Count = %d, want %d", tw.Count(), numGoroutines*recordsPerGoroutine)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify we can read all records
	tr, err := OpenTraceReader(tracePath)
	if err != nil {
		t.Fatalf("OpenTraceReader: %v", err)
	}
	defer tr.Close()

	count := 0
	for {
		_, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		count++
	}

	if count != numGoroutines*recordsPerGoroutine {
		t.Errorf("Read count = %d, want %d", count, numGoroutines*recordsPerGoroutine)
	}
}

// TestReplayTrace tests trace replay functionality.
func TestReplayTrace(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "replay.trace")

	// Create trace with operations
	tw, err := NewTraceWriter(tracePath, 100, 1, 1000)
	if err != nil {
		t.Fatalf("NewTraceWriter: %v", err)
	}

	// Simulate a series of operations
	// Key 1: Put(1) at seqno 101, Put(2) at seqno 105
	// Key 2: Put(1) at seqno 102, Delete at seqno 104
	// Key 3: Put(5) at seqno 103

	if err := tw.RecordPut(0, 1, 1, 101); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if err := tw.RecordPut(0, 2, 1, 102); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if err := tw.RecordPut(0, 3, 5, 103); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if err := tw.RecordDelete(0, 2, 104); err != nil {
		t.Fatalf("RecordDelete: %v", err)
	}
	if err := tw.RecordPut(0, 1, 2, 105); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	testCases := []struct {
		name         string
		targetSeqno  uint64
		expectedKey1 uint32 // valueBase, 0 means deleted
		key1Exists   bool
		expectedKey2 uint32
		key2Exists   bool
		expectedKey3 uint32
		key3Exists   bool
	}{
		{
			name:         "replay to 101",
			targetSeqno:  101,
			expectedKey1: 1, key1Exists: true,
			expectedKey2: 0, key2Exists: false,
			expectedKey3: 0, key3Exists: false,
		},
		{
			name:         "replay to 103",
			targetSeqno:  103,
			expectedKey1: 1, key1Exists: true,
			expectedKey2: 1, key2Exists: true,
			expectedKey3: 5, key3Exists: true,
		},
		{
			name:         "replay to 104 (key2 deleted)",
			targetSeqno:  104,
			expectedKey1: 1, key1Exists: true,
			expectedKey2: 0, key2Exists: false,
			expectedKey3: 5, key3Exists: true,
		},
		{
			name:         "replay to 105 (key1 updated)",
			targetSeqno:  105,
			expectedKey1: 2, key1Exists: true,
			expectedKey2: 0, key2Exists: false,
			expectedKey3: 5, key3Exists: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Start with empty state
			state := NewExpectedStateV2(1000, 1, 4)

			applied, err := ReplayTrace(tracePath, tc.targetSeqno, state)
			if err != nil {
				t.Fatalf("ReplayTrace: %v", err)
			}

			t.Logf("Applied %d operations for targetSeqno %d", applied, tc.targetSeqno)

			// Check key 1
			if got := state.Exists(0, 1); got != tc.key1Exists {
				t.Errorf("Key 1 exists = %v, want %v", got, tc.key1Exists)
			}
			if tc.key1Exists {
				if got := state.GetValueBase(0, 1); got != tc.expectedKey1 {
					t.Errorf("Key 1 valueBase = %d, want %d", got, tc.expectedKey1)
				}
			}

			// Check key 2
			if got := state.Exists(0, 2); got != tc.key2Exists {
				t.Errorf("Key 2 exists = %v, want %v", got, tc.key2Exists)
			}
			if tc.key2Exists {
				if got := state.GetValueBase(0, 2); got != tc.expectedKey2 {
					t.Errorf("Key 2 valueBase = %d, want %d", got, tc.expectedKey2)
				}
			}

			// Check key 3
			if got := state.Exists(0, 3); got != tc.key3Exists {
				t.Errorf("Key 3 exists = %v, want %v", got, tc.key3Exists)
			}
			if tc.key3Exists {
				if got := state.GetValueBase(0, 3); got != tc.expectedKey3 {
					t.Errorf("Key 3 valueBase = %d, want %d", got, tc.expectedKey3)
				}
			}
		})
	}
}

// TestExpectedStateRecovery tests the full SaveAtAndAfter/Restore workflow.
func TestExpectedStateRecovery(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "state")

	const numKeys int64 = 1000
	const numCFs = 1

	// Create initial state with some data
	state := NewExpectedStateV2(numKeys, numCFs, 4)

	// Simulate some initial operations (before the crash window)
	state.SyncPut(0, 10, 3) // key 10 = valueBase 3
	state.SyncPut(0, 20, 5) // key 20 = valueBase 5
	state.SyncPut(0, 30, 7) // key 30 = valueBase 7
	state.SetPersistedSeqno(100)

	// Create recovery orchestrator
	recovery := NewExpectedStateRecovery(basePath, numCFs, numKeys)

	// SaveAtAndAfter: snapshot state and start tracing
	tw, err := recovery.SaveAtAndAfter(state, 100)
	if err != nil {
		t.Fatalf("SaveAtAndAfter: %v", err)
	}

	// Simulate operations during crash window
	// These would normally happen in the stress test workers
	if err := tw.RecordPut(0, 10, 4, 101); err != nil { // key 10: 3 -> 4
		t.Fatalf("RecordPut: %v", err)
	}
	if err := tw.RecordPut(0, 40, 1, 102); err != nil { // key 40: new key
		t.Fatalf("RecordPut: %v", err)
	}
	if err := tw.RecordDelete(0, 20, 103); err != nil { // key 20: deleted
		t.Fatalf("RecordDelete: %v", err)
	}
	if err := tw.RecordPut(0, 10, 5, 104); err != nil { // key 10: 4 -> 5
		t.Fatalf("RecordPut: %v", err)
	}
	if err := tw.RecordPut(0, 50, 1, 105); err != nil { // key 50: new key
		t.Fatalf("RecordPut: %v", err)
	}

	// Stop tracing (simulates crash happened here)
	if err := recovery.StopTracing(); err != nil {
		t.Fatalf("StopTracing: %v", err)
	}

	// Verify recovery files exist
	if !recovery.HasRecoveryFiles() {
		t.Fatal("Recovery files should exist")
	}

	// Test recovery at different sequence numbers
	testCases := []struct {
		name           string
		recoveredSeqno uint64
		expectations   map[int64]struct {
			exists    bool
			valueBase uint32
		}
	}{
		{
			name:           "recover to seqno 100 (snapshot only)",
			recoveredSeqno: 100,
			expectations: map[int64]struct {
				exists    bool
				valueBase uint32
			}{
				10: {true, 3},
				20: {true, 5},
				30: {true, 7},
				40: {false, 0},
				50: {false, 0},
			},
		},
		{
			name:           "recover to seqno 102",
			recoveredSeqno: 102,
			expectations: map[int64]struct {
				exists    bool
				valueBase uint32
			}{
				10: {true, 4},  // Updated at 101
				20: {true, 5},  // Not yet deleted
				30: {true, 7},  // Unchanged
				40: {true, 1},  // Created at 102
				50: {false, 0}, // Not yet created
			},
		},
		{
			name:           "recover to seqno 103 (key 20 deleted)",
			recoveredSeqno: 103,
			expectations: map[int64]struct {
				exists    bool
				valueBase uint32
			}{
				10: {true, 4},
				20: {false, 0}, // Deleted at 103
				30: {true, 7},
				40: {true, 1},
				50: {false, 0},
			},
		},
		{
			name:           "recover to seqno 105 (all ops applied)",
			recoveredSeqno: 105,
			expectations: map[int64]struct {
				exists    bool
				valueBase uint32
			}{
				10: {true, 5},  // Updated again at 104
				20: {false, 0}, // Still deleted
				30: {true, 7},  // Unchanged
				40: {true, 1},  // Created at 102
				50: {true, 1},  // Created at 105
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recovered, applied, err := recovery.Restore(tc.recoveredSeqno)
			if err != nil {
				t.Fatalf("Restore: %v", err)
			}

			t.Logf("Applied %d operations for recoveredSeqno %d", applied, tc.recoveredSeqno)

			for key, exp := range tc.expectations {
				exists := recovered.Exists(0, key)
				if exists != exp.exists {
					t.Errorf("Key %d: exists = %v, want %v", key, exists, exp.exists)
					continue
				}
				if exp.exists {
					valueBase := recovered.GetValueBase(0, key)
					if valueBase != exp.valueBase {
						t.Errorf("Key %d: valueBase = %d, want %d", key, valueBase, exp.valueBase)
					}
				}
			}
		})
	}

	// Cleanup
	if err := recovery.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}

	if recovery.HasRecoveryFiles() {
		t.Error("Recovery files should be cleaned up")
	}
}

// TestExpectedStateRecoveryMultipleCFs tests recovery with multiple column families.
func TestExpectedStateRecoveryMultipleCFs(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "multi_cf")

	const numKeys int64 = 100
	const numCFs = 3

	state := NewExpectedStateV2(numKeys, numCFs, 4)

	// Initial state across CFs
	state.SyncPut(0, 1, 10)
	state.SyncPut(1, 1, 20)
	state.SyncPut(2, 1, 30)
	state.SetPersistedSeqno(50)

	recovery := NewExpectedStateRecovery(basePath, numCFs, numKeys)

	tw, err := recovery.SaveAtAndAfter(state, 50)
	if err != nil {
		t.Fatalf("SaveAtAndAfter: %v", err)
	}

	// Operations on different CFs
	if err := tw.RecordPut(0, 1, 11, 51); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if err := tw.RecordPut(1, 1, 21, 52); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if err := tw.RecordDelete(2, 1, 53); err != nil {
		t.Fatalf("RecordDelete: %v", err)
	}

	if err := recovery.StopTracing(); err != nil {
		t.Fatalf("StopTracing: %v", err)
	}

	// Recover to seqno 52
	recovered, applied, err := recovery.Restore(52)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if applied != 2 {
		t.Errorf("Applied = %d, want 2", applied)
	}

	// CF 0: updated
	if vb := recovered.GetValueBase(0, 1); vb != 11 {
		t.Errorf("CF0 key 1 valueBase = %d, want 11", vb)
	}

	// CF 1: updated
	if vb := recovered.GetValueBase(1, 1); vb != 21 {
		t.Errorf("CF1 key 1 valueBase = %d, want 21", vb)
	}

	// CF 2: NOT yet deleted (seqno 53 > 52)
	if !recovered.Exists(2, 1) {
		t.Error("CF2 key 1 should still exist")
	}
	if vb := recovered.GetValueBase(2, 1); vb != 30 {
		t.Errorf("CF2 key 1 valueBase = %d, want 30", vb)
	}

	recovery.Cleanup()
}

// TestTraceReaderInvalidFile tests error handling for invalid trace files.
func TestTraceReaderInvalidFile(t *testing.T) {
	dir := t.TempDir()

	// Test 1: Non-existent file
	_, err := OpenTraceReader(filepath.Join(dir, "nonexistent"))
	if err == nil {
		t.Error("Expected error for non-existent file")
	}

	// Test 2: Invalid magic
	badMagic := filepath.Join(dir, "bad_magic.trace")
	if err := os.WriteFile(badMagic, []byte("BADMAGIC0000000000000000000000000"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err = OpenTraceReader(badMagic)
	if err == nil {
		t.Error("Expected error for invalid magic")
	}

	// Test 3: Truncated header
	truncated := filepath.Join(dir, "truncated.trace")
	if err := os.WriteFile(truncated, []byte("RKYTRACE"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err = OpenTraceReader(truncated)
	if err == nil {
		t.Error("Expected error for truncated header")
	}
}

// TestSyncPutSyncDelete tests the SyncPut and SyncDelete methods on ExpectedStateV2.
func TestSyncPutSyncDelete(t *testing.T) {
	state := NewExpectedStateV2(100, 1, 4)

	// Initial state: key doesn't exist
	if state.Exists(0, 1) {
		t.Error("Key should not exist initially")
	}

	// SyncPut: set valueBase directly
	state.SyncPut(0, 1, 42)
	if !state.Exists(0, 1) {
		t.Error("Key should exist after SyncPut")
	}
	if vb := state.GetValueBase(0, 1); vb != 42 {
		t.Errorf("ValueBase = %d, want 42", vb)
	}

	// SyncPut again: change valueBase
	state.SyncPut(0, 1, 99)
	if vb := state.GetValueBase(0, 1); vb != 99 {
		t.Errorf("ValueBase = %d, want 99", vb)
	}

	// SyncDelete: mark as deleted
	state.SyncDelete(0, 1)
	if state.Exists(0, 1) {
		t.Error("Key should not exist after SyncDelete")
	}

	// SyncPut after delete: resurrect key
	state.SyncPut(0, 1, 1)
	if !state.Exists(0, 1) {
		t.Error("Key should exist after SyncPut following delete")
	}
	if vb := state.GetValueBase(0, 1); vb != 1 {
		t.Errorf("ValueBase = %d, want 1", vb)
	}
}

// TestTraceRecordChecksum tests the checksum helper function.
func TestTraceRecordChecksum(t *testing.T) {
	rec1 := TraceRecord{Op: TraceOpPut, CF: 0, Key: 42, ValueBase: 5, SeqNo: 100}
	rec2 := TraceRecord{Op: TraceOpPut, CF: 0, Key: 42, ValueBase: 5, SeqNo: 100}
	rec3 := TraceRecord{Op: TraceOpPut, CF: 0, Key: 42, ValueBase: 6, SeqNo: 100}

	c1 := TraceRecordChecksum(rec1)
	c2 := TraceRecordChecksum(rec2)
	c3 := TraceRecordChecksum(rec3)

	if c1 != c2 {
		t.Error("Identical records should have same checksum")
	}
	if c1 == c3 {
		t.Error("Different records should have different checksums")
	}
}

// TestTraceFlush tests that Flush persists data to disk.
func TestTraceFlush(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "flush.trace")

	tw, err := NewTraceWriter(tracePath, 0, 1, 100)
	if err != nil {
		t.Fatalf("NewTraceWriter: %v", err)
	}

	// Write a record
	if err := tw.RecordPut(0, 1, 1, 1); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	// Flush explicitly
	if err := tw.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Verify file exists and has content (header + 1 record)
	info, err := os.Stat(tracePath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	expectedSize := int64(traceHeaderSize + traceRecordSize)
	if info.Size() != expectedSize {
		t.Errorf("File size = %d, want %d", info.Size(), expectedSize)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestTraceWriterWriteAfterClose tests that writing after close fails.
func TestTraceWriterWriteAfterClose(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "closed.trace")

	tw, err := NewTraceWriter(tracePath, 0, 1, 100)
	if err != nil {
		t.Fatalf("NewTraceWriter: %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Try to write after close
	err = tw.RecordPut(0, 1, 1, 1)
	if err == nil {
		t.Error("Expected error when writing after close")
	}
}

// BenchmarkTraceWrite benchmarks trace write performance.
func BenchmarkTraceWrite(b *testing.B) {
	dir := b.TempDir()
	tracePath := filepath.Join(dir, "bench.trace")

	tw, err := NewTraceWriter(tracePath, 0, 1, 1000000)
	if err != nil {
		b.Fatalf("NewTraceWriter: %v", err)
	}
	defer tw.Close()

	b.ResetTimer()
	for b.Loop() {
		i := b.N
		if err := tw.RecordPut(0, int64(i%1000000), uint32(i), uint64(i)); err != nil {
			b.Fatalf("RecordPut: %v", err)
		}
	}
}

// BenchmarkReplayTrace benchmarks trace replay performance.
func BenchmarkReplayTrace(b *testing.B) {
	dir := b.TempDir()
	tracePath := filepath.Join(dir, "bench.trace")

	// Create trace with 10000 records
	tw, err := NewTraceWriter(tracePath, 0, 1, 10000)
	if err != nil {
		b.Fatalf("NewTraceWriter: %v", err)
	}

	for i := range 10000 {
		if err := tw.RecordPut(0, int64(i), uint32(i), uint64(i)); err != nil {
			b.Fatalf("RecordPut: %v", err)
		}
	}
	tw.Close()

	b.ResetTimer()
	for b.Loop() {
		state := NewExpectedStateV2(10000, 1, 4)
		if _, err := ReplayTrace(tracePath, 10000, state); err != nil {
			b.Fatalf("ReplayTrace: %v", err)
		}
	}
}
