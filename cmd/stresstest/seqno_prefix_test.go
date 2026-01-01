// seqno_prefix_test.go tests the seqno-prefix verification mode.
//
// This implements the "seqno-prefix (no holes)" verification model for
// crash recovery testing.
package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aalhour/rockyardkv"
	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/trace"
)

// TestSeqnoDomainAlignment proves that trace seqnos are in the same domain
// as database.GetLatestSequenceNumber().
//
// Contract: After a successful write, the seqno recorded in the trace must
// equal database.GetLatestSequenceNumber() at that moment. On recovery,
// database.GetLatestSequenceNumber() returns a value from the same domain,
// allowing correct replay cutoff.
func TestSeqnoDomainAlignment(t *testing.T) {
	// Create temp directory for DB and trace
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")
	tracePath := filepath.Join(dir, "test.trace")

	// Open trace file
	traceFile, err := os.Create(tracePath)
	if err != nil {
		t.Fatalf("create trace file: %v", err)
	}
	traceWriter, err := trace.NewWriter(traceFile)
	if err != nil {
		t.Fatalf("create trace writer: %v", err)
	}

	// Open database
	opts := rockyardkv.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := rockyardkv.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	// Capture seqnos after each write
	var capturedSeqnos []uint64

	// Write 1: Put key1
	key1 := []byte("0000000001")
	val1 := []byte{0, 0, 0, 1, 0, 0, 0, 0} // valBase=1
	if err := database.Put(nil, key1, val1); err != nil {
		t.Fatalf("put key1: %v", err)
	}
	seqno1 := database.GetLatestSequenceNumber()
	capturedSeqnos = append(capturedSeqnos, seqno1)

	// Record trace with captured seqno
	wb1 := batch.New()
	wb1.Put(key1, val1)
	payload1 := &trace.WritePayload{ColumnFamilyID: 0, SequenceNumber: seqno1, Data: wb1.Data()}
	if err := traceWriter.Write(trace.TypeWrite, payload1.Encode()); err != nil {
		t.Fatalf("write trace 1: %v", err)
	}

	// Write 2: Put key2
	key2 := []byte("0000000002")
	val2 := []byte{0, 0, 0, 2, 0, 0, 0, 0} // valBase=2
	if err := database.Put(nil, key2, val2); err != nil {
		t.Fatalf("put key2: %v", err)
	}
	seqno2 := database.GetLatestSequenceNumber()
	capturedSeqnos = append(capturedSeqnos, seqno2)

	// Record trace with captured seqno
	wb2 := batch.New()
	wb2.Put(key2, val2)
	payload2 := &trace.WritePayload{ColumnFamilyID: 0, SequenceNumber: seqno2, Data: wb2.Data()}
	if err := traceWriter.Write(trace.TypeWrite, payload2.Encode()); err != nil {
		t.Fatalf("write trace 2: %v", err)
	}

	// Write 3: Delete key1
	if err := database.Delete(nil, key1); err != nil {
		t.Fatalf("delete key1: %v", err)
	}
	seqno3 := database.GetLatestSequenceNumber()
	capturedSeqnos = append(capturedSeqnos, seqno3)

	// Record trace with captured seqno
	wb3 := batch.New()
	wb3.Delete(key1)
	payload3 := &trace.WritePayload{ColumnFamilyID: 0, SequenceNumber: seqno3, Data: wb3.Data()}
	if err := traceWriter.Write(trace.TypeWrite, payload3.Encode()); err != nil {
		t.Fatalf("write trace 3: %v", err)
	}

	// Verify seqnos are strictly increasing (no reuse)
	for i := 1; i < len(capturedSeqnos); i++ {
		if capturedSeqnos[i] <= capturedSeqnos[i-1] {
			t.Errorf("seqno not strictly increasing: seqno[%d]=%d <= seqno[%d]=%d",
				i, capturedSeqnos[i], i-1, capturedSeqnos[i-1])
		}
	}
	t.Logf("Captured seqnos: %v", capturedSeqnos)

	// Close trace and DB
	if err := traceFile.Close(); err != nil {
		t.Fatalf("close trace file: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Reopen database and verify seqno domain alignment
	database2, err := rockyardkv.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer database2.Close()

	recoveredSeqno := database2.GetLatestSequenceNumber()
	t.Logf("Recovered seqno: %d", recoveredSeqno)

	// Recovered seqno must be >= last captured seqno (same domain)
	if recoveredSeqno < capturedSeqnos[len(capturedSeqnos)-1] {
		t.Errorf("recovered seqno %d < last captured seqno %d (domain mismatch!)",
			recoveredSeqno, capturedSeqnos[len(capturedSeqnos)-1])
	}

	// Read trace and verify recorded seqnos match captured seqnos
	traceFile2, err := os.Open(tracePath)
	if err != nil {
		t.Fatalf("open trace file: %v", err)
	}
	defer traceFile2.Close()

	reader, err := trace.NewReader(traceFile2)
	if err != nil {
		t.Fatalf("create trace reader: %v", err)
	}

	var recordedSeqnos []uint64
	for {
		rec, err := reader.Read()
		if err != nil {
			break // EOF
		}
		if rec.Type == trace.TypeWrite {
			payload, err := trace.DecodeWritePayloadV2(rec.Payload)
			if err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			recordedSeqnos = append(recordedSeqnos, payload.SequenceNumber)
		}
	}

	// Verify recorded seqnos match captured seqnos exactly
	if len(recordedSeqnos) != len(capturedSeqnos) {
		t.Fatalf("recorded seqnos count %d != captured seqnos count %d",
			len(recordedSeqnos), len(capturedSeqnos))
	}
	for i := range capturedSeqnos {
		if recordedSeqnos[i] != capturedSeqnos[i] {
			t.Errorf("seqno[%d]: recorded=%d != captured=%d (domain mismatch!)",
				i, recordedSeqnos[i], capturedSeqnos[i])
		}
	}

	// Replay with cutoff at seqno2 (should include ops 1 and 2, exclude op 3)
	state := newSeqnoPrefixState()
	replayed, _, err := replayTraceFileSeqno(tracePath, seqno2, state)
	if err != nil {
		t.Fatalf("replay trace: %v", err)
	}
	if replayed != 2 {
		t.Errorf("replayed: got %d, want 2", replayed)
	}

	// After replay with cutoff at seqno2:
	// - key1 should exist (put at seqno1, delete at seqno3 > cutoff)
	// - key2 should exist (put at seqno2)
	if _, exists := state.get(1); !exists {
		t.Error("key1 should exist (delete was after cutoff)")
	}
	if _, exists := state.get(2); !exists {
		t.Error("key2 should exist")
	}

	t.Logf("✅ Seqno domain alignment verified")
	t.Logf("   - Captured seqnos match recorded seqnos: %v", capturedSeqnos)
	t.Logf("   - Recovered seqno (%d) >= last write seqno (%d)", recoveredSeqno, capturedSeqnos[len(capturedSeqnos)-1])
	t.Logf("   - Replay with cutoff correctly filters by seqno domain")
}

// TestSeqnoPrefixState_PutDelete tests the seqno-prefix state tracking.
func TestSeqnoPrefixState_PutDelete(t *testing.T) {
	state := newSeqnoPrefixState()

	// Initially empty
	if _, exists := state.get(1); exists {
		t.Error("key 1 should not exist initially")
	}

	// Put key 1
	state.put(1, 100)
	if val, exists := state.get(1); !exists || val != 100 {
		t.Errorf("key 1: got val=%d exists=%v, want val=100 exists=true", val, exists)
	}

	// Update key 1
	state.put(1, 200)
	if val, exists := state.get(1); !exists || val != 200 {
		t.Errorf("key 1: got val=%d exists=%v, want val=200 exists=true", val, exists)
	}

	// Delete key 1
	state.delete(1)
	if _, exists := state.get(1); exists {
		t.Error("key 1 should not exist after delete")
	}
}

// TestSeqnoStateHandler_Iterate tests batch iteration for state reconstruction.
func TestSeqnoStateHandler_Iterate(t *testing.T) {
	// Create a write batch
	wb := batch.New()
	wb.Put([]byte("0000000001"), []byte{0, 0, 0, 5}) // key 1, valBase 5
	wb.Put([]byte("0000000002"), []byte{0, 0, 0, 10})
	wb.Delete([]byte("0000000003"))

	// Apply to state
	state := newSeqnoPrefixState()
	handler := &seqnoStateHandler{state: state}
	if err := wb.Iterate(handler); err != nil {
		t.Fatalf("iterate failed: %v", err)
	}

	// Verify
	if val, exists := state.get(1); !exists || val != 5 {
		t.Errorf("key 1: got val=%d exists=%v, want val=5 exists=true", val, exists)
	}
	if val, exists := state.get(2); !exists || val != 10 {
		t.Errorf("key 2: got val=%d exists=%v, want val=10 exists=true", val, exists)
	}
	if _, exists := state.get(3); exists {
		t.Error("key 3 should not exist after delete")
	}
}

// TestReplayTraceFileSeqno tests replaying a trace file with seqno filtering.
func TestReplayTraceFileSeqno(t *testing.T) {
	// Create a temporary trace file
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "test.trace")

	// Write trace with V2 format (includes seqno)
	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatalf("create trace file: %v", err)
	}

	tw, err := trace.NewWriter(f)
	if err != nil {
		t.Fatalf("create trace writer: %v", err)
	}

	// Write operation with seqno=10
	wb1 := batch.New()
	wb1.Put([]byte("0000000001"), []byte{0, 0, 0, 1})
	payload1 := &trace.WritePayload{ColumnFamilyID: 0, SequenceNumber: 10, Data: wb1.Data()}
	if err := tw.Write(trace.TypeWrite, payload1.Encode()); err != nil {
		t.Fatalf("write trace record: %v", err)
	}

	// Write operation with seqno=20
	wb2 := batch.New()
	wb2.Put([]byte("0000000002"), []byte{0, 0, 0, 2})
	payload2 := &trace.WritePayload{ColumnFamilyID: 0, SequenceNumber: 20, Data: wb2.Data()}
	if err := tw.Write(trace.TypeWrite, payload2.Encode()); err != nil {
		t.Fatalf("write trace record: %v", err)
	}

	// Write operation with seqno=30
	wb3 := batch.New()
	wb3.Put([]byte("0000000003"), []byte{0, 0, 0, 3})
	payload3 := &trace.WritePayload{ColumnFamilyID: 0, SequenceNumber: 30, Data: wb3.Data()}
	if err := tw.Write(trace.TypeWrite, payload3.Encode()); err != nil {
		t.Fatalf("write trace record: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close trace file: %v", err)
	}

	// Replay with cutoff seqno=25 (should include seqno 10 and 20, exclude 30)
	state := newSeqnoPrefixState()
	replayed, skipped, err := replayTraceFileSeqno(tracePath, 25, state)
	if err != nil {
		t.Fatalf("replay trace file: %v", err)
	}

	// Should have replayed 2 operations
	if replayed != 2 {
		t.Errorf("replayed: got %d, want 2", replayed)
	}
	if skipped != 0 {
		t.Errorf("skipped: got %d, want 0", skipped)
	}

	// key 1 and 2 should exist, key 3 should not
	if _, exists := state.get(1); !exists {
		t.Error("key 1 should exist")
	}
	if _, exists := state.get(2); !exists {
		t.Error("key 2 should exist")
	}
	if _, exists := state.get(3); exists {
		t.Error("key 3 should not exist (seqno > cutoff)")
	}
}

// TestParseStressKeyNum tests key number parsing.
func TestParseStressKeyNum(t *testing.T) {
	tests := []struct {
		key  string
		want int
	}{
		{"0000000001", 1},
		{"0000000123", 123},
		{"0000009999", 9999},
		{"invalid", -1},
		{"", -1},
	}

	for _, tt := range tests {
		got := parseStressKeyNum([]byte(tt.key))
		if got != tt.want {
			t.Errorf("parseStressKeyNum(%q): got %d, want %d", tt.key, got, tt.want)
		}
	}
}

// TestParseStressValueBase tests value base parsing.
func TestParseStressValueBase(t *testing.T) {
	tests := []struct {
		value []byte
		want  uint32
	}{
		{[]byte{0, 0, 0, 1}, 1},
		{[]byte{0, 0, 0, 255}, 255},
		{[]byte{0, 0, 1, 0}, 256},
		{[]byte{1, 2, 3, 4}, 0x01020304},
		{[]byte{}, 0},
		{[]byte{1, 2}, 0},
	}

	for _, tt := range tests {
		got := parseStressValueBase(tt.value)
		if got != tt.want {
			t.Errorf("parseStressValueBase(%v): got %d, want %d", tt.value, got, tt.want)
		}
	}
}

// init ensures verbose flag is accessible for tests
func init() {
	// Set verbose to a new bool pointer for testing
	v := false
	verbose = &v

	// Set numKeys for verification tests
	n := int64(10000)
	numKeys = &n
}

// Suppress unused import warning
var _ = time.Now
