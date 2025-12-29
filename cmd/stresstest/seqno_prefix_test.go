// seqno_prefix_test.go tests the seqno-prefix verification mode.
//
// This implements the "seqno-prefix (no holes)" verification model from C02-03.
// Reference: docs/redteam/ISSUES/C02_issues.md
package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/trace"
)

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
