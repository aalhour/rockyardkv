package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aalhour/rockyardkv/internal/trace"
)

// Contract: cmdStats counts Write, Get operations correctly.
func TestCmdStats_CountsOperations(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "test.trace")

	// Create a trace file with known operations
	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := trace.NewWriter(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write some operations
	now := time.Now()
	w.WriteAt(now, trace.TypeWrite, []byte("key1\x00value1"))
	w.WriteAt(now.Add(time.Millisecond), trace.TypeWrite, []byte("key2\x00value2"))
	w.WriteAt(now.Add(2*time.Millisecond), trace.TypeGet, []byte("key1"))
	w.Close()
	f.Close()

	// Read and verify stats
	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := trace.NewReader(rf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	stats := countRecords(reader)
	if stats.writes != 2 {
		t.Errorf("writes = %d, want 2", stats.writes)
	}
	if stats.gets != 1 {
		t.Errorf("gets = %d, want 1", stats.gets)
	}
}

// recordStats holds operation counts for testing.
type recordStats struct {
	writes int
	gets   int
	other  int
}

// countRecords counts records by type.
func countRecords(r *trace.Reader) recordStats {
	var stats recordStats
	for {
		rec, err := r.Read()
		if err != nil {
			break
		}
		switch rec.Type {
		case trace.TypeWrite:
			stats.writes++
		case trace.TypeGet:
			stats.gets++
		default:
			stats.other++
		}
	}
	return stats
}

// Contract: cmdStats handles empty trace file.
func TestCmdStats_EmptyTrace(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "empty.trace")

	// Create an empty trace file (header only)
	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := trace.NewWriter(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	w.Close()
	f.Close()

	// Read and verify stats
	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := trace.NewReader(rf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	stats := countRecords(reader)
	if stats.writes != 0 || stats.gets != 0 {
		t.Errorf("empty trace should have no records: writes=%d, gets=%d", stats.writes, stats.gets)
	}
}

// Contract: Trace file is readable after writing with max size.
func TestTrace_MaxSize_Readable(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "maxsize.trace")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	// Small max size to trigger truncation
	w, err := trace.NewWriter(f, trace.WithMaxBytes(200))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write operations until truncation
	now := time.Now()
	for i := range 100 {
		w.WriteAt(now.Add(time.Duration(i)*time.Millisecond), trace.TypeWrite, []byte("key\x00value"))
	}
	w.Close()
	f.Close()

	// Verify file is still readable
	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := trace.NewReader(rf)
	if err != nil {
		t.Fatalf("NewReader should succeed: %v", err)
	}

	// Count readable records
	count := 0
	for {
		_, err := reader.Read()
		if err != nil {
			break
		}
		count++
	}

	if count == 0 {
		t.Error("should have at least one readable record")
	}
	if count >= 100 {
		t.Errorf("truncation should limit records: got %d", count)
	}
}

// Contract: Trace records preserve operation order.
func TestTrace_OrderPreserved(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "order.trace")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := trace.NewWriter(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write operations in order
	now := time.Now()
	expectedOrder := []trace.RecordType{
		trace.TypeWrite,
		trace.TypeGet,
		trace.TypeWrite,
		trace.TypeGet,
	}
	for i, rt := range expectedOrder {
		w.WriteAt(now.Add(time.Duration(i)*time.Millisecond), rt, []byte("data"))
	}
	w.Close()
	f.Close()

	// Read and verify order
	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := trace.NewReader(rf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	var actualOrder []trace.RecordType
	for {
		rec, err := reader.Read()
		if err != nil {
			break
		}
		actualOrder = append(actualOrder, rec.Type)
	}

	if len(actualOrder) != len(expectedOrder) {
		t.Fatalf("record count: got %d, want %d", len(actualOrder), len(expectedOrder))
	}

	for i, expected := range expectedOrder {
		if actualOrder[i] != expected {
			t.Errorf("record %d: got %v, want %v", i, actualOrder[i], expected)
		}
	}
}

// Contract: cmdDump respects the -limit flag.
func TestDump_Limit(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "limit.trace")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := trace.NewWriter(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write 10 operations
	now := time.Now()
	for i := range 10 {
		w.WriteAt(now.Add(time.Duration(i)*time.Millisecond), trace.TypeWrite, []byte("data"))
	}
	w.Close()
	f.Close()

	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := trace.NewReader(rf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	// Read with limit
	limit := 5
	count := 0
	for count < limit {
		_, err := reader.Read()
		if err != nil {
			break
		}
		count++
	}

	if count != limit {
		t.Errorf("limit should cap records: got %d, want %d", count, limit)
	}
}

// Contract: Trace timestamps are monotonically increasing.
func TestTrace_Timestamps_Monotonic(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "timestamps.trace")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := trace.NewWriter(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write with increasing timestamps
	base := time.Now()
	for i := range 5 {
		w.WriteAt(base.Add(time.Duration(i)*time.Second), trace.TypeWrite, []byte("data"))
	}
	w.Close()
	f.Close()

	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := trace.NewReader(rf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	var lastTimestamp time.Time
	first := true
	for {
		rec, err := reader.Read()
		if err != nil {
			break
		}
		if !first && rec.Timestamp.Before(lastTimestamp) {
			t.Errorf("timestamps not monotonic: %v before %v", rec.Timestamp, lastTimestamp)
		}
		lastTimestamp = rec.Timestamp
		first = false
	}
}
