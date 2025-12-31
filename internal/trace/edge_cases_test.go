package trace

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Contract: Reader handles truncated trace file gracefully.
func TestReader_TruncatedFile_ReturnsPartialRecords(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "truncated.trace")

	// Create a trace file
	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write several records
	now := time.Now()
	for i := range 10 {
		w.WriteAt(now.Add(time.Duration(i)*time.Millisecond), TypeWrite, []byte("key\x00value"))
	}
	w.Close()
	f.Close()

	// Get file size and truncate it mid-record
	info, _ := os.Stat(tracePath)
	originalSize := info.Size()
	truncateAt := originalSize * 2 / 3 // Truncate at 2/3 of file

	if err := os.Truncate(tracePath, truncateAt); err != nil {
		t.Fatal(err)
	}

	// Read should return partial records without panicking
	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := NewReader(rf)
	if err != nil {
		t.Fatalf("NewReader should succeed on truncated file: %v", err)
	}

	count := 0
	for {
		_, err := reader.Read()
		if err != nil {
			break // Expected - file is truncated
		}
		count++
	}

	if count == 0 {
		t.Error("should read at least one record before truncation point")
	}
	if count >= 10 {
		t.Error("should not read all records from truncated file")
	}
}

// Contract: MaxBytes=0 means unlimited writes.
func TestWriter_MaxBytesZero_Unlimited(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "unlimited.trace")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	// WithMaxBytes(0) means unlimited
	w, err := NewWriter(f, WithMaxBytes(0))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write many records
	now := time.Now()
	for i := range 1000 {
		w.WriteAt(now.Add(time.Duration(i)*time.Microsecond), TypeWrite, []byte("key\x00value"))
	}

	if w.Truncated() {
		t.Error("writer with maxBytes=0 should never truncate")
	}

	w.Close()
	f.Close()

	// Verify all records written
	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := NewReader(rf)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for {
		_, err := reader.Read()
		if err != nil {
			break
		}
		count++
	}

	if count != 1000 {
		t.Errorf("expected 1000 records, got %d", count)
	}
}

// Contract: Writer stops writing at maxBytes but file remains valid.
func TestWriter_MaxBytes_StopsWriting(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "limited.trace")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f, WithMaxBytes(500))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write until truncation
	now := time.Now()
	writeCount := 0
	for i := range 100 {
		err := w.WriteAt(now.Add(time.Duration(i)*time.Millisecond), TypeWrite, []byte("key\x00value"))
		if err != nil {
			break
		}
		writeCount++
	}

	w.Close()
	f.Close()

	// File should be readable
	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := NewReader(rf)
	if err != nil {
		t.Fatalf("truncated file should be readable: %v", err)
	}

	readCount := 0
	for {
		_, err := reader.Read()
		if err != nil {
			break
		}
		readCount++
	}

	// Should have fewer records than we attempted to write
	if readCount >= writeCount && writeCount > 10 {
		t.Errorf("maxBytes should limit records: wrote %d attempts, read %d", writeCount, readCount)
	}
}

// Contract: BytesWritten tracks header and records correctly.
func TestWriter_BytesWritten_IncludesHeader(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "bytes.trace")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Header should be counted
	initialBytes := w.BytesWritten()
	if initialBytes == 0 {
		t.Error("BytesWritten should include header bytes")
	}

	// Write a record
	w.WriteAt(time.Now(), TypeWrite, []byte("key\x00value"))
	afterWrite := w.BytesWritten()

	if afterWrite <= initialBytes {
		t.Errorf("BytesWritten should increase after write: %d -> %d", initialBytes, afterWrite)
	}

	w.Close()
	f.Close()

	// Should match file size
	info, _ := os.Stat(tracePath)
	if afterWrite != info.Size() {
		t.Errorf("BytesWritten=%d, file size=%d", afterWrite, info.Size())
	}
}

// Contract: Truncated returns false initially, true after limit exceeded.
func TestWriter_Truncated_StateTransition(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "state.trace")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f, WithMaxBytes(200))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Initially not truncated
	if w.Truncated() {
		t.Error("should not be truncated initially")
	}

	// Write until truncation
	now := time.Now()
	for i := range 50 {
		w.WriteAt(now.Add(time.Duration(i)*time.Millisecond), TypeWrite, []byte("key\x00value"))
	}

	// Should now be truncated
	if !w.Truncated() {
		t.Error("should be truncated after exceeding limit")
	}

	w.Close()
	f.Close()
}

// Contract: Writer does not return error when writing beyond limit.
func TestWriter_WriteAfterTruncation_NoError(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "noerror.trace")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f, WithMaxBytes(100))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write until truncated
	now := time.Now()
	for i := range 20 {
		w.WriteAt(now.Add(time.Duration(i)*time.Millisecond), TypeWrite, []byte("key\x00value"))
	}

	// Continue writing after truncation - should not error
	for i := range 10 {
		err := w.WriteAt(now.Add(time.Duration(i+20)*time.Millisecond), TypeWrite, []byte("after\x00truncation"))
		if err != nil {
			t.Errorf("write after truncation should not error: %v", err)
		}
	}

	w.Close()
	f.Close()
}

// Contract: Empty payload is valid.
func TestWriter_EmptyPayload_Valid(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "empty.trace")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write empty payload
	err = w.WriteAt(time.Now(), TypeGet, []byte{})
	if err != nil {
		t.Errorf("empty payload should be valid: %v", err)
	}

	w.Close()
	f.Close()

	// Should be readable
	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := NewReader(rf)
	if err != nil {
		t.Fatal(err)
	}

	rec, err := reader.Read()
	if err != nil {
		t.Fatalf("should read record with empty payload: %v", err)
	}

	if len(rec.Payload) != 0 {
		t.Errorf("payload should be empty, got %d bytes", len(rec.Payload))
	}
}
