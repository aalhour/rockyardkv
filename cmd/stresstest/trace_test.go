package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aalhour/rockyardkv/internal/trace"
)

// Contract: -trace-max-size stops trace writes at the configured limit.
func TestStresstest_TraceMaxSize_TruncatesAtLimit(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "trace.bin")

	// Create trace writer with small limit
	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	maxBytes := int64(500) // Very small limit
	w, err := trace.NewWriter(f, trace.WithMaxBytes(maxBytes))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write many records
	now := time.Now()
	for i := range 100 {
		w.WriteAt(now.Add(time.Duration(i)*time.Millisecond), trace.TypeWrite, []byte("key"+string(rune(i))+"\x00value"))
	}

	if !w.Truncated() {
		t.Error("writer should be truncated after exceeding limit")
	}

	bytesWritten := w.BytesWritten()
	w.Close()
	f.Close()

	// Verify file size is close to limit
	info, err := os.Stat(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	// File size should be approximately maxBytes (may be slightly less due to stopping at record boundary)
	if info.Size() > maxBytes+100 {
		t.Errorf("file size %d exceeds limit %d by too much", info.Size(), maxBytes)
	}

	if bytesWritten > maxBytes+100 {
		t.Errorf("bytes written %d exceeds limit %d by too much", bytesWritten, maxBytes)
	}
}

// Contract: -trace-max-size=0 means unlimited writes.
func TestStresstest_TraceMaxSizeZero_Unlimited(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "trace.bin")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	// MaxBytes=0 means unlimited
	w, err := trace.NewWriter(f, trace.WithMaxBytes(0))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write many records
	now := time.Now()
	for i := range 100 {
		w.WriteAt(now.Add(time.Duration(i)*time.Millisecond), trace.TypeWrite, []byte("key\x00value"))
	}

	if w.Truncated() {
		t.Error("writer with maxBytes=0 should not truncate")
	}

	w.Close()
	f.Close()

	// Verify all records were written
	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := trace.NewReader(rf)
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

	if count != 100 {
		t.Errorf("expected 100 records, got %d", count)
	}
}

// Contract: Trace file remains readable after truncation.
func TestStresstest_TraceAfterTruncation_Readable(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "trace.bin")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	// Small limit to force truncation
	w, err := trace.NewWriter(f, trace.WithMaxBytes(300))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write until truncation
	now := time.Now()
	for i := range 50 {
		w.WriteAt(now.Add(time.Duration(i)*time.Millisecond), trace.TypeWrite, []byte("key\x00value"))
	}
	w.Close()
	f.Close()

	// File should be readable
	rf, err := os.Open(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	reader, err := trace.NewReader(rf)
	if err != nil {
		t.Fatalf("truncated file should be readable: %v", err)
	}

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
}

// Contract: Trace writer tracks bytes written correctly.
func TestStresstest_TraceBytesWritten_Accurate(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "trace.bin")

	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	w, err := trace.NewWriter(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write some records
	now := time.Now()
	for i := range 10 {
		w.WriteAt(now.Add(time.Duration(i)*time.Millisecond), trace.TypeWrite, []byte("key\x00value"))
	}

	bytesWritten := w.BytesWritten()
	w.Close()
	f.Close()

	// Verify bytes written matches file size
	info, err := os.Stat(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	if bytesWritten != info.Size() {
		t.Errorf("BytesWritten=%d, file size=%d", bytesWritten, info.Size())
	}
}
