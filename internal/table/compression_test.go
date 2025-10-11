package table

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/aalhour/rockyardkv/internal/compression"
)

func TestTableCompressionSnappy(t *testing.T) {
	opts := DefaultBuilderOptions()
	opts.Compression = compression.SnappyCompression
	opts.BlockSize = 100 // Small blocks to force multiple blocks

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	// Add entries with repeated data (compressible)
	for i := range 50 {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d_repeated_repeated_repeated_data", i)
		builder.Add([]byte(key), []byte(value))
	}
	builder.Finish()

	t.Logf("SST size: %d bytes", buf.Len())

	// Read it back
	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// Verify properties
	props, err := reader.Properties()
	if err != nil {
		t.Fatalf("Properties failed: %v", err)
	}
	t.Logf("Compression: %s", props.CompressionName)

	// Verify all entries
	iter := reader.NewIterator()
	iter.SeekToFirst()
	count := 0
	for iter.Valid() {
		expectedKey := fmt.Sprintf("key%05d", count)
		expectedValue := fmt.Sprintf("value%05d_repeated_repeated_repeated_data", count)
		if string(iter.Key()) != expectedKey {
			t.Errorf("key mismatch at %d: got %q, want %q", count, iter.Key(), expectedKey)
		}
		if string(iter.Value()) != expectedValue {
			t.Errorf("value mismatch at %d: got %q, want %q", count, iter.Value(), expectedValue)
		}
		iter.Next()
		count++
	}
	if count != 50 {
		t.Errorf("expected 50 entries, got %d", count)
	}
}

func TestTableCompressionZlib(t *testing.T) {
	opts := DefaultBuilderOptions()
	opts.Compression = compression.ZlibCompression
	opts.BlockSize = 200

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	for i := range 30 {
		key := fmt.Sprintf("zlib_key_%04d", i)
		value := bytes.Repeat([]byte("compress_me_"), 10)
		builder.Add([]byte(key), value)
	}
	builder.Finish()

	t.Logf("SST size: %d bytes", buf.Len())

	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()
	iter.SeekToFirst()
	count := 0
	for iter.Valid() {
		expectedKey := fmt.Sprintf("zlib_key_%04d", count)
		expectedValue := bytes.Repeat([]byte("compress_me_"), 10)
		if string(iter.Key()) != expectedKey {
			t.Errorf("key mismatch at %d: got %q", count, iter.Key())
		}
		if !bytes.Equal(iter.Value(), expectedValue) {
			t.Errorf("value mismatch at %d", count)
		}
		iter.Next()
		count++
	}
	if count != 30 {
		t.Errorf("expected 30 entries, got %d", count)
	}
}

func TestTableCompressionNone(t *testing.T) {
	opts := DefaultBuilderOptions()
	opts.Compression = compression.NoCompression
	opts.BlockSize = 100

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	for i := range 20 {
		key := fmt.Sprintf("nocomp_key_%04d", i)
		value := []byte("uncompressed_value_data")
		builder.Add([]byte(key), value)
	}
	builder.Finish()

	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()
	iter.SeekToFirst()
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	if count != 20 {
		t.Errorf("expected 20 entries, got %d", count)
	}
}

func TestTableCompressionNextAndPrev(t *testing.T) {
	opts := DefaultBuilderOptions()
	opts.Compression = compression.SnappyCompression
	opts.BlockSize = 80

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	numEntries := 20
	for i := range numEntries {
		key := fmt.Sprintf("comp_key_%05d", i)
		value := bytes.Repeat([]byte("x"), 50)
		builder.Add([]byte(key), value)
	}
	builder.Finish()

	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Collect keys via forward iteration
	var forwardKeys []string
	iter.SeekToFirst()
	for iter.Valid() {
		forwardKeys = append(forwardKeys, string(iter.Key()))
		iter.Next()
	}
	if len(forwardKeys) != numEntries {
		t.Fatalf("Forward iteration: got %d entries, want %d", len(forwardKeys), numEntries)
	}

	// Collect keys via backward iteration
	var backwardKeys []string
	iter.SeekToLast()
	for iter.Valid() {
		backwardKeys = append(backwardKeys, string(iter.Key()))
		iter.Prev()
	}
	if len(backwardKeys) != numEntries {
		t.Fatalf("Backward iteration: got %d entries, want %d", len(backwardKeys), numEntries)
	}

	// Verify reverse order
	for i := range numEntries {
		if forwardKeys[i] != backwardKeys[numEntries-1-i] {
			t.Errorf("Mismatch at %d: forward=%q, backward=%q",
				i, forwardKeys[i], backwardKeys[numEntries-1-i])
		}
	}

	// Test bidirectional: go forward then backward
	iter.SeekToFirst()
	for range 5 {
		iter.Next()
	}
	if !iter.Valid() || string(iter.Key()) != forwardKeys[5] {
		t.Errorf("After 5 Next: expected %q, got %q", forwardKeys[5], iter.Key())
	}

	iter.Prev()
	if !iter.Valid() || string(iter.Key()) != forwardKeys[4] {
		t.Errorf("After Prev: expected %q, got %q", forwardKeys[4], iter.Key())
	}
}

func BenchmarkTableCompressionSnappy(b *testing.B) {
	opts := DefaultBuilderOptions()
	opts.Compression = compression.SnappyCompression

	for b.Loop() {
		buf := &bytes.Buffer{}
		builder := NewTableBuilder(buf, opts)

		for j := range 100 {
			key := fmt.Sprintf("bench_key_%05d", j)
			value := bytes.Repeat([]byte("v"), 100)
			builder.Add([]byte(key), value)
		}
		builder.Finish()
	}
}

func BenchmarkTableCompressionNone(b *testing.B) {
	opts := DefaultBuilderOptions()
	opts.Compression = compression.NoCompression

	for b.Loop() {
		buf := &bytes.Buffer{}
		builder := NewTableBuilder(buf, opts)

		for j := range 100 {
			key := fmt.Sprintf("bench_key_%05d", j)
			value := bytes.Repeat([]byte("v"), 100)
			builder.Add([]byte(key), value)
		}
		builder.Finish()
	}
}
