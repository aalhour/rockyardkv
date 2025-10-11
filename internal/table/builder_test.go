package table

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/aalhour/rockyardkv/internal/block"
	"github.com/aalhour/rockyardkv/internal/checksum"
)

func TestTableBuilderEmpty(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	tb := NewTableBuilder(&buf, opts)

	if err := tb.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	if tb.NumEntries() != 0 {
		t.Errorf("NumEntries() = %d, want 0", tb.NumEntries())
	}

	// Should still have valid footer
	if tb.FileSize() < uint64(block.NewVersionsEncodedLength) {
		t.Errorf("FileSize() = %d, too small for footer", tb.FileSize())
	}
}

func TestTableBuilderSingleEntry(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	tb := NewTableBuilder(&buf, opts)

	if err := tb.Add([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	if err := tb.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	if tb.NumEntries() != 1 {
		t.Errorf("NumEntries() = %d, want 1", tb.NumEntries())
	}

	if tb.FileSize() == 0 {
		t.Error("FileSize() = 0, want > 0")
	}
}

func TestTableBuilderMultipleEntries(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.BlockSize = 100 // Small block size to trigger multiple blocks
	tb := NewTableBuilder(&buf, opts)

	entries := []struct {
		key   string
		value string
	}{
		{"aaa", "value1"},
		{"bbb", "value2"},
		{"ccc", "value3"},
		{"ddd", "value4"},
		{"eee", "value5"},
	}

	for _, e := range entries {
		if err := tb.Add([]byte(e.key), []byte(e.value)); err != nil {
			t.Fatalf("Add(%s) error = %v", e.key, err)
		}
	}

	if err := tb.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	if tb.NumEntries() != uint64(len(entries)) {
		t.Errorf("NumEntries() = %d, want %d", tb.NumEntries(), len(entries))
	}
}

func TestTableBuilderLargeValues(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	tb := NewTableBuilder(&buf, opts)

	// Add entries with large values
	for i := range 10 {
		key := []byte{byte('a' + i)}
		value := make([]byte, 1000)
		for j := range value {
			value[j] = byte(i)
		}
		if err := tb.Add(key, value); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	if err := tb.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	if tb.NumEntries() != 10 {
		t.Errorf("NumEntries() = %d, want 10", tb.NumEntries())
	}
}

func TestTableBuilderDefaultOptions(t *testing.T) {
	opts := DefaultBuilderOptions()

	if opts.BlockSize != 4096 {
		t.Errorf("BlockSize = %d, want 4096", opts.BlockSize)
	}
	if opts.BlockRestartInterval != 16 {
		t.Errorf("BlockRestartInterval = %d, want 16", opts.BlockRestartInterval)
	}
	if opts.FormatVersion != 3 {
		t.Errorf("FormatVersion = %d, want 3", opts.FormatVersion)
	}
	if opts.ChecksumType != checksum.TypeCRC32C {
		t.Errorf("ChecksumType = %d, want %d (CRC32C)", opts.ChecksumType, checksum.TypeCRC32C)
	}
}

func TestTableBuilderCustomOptions(t *testing.T) {
	var buf bytes.Buffer
	opts := BuilderOptions{
		BlockSize:            1024,
		BlockRestartInterval: 8,
		FormatVersion:        5,
		ChecksumType:         checksum.TypeCRC32C,
		ComparatorName:       "test.comparator",
		ColumnFamilyID:       42,
		ColumnFamilyName:     "test_cf",
	}
	tb := NewTableBuilder(&buf, opts)

	if err := tb.Add([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	if err := tb.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	// Verify the file was created
	if tb.FileSize() == 0 {
		t.Error("FileSize() = 0, want > 0")
	}
}

func TestTableBuilderAbandon(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	tb := NewTableBuilder(&buf, opts)

	if err := tb.Add([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	tb.Abandon()

	// After abandon, should not be able to add or finish
	if err := tb.Add([]byte("key2"), []byte("value2")); err == nil {
		t.Error("Add() after Abandon should fail")
	}

	if err := tb.Finish(); err == nil {
		t.Error("Finish() after Abandon should fail")
	}
}

func TestTableBuilderDoubleFinish(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	tb := NewTableBuilder(&buf, opts)

	if err := tb.Finish(); err != nil {
		t.Fatalf("First Finish() error = %v", err)
	}

	if err := tb.Finish(); err == nil {
		t.Error("Second Finish() should fail")
	}
}

func TestTableBuilderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	tb := NewTableBuilder(&buf, opts)

	entries := []struct {
		key   string
		value string
	}{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "red"},
		{"date", "brown"},
		{"elderberry", "purple"},
	}

	for _, e := range entries {
		if err := tb.Add([]byte(e.key), []byte(e.value)); err != nil {
			t.Fatalf("Add(%s) error = %v", e.key, err)
		}
	}

	if err := tb.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	// Now try to read it back using the Reader
	data := buf.Bytes()
	file := &memFile{data: data}

	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer reader.Close()

	// Verify footer
	footer := reader.Footer()
	if footer.FormatVersion != opts.FormatVersion {
		t.Errorf("FormatVersion = %d, want %d", footer.FormatVersion, opts.FormatVersion)
	}
	if footer.TableMagicNumber != block.BlockBasedTableMagicNumber {
		t.Errorf("Magic = 0x%x, want 0x%x", footer.TableMagicNumber, block.BlockBasedTableMagicNumber)
	}

	// Iterate and verify all entries
	iter := reader.NewIterator()
	iter.SeekToFirst()

	i := 0
	for iter.Valid() {
		if i >= len(entries) {
			t.Errorf("Too many entries: got more than %d", len(entries))
			break
		}

		key := string(iter.Key())
		value := string(iter.Value())

		if key != entries[i].key {
			t.Errorf("Entry %d: key = %q, want %q", i, key, entries[i].key)
		}
		if value != entries[i].value {
			t.Errorf("Entry %d: value = %q, want %q", i, value, entries[i].value)
		}

		iter.Next()
		i++
	}

	if i != len(entries) {
		t.Errorf("Got %d entries, want %d", i, len(entries))
	}

	if err := iter.Error(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}
}

func TestTableBuilderManyEntries(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.BlockSize = 256 // Small block size to create many blocks
	tb := NewTableBuilder(&buf, opts)

	// Add 1000 entries
	numEntries := 1000
	for i := range numEntries {
		key := fmt.Appendf(nil, "key%05d", i)
		value := fmt.Appendf(nil, "value%05d", i)
		if err := tb.Add(key, value); err != nil {
			t.Fatalf("Add(%s) error = %v", key, err)
		}
	}

	if err := tb.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	if tb.NumEntries() != uint64(numEntries) {
		t.Errorf("NumEntries() = %d, want %d", tb.NumEntries(), numEntries)
	}

	// Read it back
	data := buf.Bytes()
	file := &memFile{data: data}

	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer reader.Close()

	// Count entries via iteration
	iter := reader.NewIterator()
	iter.SeekToFirst()
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}

	if count != numEntries {
		t.Errorf("Read %d entries, want %d", count, numEntries)
	}
}

func TestTableBuilderEmptyKey(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	tb := NewTableBuilder(&buf, opts)

	// Empty key should work
	if err := tb.Add([]byte{}, []byte("value")); err != nil {
		t.Fatalf("Add(empty key) error = %v", err)
	}

	if err := tb.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	if tb.NumEntries() != 1 {
		t.Errorf("NumEntries() = %d, want 1", tb.NumEntries())
	}
}

func TestTableBuilderEmptyValue(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	tb := NewTableBuilder(&buf, opts)

	if err := tb.Add([]byte("key"), []byte{}); err != nil {
		t.Fatalf("Add(empty value) error = %v", err)
	}

	if err := tb.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	// Read back and verify empty value
	data := buf.Bytes()
	file := &memFile{data: data}

	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("Iterator not valid")
	}
	if len(iter.Value()) != 0 {
		t.Errorf("Value length = %d, want 0", len(iter.Value()))
	}
}

func TestTableBuilderBinaryData(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	tb := NewTableBuilder(&buf, opts)

	// Key and value with null bytes and other binary data
	key := []byte{0x00, 0x01, 0x02, 0xff, 0xfe}
	value := []byte{0xff, 0x00, 0xaa, 0x55, 0x00}

	if err := tb.Add(key, value); err != nil {
		t.Fatalf("Add(binary) error = %v", err)
	}

	if err := tb.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	// Read back and verify
	data := buf.Bytes()
	file := &memFile{data: data}

	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("Iterator not valid")
	}
	if !bytes.Equal(iter.Key(), key) {
		t.Errorf("Key = %v, want %v", iter.Key(), key)
	}
	if !bytes.Equal(iter.Value(), value) {
		t.Errorf("Value = %v, want %v", iter.Value(), value)
	}
}

// memFile implements ReadableFile for testing
type memFile struct {
	data   []byte
	closed bool
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, nil
	}
	n := copy(p, f.data[off:])
	return n, nil
}

func (f *memFile) Close() error {
	f.closed = true
	return nil
}

func (f *memFile) Size() int64 {
	return int64(len(f.data))
}
