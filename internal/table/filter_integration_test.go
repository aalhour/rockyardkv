package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// makeInternalKey creates an internal key from user key, sequence number, and value type.
func makeInternalKey(userKey []byte, seq uint64, valueType byte) []byte {
	result := make([]byte, len(userKey)+8)
	copy(result, userKey)
	// Pack (seq << 8) | valueType in little-endian
	packed := (seq << 8) | uint64(valueType)
	binary.LittleEndian.PutUint64(result[len(userKey):], packed)
	return result
}

func TestTableBuilderWithFilter(t *testing.T) {
	// Create a builder with filter enabled
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 10 // ~1% FP rate

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	// Add some keys
	numKeys := 1000
	for i := range numKeys {
		key := makeInternalKey(fmt.Appendf(nil, "key%08d", i), uint64(i+1), 0x01)
		value := fmt.Appendf(nil, "value%08d", i)
		if err := builder.Add(key, value); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	t.Logf("SST with filter: %d bytes", buf.Len())

	// Open and verify filter is present
	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	if !reader.HasFilter() {
		t.Fatal("Expected filter to be present")
	}

	// Test filter: all added keys should match
	for i := range numKeys {
		userKey := fmt.Appendf(nil, "key%08d", i)
		if !reader.KeyMayMatch(userKey) {
			t.Errorf("KeyMayMatch(%q) = false, expected true", userKey)
		}
	}

	// Test filter: most non-existent keys should not match
	falsePositives := 0
	numTests := 10000
	for i := range numTests {
		userKey := fmt.Appendf(nil, "notkey%08d", i)
		if reader.KeyMayMatch(userKey) {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / float64(numTests)
	t.Logf("False positive rate: %.2f%% (%d/%d)", fpRate*100, falsePositives, numTests)

	// With 10 bits/key, we expect ~1% FP rate
	if fpRate > 0.02 {
		t.Errorf("FP rate %.4f is too high (expected < 2%%)", fpRate)
	}
}

func TestTableBuilderWithoutFilter(t *testing.T) {
	// Create a builder with filter disabled
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 0 // Disable filter

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	// Add some keys
	for i := range 100 {
		key := makeInternalKey(fmt.Appendf(nil, "key%08d", i), uint64(i+1), 0x01)
		value := fmt.Appendf(nil, "value%08d", i)
		if err := builder.Add(key, value); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	sizeWithoutFilter := buf.Len()
	t.Logf("SST without filter: %d bytes", sizeWithoutFilter)

	// Open and verify filter is not present
	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	if reader.HasFilter() {
		t.Fatal("Expected no filter")
	}

	// Without filter, KeyMayMatch should always return true
	if !reader.KeyMayMatch([]byte("anykey")) {
		t.Error("KeyMayMatch should return true when no filter")
	}
}

func TestTableFilterSizeReported(t *testing.T) {
	// Create a builder with filter enabled
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 10

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	// Add keys
	for i := range 1000 {
		key := makeInternalKey(fmt.Appendf(nil, "key%08d", i), uint64(i+1), 0x01)
		value := fmt.Appendf(nil, "value%08d", i)
		builder.Add(key, value)
	}
	builder.Finish()

	// Open and check properties
	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	props, err := reader.Properties()
	if err != nil {
		t.Fatalf("Properties failed: %v", err)
	}

	if props.FilterSize == 0 {
		t.Error("Expected FilterSize > 0")
	}
	t.Logf("Filter size: %d bytes", props.FilterSize)

	// 1000 keys * 10 bits = 10000 bits = ~1250 bytes (plus overhead)
	expectedMin := 1000
	if int(props.FilterSize) < expectedMin {
		t.Errorf("FilterSize %d is smaller than expected minimum %d", props.FilterSize, expectedMin)
	}
}

func TestTableFilterBlockContents(t *testing.T) {
	// Verify the filter block is properly written and read
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 10

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, k := range keys {
		key := makeInternalKey([]byte(k), uint64(i+1), 0x01)
		builder.Add(key, []byte("v"))
	}
	builder.Finish()

	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// All keys should match
	for _, k := range keys {
		if !reader.KeyMayMatch([]byte(k)) {
			t.Errorf("Key %q should match", k)
		}
	}

	// Keys not added should mostly not match
	notAdded := []string{"fig", "grape", "honeydew", "jackfruit", "kiwi"}
	matches := 0
	for _, k := range notAdded {
		if reader.KeyMayMatch([]byte(k)) {
			matches++
		}
	}
	t.Logf("Non-existent keys matching: %d/%d", matches, len(notAdded))
}

// MemFile is a simple in-memory file for testing.
type MemFile struct {
	data []byte
}

// NewMemFile creates a MemFile from data.
func NewMemFile(data []byte) *MemFile {
	return &MemFile{data: data}
}

func (m *MemFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, nil
	}
	n := copy(p, m.data[off:])
	return n, nil
}

func (m *MemFile) Close() error {
	return nil
}

func (m *MemFile) Size() int64 {
	return int64(len(m.data))
}

func BenchmarkTableFilterLookup(b *testing.B) {
	// Build a table with filter
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 10

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	for i := range 100000 {
		key := makeInternalKey(fmt.Appendf(nil, "key%08d", i), uint64(i+1), 0x01)
		builder.Add(key, []byte("value"))
	}
	builder.Finish()

	memFile := NewMemFile(buf.Bytes())
	reader, _ := Open(memFile, ReaderOptions{})
	defer reader.Close()

	lookupKey := []byte("lookupkey00000")

	for b.Loop() {
		reader.KeyMayMatch(lookupKey)
	}
}
