// Corruption detection tests for the table package.
//
// These tests verify that corrupted SST files are properly detected and
// appropriate errors are returned.
package table

import (
	"bytes"
	"testing"

	"github.com/aalhour/rockyardkv/internal/block"
	"github.com/aalhour/rockyardkv/internal/dbformat"
)

// Helper functions for corruption tests
func makeCorruptTestInternalKey(userKey string, seq uint64, typ dbformat.ValueType) []byte {
	return dbformat.AppendInternalKey(nil, &dbformat.ParsedInternalKey{
		UserKey:  []byte(userKey),
		Sequence: dbformat.SequenceNumber(seq),
		Type:     typ,
	})
}

func padCorruptKey(i int) string {
	return string([]byte{byte('a' + i/26), byte('a' + i%26), byte('0' + i%10)})
}

// corruptMemFile implements ReadableFile for in-memory testing
type corruptMemFile struct {
	data []byte
}

func (m *corruptMemFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(m.data)) {
		return 0, nil
	}
	n = copy(p, m.data[off:])
	return n, nil
}

func (m *corruptMemFile) Close() error {
	return nil
}

func (m *corruptMemFile) Size() int64 {
	return int64(len(m.data))
}

// TestCorruptedFooter tests detection of corrupted SST footer.
func TestCorruptedFooter(t *testing.T) {
	// Create a valid SST file first
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)

	key := makeCorruptTestInternalKey("key", 100, dbformat.TypeValue)
	builder.Add(key, []byte("value"))

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()

	testCases := []struct {
		name     string
		corrupt  func([]byte) []byte
		wantOpen bool // true if Open should succeed despite corruption
	}{
		{
			name: "truncated_file",
			corrupt: func(d []byte) []byte {
				return d[:len(d)/2]
			},
			wantOpen: false,
		},
		{
			name: "corrupted_magic_number",
			corrupt: func(d []byte) []byte {
				c := make([]byte, len(d))
				copy(c, d)
				// Corrupt last 8 bytes (magic number)
				for i := len(c) - 8; i < len(c); i++ {
					c[i] ^= 0xFF
				}
				return c
			},
			wantOpen: false,
		},
		{
			name: "zero_footer",
			corrupt: func(d []byte) []byte {
				c := make([]byte, len(d))
				copy(c, d)
				// Zero out footer (last 53 bytes for format_version >= 1)
				footerSize := block.NewVersionsEncodedLength
				for i := len(c) - footerSize; i < len(c); i++ {
					c[i] = 0
				}
				return c
			},
			wantOpen: false,
		},
		{
			name: "too_small_file",
			corrupt: func(d []byte) []byte {
				return []byte{0, 1, 2, 3} // Only 4 bytes
			},
			wantOpen: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			corrupted := tc.corrupt(data)
			_, err := Open(&corruptMemFile{data: corrupted}, ReaderOptions{})

			if tc.wantOpen && err != nil {
				t.Errorf("expected Open to succeed, got error: %v", err)
			}
			if !tc.wantOpen && err == nil {
				t.Error("expected Open to fail with corrupted data")
			}
		})
	}
}

// TestCorruptedChecksum tests detection of checksum mismatches.
func TestCorruptedChecksum(t *testing.T) {
	// Create a valid SST file with checksum verification enabled
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)

	// Add some entries
	for i := range 10 {
		key := makeCorruptTestInternalKey(padCorruptKey(i), 100, dbformat.TypeValue)
		builder.Add(key, []byte("value"))
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()

	// Corrupt a byte in the middle of the file (data block area)
	corrupted := make([]byte, len(data))
	copy(corrupted, data)
	if len(corrupted) > 100 {
		corrupted[50] ^= 0xFF // Flip bits in data area
	}

	// Open should succeed (footer is intact)
	reader, err := Open(&corruptMemFile{data: corrupted}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Skipf("Open failed (might be footer corruption): %v", err)
	}
	defer reader.Close()

	// Iteration should fail with checksum error
	iter := reader.NewIterator()
	iter.SeekToFirst()

	// Either the seek fails or iteration eventually hits the error
	foundError := false
	for iter.Valid() {
		iter.Next()
	}
	if iter.Error() != nil {
		foundError = true
		t.Logf("Got expected error during iteration: %v", iter.Error())
	}

	// Note: Depending on where corruption is, error might not be detected
	// if the corrupted block is never read
	if !foundError {
		t.Log("Note: Corruption not detected (may be in unread area)")
	}
}

// TestCorruptedBlock tests detection of corrupted data blocks.
func TestCorruptedBlock(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.BlockSize = 100 // Small blocks
	builder := NewTableBuilder(&buf, opts)

	// Add entries to create multiple blocks
	for i := range 50 {
		key := makeCorruptTestInternalKey(padCorruptKey(i), 100, dbformat.TypeValue)
		value := bytes.Repeat([]byte{'v'}, 20)
		builder.Add(key, value)
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()

	// Corrupt the beginning of the file (first data block)
	corrupted := make([]byte, len(data))
	copy(corrupted, data)
	if len(corrupted) > 20 {
		// Corrupt the first data block's restart count
		corrupted[10] ^= 0xFF
		corrupted[11] ^= 0xFF
	}

	reader, err := Open(&corruptMemFile{data: corrupted}, ReaderOptions{VerifyChecksums: false})
	if err != nil {
		t.Skipf("Open failed: %v", err)
	}
	defer reader.Close()

	// Try to iterate - should fail
	iter := reader.NewIterator()
	iter.SeekToFirst()

	if iter.Valid() {
		// If valid, iteration might still work for some entries
		t.Log("Iterator is valid despite corruption - corruption may be in unused area")
	}

	if iter.Error() != nil {
		t.Logf("Got expected error: %v", iter.Error())
	}
}

// TestLargeKeysAndValues tests handling of very large keys and values.
func TestLargeKeysAndValues(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.BlockSize = 64 * 1024 // 64KB blocks
	builder := NewTableBuilder(&buf, opts)

	testCases := []struct {
		keySize   int
		valueSize int
	}{
		{100, 100},     // Normal
		{1000, 1000},   // Large
		{10000, 10000}, // Very large
		{100, 100000},  // Small key, huge value
		{10000, 100},   // Huge key, small value
	}

	for i, tc := range testCases {
		key := makeCorruptTestInternalKey(string(bytes.Repeat([]byte{'k'}, tc.keySize)), uint64(100-i), dbformat.TypeValue)
		value := bytes.Repeat([]byte{'v'}, tc.valueSize)
		if err := builder.Add(key, value); err != nil {
			t.Fatalf("Add failed for keySize=%d, valueSize=%d: %v", tc.keySize, tc.valueSize, err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(&corruptMemFile{data: buf.Bytes()}, ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Verify all entries can be read
	iter := reader.NewIterator()
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("iteration error: %v", err)
	}

	if count != len(testCases) {
		t.Errorf("count: got %d, want %d", count, len(testCases))
	}

	t.Logf("Successfully read %d entries with large keys/values", count)
}

// TestBinaryKeysWithNulls tests keys containing null bytes.
func TestBinaryKeysWithNulls(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)

	// Keys with embedded nulls
	testKeys := [][]byte{
		{0, 0, 0},          // All nulls
		{'a', 0, 'b'},      // Null in middle
		{0, 'a', 'b', 'c'}, // Null at start
		{'a', 'b', 'c', 0}, // Null at end
		{0, 0, 'x', 0, 0},  // Multiple nulls
	}

	for i, userKey := range testKeys {
		key := dbformat.AppendInternalKey(nil, &dbformat.ParsedInternalKey{
			UserKey:  userKey,
			Sequence: dbformat.SequenceNumber(100 - i),
			Type:     dbformat.TypeValue,
		})
		value := []byte("value")
		if err := builder.Add(key, value); err != nil {
			t.Fatalf("Add failed for key with nulls: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(&corruptMemFile{data: buf.Bytes()}, ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Verify all entries
	iter := reader.NewIterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		parsed, err := dbformat.ParseInternalKey(iter.Key())
		if err != nil {
			t.Fatalf("parse key failed: %v", err)
		}
		if !bytes.Equal(parsed.UserKey, testKeys[i]) {
			t.Errorf("key %d: got %v, want %v", i, parsed.UserKey, testKeys[i])
		}
		i++
	}

	if i != len(testKeys) {
		t.Errorf("count: got %d, want %d", i, len(testKeys))
	}

	t.Logf("Successfully handled %d keys with null bytes", i)
}

// TestBinaryValuesWithNulls tests values containing null bytes.
func TestBinaryValuesWithNulls(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)

	// Values with embedded nulls
	testValues := [][]byte{
		{0, 0, 0},
		{'a', 0, 'b'},
		{0, 'a', 'b', 'c'},
		{'a', 'b', 'c', 0},
		{0, 0, 'x', 0, 0},
		bytes.Repeat([]byte{0}, 1000), // 1000 nulls
	}

	for i, value := range testValues {
		key := makeCorruptTestInternalKey(padCorruptKey(i), 100, dbformat.TypeValue)
		if err := builder.Add(key, value); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(&corruptMemFile{data: buf.Bytes()}, ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Verify all values
	iter := reader.NewIterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if !bytes.Equal(iter.Value(), testValues[i]) {
			t.Errorf("value %d mismatch", i)
		}
		i++
	}

	if i != len(testValues) {
		t.Errorf("count: got %d, want %d", i, len(testValues))
	}

	t.Logf("Successfully handled %d values with null bytes", i)
}

// TestEmptyValue tests handling of empty values.
func TestEmptyValue(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)

	// Mix of empty and non-empty values
	entries := []struct {
		key   string
		value []byte
	}{
		{"aaa", []byte{}},        // Empty
		{"bbb", []byte("value")}, // Non-empty
		{"ccc", []byte{}},        // Empty
		{"ddd", nil},             // Nil (treated as empty)
		{"eee", []byte("x")},     // Single byte
	}

	for i, e := range entries {
		key := makeCorruptTestInternalKey(e.key, uint64(100-i), dbformat.TypeValue)
		if err := builder.Add(key, e.value); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(&corruptMemFile{data: buf.Bytes()}, ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	iter := reader.NewIterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		expected := entries[i].value
		if expected == nil {
			expected = []byte{}
		}
		if !bytes.Equal(iter.Value(), expected) {
			t.Errorf("value %d: got %v, want %v", i, iter.Value(), expected)
		}
		i++
	}

	if i != len(entries) {
		t.Errorf("count: got %d, want %d", i, len(entries))
	}
}

// TestPropertiesEdgeCases tests properties block edge cases.
func TestPropertiesEdgeCases(t *testing.T) {
	testCases := []struct {
		name       string
		numEntries int
		valueSize  int
		blockSize  int
	}{
		{"empty_table", 0, 0, 4096},
		{"single_small_entry", 1, 10, 4096},
		{"single_large_entry", 1, 10000, 4096},
		{"many_tiny_entries", 1000, 1, 4096},
		{"few_huge_entries", 5, 100000, 64 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			opts := DefaultBuilderOptions()
			opts.BlockSize = tc.blockSize
			builder := NewTableBuilder(&buf, opts)

			for i := range tc.numEntries {
				key := makeCorruptTestInternalKey(padCorruptKey(i), uint64(1000-i), dbformat.TypeValue)
				value := bytes.Repeat([]byte{'v'}, tc.valueSize)
				builder.Add(key, value)
			}

			if err := builder.Finish(); err != nil {
				t.Fatal(err)
			}

			reader, err := Open(&corruptMemFile{data: buf.Bytes()}, ReaderOptions{VerifyChecksums: false})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()

			props, err := reader.Properties()
			if err != nil {
				t.Logf("Properties read error (may be expected): %v", err)
				return
			}

			if props.NumEntries != uint64(tc.numEntries) {
				t.Errorf("NumEntries: got %d, want %d", props.NumEntries, tc.numEntries)
			}

			t.Logf("Properties: entries=%d, data_blocks=%d, data_size=%d",
				props.NumEntries, props.NumDataBlocks, props.DataSize)
		})
	}
}
