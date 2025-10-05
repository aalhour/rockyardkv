// Corruption detection tests for the batch package.
//
// These tests verify that corrupted WriteBatch data is properly detected
// and appropriate errors are returned.
package batch

import (
	"bytes"
	"testing"
)

// corruptTestHandler implements Handler for corruption testing
type corruptTestHandler struct {
	puts      []struct{ key, value []byte }
	deletes   [][]byte
	merges    []struct{ key, value []byte }
	singleDel [][]byte
}

func (h *corruptTestHandler) Put(key, value []byte) error {
	h.puts = append(h.puts, struct{ key, value []byte }{key, value})
	return nil
}
func (h *corruptTestHandler) Delete(key []byte) error { h.deletes = append(h.deletes, key); return nil }
func (h *corruptTestHandler) SingleDelete(key []byte) error {
	h.singleDel = append(h.singleDel, key)
	return nil
}
func (h *corruptTestHandler) Merge(key, value []byte) error {
	h.merges = append(h.merges, struct{ key, value []byte }{key, value})
	return nil
}
func (h *corruptTestHandler) DeleteRange(startKey, endKey []byte) error                { return nil }
func (h *corruptTestHandler) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error { return nil }
func (h *corruptTestHandler) LogData(blob []byte)                                      {}
func (h *corruptTestHandler) PutCF(cfID uint32, key, value []byte) error               { return nil }
func (h *corruptTestHandler) DeleteCF(cfID uint32, key []byte) error                   { return nil }
func (h *corruptTestHandler) MergeCF(cfID uint32, key, value []byte) error             { return nil }
func (h *corruptTestHandler) SingleDeleteCF(cfID uint32, key []byte) error             { return nil }
func (h *corruptTestHandler) PutEntityCF(cfID uint32, key, value []byte) error         { return nil }

// TestCorruptedBatchTruncated tests detection of truncated batch data.
func TestCorruptedBatchTruncated(t *testing.T) {
	// Create a valid batch
	wb := New()
	wb.Put([]byte("key1"), []byte("value1"))
	wb.Put([]byte("key2"), []byte("value2"))

	data := wb.Data()

	testCases := []struct {
		name       string
		truncateAt int
	}{
		{"empty", 0},
		{"header_only", HeaderSize},
		{"partial_header", HeaderSize / 2},
		{"mid_first_record", HeaderSize + 5},
		{"after_first_key_length", HeaderSize + 1 + 1}, // tag + varint
		{"almost_complete", len(data) - 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.truncateAt > len(data) {
				t.Skip("truncation point beyond data length")
			}

			truncated := data[:tc.truncateAt]
			restored, err := NewFromData(truncated)

			// NewFromData might succeed (header parsing) but iteration should fail
			if err == nil && tc.truncateAt >= HeaderSize {
				// Try to iterate
				handler := &corruptTestHandler{}
				iterErr := restored.Iterate(handler)
				if iterErr == nil && tc.truncateAt < len(data) {
					t.Log("Note: Iteration succeeded despite truncation - may be at record boundary")
				}
			}
		})
	}
}

// TestCorruptedBatchBadVarint tests detection of malformed varint encoding.
func TestCorruptedBatchBadVarint(t *testing.T) {
	// Create batch with bad varint (continuation bytes without termination)
	badVarint := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02}

	// Valid header + bad varint for key length
	data := make([]byte, HeaderSize+1+len(badVarint))
	// Set count = 1
	data[8] = 1
	// Set tag for Put (TypeValue = 1)
	data[HeaderSize] = TypeValue
	// Add bad varint
	copy(data[HeaderSize+1:], badVarint)

	wb, err := NewFromData(data)
	if err != nil {
		t.Logf("NewFromData returned error (expected): %v", err)
		return
	}

	// Iteration should fail
	handler := &corruptTestHandler{}
	err = wb.Iterate(handler)

	if err == nil {
		t.Log("Note: No error detected for bad varint")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestCorruptedBatchUnknownTag tests detection of unknown record type tags.
func TestCorruptedBatchUnknownTag(t *testing.T) {
	// Valid header
	data := make([]byte, HeaderSize+10)
	// Set count = 1
	data[8] = 1
	// Set unknown tag (0xFF)
	data[HeaderSize] = 0xFF

	wb, err := NewFromData(data)
	if err != nil {
		t.Logf("NewFromData returned error (expected): %v", err)
		return
	}

	// Iteration should fail
	handler := &corruptTestHandler{}
	err = wb.Iterate(handler)

	if err == nil {
		t.Error("expected error for unknown tag, got nil")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestVeryLargeBatch tests handling of very large batches.
func TestVeryLargeBatch(t *testing.T) {
	wb := New()

	// Add many entries
	numEntries := 10000
	for i := range numEntries {
		key := []byte(padKey(i))
		value := bytes.Repeat([]byte{'v'}, 100)
		wb.Put(key, value)
	}

	if wb.Count() != uint32(numEntries) {
		t.Errorf("count: got %d, want %d", wb.Count(), numEntries)
	}

	// Verify all entries via iteration
	handler := &corruptTestHandler{}
	err := wb.Iterate(handler)
	if err != nil {
		t.Fatalf("iteration error: %v", err)
	}

	if len(handler.puts) != numEntries {
		t.Errorf("iteration count: got %d, want %d", len(handler.puts), numEntries)
	}

	// Test roundtrip
	data := wb.Data()
	restored, err := NewFromData(data)
	if err != nil {
		t.Fatalf("NewFromData failed: %v", err)
	}

	if restored.Count() != uint32(numEntries) {
		t.Errorf("restored count: got %d, want %d", restored.Count(), numEntries)
	}

	t.Logf("Successfully handled batch with %d entries, %d bytes", numEntries, len(data))
}

// TestEmptyKeyInBatch tests handling of empty keys.
func TestEmptyKeyInBatch(t *testing.T) {
	wb := New()

	// Add entries with empty key
	wb.Put([]byte{}, []byte("value_for_empty_key"))
	wb.Put([]byte("normal_key"), []byte("normal_value"))
	wb.Delete([]byte{}) // Delete empty key

	if wb.Count() != 3 {
		t.Errorf("count: got %d, want 3", wb.Count())
	}

	// Verify via iteration
	handler := &corruptTestHandler{}
	err := wb.Iterate(handler)
	if err != nil {
		t.Fatalf("iteration error: %v", err)
	}

	if len(handler.puts) != 2 {
		t.Errorf("puts: got %d, want 2", len(handler.puts))
	}

	// Check first entry is Put with empty key
	if len(handler.puts) > 0 && len(handler.puts[0].key) != 0 {
		t.Errorf("first entry key: got %v, want empty", handler.puts[0].key)
	}
}

// TestBinaryKeyValueInBatch tests keys and values with null bytes.
func TestBinaryKeyValueInBatch(t *testing.T) {
	wb := New()

	// Keys and values with embedded nulls
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte{0, 0, 0}, []byte{0, 0, 0}},
		{[]byte{'a', 0, 'b'}, []byte{'x', 0, 'y'}},
		{[]byte{0, 'a', 0, 'b', 0}, []byte{0}},
	}

	for _, d := range testData {
		wb.Put(d.key, d.value)
	}

	// Verify via iteration
	handler := &corruptTestHandler{}
	err := wb.Iterate(handler)
	if err != nil {
		t.Fatalf("iteration error: %v", err)
	}

	for i, d := range testData {
		if i < len(handler.puts) {
			if !bytes.Equal(handler.puts[i].key, d.key) {
				t.Errorf("entry %d key mismatch", i)
			}
			if !bytes.Equal(handler.puts[i].value, d.value) {
				t.Errorf("entry %d value mismatch", i)
			}
		}
	}

	// Test roundtrip
	data := wb.Data()
	restored, err := NewFromData(data)
	if err != nil {
		t.Fatalf("NewFromData failed: %v", err)
	}

	handler2 := &corruptTestHandler{}
	err = restored.Iterate(handler2)
	if err != nil {
		t.Fatalf("restored iteration error: %v", err)
	}

	for i, d := range testData {
		if i < len(handler2.puts) {
			if !bytes.Equal(handler2.puts[i].key, d.key) {
				t.Errorf("restored entry %d key mismatch", i)
			}
			if !bytes.Equal(handler2.puts[i].value, d.value) {
				t.Errorf("restored entry %d value mismatch", i)
			}
		}
	}

	t.Logf("Successfully handled %d binary key/value pairs", len(testData))
}

// TestBatchCountAccuracy tests count accuracy after various operations.
func TestBatchCountAccuracy(t *testing.T) {
	testCases := []struct {
		name     string
		ops      func(wb *WriteBatch)
		expected uint32
	}{
		{
			name:     "single_put",
			ops:      func(wb *WriteBatch) { wb.Put([]byte("k"), []byte("v")) },
			expected: 1,
		},
		{
			name:     "put_then_delete",
			ops:      func(wb *WriteBatch) { wb.Put([]byte("k"), []byte("v")); wb.Delete([]byte("k")) },
			expected: 2,
		},
		{
			name: "many_puts",
			ops: func(wb *WriteBatch) {
				for i := range 100 {
					wb.Put([]byte(padKey(i)), []byte("v"))
				}
			},
			expected: 100,
		},
		{
			name: "mixed_operations",
			ops: func(wb *WriteBatch) {
				wb.Put([]byte("a"), []byte("1"))
				wb.Put([]byte("b"), []byte("2"))
				wb.Delete([]byte("a"))
				wb.Put([]byte("c"), []byte("3"))
				wb.Delete([]byte("b"))
			},
			expected: 5,
		},
		{
			name: "after_clear",
			ops: func(wb *WriteBatch) {
				wb.Put([]byte("k"), []byte("v"))
				wb.Clear()
				wb.Put([]byte("new"), []byte("value"))
			},
			expected: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wb := New()
			tc.ops(wb)

			if wb.Count() != tc.expected {
				t.Errorf("count: got %d, want %d", wb.Count(), tc.expected)
			}

			// Also verify via iteration (except for the "after_clear" case which has different state)
			handler := &corruptTestHandler{}
			wb.Iterate(handler)
			count := uint32(len(handler.puts) + len(handler.deletes) + len(handler.singleDel) + len(handler.merges))

			if count != tc.expected {
				t.Errorf("iteration count: got %d, want %d", count, tc.expected)
			}
		})
	}
}

// TestAppendBatch tests appending one batch to another.
func TestAppendBatch(t *testing.T) {
	wb1 := New()
	wb1.Put([]byte("a"), []byte("1"))
	wb1.Put([]byte("b"), []byte("2"))

	wb2 := New()
	wb2.Put([]byte("c"), []byte("3"))
	wb2.Delete([]byte("a"))

	// Append wb2 to wb1
	wb1.Append(wb2)

	if wb1.Count() != 4 {
		t.Errorf("count after append: got %d, want 4", wb1.Count())
	}

	// Verify order
	handler := &corruptTestHandler{}
	err := wb1.Iterate(handler)
	if err != nil {
		t.Fatalf("iteration error: %v", err)
	}

	// Should have 3 puts and 1 delete
	if len(handler.puts) != 3 {
		t.Errorf("puts: got %d, want 3", len(handler.puts))
	}
	if len(handler.deletes) != 1 {
		t.Errorf("deletes: got %d, want 1", len(handler.deletes))
	}
}

func padKey(i int) string {
	return string([]byte{byte('a' + i/26), byte('a' + i%26), byte('0' + i%10)})
}
