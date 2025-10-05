// Fuzz tests for the batch package.
//
// These tests generate random data to verify that the batch parser handles
// malformed inputs gracefully without panicking.
//
// Run with: go test -fuzz=Fuzz -fuzztime=30s ./internal/batch/...
package batch

import (
	"bytes"
	"testing"
)

// fuzzHandler implements Handler for fuzz testing
type fuzzHandler struct{}

func (h *fuzzHandler) Put(key, value []byte) error                              { return nil }
func (h *fuzzHandler) Delete(key []byte) error                                  { return nil }
func (h *fuzzHandler) SingleDelete(key []byte) error                            { return nil }
func (h *fuzzHandler) Merge(key, value []byte) error                            { return nil }
func (h *fuzzHandler) DeleteRange(startKey, endKey []byte) error                { return nil }
func (h *fuzzHandler) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error { return nil }
func (h *fuzzHandler) LogData(blob []byte)                                      {}
func (h *fuzzHandler) PutCF(cfID uint32, key, value []byte) error               { return nil }
func (h *fuzzHandler) DeleteCF(cfID uint32, key []byte) error                   { return nil }
func (h *fuzzHandler) MergeCF(cfID uint32, key, value []byte) error             { return nil }
func (h *fuzzHandler) SingleDeleteCF(cfID uint32, key []byte) error             { return nil }
func (h *fuzzHandler) PutEntityCF(cfID uint32, key, value []byte) error         { return nil }

// FuzzBatchParse tests the batch parser with random data.
// This verifies that malformed inputs don't cause panics.
func FuzzBatchParse(f *testing.F) {
	// Seed with interesting cases
	f.Add([]byte{})                               // Empty
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})         // Too small
	f.Add(bytes.Repeat([]byte{0xFF}, 100))        // All 0xFF
	f.Add(bytes.Repeat([]byte{0x00}, 100))        // All zeros
	f.Add(make([]byte, HeaderSize))               // Just header
	f.Add(append(make([]byte, HeaderSize), 0xFF)) // Unknown tag

	// Add a valid batch as seed
	wb := New()
	wb.Put([]byte("key"), []byte("value"))
	f.Add(wb.Data())

	f.Fuzz(func(t *testing.T, data []byte) {
		// Try to parse - should not panic
		wb, err := NewFromData(data)
		if err != nil {
			return // Expected for most random data
		}

		// Try to iterate - should not panic
		handler := &fuzzHandler{}
		_ = wb.Iterate(handler)
		// Error is OK, panic is not

		// Try accessing properties - should not panic
		_ = wb.Count()
		_ = wb.Sequence()
		_ = wb.Size()
	})
}

// FuzzBatchRoundtrip tests that write operations produce parseable batches.
func FuzzBatchRoundtrip(f *testing.F) {
	f.Add([]byte("key"), []byte("value"))
	f.Add([]byte{}, []byte("value"))
	f.Add([]byte("key"), []byte{})
	f.Add([]byte{}, []byte{})
	f.Add([]byte{0, 0, 0}, []byte{0})
	f.Add(bytes.Repeat([]byte{'a'}, 1000), bytes.Repeat([]byte{'b'}, 1000))

	f.Fuzz(func(t *testing.T, key, value []byte) {
		wb := New()
		wb.Put(key, value)

		// Roundtrip
		data := wb.Data()
		restored, err := NewFromData(data)
		if err != nil {
			t.Fatalf("NewFromData failed: %v", err)
		}

		if restored.Count() != 1 {
			t.Errorf("count mismatch: got %d, want 1", restored.Count())
		}

		// Verify iteration works
		var gotKey, gotValue []byte
		handler := &collectHandler{}
		if err := restored.Iterate(handler); err != nil {
			t.Fatalf("Iterate failed: %v", err)
		}

		if len(handler.puts) != 1 {
			t.Fatalf("expected 1 put, got %d", len(handler.puts))
		}

		gotKey = handler.puts[0].key
		gotValue = handler.puts[0].value

		if !bytes.Equal(gotKey, key) {
			t.Errorf("key mismatch: got %v, want %v", gotKey, key)
		}
		if !bytes.Equal(gotValue, value) {
			t.Errorf("value mismatch: got %v, want %v", gotValue, value)
		}
	})
}

// FuzzBatchManyOperations tests batches with multiple random operations.
func FuzzBatchManyOperations(f *testing.F) {
	f.Add(uint8(5), []byte("seed"))

	f.Fuzz(func(t *testing.T, numOps uint8, seed []byte) {
		if numOps == 0 || numOps > 100 {
			numOps = 10
		}

		wb := New()

		for i := range numOps {
			key := append(append([]byte{}, seed...), byte(i))
			value := append([]byte("value"), byte(i))

			// Alternate between operations
			switch i % 3 {
			case 0:
				wb.Put(key, value)
			case 1:
				wb.Delete(key)
			case 2:
				wb.Merge(key, value)
			}
		}

		// Verify roundtrip
		data := wb.Data()
		restored, err := NewFromData(data)
		if err != nil {
			t.Fatalf("NewFromData failed: %v", err)
		}

		if restored.Count() != uint32(numOps) {
			t.Errorf("count mismatch: got %d, want %d", restored.Count(), numOps)
		}

		// Verify iteration works
		handler := &fuzzHandler{}
		if err := restored.Iterate(handler); err != nil {
			t.Errorf("Iterate failed: %v", err)
		}
	})
}

// collectHandler collects operations for verification
type collectHandler struct {
	puts []struct{ key, value []byte }
}

func (h *collectHandler) Put(key, value []byte) error {
	h.puts = append(h.puts, struct{ key, value []byte }{
		key:   append([]byte{}, key...),
		value: append([]byte{}, value...),
	})
	return nil
}
func (h *collectHandler) Delete(key []byte) error                                  { return nil }
func (h *collectHandler) SingleDelete(key []byte) error                            { return nil }
func (h *collectHandler) Merge(key, value []byte) error                            { return nil }
func (h *collectHandler) DeleteRange(startKey, endKey []byte) error                { return nil }
func (h *collectHandler) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error { return nil }
func (h *collectHandler) LogData(blob []byte)                                      {}
func (h *collectHandler) PutCF(cfID uint32, key, value []byte) error               { return nil }
func (h *collectHandler) DeleteCF(cfID uint32, key []byte) error                   { return nil }
func (h *collectHandler) MergeCF(cfID uint32, key, value []byte) error             { return nil }
func (h *collectHandler) SingleDeleteCF(cfID uint32, key []byte) error             { return nil }
func (h *collectHandler) PutEntityCF(cfID uint32, key, value []byte) error         { return nil }
