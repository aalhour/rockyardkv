// Test to demonstrate sequence number mismatch between Count() and actual applied operations
package batch

import (
	"testing"
)

// TestSequenceConsumption verifies that Count() matches the number of sequence-consuming operations.
// This is critical for correctness: db.seq is advanced by Count(), so Count() must accurately
// reflect the number of operations that will consume sequences when applied to memtable.
func TestSequenceConsumption(t *testing.T) {
	tests := []struct {
		name           string
		setupBatch     func(*WriteBatch)
		expectedCount  uint32
		expectedSeqOps uint32 // number of operations that consume sequences
	}{
		{
			name: "simple put operations",
			setupBatch: func(wb *WriteBatch) {
				wb.Put([]byte("key1"), []byte("val1"))
				wb.Put([]byte("key2"), []byte("val2"))
				wb.Put([]byte("key3"), []byte("val3"))
			},
			expectedCount:  3,
			expectedSeqOps: 3,
		},
		{
			name: "mixed operations",
			setupBatch: func(wb *WriteBatch) {
				wb.Put([]byte("key1"), []byte("val1"))
				wb.Delete([]byte("key2"))
				wb.Put([]byte("key3"), []byte("val3"))
				wb.Merge([]byte("key4"), []byte("val4"))
			},
			expectedCount:  4,
			expectedSeqOps: 4,
		},
		{
			name: "with range deletion",
			setupBatch: func(wb *WriteBatch) {
				wb.Put([]byte("key1"), []byte("val1"))
				wb.DeleteRange([]byte("a"), []byte("z"))
				wb.Put([]byte("key2"), []byte("val2"))
			},
			expectedCount:  3,
			expectedSeqOps: 3,
		},
		{
			name: "with LogData (should not consume sequence)",
			setupBatch: func(wb *WriteBatch) {
				wb.Put([]byte("key1"), []byte("val1"))
				wb.PutLogData([]byte("log entry"))
				wb.Put([]byte("key2"), []byte("val2"))
			},
			expectedCount:  2, // LogData doesn't increment count
			expectedSeqOps: 2, // LogData doesn't consume sequence
		},
		{
			name: "with column family operations",
			setupBatch: func(wb *WriteBatch) {
				wb.PutCF(1, []byte("key1"), []byte("val1"))
				wb.DeleteCF(1, []byte("key2"))
				wb.Put([]byte("key3"), []byte("val3"))
			},
			expectedCount:  3,
			expectedSeqOps: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wb := New()
			tt.setupBatch(wb)

			// Verify Count() matches expected
			if wb.Count() != tt.expectedCount {
				t.Errorf("Count() = %d, want %d", wb.Count(), tt.expectedCount)
			}

			// Now count actual sequence-consuming operations by iterating
			counter := &sequenceCounter{}
			wb.SetSequence(100) // Start from sequence 100
			if err := wb.Iterate(counter); err != nil {
				t.Fatalf("Iterate failed: %v", err)
			}

			actualSeqOps := counter.count
			if actualSeqOps != tt.expectedSeqOps {
				t.Errorf("Actual sequence-consuming ops = %d, want %d", actualSeqOps, tt.expectedSeqOps)
			}

			// CRITICAL: Count() must match actual sequence-consuming operations
			if wb.Count() != actualSeqOps {
				t.Errorf("MISMATCH: Count() = %d, but actual sequence-consuming ops = %d",
					wb.Count(), actualSeqOps)
				t.Error("This will cause sequence number reuse after flush+crash+reopen!")
			}
		})
	}
}

// sequenceCounter counts how many operations actually consume sequence numbers
type sequenceCounter struct {
	count uint32
}

func (c *sequenceCounter) Put(key, value []byte) error {
	c.count++
	return nil
}

func (c *sequenceCounter) Delete(key []byte) error {
	c.count++
	return nil
}

func (c *sequenceCounter) SingleDelete(key []byte) error {
	c.count++
	return nil
}

func (c *sequenceCounter) Merge(key, value []byte) error {
	c.count++
	return nil
}

func (c *sequenceCounter) DeleteRange(startKey, endKey []byte) error {
	c.count++
	return nil
}

func (c *sequenceCounter) LogData(blob []byte) {
	// LogData does NOT consume a sequence number
}

func (c *sequenceCounter) PutCF(cfID uint32, key, value []byte) error {
	c.count++
	return nil
}

func (c *sequenceCounter) DeleteCF(cfID uint32, key []byte) error {
	c.count++
	return nil
}

func (c *sequenceCounter) SingleDeleteCF(cfID uint32, key []byte) error {
	c.count++
	return nil
}

func (c *sequenceCounter) MergeCF(cfID uint32, key, value []byte) error {
	c.count++
	return nil
}

func (c *sequenceCounter) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error {
	c.count++
	return nil
}
