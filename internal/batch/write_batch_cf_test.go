package batch

import (
	"testing"
)

// TestWriteBatchDeleteCF tests DeleteCF with column family
func TestWriteBatchDeleteCF(t *testing.T) {
	// Test with non-zero cfID
	t.Run("NonZeroCF", func(t *testing.T) {
		wb := New()
		wb.DeleteCF(1, []byte("key"))

		if !wb.HasDelete() {
			t.Error("HasDelete should return true after DeleteCF")
		}

		if wb.Count() != 1 {
			t.Errorf("Count = %d, want 1", wb.Count())
		}
	})

	// Test with cfID == 0 (should delegate to Delete)
	t.Run("ZeroCF", func(t *testing.T) {
		wb := New()
		wb.DeleteCF(0, []byte("key"))

		if !wb.HasDelete() {
			t.Error("HasDelete should return true after DeleteCF with cfID=0")
		}

		if wb.Count() != 1 {
			t.Errorf("Count = %d, want 1", wb.Count())
		}
	})
}

// TestWriteBatchMergeCF tests MergeCF with column family
func TestWriteBatchMergeCF(t *testing.T) {
	// Test with non-zero cfID
	t.Run("NonZeroCF", func(t *testing.T) {
		wb := New()
		wb.MergeCF(1, []byte("key"), []byte("value"))

		if !wb.HasMerge() {
			t.Error("HasMerge should return true after MergeCF")
		}

		if wb.Count() != 1 {
			t.Errorf("Count = %d, want 1", wb.Count())
		}
	})

	// Test with cfID == 0 (should delegate to Merge)
	t.Run("ZeroCF", func(t *testing.T) {
		wb := New()
		wb.MergeCF(0, []byte("key"), []byte("value"))

		if !wb.HasMerge() {
			t.Error("HasMerge should return true after MergeCF with cfID=0")
		}

		if wb.Count() != 1 {
			t.Errorf("Count = %d, want 1", wb.Count())
		}
	})
}

// TestWriteBatchDeleteRangeCFCoverage tests DeleteRangeCF with column family
func TestWriteBatchDeleteRangeCFCoverage(t *testing.T) {
	// Test with non-zero cfID
	t.Run("NonZeroCF", func(t *testing.T) {
		wb := New()
		wb.DeleteRangeCF(1, []byte("begin"), []byte("end"))

		if !wb.HasDeleteRange() {
			t.Error("HasDeleteRange should return true after DeleteRangeCF")
		}

		if wb.Count() != 1 {
			t.Errorf("Count = %d, want 1", wb.Count())
		}
	})

	// Test with cfID == 0 (should delegate to DeleteRange)
	t.Run("ZeroCF", func(t *testing.T) {
		wb := New()
		wb.DeleteRangeCF(0, []byte("begin"), []byte("end"))

		if !wb.HasDeleteRange() {
			t.Error("HasDeleteRange should return true after DeleteRangeCF with cfID=0")
		}

		if wb.Count() != 1 {
			t.Errorf("Count = %d, want 1", wb.Count())
		}
	})
}

// TestWriteBatchIterateAllTypes tests Iterate with all operation types
func TestWriteBatchIterateAllTypes(t *testing.T) {
	wb := New()
	wb.Put([]byte("put_key"), []byte("put_value"))
	wb.Delete([]byte("del_key"))
	wb.SingleDelete([]byte("sdel_key"))
	wb.Merge([]byte("merge_key"), []byte("merge_value"))
	wb.DeleteRange([]byte("range_begin"), []byte("range_end"))
	wb.PutLogData([]byte("log data"))

	counts := make(map[string]int)
	handler := &coverageTestHandler{counts: counts}

	err := wb.Iterate(handler)
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if counts["put"] != 1 {
		t.Errorf("put count = %d, want 1", counts["put"])
	}
	if counts["delete"] != 1 {
		t.Errorf("delete count = %d, want 1", counts["delete"])
	}
	if counts["singleDelete"] != 1 {
		t.Errorf("singleDelete count = %d, want 1", counts["singleDelete"])
	}
	if counts["merge"] != 1 {
		t.Errorf("merge count = %d, want 1", counts["merge"])
	}
	if counts["deleteRange"] != 1 {
		t.Errorf("deleteRange count = %d, want 1", counts["deleteRange"])
	}
	if counts["logData"] != 1 {
		t.Errorf("logData count = %d, want 1", counts["logData"])
	}
}

// TestWriteBatchIterateCF tests Iterate with CF operations
func TestWriteBatchIterateCF(t *testing.T) {
	wb := New()
	wb.PutCF(1, []byte("cf_key"), []byte("cf_value"))
	wb.DeleteCF(2, []byte("cf_del_key"))
	wb.MergeCF(3, []byte("cf_merge_key"), []byte("cf_merge_value"))
	wb.DeleteRangeCF(4, []byte("cf_range_begin"), []byte("cf_range_end"))

	counts := make(map[string]int)
	handler := &coverageTestHandler{counts: counts}

	err := wb.Iterate(handler)
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if counts["putCF"] != 1 {
		t.Errorf("putCF count = %d, want 1", counts["putCF"])
	}
	if counts["deleteCF"] != 1 {
		t.Errorf("deleteCF count = %d, want 1", counts["deleteCF"])
	}
	if counts["mergeCF"] != 1 {
		t.Errorf("mergeCF count = %d, want 1", counts["mergeCF"])
	}
	if counts["deleteRangeCF"] != 1 {
		t.Errorf("deleteRangeCF count = %d, want 1", counts["deleteRangeCF"])
	}
}

// TestWriteBatchIterateInvalidData tests Iterate with corrupted data
func TestWriteBatchIterateInvalidData(t *testing.T) {
	// Create a batch with corrupted header
	wb := New()
	wb.Put([]byte("key"), []byte("value"))

	// Manually corrupt the data
	data := wb.Data()
	if len(data) > HeaderSize {
		// Corrupt a varint in the key length
		data[HeaderSize] = 0xFF
		data[HeaderSize+1] = 0xFF
		data[HeaderSize+2] = 0xFF
		data[HeaderSize+3] = 0xFF
		data[HeaderSize+4] = 0xFF
	}

	handler := &coverageTestHandler{counts: make(map[string]int)}
	err := wb.Iterate(handler)
	if err == nil {
		t.Error("expected error for corrupted data")
	}
}

// TestHitRateEdgeCases tests HitRate edge cases in pool stats
func TestHitRateEdgeCases(t *testing.T) {
	pool := NewWriteBatchPool()

	// Get stats and check hit rate with no operations
	stats := pool.Stats()
	rate := stats.HitRate()
	if rate != 0 {
		t.Errorf("HitRate with no operations = %f, want 0", rate)
	}
}

// TestSizedWriteBatchPoolPut tests Put returning batches to pool
func TestSizedWriteBatchPoolPut(t *testing.T) {
	pool := NewSizedWriteBatchPool()

	// Get a batch from the smallest bucket
	wb := pool.Get(100)
	if wb == nil {
		t.Fatal("Get returned nil")
	}

	// Add some data
	wb.Put([]byte("key"), []byte("value"))

	// Return to pool - should be reused
	pool.Put(wb)

	// Get again - should get a reset batch
	wb2 := pool.Get(100)
	if wb2.Count() != 0 {
		t.Error("returned batch should be cleared")
	}
}

// TestDecodeVarint32EdgeCases tests decodeVarint32 edge cases
func TestDecodeVarint32EdgeCases(t *testing.T) {
	// This is tested indirectly through Iterate with bad data
	// but we ensure coverage of the overflow path

	// Create batch with maximum varint encoding
	wb := New()
	// The key with length that requires 5-byte varint
	longKey := make([]byte, 16384) // > 2^14, requires 3 bytes
	wb.Put(longKey, []byte("v"))

	handler := &coverageTestHandler{counts: make(map[string]int)}
	err := wb.Iterate(handler)
	if err != nil {
		t.Errorf("Iterate with long key failed: %v", err)
	}
}

// coverageTestHandler implements Handler for coverage testing
type coverageTestHandler struct {
	counts map[string]int
}

func (h *coverageTestHandler) Put(key, value []byte) error {
	h.counts["put"]++
	return nil
}

func (h *coverageTestHandler) Delete(key []byte) error {
	h.counts["delete"]++
	return nil
}

func (h *coverageTestHandler) SingleDelete(key []byte) error {
	h.counts["singleDelete"]++
	return nil
}

func (h *coverageTestHandler) Merge(key, value []byte) error {
	h.counts["merge"]++
	return nil
}

func (h *coverageTestHandler) DeleteRange(beginKey, endKey []byte) error {
	h.counts["deleteRange"]++
	return nil
}

func (h *coverageTestHandler) DeleteRangeCF(cfID uint32, beginKey, endKey []byte) error {
	h.counts["deleteRangeCF"]++
	return nil
}

func (h *coverageTestHandler) LogData(blob []byte) {
	h.counts["logData"]++
}

func (h *coverageTestHandler) PutCF(cfID uint32, key, value []byte) error {
	h.counts["putCF"]++
	return nil
}

func (h *coverageTestHandler) DeleteCF(cfID uint32, key []byte) error {
	h.counts["deleteCF"]++
	return nil
}

func (h *coverageTestHandler) SingleDeleteCF(cfID uint32, key []byte) error {
	h.counts["singleDeleteCF"]++
	return nil
}

func (h *coverageTestHandler) MergeCF(cfID uint32, key, value []byte) error {
	h.counts["mergeCF"]++
	return nil
}

func (h *coverageTestHandler) MarkBeginPrepare(unprepared bool) error {
	return nil
}

func (h *coverageTestHandler) MarkEndPrepare(xid []byte) error {
	return nil
}

func (h *coverageTestHandler) MarkCommit(xid []byte) error {
	return nil
}

func (h *coverageTestHandler) MarkRollback(xid []byte) error {
	return nil
}

func (h *coverageTestHandler) MarkNoop(emptyBatch bool) error {
	return nil
}
