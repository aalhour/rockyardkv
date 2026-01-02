package rockyardkv

// compaction_filter_test.go implements tests for compaction filter.

import (
	"os"
	"strconv"
	"testing"
)

// TestCompactionFilter implements a simple test filter.
type TestCompactionFilter struct {
	BaseCompactionFilter
	removedKeys int
}

func (f *TestCompactionFilter) Name() string {
	return "TestCompactionFilter"
}

func (f *TestCompactionFilter) Filter(level int, key, oldValue []byte) (CompactionFilterDecision, []byte) {
	// Remove keys that start with "delete_"
	if len(key) >= 7 && string(key[:7]) == "delete_" {
		f.removedKeys++
		return FilterRemove, nil
	}
	return FilterKeep, nil
}

func TestCompactionFilterBasic(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "rockyard-compaction-filter-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create filter
	filter := &TestCompactionFilter{}

	// Open database with compaction filter
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.CompactionFilter = filter
	opts.WriteBufferSize = 1024 // Small buffer to trigger flushes
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	// Write some data - mix of regular keys and keys to be deleted
	for i := range 50 {
		key := []byte("keep_" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		database.Put(nil, key, value)
	}
	for i := range 50 {
		key := []byte("delete_" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		database.Put(nil, key, value)
	}

	// Flush to create SST files
	database.Flush(nil)

	// Verify keys exist before compaction
	for i := range 50 {
		key := []byte("delete_" + strconv.Itoa(i))
		_, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Key %s should exist before compaction: %v", key, err)
		}
	}

	database.Close()

	// Note: Full compaction filter integration requires modifying the compaction job
	// to call the filter. For now, this test verifies the filter interface works.
	t.Log("Compaction filter interface test passed")
}

// TestCompactionFilterIntegration tests that compaction filter is actually
// called during compaction and keys are removed.
func TestCompactionFilterIntegration(t *testing.T) {

	dir, err := os.MkdirTemp("", "rockyard-compaction-filter-int-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	filter := &TestCompactionFilter{}

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.CompactionFilter = filter
	opts.WriteBufferSize = 1024
	opts.DisableAutoCompactions = true // Manual compaction only

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	// Write keys to keep and keys to delete
	for i := range 50 {
		database.Put(nil, []byte("keep_"+strconv.Itoa(i)), []byte("value"))
	}
	for i := range 50 {
		database.Put(nil, []byte("delete_"+strconv.Itoa(i)), []byte("value"))
	}

	database.Flush(nil)

	// Trigger compaction
	database.CompactRange(nil, nil, nil)

	// After compaction, "delete_" keys should be gone
	deletedCount := 0
	for i := range 50 {
		key := []byte("delete_" + strconv.Itoa(i))
		_, err := database.Get(nil, key)
		if err != nil {
			deletedCount++
		}
	}

	// "keep_" keys should still exist
	keptCount := 0
	for i := range 50 {
		key := []byte("keep_" + strconv.Itoa(i))
		_, err := database.Get(nil, key)
		if err == nil {
			keptCount++
		}
	}

	database.Close()

	if deletedCount != 50 {
		t.Errorf("Compaction filter should have deleted 50 keys, deleted %d", deletedCount)
	}
	if keptCount != 50 {
		t.Errorf("Compaction filter should have kept 50 keys, kept %d", keptCount)
	}
	if filter.removedKeys != 50 {
		t.Errorf("Filter reported removing %d keys, expected 50", filter.removedKeys)
	}
}

// TestCompactionFilterDeletesAll tests a filter that removes ALL keys.
// Port of: TEST_F(DBTestCompactionFilter, CompactionFilterDeletesAll)
func TestCompactionFilterDeletesAll(t *testing.T) {

	dir, err := os.MkdirTemp("", "rockyard-compaction-filter-all-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Filter that removes everything
	filter := &deleteAllFilter{}

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.CompactionFilter = filter
	opts.DisableAutoCompactions = true

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	// Write data across multiple flushes
	for table := range 4 {
		for i := range 10 + table {
			key := []byte(strconv.Itoa(table*100 + i))
			database.Put(nil, key, []byte("val"))
		}
		database.Flush(nil)
	}

	// Compact - should delete everything
	database.CompactRange(nil, nil, nil)

	// Verify database is empty
	iter := database.NewIterator(nil)
	iter.SeekToFirst()
	if iter.Valid() {
		t.Errorf("Database should be empty after compaction with delete-all filter, found key: %s", iter.Key())
	}
	iter.Close()

	database.Close()
}

// deleteAllFilter removes all keys during compaction.
type deleteAllFilter struct {
	BaseCompactionFilter
}

func (f *deleteAllFilter) Name() string {
	return "DeleteAllFilter"
}

func (f *deleteAllFilter) Filter(level int, key, oldValue []byte) (CompactionFilterDecision, []byte) {
	return FilterRemove, nil
}

func TestRemoveByPrefixFilter(t *testing.T) {
	filter := &RemoveByPrefixFilter{Prefix: []byte("temp_")}

	// Should remove keys with prefix
	decision, _ := filter.Filter(0, []byte("temp_key1"), []byte("value"))
	if decision != FilterRemove {
		t.Error("Should remove key with temp_ prefix")
	}

	decision, _ = filter.Filter(0, []byte("temp_key2"), []byte("value"))
	if decision != FilterRemove {
		t.Error("Should remove key with temp_ prefix")
	}

	// Should keep keys without prefix
	decision, _ = filter.Filter(0, []byte("keep_key1"), []byte("value"))
	if decision != FilterKeep {
		t.Error("Should keep key without temp_ prefix")
	}

	decision, _ = filter.Filter(0, []byte("temp_"), []byte("value"))
	if decision != FilterRemove {
		t.Error("Should remove key that exactly matches prefix")
	}

	decision, _ = filter.Filter(0, []byte("tem"), []byte("value"))
	if decision != FilterKeep {
		t.Error("Should keep key shorter than prefix")
	}
}

func TestRemoveByRangeFilter(t *testing.T) {
	filter := &RemoveByRangeFilter{
		StartKey: []byte("b"),
		EndKey:   []byte("d"),
	}

	// Should remove keys in range [b, d)
	decision, _ := filter.Filter(0, []byte("b"), []byte("value"))
	if decision != FilterRemove {
		t.Error("Should remove 'b' (start of range)")
	}

	decision, _ = filter.Filter(0, []byte("c"), []byte("value"))
	if decision != FilterRemove {
		t.Error("Should remove 'c' (in range)")
	}

	decision, _ = filter.Filter(0, []byte("cc"), []byte("value"))
	if decision != FilterRemove {
		t.Error("Should remove 'cc' (in range)")
	}

	// Should keep keys outside range
	decision, _ = filter.Filter(0, []byte("a"), []byte("value"))
	if decision != FilterKeep {
		t.Error("Should keep 'a' (before range)")
	}

	decision, _ = filter.Filter(0, []byte("d"), []byte("value"))
	if decision != FilterKeep {
		t.Error("Should keep 'd' (end of range, exclusive)")
	}

	decision, _ = filter.Filter(0, []byte("e"), []byte("value"))
	if decision != FilterKeep {
		t.Error("Should keep 'e' (after range)")
	}
}

func TestCompactionFilterDecisions(t *testing.T) {
	// Test the base filter
	base := &BaseCompactionFilter{}

	if base.Name() != "BaseCompactionFilter" {
		t.Errorf("Name = %s, want BaseCompactionFilter", base.Name())
	}

	decision, _ := base.Filter(0, []byte("key"), []byte("value"))
	if decision != FilterKeep {
		t.Error("Base filter should keep all entries")
	}

	decision = base.FilterMergeOperand(0, []byte("key"), []byte("operand"))
	if decision != FilterKeep {
		t.Error("Base filter should keep all merge operands")
	}
}

// ValueTransformFilter changes values during compaction.
type ValueTransformFilter struct {
	BaseCompactionFilter
	Prefix []byte
}

func (f *ValueTransformFilter) Name() string {
	return "ValueTransformFilter"
}

func (f *ValueTransformFilter) Filter(level int, key, oldValue []byte) (CompactionFilterDecision, []byte) {
	// Add prefix to all values
	newValue := append(f.Prefix, oldValue...)
	return FilterChange, newValue
}

func TestValueTransformFilter(t *testing.T) {
	filter := &ValueTransformFilter{Prefix: []byte("transformed_")}

	decision, newValue := filter.Filter(0, []byte("key"), []byte("original"))
	if decision != FilterChange {
		t.Error("Should change value")
	}
	if string(newValue) != "transformed_original" {
		t.Errorf("New value = %s, want transformed_original", newValue)
	}
}
