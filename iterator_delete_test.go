package rockyardkv

// iterator_delete_test.go implements tests for iterator delete.


import (
	"fmt"
	"os"
	"testing"
)

// TestIteratorTombstoneHandlingAfterFlush tests that the iterator correctly
// skips deleted keys when deletion tombstones are stored in SST files.
// This is a regression test for a bug where the iterator's minKey/maxKey
// slice was aliasing the underlying iterator buffer, causing incorrect
// behavior when skipping deletion markers.
func TestIteratorTombstoneHandlingAfterFlush(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test_tombstone")
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer database.Close()

	// Insert 10 keys
	for i := range 10 {
		key := fmt.Appendf(nil, "key%02d", i)
		database.Put(nil, key, []byte("value"))
	}
	database.Flush(nil)

	// Delete even keys
	for i := 0; i < 10; i += 2 {
		key := fmt.Appendf(nil, "key%02d", i)
		database.Delete(nil, key)
	}
	database.Flush(nil)

	// Iterate without reopen - this is the key test case
	iter := database.NewIterator(nil)
	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Close()

	// Should have 5 odd keys
	expected := []string{"key01", "key03", "key05", "key07", "key09"}
	if len(keys) != len(expected) {
		t.Errorf("Expected %d keys, got %d: %v", len(expected), len(keys), keys)
	}
	for i, k := range expected {
		if i >= len(keys) || keys[i] != k {
			t.Errorf("Expected key %s at position %d, got %v", k, i, keys)
			break
		}
	}
}

// TestIteratorTombstoneWithMultipleDeletions tests tombstone handling
// with multiple interleaved deletions across multiple SST files.
func TestIteratorTombstoneWithMultipleDeletions(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test_tombstone_multi")
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer database.Close()

	// SST1: a, b, c
	database.Put(nil, []byte("a"), []byte("va"))
	database.Put(nil, []byte("b"), []byte("vb"))
	database.Put(nil, []byte("c"), []byte("vc"))
	database.Flush(nil)

	// SST2: delete b
	database.Delete(nil, []byte("b"))
	database.Flush(nil)

	// Iterate
	iter := database.NewIterator(nil)
	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Close()

	expected := []string{"a", "c"}
	if len(keys) != len(expected) {
		t.Errorf("Expected %v, got %v", expected, keys)
	}
	for i, k := range expected {
		if i >= len(keys) || keys[i] != k {
			t.Errorf("Expected key %s at position %d, got %v", k, i, keys)
			break
		}
	}
}

// TestIteratorTombstoneReverse tests tombstone handling during reverse iteration.
func TestIteratorTombstoneReverse(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test_tombstone_rev")
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer database.Close()

	// SST1: a, b, c, d, e
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		database.Put(nil, []byte(k), []byte("v"+k))
	}
	database.Flush(nil)

	// SST2: delete b, d
	database.Delete(nil, []byte("b"))
	database.Delete(nil, []byte("d"))
	database.Flush(nil)

	// Iterate in reverse
	iter := database.NewIterator(nil)
	var keys []string
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Close()

	expected := []string{"e", "c", "a"}
	if len(keys) != len(expected) {
		t.Errorf("Expected %v, got %v", expected, keys)
	}
	for i, k := range expected {
		if i >= len(keys) || keys[i] != k {
			t.Errorf("Expected key %s at position %d, got %v", k, i, keys)
			break
		}
	}
}
