package memtable

import (
	"bytes"
	"sync"
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

// -----------------------------------------------------------------------------
// MemTable Edge Case and Concurrency Tests
// Based on RocksDB memtable/memtable_test.cc
// (Named to avoid conflicts with existing tests)
// -----------------------------------------------------------------------------

// TestMemTableEmptyKeyEdge tests handling of empty user keys.
func TestMemTableEmptyKeyEdge(t *testing.T) {
	mem := NewMemTable(nil)

	// Add entry with empty key
	mem.Add(1, dbformat.TypeValue, []byte{}, []byte("value"))

	// Should be able to retrieve it
	val, found, deleted := mem.Get([]byte{}, 100)
	if !found {
		t.Error("Empty key not found")
	}
	if deleted {
		t.Error("Empty key unexpectedly marked deleted")
	}
	if string(val) != "value" {
		t.Errorf("Value mismatch: got %s, want value", val)
	}
}

// TestMemTableBinaryKeysEdge tests handling of binary keys.
func TestMemTableBinaryKeysEdge(t *testing.T) {
	mem := NewMemTable(nil)

	// Key with null bytes and other binary data
	binaryKey := []byte{0x00, 0x01, 0xFF, 0xFE, 0x00, 0x42}
	mem.Add(1, dbformat.TypeValue, binaryKey, []byte("value"))

	val, found, deleted := mem.Get(binaryKey, 100)
	if !found {
		t.Error("Binary key not found")
	}
	if deleted {
		t.Error("Binary key unexpectedly marked deleted")
	}
	if string(val) != "value" {
		t.Error("Value mismatch for binary key")
	}
}

// TestMemTableOverwrite tests overwriting keys.
func TestMemTableOverwrite(t *testing.T) {
	mem := NewMemTable(nil)

	key := []byte("key")

	// Add first version
	mem.Add(10, dbformat.TypeValue, key, []byte("value1"))

	// Add second version with higher sequence
	mem.Add(20, dbformat.TypeValue, key, []byte("value2"))

	// Should get the newer version
	val, found, deleted := mem.Get(key, 100)
	if !found {
		t.Error("Key not found after overwrite")
	}
	if deleted {
		t.Error("Key unexpectedly deleted")
	}
	if string(val) != "value2" {
		t.Errorf("Expected value2, got %s", val)
	}

	// Read with older sequence should get older value
	val, found, deleted = mem.Get(key, 15)
	if !found {
		t.Error("Key not found with older sequence")
	}
	if deleted {
		t.Error("Key unexpectedly deleted")
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1 with older seq, got %s", val)
	}
}

// TestMemTableDeleteEdge tests deletion markers.
func TestMemTableDeleteEdge(t *testing.T) {
	mem := NewMemTable(nil)

	key := []byte("key")

	// Add value
	mem.Add(10, dbformat.TypeValue, key, []byte("value"))

	// Add deletion with higher sequence
	mem.Add(20, dbformat.TypeDeletion, key, nil)

	// Key should be "deleted" when reading with high sequence
	_, found, deleted := mem.Get(key, 100)
	if !found {
		t.Error("Key should be found (as deleted)")
	}
	if !deleted {
		t.Error("Key should be marked as deleted")
	}

	// Key should still be readable with older sequence
	val, found, deleted := mem.Get(key, 15)
	if !found || deleted {
		t.Error("Key should be found and not deleted with older sequence")
	}
	if string(val) != "value" {
		t.Errorf("Expected value with older seq, got %s", val)
	}
}

// TestMemTableManyEntries tests many entries.
func TestMemTableManyEntries(t *testing.T) {
	mem := NewMemTable(nil)

	numEntries := 10000
	for i := range numEntries {
		key := []byte{byte(i / 256), byte(i % 256)}
		value := []byte{byte(i % 256)}
		mem.Add(dbformat.SequenceNumber(i+1), dbformat.TypeValue, key, value)
	}

	// Verify all entries
	for i := range numEntries {
		key := []byte{byte(i / 256), byte(i % 256)}
		val, found, _ := mem.Get(key, dbformat.SequenceNumber(numEntries+1))
		if !found {
			t.Errorf("Key %d not found", i)
			continue
		}
		expected := []byte{byte(i % 256)}
		if !bytes.Equal(val, expected) {
			t.Errorf("Key %d: value mismatch", i)
		}
	}
}

// TestMemTableConcurrentReads tests concurrent reads.
func TestMemTableConcurrentReads(t *testing.T) {
	mem := NewMemTable(nil)

	// Populate memtable
	for i := range 100 {
		key := []byte{byte(i)}
		mem.Add(dbformat.SequenceNumber(i+1), dbformat.TypeValue, key, []byte{byte(i)})
	}

	// Concurrent reads
	var wg sync.WaitGroup
	numReaders := 10
	readsPerReader := 1000

	for range numReaders {
		wg.Go(func() {
			for i := range readsPerReader {
				key := []byte{byte(i % 100)}
				val, found, _ := mem.Get(key, 1000)
				if !found {
					t.Error("Concurrent read failed to find key")
				}
				if len(val) != 1 || val[0] != key[0] {
					t.Error("Concurrent read got wrong value")
				}
			}
		})
	}

	wg.Wait()
}

// TestMemTableConcurrentWritesAndReads tests concurrent writes and reads.
func TestMemTableConcurrentWritesAndReads(t *testing.T) {
	mem := NewMemTable(nil)

	var wg sync.WaitGroup
	numWriters := 5
	numReaders := 5
	writesPerWriter := 100

	// Writers
	for w := range numWriters {
		wg.Go(func() {
			for i := range writesPerWriter {
				key := []byte{byte(w), byte(i)}
				seq := dbformat.SequenceNumber(w*1000 + i + 1)
				mem.Add(seq, dbformat.TypeValue, key, []byte{byte(i)})
			}
		})
	}

	// Readers (may read partially written data)
	for range numReaders {
		wg.Go(func() {
			for i := range 500 {
				key := []byte{byte(i % numWriters), byte(i % writesPerWriter)}
				mem.Get(key, 100000) // Just verify no panic
			}
		})
	}

	wg.Wait()
}

// TestMemTableIteratorEmpty tests iterator on empty memtable.
func TestMemTableIteratorEmpty(t *testing.T) {
	mem := NewMemTable(nil)
	iter := mem.NewIterator()

	iter.SeekToFirst()
	if iter.Valid() {
		t.Error("Empty memtable iterator should not be valid")
	}

	iter.SeekToLast()
	if iter.Valid() {
		t.Error("Empty memtable iterator SeekToLast should not be valid")
	}
}

// TestMemTableIteratorSeekEdge tests iterator seek operations.
func TestMemTableIteratorSeekEdge(t *testing.T) {
	mem := NewMemTable(nil)

	// Add some entries
	keys := []string{"bbb", "ddd", "fff"}
	for i, k := range keys {
		mem.Add(dbformat.SequenceNumber(i+1), dbformat.TypeValue, []byte(k), []byte("value"))
	}

	iter := mem.NewIterator()

	// Seek to first
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Error("SeekToFirst should be valid")
	}
}

// TestMemTableApproximateMemory tests memory usage tracking.
func TestMemTableApproximateMemory(t *testing.T) {
	mem := NewMemTable(nil)

	initialMem := mem.ApproximateMemoryUsage()

	// Add some data
	for i := range 100 {
		key := make([]byte, 100)
		value := make([]byte, 1000)
		mem.Add(dbformat.SequenceNumber(i+1), dbformat.TypeValue, key, value)
	}

	afterMem := mem.ApproximateMemoryUsage()

	if afterMem <= initialMem {
		t.Error("Memory usage should increase after adding entries")
	}

	// Rough check: we added ~110KB of data
	expectedMin := int64(100 * (100 + 1000))
	if afterMem-initialMem < expectedMin/2 {
		t.Errorf("Memory increase too small: got %d, expected at least %d", afterMem-initialMem, expectedMin/2)
	}
}

// TestMemTableLargeKeys tests very large keys.
func TestMemTableLargeKeys(t *testing.T) {
	mem := NewMemTable(nil)

	// 64KB key
	largeKey := make([]byte, 64*1024)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}

	mem.Add(1, dbformat.TypeValue, largeKey, []byte("value"))

	val, found, _ := mem.Get(largeKey, 100)
	if !found {
		t.Error("Large key not found")
	}
	if string(val) != "value" {
		t.Error("Value mismatch for large key")
	}
}

// TestMemTableLargeValues tests very large values.
func TestMemTableLargeValues(t *testing.T) {
	mem := NewMemTable(nil)

	// 1MB value
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	mem.Add(1, dbformat.TypeValue, []byte("key"), largeValue)

	val, found, _ := mem.Get([]byte("key"), 100)
	if !found {
		t.Error("Key with large value not found")
	}
	if !bytes.Equal(val, largeValue) {
		t.Error("Large value mismatch")
	}
}

// TestMemTableEmptyValueEdge tests handling of empty values.
func TestMemTableEmptyValueEdge(t *testing.T) {
	mem := NewMemTable(nil)

	mem.Add(1, dbformat.TypeValue, []byte("key"), []byte{}) // Empty value

	val, found, _ := mem.Get([]byte("key"), 100)
	if !found {
		t.Error("Key with empty value not found")
	}
	if len(val) != 0 {
		t.Errorf("Expected empty value, got %d bytes", len(val))
	}
}
