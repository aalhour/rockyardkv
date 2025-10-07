package memtable

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

// TestMemTableIteratorPrev tests the Prev method
func TestMemTableIteratorPrev(t *testing.T) {
	mt := NewMemTable(nil)

	// Add entries
	for i := range 5 {
		key := []byte{byte('a' + i)}
		mt.Add(dbformat.SequenceNumber(i+1), dbformat.TypeValue, key, []byte("value"))
	}

	iter := mt.NewIterator()

	// Seek to last
	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatal("iterator should be valid after SeekToLast")
	}

	// Use Prev
	iter.Prev()
	if !iter.Valid() {
		t.Fatal("iterator should be valid after Prev")
	}
}

// TestMemTableIteratorKey tests the Key method (returns internal key)
func TestMemTableIteratorKey(t *testing.T) {
	mt := NewMemTable(nil)
	mt.Add(1, dbformat.TypeValue, []byte("test"), []byte("value"))

	iter := mt.NewIterator()
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("iterator should be valid")
	}

	key := iter.Key()
	if key == nil {
		t.Error("Key() should return non-nil")
	}
	// Key includes internal key suffix (8 bytes)
	if len(key) != len("test")+8 {
		t.Errorf("Key() length = %d, want %d", len(key), len("test")+8)
	}
}

// TestMemTableIteratorValue tests the Value method
func TestMemTableIteratorValue(t *testing.T) {
	mt := NewMemTable(nil)
	mt.Add(1, dbformat.TypeValue, []byte("test"), []byte("myvalue"))

	iter := mt.NewIterator()
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("iterator should be valid")
	}

	val := iter.Value()
	if string(val) != "myvalue" {
		t.Errorf("Value() = %q, want %q", val, "myvalue")
	}
}

// TestMemTableIteratorError tests the Error method
func TestMemTableIteratorError(t *testing.T) {
	mt := NewMemTable(nil)
	iter := mt.NewIterator()

	// Error should be nil for a valid iterator
	if err := iter.Error(); err != nil {
		t.Errorf("Error() = %v, want nil", err)
	}
}

// TestMemTableIteratorSequence tests the Sequence method
func TestMemTableIteratorSequence(t *testing.T) {
	mt := NewMemTable(nil)
	mt.Add(42, dbformat.TypeValue, []byte("test"), []byte("value"))

	iter := mt.NewIterator()
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("iterator should be valid")
	}

	seq := iter.Sequence()
	if seq != 42 {
		t.Errorf("Sequence() = %d, want 42", seq)
	}
}

// TestMemTableIteratorType tests the Type method
func TestMemTableIteratorType(t *testing.T) {
	mt := NewMemTable(nil)
	mt.Add(1, dbformat.TypeValue, []byte("put"), []byte("value"))
	mt.Add(2, dbformat.TypeDeletion, []byte("del"), nil)

	iter := mt.NewIterator()

	// Check put entry type
	iter.Seek([]byte("put"))
	if !iter.Valid() {
		t.Fatal("iterator should be valid for 'put'")
	}
	if iter.Type() != dbformat.TypeValue {
		t.Errorf("Type() = %d, want %d", iter.Type(), dbformat.TypeValue)
	}

	// Check delete entry type
	iter.Seek([]byte("del"))
	if !iter.Valid() {
		t.Fatal("iterator should be valid for 'del'")
	}
	if iter.Type() != dbformat.TypeDeletion {
		t.Errorf("Type() = %d, want %d", iter.Type(), dbformat.TypeDeletion)
	}
}

// TestSkipListWithInvalidParams tests NewSkipListWithParams with edge cases
func TestSkipListWithInvalidParams(t *testing.T) {
	// Test with nil comparator - should use default
	sl := NewSkipListWithParams(nil, 0, 0)
	if sl == nil {
		t.Fatal("NewSkipListWithParams returned nil")
	}

	sl.Insert([]byte("test"))
	if !sl.Contains([]byte("test")) {
		t.Error("skiplist should contain inserted key")
	}
}

// TestSkipListCount tests the Count method
func TestSkipListCount(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	if sl.Count() != 0 {
		t.Errorf("Count() = %d, want 0 for empty list", sl.Count())
	}

	sl.Insert([]byte("key1"))
	sl.Insert([]byte("key2"))
	sl.Insert([]byte("key3"))

	if sl.Count() != 3 {
		t.Errorf("Count() = %d, want 3", sl.Count())
	}
}
