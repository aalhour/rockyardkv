package memtable

import (
	"bytes"
	"testing"
)

// FuzzSkipListInsertContains tests that inserted keys are found.
func FuzzSkipListInsertContains(f *testing.F) {
	f.Add([]byte("key1"))
	f.Add([]byte(""))
	f.Add([]byte{0x00, 0x01, 0x02})
	f.Add([]byte{0xFF, 0xFE, 0xFD})

	sl := NewSkipList(BytewiseComparator)

	f.Fuzz(func(t *testing.T, key []byte) {
		sl.Insert(key)
		if !sl.Contains(key) {
			t.Errorf("Just inserted %v but Contains returned false", key)
		}
	})
}

// FuzzSkipListIteratorConsistency tests iterator consistency.
func FuzzSkipListIteratorConsistency(f *testing.F) {
	f.Add([]byte("a"), []byte("b"), []byte("c"))
	f.Add([]byte("z"), []byte("y"), []byte("x"))
	f.Add([]byte{0x00}, []byte{0x01}, []byte{0x02})

	f.Fuzz(func(t *testing.T, k1, k2, k3 []byte) {
		sl := NewSkipList(BytewiseComparator)

		// Avoid duplicates
		keys := [][]byte{k1, k2, k3}
		seen := make(map[string]bool)
		for _, k := range keys {
			if !seen[string(k)] {
				sl.Insert(k)
				seen[string(k)] = true
			}
		}

		// Iterate forward and check order
		iter := sl.NewIterator()
		iter.SeekToFirst()

		var prev []byte
		count := 0
		for iter.Valid() {
			key := iter.Key()
			if prev != nil && bytes.Compare(prev, key) >= 0 {
				t.Errorf("Keys not in ascending order: %v >= %v", prev, key)
			}
			prev = append(prev[:0], key...)
			count++
			iter.Next()
		}

		if int64(count) != sl.Count() {
			t.Errorf("Iterated %d keys, but Count() = %d", count, sl.Count())
		}
	})
}
