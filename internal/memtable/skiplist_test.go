package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func TestSkipListEmpty(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	if sl.Count() != 0 {
		t.Errorf("Count = %d, want 0", sl.Count())
	}

	if sl.Contains([]byte("key")) {
		t.Error("Empty list should not contain any key")
	}

	iter := sl.NewIterator()
	iter.SeekToFirst()
	if iter.Valid() {
		t.Error("Iterator should be invalid on empty list")
	}

	iter.SeekToLast()
	if iter.Valid() {
		t.Error("Iterator should be invalid on empty list (SeekToLast)")
	}
}

func TestSkipListSingleInsert(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	sl.Insert([]byte("key1"))

	if sl.Count() != 1 {
		t.Errorf("Count = %d, want 1", sl.Count())
	}

	if !sl.Contains([]byte("key1")) {
		t.Error("Should contain key1")
	}

	if sl.Contains([]byte("key2")) {
		t.Error("Should not contain key2")
	}
}

func TestSkipListMultipleInserts(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	keys := []string{"d", "b", "f", "a", "e", "c"}
	for _, k := range keys {
		sl.Insert([]byte(k))
	}

	if sl.Count() != 6 {
		t.Errorf("Count = %d, want 6", sl.Count())
	}

	for _, k := range keys {
		if !sl.Contains([]byte(k)) {
			t.Errorf("Should contain %q", k)
		}
	}

	// Verify sorted order
	iter := sl.NewIterator()
	iter.SeekToFirst()

	expected := []string{"a", "b", "c", "d", "e", "f"}
	i := 0
	for iter.Valid() {
		if string(iter.Key()) != expected[i] {
			t.Errorf("Key[%d] = %q, want %q", i, iter.Key(), expected[i])
		}
		i++
		iter.Next()
	}

	if i != len(expected) {
		t.Errorf("Iterated %d keys, want %d", i, len(expected))
	}
}

func TestSkipListIteratorSeek(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	keys := []string{"b", "d", "f", "h"}
	for _, k := range keys {
		sl.Insert([]byte(k))
	}

	iter := sl.NewIterator()

	// Seek to exact key
	iter.Seek([]byte("d"))
	if !iter.Valid() {
		t.Fatal("Iterator should be valid after Seek to existing key")
	}
	if string(iter.Key()) != "d" {
		t.Errorf("Key = %q, want 'd'", iter.Key())
	}

	// Seek between keys
	iter.Seek([]byte("c"))
	if !iter.Valid() {
		t.Fatal("Iterator should be valid after Seek between keys")
	}
	if string(iter.Key()) != "d" {
		t.Errorf("Key = %q, want 'd' (first >= 'c')", iter.Key())
	}

	// Seek before first
	iter.Seek([]byte("a"))
	if !iter.Valid() {
		t.Fatal("Iterator should be valid after Seek before first")
	}
	if string(iter.Key()) != "b" {
		t.Errorf("Key = %q, want 'b'", iter.Key())
	}

	// Seek past last
	iter.Seek([]byte("z"))
	if iter.Valid() {
		t.Error("Iterator should be invalid after Seek past last")
	}
}

func TestSkipListIteratorSeekToLast(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	keys := []string{"a", "b", "c", "d"}
	for _, k := range keys {
		sl.Insert([]byte(k))
	}

	iter := sl.NewIterator()
	iter.SeekToLast()

	if !iter.Valid() {
		t.Fatal("Iterator should be valid after SeekToLast")
	}
	if string(iter.Key()) != "d" {
		t.Errorf("Key = %q, want 'd'", iter.Key())
	}
}

func TestSkipListIteratorPrev(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	keys := []string{"a", "b", "c", "d"}
	for _, k := range keys {
		sl.Insert([]byte(k))
	}

	iter := sl.NewIterator()
	iter.SeekToLast()

	// Iterate backwards
	expected := []string{"d", "c", "b", "a"}
	i := 0
	for iter.Valid() && i < len(expected) {
		if string(iter.Key()) != expected[i] {
			t.Errorf("Key[%d] = %q, want %q", i, iter.Key(), expected[i])
		}
		i++
		iter.Prev()
	}

	if i != len(expected) {
		t.Errorf("Iterated %d keys, want %d", i, len(expected))
	}
}

func TestSkipListIteratorSeekForPrev(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	keys := []string{"b", "d", "f", "h"}
	for _, k := range keys {
		sl.Insert([]byte(k))
	}

	iter := sl.NewIterator()

	// SeekForPrev to exact key
	iter.SeekForPrev([]byte("d"))
	if !iter.Valid() {
		t.Fatal("Iterator should be valid after SeekForPrev to existing key")
	}
	if string(iter.Key()) != "d" {
		t.Errorf("Key = %q, want 'd'", iter.Key())
	}

	// SeekForPrev between keys
	iter.SeekForPrev([]byte("e"))
	if !iter.Valid() {
		t.Fatal("Iterator should be valid after SeekForPrev between keys")
	}
	if string(iter.Key()) != "d" {
		t.Errorf("Key = %q, want 'd' (last <= 'e')", iter.Key())
	}

	// SeekForPrev before first
	iter.SeekForPrev([]byte("a"))
	if iter.Valid() {
		t.Error("Iterator should be invalid after SeekForPrev before first key")
	}

	// SeekForPrev past last
	iter.SeekForPrev([]byte("z"))
	if !iter.Valid() {
		t.Fatal("Iterator should be valid after SeekForPrev past last")
	}
	if string(iter.Key()) != "h" {
		t.Errorf("Key = %q, want 'h'", iter.Key())
	}
}

func TestSkipListLargeInserts(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	n := 1000
	keys := make([][]byte, n)
	for i := range n {
		keys[i] = fmt.Appendf(nil, "key%05d", i)
	}

	// Insert in random order
	r := rand.New(rand.NewSource(42))
	r.Shuffle(n, func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for _, k := range keys {
		sl.Insert(k)
	}

	if sl.Count() != int64(n) {
		t.Errorf("Count = %d, want %d", sl.Count(), n)
	}

	// Verify all keys present
	for i := range n {
		k := fmt.Appendf(nil, "key%05d", i)
		if !sl.Contains(k) {
			t.Errorf("Should contain %s", k)
		}
	}

	// Verify sorted order
	iter := sl.NewIterator()
	iter.SeekToFirst()

	count := 0
	var prev []byte
	for iter.Valid() {
		if prev != nil && bytes.Compare(prev, iter.Key()) >= 0 {
			t.Errorf("Keys not in order: %q >= %q", prev, iter.Key())
		}
		prev = append(prev[:0], iter.Key()...)
		count++
		iter.Next()
	}

	if count != n {
		t.Errorf("Iterated %d keys, want %d", count, n)
	}
}

func TestSkipListConcurrentReads(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	// Pre-populate
	for i := range 100 {
		sl.Insert(fmt.Appendf(nil, "key%03d", i))
	}

	// Concurrent reads (should not race)
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			iter := sl.NewIterator()
			for range 100 {
				iter.SeekToFirst()
				for iter.Valid() {
					_ = iter.Key()
					iter.Next()
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestSkipListCustomComparator(t *testing.T) {
	// Reverse comparator
	reverseCompare := func(a, b []byte) int {
		return -bytes.Compare(a, b)
	}

	sl := NewSkipList(reverseCompare)

	keys := []string{"a", "b", "c", "d"}
	for _, k := range keys {
		sl.Insert([]byte(k))
	}

	// Should be in reverse order
	iter := sl.NewIterator()
	iter.SeekToFirst()

	expected := []string{"d", "c", "b", "a"}
	i := 0
	for iter.Valid() && i < len(expected) {
		if string(iter.Key()) != expected[i] {
			t.Errorf("Key[%d] = %q, want %q (reverse order)", i, iter.Key(), expected[i])
		}
		i++
		iter.Next()
	}
}

func TestSkipListBinaryKeys(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	// Keys with null bytes
	keys := [][]byte{
		{0x00},
		{0x00, 0x01},
		{0x01, 0x00},
		{0xFF},
		{0xFF, 0xFF},
	}

	for _, k := range keys {
		sl.Insert(k)
	}

	for _, k := range keys {
		if !sl.Contains(k) {
			t.Errorf("Should contain %v", k)
		}
	}
}

func TestSkipListEmptyKey(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)

	sl.Insert([]byte{})

	if !sl.Contains([]byte{}) {
		t.Error("Should contain empty key")
	}

	iter := sl.NewIterator()
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("Iterator should be valid")
	}
	if len(iter.Key()) != 0 {
		t.Errorf("Key should be empty, got %v", iter.Key())
	}
}

func TestSkipListRandomHeight(t *testing.T) {
	sl := NewSkipListWithParams(BytewiseComparator, 20, 4)

	// Insert many keys and check height distribution
	heights := make(map[int]int)
	for range 10000 {
		h := sl.randomHeight()
		heights[h]++
		if h < 1 || h > 20 {
			t.Errorf("Height %d out of bounds", h)
		}
	}

	// Height 1 should be most common (~75%)
	// Height 2 should be ~25% of height 1
	t.Logf("Height distribution: %v", heights)

	if heights[1] < 6000 {
		t.Errorf("Height 1 should be most common, got %d", heights[1])
	}
}

func TestSkipListWithParams(t *testing.T) {
	// Test with different parameters
	sl := NewSkipListWithParams(BytewiseComparator, 4, 2)

	for i := range 100 {
		sl.Insert(fmt.Appendf(nil, "key%03d", i))
	}

	if sl.Count() != 100 {
		t.Errorf("Count = %d, want 100", sl.Count())
	}
}

func TestSkipListIteratorInvalidOperations(t *testing.T) {
	sl := NewSkipList(BytewiseComparator)
	iter := sl.NewIterator()

	// Operations on invalid iterator should not panic
	if iter.Valid() {
		t.Error("New iterator should be invalid")
	}
	if iter.Key() != nil {
		t.Error("Key should be nil on invalid iterator")
	}

	iter.Next() // Should not panic
	iter.Prev() // Should not panic

	if iter.Valid() {
		t.Error("Iterator should still be invalid")
	}
}

// Benchmarks
func BenchmarkSkipListInsert(b *testing.B) {
	sl := NewSkipList(BytewiseComparator)
	keys := make([][]byte, b.N)
	for i := range b.N {
		keys[i] = fmt.Appendf(nil, "key%010d", i)
	}

	b.ResetTimer()
	for i := range b.N {
		sl.Insert(keys[i])
	}
}

func BenchmarkSkipListContains(b *testing.B) {
	sl := NewSkipList(BytewiseComparator)
	n := 10000
	for i := range n {
		sl.Insert(fmt.Appendf(nil, "key%05d", i))
	}

	keys := make([][]byte, b.N)
	r := rand.New(rand.NewSource(42))
	for i := range b.N {
		keys[i] = fmt.Appendf(nil, "key%05d", r.Intn(n))
	}

	b.ResetTimer()
	for i := range b.N {
		sl.Contains(keys[i])
	}
}

func BenchmarkSkipListIterateSeq(b *testing.B) {
	sl := NewSkipList(BytewiseComparator)
	for i := range 10000 {
		sl.Insert(fmt.Appendf(nil, "key%05d", i))
	}

	for b.Loop() {
		iter := sl.NewIterator()
		iter.SeekToFirst()
		for iter.Valid() {
			_ = iter.Key()
			iter.Next()
		}
	}
}

func BenchmarkSkipListSeek(b *testing.B) {
	sl := NewSkipList(BytewiseComparator)
	n := 10000
	for i := range n {
		sl.Insert(fmt.Appendf(nil, "key%05d", i))
	}

	keys := make([][]byte, b.N)
	r := rand.New(rand.NewSource(42))
	for i := range b.N {
		keys[i] = fmt.Appendf(nil, "key%05d", r.Intn(n))
	}

	b.ResetTimer()
	for i := range b.N {
		iter := sl.NewIterator()
		iter.Seek(keys[i])
	}
}
