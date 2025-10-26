// Package iterator provides iterator implementations for RockyardKV.
//
// MergingIterator provides the union of data from multiple child iterators,
// merging them in sorted order using a heap.
//
// Reference: RocksDB v10.7.5
//   - table/merging_iterator.h
//   - table/merging_iterator.cc
package iterator

import (
	"container/heap"

	"github.com/aalhour/rockyardkv/internal/block"
)

// Iterator is the interface for all iterators in RockyardKV.
type Iterator interface {
	// Valid returns true if the iterator is positioned at a valid entry.
	Valid() bool

	// Key returns the current key. The key is valid until the next call to Next/Seek/etc.
	Key() []byte

	// Value returns the current value.
	Value() []byte

	// SeekToFirst positions the iterator at the first entry.
	SeekToFirst()

	// SeekToLast positions the iterator at the last entry.
	SeekToLast()

	// Seek positions the iterator at the first entry with key >= target.
	Seek(target []byte)

	// Next advances to the next entry.
	Next()

	// Prev moves to the previous entry.
	Prev()

	// Error returns any error encountered during iteration.
	Error() error
}

// -----------------------------------------------------------------------------
// MergingIterator
// -----------------------------------------------------------------------------

// MergingIterator merges multiple sorted iterators into one sorted iterator.
// It uses a min-heap to efficiently find the smallest key across all child iterators.
// This is used for compaction (merging multiple SST files) and for DB iteration
// (merging memtable + immutable memtables + SST files).
type MergingIterator struct {
	children   []Iterator
	comparator func(a, b []byte) int
	minHeap    *iterHeap
	current    int // index of current iterator in children, -1 if invalid
	err        error
}

// NewMergingIterator creates a new merging iterator over the given children.
// The comparator should compare internal keys.
func NewMergingIterator(children []Iterator, comparator func(a, b []byte) int) *MergingIterator {
	if comparator == nil {
		comparator = block.CompareInternalKeys
	}
	mi := &MergingIterator{
		children:   children,
		comparator: comparator,
		current:    -1,
	}
	mi.minHeap = &iterHeap{
		items: make([]heapItem, 0, len(children)),
		cmp:   comparator,
	}
	return mi
}

// Valid returns true if the iterator is positioned at a valid entry.
func (mi *MergingIterator) Valid() bool {
	return mi.current >= 0 && mi.current < len(mi.children)
}

// Key returns the current key.
func (mi *MergingIterator) Key() []byte {
	if !mi.Valid() {
		return nil
	}
	return mi.children[mi.current].Key()
}

// Value returns the current value.
func (mi *MergingIterator) Value() []byte {
	if !mi.Valid() {
		return nil
	}
	return mi.children[mi.current].Value()
}

// SeekToFirst positions the iterator at the smallest key across all children.
func (mi *MergingIterator) SeekToFirst() {
	mi.err = nil
	mi.minHeap.items = mi.minHeap.items[:0]

	for i, child := range mi.children {
		child.SeekToFirst()
		if child.Valid() {
			mi.minHeap.items = append(mi.minHeap.items, heapItem{
				index: i,
				key:   child.Key(),
			})
		}
		if err := child.Error(); err != nil {
			mi.err = err
			mi.current = -1
			return
		}
	}

	heap.Init(mi.minHeap)
	mi.findSmallest()
}

// SeekToLast positions the iterator at the largest key across all children.
// Note: MergingIterator is optimized for forward iteration; SeekToLast is less efficient.
func (mi *MergingIterator) SeekToLast() {
	mi.err = nil
	mi.current = -1

	// Find the child with the largest key
	var largestIdx = -1
	var largestKey []byte

	for i, child := range mi.children {
		child.SeekToLast()
		if child.Valid() {
			if largestIdx == -1 || mi.comparator(child.Key(), largestKey) > 0 {
				largestIdx = i
				largestKey = child.Key()
			}
		}
		if err := child.Error(); err != nil {
			mi.err = err
			return
		}
	}

	mi.current = largestIdx
}

// Seek positions the iterator at the first key >= target.
func (mi *MergingIterator) Seek(target []byte) {
	mi.err = nil
	mi.minHeap.items = mi.minHeap.items[:0]

	for i, child := range mi.children {
		child.Seek(target)
		if child.Valid() {
			mi.minHeap.items = append(mi.minHeap.items, heapItem{
				index: i,
				key:   child.Key(),
			})
		}
		if err := child.Error(); err != nil {
			mi.err = err
			mi.current = -1
			return
		}
	}

	heap.Init(mi.minHeap)
	mi.findSmallest()
}

// Next advances to the next entry.
func (mi *MergingIterator) Next() {
	if !mi.Valid() {
		return
	}

	// Advance the current child iterator
	mi.children[mi.current].Next()

	if mi.children[mi.current].Valid() {
		// Update the key in the heap and re-heapify
		mi.minHeap.items[0].key = mi.children[mi.current].Key()
		heap.Fix(mi.minHeap, 0)
	} else {
		// Remove from heap
		heap.Pop(mi.minHeap)
	}

	if err := mi.children[mi.current].Error(); err != nil {
		mi.err = err
		mi.current = -1
		return
	}

	mi.findSmallest()
}

// Prev moves to the previous entry.
// Note: MergingIterator is optimized for forward iteration; Prev is less efficient.
func (mi *MergingIterator) Prev() {
	if !mi.Valid() {
		return
	}

	// For Prev, we need to find the largest key that is smaller than the current key.
	// This is more complex than Next because we need to use a max-heap approach.
	// For now, use a simpler O(n) approach: find the largest key < current among all children.

	currentKey := append([]byte(nil), mi.children[mi.current].Key()...)

	// Move the current child back
	mi.children[mi.current].Prev()

	// Find the child with the largest key that is still < currentKey
	var largestIdx = -1
	var largestKey []byte

	for i, child := range mi.children {
		if child.Valid() {
			k := child.Key()
			if mi.comparator(k, currentKey) < 0 {
				if largestIdx == -1 || mi.comparator(k, largestKey) > 0 {
					largestIdx = i
					largestKey = k
				}
			}
		}
		if err := child.Error(); err != nil {
			mi.err = err
			mi.current = -1
			return
		}
	}

	mi.current = largestIdx
}

// Error returns any error encountered during iteration.
func (mi *MergingIterator) Error() error {
	return mi.err
}

// findSmallest sets current to the iterator with the smallest key.
func (mi *MergingIterator) findSmallest() {
	if mi.minHeap.Len() == 0 {
		mi.current = -1
		return
	}
	mi.current = mi.minHeap.items[0].index
}

// -----------------------------------------------------------------------------
// Min-Heap implementation for iterator merging
// -----------------------------------------------------------------------------

type heapItem struct {
	index int    // index into children slice
	key   []byte // current key for this iterator
}

type iterHeap struct {
	items []heapItem
	cmp   func(a, b []byte) int
}

func (h *iterHeap) Len() int { return len(h.items) }

func (h *iterHeap) Less(i, j int) bool {
	return h.cmp(h.items[i].key, h.items[j].key) < 0
}

func (h *iterHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *iterHeap) Push(x any) {
	item, ok := x.(heapItem)
	if !ok {
		return // Type safety - heap.Push should only be called with heapItem
	}
	h.items = append(h.items, item)
}

func (h *iterHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}
