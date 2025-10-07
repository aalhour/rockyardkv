// Package memtable implements in-memory sorted data structures for RocksDB.
//
// This package provides a SkipList implementation that matches RocksDB's
// memtable/skiplist.h semantics, including:
// - Lock-free reads (concurrent reads are safe without locking)
// - Writes require external synchronization
// - Nodes are never deleted until the SkipList is destroyed
//
// Reference: RocksDB v10.7.5 memtable/skiplist.h
package memtable

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const (
	// DefaultMaxHeight is the default maximum height for skip list nodes.
	DefaultMaxHeight = 12

	// DefaultBranchingFactor is the default branching factor.
	// On average, 1/branchingFactor nodes will be promoted to next level.
	DefaultBranchingFactor = 4
)

// Comparator compares two keys and returns:
//   - negative if a < b
//   - zero if a == b
//   - positive if a > b
type Comparator func(a, b []byte) int

// BytewiseComparator is the default comparator using bytes.Compare.
func BytewiseComparator(a, b []byte) int {
	return bytes.Compare(a, b)
}

// skipNode represents a node in the skip list.
// The node contains the key and an array of forward pointers.
type skipNode struct {
	key []byte
	// next[i] is the next node at level i.
	// We use atomic pointers for lock-free reads.
	next []*atomic.Pointer[skipNode]
}

// newSkipNode creates a new skip node with the given key and height.
func newSkipNode(key []byte, height int) *skipNode {
	node := &skipNode{
		key:  key,
		next: make([]*atomic.Pointer[skipNode], height),
	}
	for i := range node.next {
		node.next[i] = &atomic.Pointer[skipNode]{}
	}
	return node
}

// getNext returns the next node at the given level.
func (n *skipNode) getNext(level int) *skipNode {
	return n.next[level].Load()
}

// setNext sets the next node at the given level.
func (n *skipNode) setNext(level int, node *skipNode) {
	n.next[level].Store(node)
}

// SkipList is a lock-free (for reads) skip list implementation.
// Writes require external synchronization.
type SkipList struct {
	head      *skipNode
	maxHeight int32 // Current max height (atomically accessed)
	compare   Comparator
	rng       *rand.Rand

	// Configuration
	kMaxHeight  int
	kBranching  int
	kScaledInvB uint32 // scaled inverse of branching factor

	// Statistics
	count int64
}

// NewSkipList creates a new skip list with the given comparator.
func NewSkipList(cmp Comparator) *SkipList {
	return NewSkipListWithParams(cmp, DefaultMaxHeight, DefaultBranchingFactor)
}

// NewSkipListWithParams creates a new skip list with custom parameters.
func NewSkipListWithParams(cmp Comparator, maxHeight, branchingFactor int) *SkipList {
	if cmp == nil {
		cmp = BytewiseComparator
	}
	if maxHeight <= 0 {
		maxHeight = DefaultMaxHeight
	}
	if branchingFactor <= 0 {
		branchingFactor = DefaultBranchingFactor
	}

	sl := &SkipList{
		head:       newSkipNode(nil, maxHeight),
		maxHeight:  1,
		compare:    cmp,
		rng:        rand.New(rand.NewSource(0xDEADBEEF)),
		kMaxHeight: maxHeight,
		kBranching: branchingFactor,
		// kScaledInvB = (1 << 32) / branchingFactor
		kScaledInvB: uint32(0xFFFFFFFF) / uint32(branchingFactor),
	}
	return sl
}

// Insert adds a key to the skip list.
// REQUIRES: External synchronization (mutex).
// REQUIRES: Nothing equal to key is currently in the list.
func (sl *SkipList) Insert(key []byte) {
	// Find predecessors at each level
	prev := make([]*skipNode, sl.kMaxHeight)
	x := sl.findGreaterOrEqual(key, prev)

	// Duplicate keys not allowed (caller should ensure this)
	if x != nil && sl.compare(key, x.key) == 0 {
		return // Already exists - this shouldn't happen per contract
	}

	height := sl.randomHeight()

	// Update maxHeight if needed
	maxH := int(atomic.LoadInt32(&sl.maxHeight))
	if height > maxH {
		for i := maxH; i < height; i++ {
			prev[i] = sl.head
		}
		atomic.StoreInt32(&sl.maxHeight, int32(height))
	}

	// Create new node
	node := newSkipNode(key, height)

	// Link the node at each level
	for i := range height {
		node.setNext(i, prev[i].getNext(i))
		prev[i].setNext(i, node)
	}

	atomic.AddInt64(&sl.count, 1)
}

// Contains returns true if the key is in the skip list.
func (sl *SkipList) Contains(key []byte) bool {
	x := sl.findGreaterOrEqual(key, nil)
	return x != nil && sl.compare(key, x.key) == 0
}

// Count returns the number of entries in the skip list.
func (sl *SkipList) Count() int64 {
	return atomic.LoadInt64(&sl.count)
}

// findGreaterOrEqual finds the first node with key >= given key.
// If prev is not nil, fills in prev[level] with the predecessor at each level.
func (sl *SkipList) findGreaterOrEqual(key []byte, prev []*skipNode) *skipNode {
	x := sl.head
	level := int(atomic.LoadInt32(&sl.maxHeight)) - 1

	for {
		next := x.getNext(level)
		if next != nil && sl.compare(key, next.key) > 0 {
			// Keep searching at this level
			x = next
		} else {
			// Record predecessor
			if prev != nil {
				prev[level] = x
			}
			if level == 0 {
				return next
			}
			// Move to next level
			level--
		}
	}
}

// findLessThan returns the last node with key < given key.
// Returns nil if no such node exists (key is smaller than all keys).
func (sl *SkipList) findLessThan(key []byte) *skipNode {
	x := sl.head
	level := int(atomic.LoadInt32(&sl.maxHeight)) - 1

	for {
		next := x.getNext(level)
		if next != nil && sl.compare(next.key, key) < 0 {
			x = next
		} else {
			if level == 0 {
				if x == sl.head {
					return nil
				}
				return x
			}
			level--
		}
	}
}

// findLast returns the last node in the list.
// Returns nil if the list is empty.
func (sl *SkipList) findLast() *skipNode {
	x := sl.head
	level := int(atomic.LoadInt32(&sl.maxHeight)) - 1

	for {
		next := x.getNext(level)
		if next != nil {
			x = next
		} else {
			if level == 0 {
				if x == sl.head {
					return nil
				}
				return x
			}
			level--
		}
	}
}

// randomHeight generates a random height for a new node.
func (sl *SkipList) randomHeight() int {
	height := 1
	for height < sl.kMaxHeight {
		if sl.rng.Uint32() < sl.kScaledInvB {
			height++
		} else {
			break
		}
	}
	return height
}

// Iterator provides iteration over the skip list.
type Iterator struct {
	list *SkipList
	node *skipNode
}

// NewIterator creates a new iterator over the skip list.
// The iterator is not valid until a Seek method is called.
func (sl *SkipList) NewIterator() *Iterator {
	return &Iterator{list: sl}
}

// Valid returns true if the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool {
	return it.node != nil
}

// Key returns the key at the current position.
// REQUIRES: Valid()
func (it *Iterator) Key() []byte {
	if it.node == nil {
		return nil
	}
	return it.node.key
}

// Next advances to the next position.
// REQUIRES: Valid()
func (it *Iterator) Next() {
	if it.node == nil {
		return
	}
	it.node = it.node.getNext(0)
}

// Prev moves to the previous position.
// REQUIRES: Valid()
func (it *Iterator) Prev() {
	if it.node == nil {
		return
	}
	// We need to find the node before current
	it.node = it.list.findLessThan(it.node.key)
}

// Seek positions the iterator at the first entry with key >= target.
func (it *Iterator) Seek(target []byte) {
	it.node = it.list.findGreaterOrEqual(target, nil)
}

// SeekForPrev positions the iterator at the last entry with key <= target.
func (it *Iterator) SeekForPrev(target []byte) {
	it.Seek(target)
	if !it.Valid() {
		// All keys are < target, go to last
		it.SeekToLast()
	} else if it.list.compare(it.node.key, target) > 0 {
		// Positioned at key > target, move back
		it.Prev()
	}
	// else: positioned at key == target, which is correct
}

// SeekToFirst positions the iterator at the first entry.
func (it *Iterator) SeekToFirst() {
	it.node = it.list.head.getNext(0)
}

// SeekToLast positions the iterator at the last entry.
func (it *Iterator) SeekToLast() {
	it.node = it.list.findLast()
}

// Size returns the approximate memory usage of the node.
func (n *skipNode) Size() int {
	if n == nil {
		return 0
	}
	// Key bytes + slice header + pointer array
	return len(n.key) + int(unsafe.Sizeof([]byte{})) + len(n.next)*int(unsafe.Sizeof(&atomic.Pointer[skipNode]{}))
}
