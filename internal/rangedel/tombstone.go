// Package rangedel implements range deletion (DeleteRange) support.
//
// Range deletions in RocksDB work by storing "tombstones" that mark a range
// of keys as deleted. When reading, keys covered by tombstones are skipped.
// During compaction, keys covered by tombstones can be dropped.
//
// Key concepts:
// - RangeTombstone: A single [startKey, endKey) range with a sequence number
// - FragmentedRangeTombstoneList: Non-overlapping tombstones for efficient lookup
// - RangeDelAggregator: Combines tombstones from multiple levels for reads
//
// Reference: RocksDB db/range_del_aggregator.h, db/range_tombstone_fragmenter.h
package rangedel

import (
	"bytes"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

// RangeTombstone represents a range deletion covering [StartKey, EndKey).
// The start key is inclusive and the end key is exclusive.
type RangeTombstone struct {
	// StartKey is the inclusive start of the deleted range (user key).
	StartKey []byte

	// EndKey is the exclusive end of the deleted range (user key).
	EndKey []byte

	// SequenceNum is the sequence number when this tombstone was created.
	// Keys with sequence numbers less than this are deleted by this tombstone.
	SequenceNum dbformat.SequenceNumber
}

// NewRangeTombstone creates a new range tombstone.
func NewRangeTombstone(startKey, endKey []byte, seqNum dbformat.SequenceNumber) *RangeTombstone {
	return &RangeTombstone{
		StartKey:    append([]byte(nil), startKey...),
		EndKey:      append([]byte(nil), endKey...),
		SequenceNum: seqNum,
	}
}

// Contains returns true if the given user key falls within [StartKey, EndKey).
func (t *RangeTombstone) Contains(userKey []byte) bool {
	return bytes.Compare(userKey, t.StartKey) >= 0 && bytes.Compare(userKey, t.EndKey) < 0
}

// Covers returns true if this tombstone deletes the given key at the given sequence number.
// A tombstone covers a key if:
// 1. The key is within [StartKey, EndKey)
// 2. The key's sequence number is less than the tombstone's sequence number
func (t *RangeTombstone) Covers(userKey []byte, keySeqNum dbformat.SequenceNumber) bool {
	return t.Contains(userKey) && keySeqNum < t.SequenceNum
}

// IsEmpty returns true if this is an empty range (start >= end).
func (t *RangeTombstone) IsEmpty() bool {
	return bytes.Compare(t.StartKey, t.EndKey) >= 0
}

// Overlaps returns true if this tombstone overlaps with another.
func (t *RangeTombstone) Overlaps(other *RangeTombstone) bool {
	// Two ranges [a, b) and [c, d) overlap if a < d && c < b
	return bytes.Compare(t.StartKey, other.EndKey) < 0 &&
		bytes.Compare(other.StartKey, t.EndKey) < 0
}

// Clone returns a deep copy of the tombstone.
func (t *RangeTombstone) Clone() *RangeTombstone {
	return NewRangeTombstone(t.StartKey, t.EndKey, t.SequenceNum)
}

// Compare compares two tombstones by start key, then by sequence number (descending).
// This ordering is used for the fragmented list.
func (t *RangeTombstone) Compare(other *RangeTombstone) int {
	cmp := bytes.Compare(t.StartKey, other.StartKey)
	if cmp != 0 {
		return cmp
	}
	// Higher sequence number comes first
	if t.SequenceNum > other.SequenceNum {
		return -1
	}
	if t.SequenceNum < other.SequenceNum {
		return 1
	}
	return 0
}

// InternalKey returns the internal key representation of this tombstone's start key.
// The internal key format is: userKey + (seqNum << 8 | TypeRangeDeletion)
func (t *RangeTombstone) InternalKey() dbformat.InternalKey {
	return dbformat.NewInternalKey(t.StartKey, t.SequenceNum, dbformat.TypeRangeDeletion)
}

// TombstoneList is a simple list of range tombstones.
// This is used before fragmentation.
type TombstoneList struct {
	tombstones []*RangeTombstone
}

// NewTombstoneList creates an empty tombstone list.
func NewTombstoneList() *TombstoneList {
	return &TombstoneList{
		tombstones: make([]*RangeTombstone, 0),
	}
}

// Add adds a tombstone to the list.
func (l *TombstoneList) Add(t *RangeTombstone) {
	l.tombstones = append(l.tombstones, t)
}

// AddRange adds a new range tombstone with the given bounds and sequence number.
func (l *TombstoneList) AddRange(startKey, endKey []byte, seqNum dbformat.SequenceNumber) {
	l.Add(NewRangeTombstone(startKey, endKey, seqNum))
}

// Len returns the number of tombstones.
func (l *TombstoneList) Len() int {
	return len(l.tombstones)
}

// Get returns the tombstone at the given index.
func (l *TombstoneList) Get(i int) *RangeTombstone {
	if i < 0 || i >= len(l.tombstones) {
		return nil
	}
	return l.tombstones[i]
}

// IsEmpty returns true if the list has no tombstones.
func (l *TombstoneList) IsEmpty() bool {
	return len(l.tombstones) == 0
}

// Clear removes all tombstones from the list.
func (l *TombstoneList) Clear() {
	l.tombstones = l.tombstones[:0]
}

// All returns all tombstones in the list.
func (l *TombstoneList) All() []*RangeTombstone {
	return l.tombstones
}

// ContainsKey returns true if any tombstone in the list contains the given key.
// This is a linear scan - use FragmentedRangeTombstoneList for efficient lookups.
func (l *TombstoneList) ContainsKey(userKey []byte) bool {
	for _, t := range l.tombstones {
		if t.Contains(userKey) {
			return true
		}
	}
	return false
}

// MaxSequenceNum returns the maximum sequence number among all tombstones.
func (l *TombstoneList) MaxSequenceNum() dbformat.SequenceNumber {
	var maxSeq dbformat.SequenceNumber
	for _, t := range l.tombstones {
		if t.SequenceNum > maxSeq {
			maxSeq = t.SequenceNum
		}
	}
	return maxSeq
}
