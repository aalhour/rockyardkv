package rangedel

import (
	"github.com/aalhour/rockyardkv/internal/dbformat"
)

// RangeDelAggregator combines range tombstones from multiple sources
// (memtable, L0 files, compacting files) to efficiently check if a key
// should be deleted.
//
// The aggregator maintains per-level tombstone lists. When checking a key,
// it consults all levels to find the tombstone with the highest sequence
// number that covers the key.
//
// Reference: RocksDB db/range_del_aggregator.h
type RangeDelAggregator struct {
	// tombstones holds fragmented tombstone lists, indexed by level.
	// Level -1 (index 0) is for memtable tombstones.
	// Levels 0-6 are indexed 1-7.
	tombstones []*FragmentedRangeTombstoneList

	// upperBound is the snapshot sequence number.
	// Tombstones with seq > upperBound are invisible.
	upperBound dbformat.SequenceNumber

	// numLevels is the number of levels including memtable
	numLevels int
}

// MaxLevels is the maximum number of levels supported.
const MaxLevels = 8 // memtable + 7 SST levels

// NewRangeDelAggregator creates a new aggregator.
// The upperBound is the snapshot sequence number - tombstones with seq > upperBound
// are not visible (for snapshot isolation).
func NewRangeDelAggregator(upperBound dbformat.SequenceNumber) *RangeDelAggregator {
	return &RangeDelAggregator{
		tombstones: make([]*FragmentedRangeTombstoneList, MaxLevels),
		upperBound: upperBound,
		numLevels:  MaxLevels,
	}
}

// AddTombstones adds a fragmented tombstone list for the given level.
// Use level -1 for memtable tombstones, 0-6 for SST levels.
func (a *RangeDelAggregator) AddTombstones(level int, list *FragmentedRangeTombstoneList) {
	if list == nil || list.IsEmpty() {
		return
	}
	idx := a.levelToIndex(level)
	if idx < 0 || idx >= len(a.tombstones) {
		return
	}
	// Merge with any existing tombstones for this level.
	//
	// Important: during point lookups we may add tombstones from multiple files in the
	// same level (especially L0). Overwriting would drop previously-added tombstones
	// and can incorrectly resurrect keys that should be deleted.
	if existing := a.tombstones[idx]; existing != nil && !existing.IsEmpty() {
		f := NewFragmenter()
		for _, t := range existing.All() {
			f.AddTombstone(t)
		}
		for _, t := range list.All() {
			f.AddTombstone(t)
		}
		a.tombstones[idx] = f.Finish()
		return
	}
	a.tombstones[idx] = list
}

// AddTombstoneList adds a raw tombstone list (will be fragmented).
func (a *RangeDelAggregator) AddTombstoneList(level int, list *TombstoneList) {
	if list == nil || list.IsEmpty() {
		return
	}

	f := NewFragmenter()
	for _, t := range list.All() {
		f.AddTombstone(t)
	}
	a.AddTombstones(level, f.Finish())
}

// ShouldDelete returns true if the given key at the given sequence number
// should be deleted (covered by a range tombstone).
//
// The check considers:
// 1. The key must be within a tombstone's range
// 2. The key's sequence number must be less than the tombstone's
// 3. The tombstone's sequence number must be <= upperBound (visible)
func (a *RangeDelAggregator) ShouldDelete(userKey []byte, keySeqNum dbformat.SequenceNumber) bool {
	for _, list := range a.tombstones {
		if list == nil || list.IsEmpty() {
			continue
		}

		// Find the tombstone that might cover this key
		idx := list.searchForKey(userKey)
		if idx < 0 || idx >= list.Len() {
			continue
		}

		fragment := list.Get(idx)
		if !fragment.Contains(userKey) {
			continue
		}

		// Check visibility: tombstone must be visible (seq <= upperBound)
		if fragment.SequenceNum > a.upperBound {
			continue
		}

		// Check coverage: tombstone must have higher seq than key
		if keySeqNum < fragment.SequenceNum {
			return true
		}
	}
	return false
}

// ShouldDeleteKey is a convenience method that extracts the user key
// and sequence number from an internal key.
func (a *RangeDelAggregator) ShouldDeleteKey(internalKey []byte) bool {
	if len(internalKey) < dbformat.NumInternalBytes {
		return false
	}

	userKey := dbformat.ExtractUserKey(internalKey)
	seqNum := dbformat.ExtractSequenceNumber(internalKey)
	return a.ShouldDelete(userKey, seqNum)
}

// GetMaxCoveringTombstoneSeqNum returns the highest sequence number
// of any tombstone covering the given key, or 0 if no tombstone covers it.
func (a *RangeDelAggregator) GetMaxCoveringTombstoneSeqNum(userKey []byte) dbformat.SequenceNumber {
	var maxSeq dbformat.SequenceNumber
	for _, list := range a.tombstones {
		if list == nil || list.IsEmpty() {
			continue
		}

		idx := list.searchForKey(userKey)
		if idx < 0 || idx >= list.Len() {
			continue
		}

		fragment := list.Get(idx)
		if !fragment.Contains(userKey) {
			continue
		}

		// Check visibility
		if fragment.SequenceNum > a.upperBound {
			continue
		}

		if fragment.SequenceNum > maxSeq {
			maxSeq = fragment.SequenceNum
		}
	}
	return maxSeq
}

// IsEmpty returns true if no tombstones have been added.
func (a *RangeDelAggregator) IsEmpty() bool {
	for _, list := range a.tombstones {
		if list != nil && !list.IsEmpty() {
			return false
		}
	}
	return true
}

// NumTombstones returns the total number of tombstone fragments across all levels.
func (a *RangeDelAggregator) NumTombstones() int {
	count := 0
	for _, list := range a.tombstones {
		if list != nil {
			count += list.Len()
		}
	}
	return count
}

// Clear removes all tombstones from the aggregator.
func (a *RangeDelAggregator) Clear() {
	for i := range a.tombstones {
		a.tombstones[i] = nil
	}
}

// levelToIndex converts a level number to an index in the tombstones slice.
// Level -1 (memtable) -> index 0
// Level 0 -> index 1
// Level 1 -> index 2, etc.
func (a *RangeDelAggregator) levelToIndex(level int) int {
	return level + 1
}

// UpperBound returns the snapshot sequence number.
func (a *RangeDelAggregator) UpperBound() dbformat.SequenceNumber {
	return a.upperBound
}

// SetUpperBound updates the snapshot sequence number.
func (a *RangeDelAggregator) SetUpperBound(seq dbformat.SequenceNumber) {
	a.upperBound = seq
}

// ReadRangeDelAggregator is a specialized aggregator for read operations.
// It wraps RangeDelAggregator with additional read-path optimizations.
type ReadRangeDelAggregator struct {
	*RangeDelAggregator
}

// NewReadRangeDelAggregator creates a new read aggregator.
func NewReadRangeDelAggregator(upperBound dbformat.SequenceNumber) *ReadRangeDelAggregator {
	return &ReadRangeDelAggregator{
		RangeDelAggregator: NewRangeDelAggregator(upperBound),
	}
}

// CompactionRangeDelAggregator is a specialized aggregator for compaction.
// It tracks which tombstones can be dropped during compaction.
type CompactionRangeDelAggregator struct {
	*RangeDelAggregator

	// earliestSnapshot is the earliest snapshot that's still active.
	// Tombstones with seq < earliestSnapshot can potentially be dropped.
	earliestSnapshot dbformat.SequenceNumber
}

// NewCompactionRangeDelAggregator creates a new compaction aggregator.
func NewCompactionRangeDelAggregator(earliestSnapshot dbformat.SequenceNumber) *CompactionRangeDelAggregator {
	return &CompactionRangeDelAggregator{
		RangeDelAggregator: NewRangeDelAggregator(dbformat.MaxSequenceNumber),
		earliestSnapshot:   earliestSnapshot,
	}
}

// ShouldDropKey returns true if a key can be dropped during compaction
// because it's covered by a range tombstone and there are no snapshots
// that might need to see it.
func (c *CompactionRangeDelAggregator) ShouldDropKey(userKey []byte, keySeqNum dbformat.SequenceNumber) bool {
	// Get the max covering tombstone
	maxCoveringSeq := c.GetMaxCoveringTombstoneSeqNum(userKey)
	if maxCoveringSeq == 0 {
		return false // No tombstone covers this key
	}

	// Key can be dropped if:
	// 1. It's covered by a tombstone (keySeqNum < maxCoveringSeq)
	// 2. Both key and tombstone are older than earliest snapshot
	if keySeqNum >= maxCoveringSeq {
		return false // Key is newer than tombstone
	}

	// If both key and tombstone are below earliest snapshot,
	// no snapshot needs to see this key
	if keySeqNum < c.earliestSnapshot && maxCoveringSeq <= c.earliestSnapshot {
		return true
	}

	return false
}
