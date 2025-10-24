// fragmenter.go implements range tombstone fragmentation.
//
// Fragmentation converts overlapping range tombstones into non-overlapping
// fragments, each with the maximum sequence number that covers it.
//
// Reference: RocksDB v10.7.5
//   - db/range_tombstone_fragmenter.h
//   - db/range_tombstone_fragmenter.cc
package rangedel

import (
	"bytes"
	"sort"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

// FragmentedRangeTombstoneList holds a list of non-overlapping range tombstones.
// After fragmentation, tombstones are guaranteed to be:
// 1. Non-overlapping
// 2. Sorted by start key
// 3. Each fragment has the maximum sequence number that covers it
//
// This allows for efficient binary search when checking if a key is deleted.
type FragmentedRangeTombstoneList struct {
	fragments []*RangeTombstone
}

// NewFragmentedRangeTombstoneList creates an empty fragmented list.
func NewFragmentedRangeTombstoneList() *FragmentedRangeTombstoneList {
	return &FragmentedRangeTombstoneList{
		fragments: make([]*RangeTombstone, 0),
	}
}

// Len returns the number of fragments.
func (f *FragmentedRangeTombstoneList) Len() int {
	return len(f.fragments)
}

// IsEmpty returns true if there are no fragments.
func (f *FragmentedRangeTombstoneList) IsEmpty() bool {
	return len(f.fragments) == 0
}

// Get returns the fragment at the given index.
func (f *FragmentedRangeTombstoneList) Get(i int) *RangeTombstone {
	if i < 0 || i >= len(f.fragments) {
		return nil
	}
	return f.fragments[i]
}

// All returns all fragments.
func (f *FragmentedRangeTombstoneList) All() []*RangeTombstone {
	return f.fragments
}

// ShouldDelete returns true if the given key at the given sequence number
// is covered by a range tombstone (and should be deleted/skipped).
func (f *FragmentedRangeTombstoneList) ShouldDelete(userKey []byte, keySeqNum dbformat.SequenceNumber) bool {
	// Binary search for the fragment that might contain this key
	idx := f.searchForKey(userKey)
	if idx < 0 || idx >= len(f.fragments) {
		return false
	}

	fragment := f.fragments[idx]
	return fragment.Covers(userKey, keySeqNum)
}

// searchForKey finds the fragment that might contain the given key.
// Returns the index of the fragment with the largest start key <= userKey,
// or -1 if no such fragment exists.
func (f *FragmentedRangeTombstoneList) searchForKey(userKey []byte) int {
	if len(f.fragments) == 0 {
		return -1
	}

	// Binary search for the rightmost fragment with start key <= userKey
	idx := sort.Search(len(f.fragments), func(i int) bool {
		return bytes.Compare(f.fragments[i].StartKey, userKey) > 0
	})

	// idx is the first fragment with start key > userKey
	// We want the one before it (with start key <= userKey)
	return idx - 1
}

// MaxSequenceNum returns the maximum sequence number among all fragments.
func (f *FragmentedRangeTombstoneList) MaxSequenceNum() dbformat.SequenceNumber {
	var maxSeq dbformat.SequenceNumber
	for _, frag := range f.fragments {
		if frag.SequenceNum > maxSeq {
			maxSeq = frag.SequenceNum
		}
	}
	return maxSeq
}

// ContainsRange returns true if any fragment overlaps with the given range.
// This is useful for checking if a compaction input overlaps with tombstones.
func (f *FragmentedRangeTombstoneList) ContainsRange(startKey, endKey []byte) bool {
	if len(f.fragments) == 0 {
		return false
	}

	// Find fragments that overlap with [startKey, endKey)
	for _, frag := range f.fragments {
		// Check if [frag.StartKey, frag.EndKey) overlaps with [startKey, endKey)
		if bytes.Compare(frag.StartKey, endKey) < 0 && bytes.Compare(startKey, frag.EndKey) < 0 {
			return true
		}
	}
	return false
}

// Fragmenter takes a list of potentially overlapping tombstones and
// produces a FragmentedRangeTombstoneList with non-overlapping fragments.
//
// The fragmentation algorithm:
// 1. Collect all unique boundary points (start/end keys)
// 2. Sort boundary points
// 3. For each adjacent pair of boundaries, create a fragment
// 4. Assign the maximum sequence number from overlapping tombstones
type Fragmenter struct {
	tombstones []*RangeTombstone
}

// NewFragmenter creates a new fragmenter.
func NewFragmenter() *Fragmenter {
	return &Fragmenter{
		tombstones: make([]*RangeTombstone, 0),
	}
}

// Add adds a tombstone to be fragmented.
func (f *Fragmenter) Add(startKey, endKey []byte, seqNum dbformat.SequenceNumber) {
	if bytes.Compare(startKey, endKey) >= 0 {
		// Empty or invalid range, skip
		return
	}
	f.tombstones = append(f.tombstones, NewRangeTombstone(startKey, endKey, seqNum))
}

// AddTombstone adds an existing tombstone to be fragmented.
func (f *Fragmenter) AddTombstone(t *RangeTombstone) {
	if t.IsEmpty() {
		return
	}
	f.tombstones = append(f.tombstones, t.Clone())
}

// Finish fragments all added tombstones and returns the result.
func (f *Fragmenter) Finish() *FragmentedRangeTombstoneList {
	if len(f.tombstones) == 0 {
		return NewFragmentedRangeTombstoneList()
	}

	// Collect all boundary points
	boundaries := f.collectBoundaries()

	// Create fragments for each pair of adjacent boundaries
	result := NewFragmentedRangeTombstoneList()
	for i := range len(boundaries) - 1 {
		startKey := boundaries[i]
		endKey := boundaries[i+1]

		// Find the maximum sequence number for tombstones covering this range
		maxSeq := f.maxSeqForRange(startKey, endKey)
		if maxSeq > 0 {
			result.fragments = append(result.fragments,
				NewRangeTombstone(startKey, endKey, maxSeq))
		}
	}

	return result
}

// collectBoundaries returns all unique start/end keys, sorted.
func (f *Fragmenter) collectBoundaries() [][]byte {
	// Use a map to deduplicate
	boundarySet := make(map[string]struct{})
	for _, t := range f.tombstones {
		boundarySet[string(t.StartKey)] = struct{}{}
		boundarySet[string(t.EndKey)] = struct{}{}
	}

	// Convert to slice
	boundaries := make([][]byte, 0, len(boundarySet))
	for key := range boundarySet {
		boundaries = append(boundaries, []byte(key))
	}

	// Sort
	sort.Slice(boundaries, func(i, j int) bool {
		return bytes.Compare(boundaries[i], boundaries[j]) < 0
	})

	return boundaries
}

// maxSeqForRange finds the maximum sequence number among tombstones
// that fully cover the range [startKey, endKey).
func (f *Fragmenter) maxSeqForRange(startKey, endKey []byte) dbformat.SequenceNumber {
	var maxSeq dbformat.SequenceNumber
	for _, t := range f.tombstones {
		// A tombstone covers [startKey, endKey) if:
		// t.StartKey <= startKey AND t.EndKey >= endKey
		if bytes.Compare(t.StartKey, startKey) <= 0 && bytes.Compare(t.EndKey, endKey) >= 0 {
			if t.SequenceNum > maxSeq {
				maxSeq = t.SequenceNum
			}
		}
	}
	return maxSeq
}

// Clear removes all tombstones from the fragmenter.
func (f *Fragmenter) Clear() {
	f.tombstones = f.tombstones[:0]
}

// Len returns the number of tombstones added (before fragmentation).
func (f *Fragmenter) Len() int {
	return len(f.tombstones)
}
