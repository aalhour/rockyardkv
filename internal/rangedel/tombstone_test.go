package rangedel

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

func TestRangeTombstoneContains(t *testing.T) {
	tomb := NewRangeTombstone([]byte("b"), []byte("e"), 100)

	testCases := []struct {
		key  string
		want bool
	}{
		{"a", false}, // before range
		{"b", true},  // start (inclusive)
		{"c", true},  // middle
		{"d", true},  // middle
		{"e", false}, // end (exclusive)
		{"f", false}, // after range
	}

	for _, tc := range testCases {
		got := tomb.Contains([]byte(tc.key))
		if got != tc.want {
			t.Errorf("Contains(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestRangeTombstoneCovers(t *testing.T) {
	tomb := NewRangeTombstone([]byte("b"), []byte("e"), 100)

	testCases := []struct {
		key    string
		seqNum dbformat.SequenceNumber
		want   bool
	}{
		{"c", 50, true},   // in range, lower seq
		{"c", 99, true},   // in range, seq just below
		{"c", 100, false}, // in range, same seq (not covered)
		{"c", 150, false}, // in range, higher seq
		{"a", 50, false},  // out of range
		{"e", 50, false},  // out of range (end exclusive)
	}

	for _, tc := range testCases {
		got := tomb.Covers([]byte(tc.key), tc.seqNum)
		if got != tc.want {
			t.Errorf("Covers(%q, %d) = %v, want %v", tc.key, tc.seqNum, got, tc.want)
		}
	}
}

func TestRangeTombstoneIsEmpty(t *testing.T) {
	testCases := []struct {
		start, end string
		empty      bool
	}{
		{"a", "b", false}, // valid range
		{"a", "a", true},  // empty (start == end)
		{"b", "a", true},  // inverted (start > end)
	}

	for _, tc := range testCases {
		tomb := NewRangeTombstone([]byte(tc.start), []byte(tc.end), 1)
		if got := tomb.IsEmpty(); got != tc.empty {
			t.Errorf("IsEmpty([%s, %s)) = %v, want %v", tc.start, tc.end, got, tc.empty)
		}
	}
}

func TestRangeTombstoneOverlaps(t *testing.T) {
	testCases := []struct {
		t1Start, t1End string
		t2Start, t2End string
		overlaps       bool
	}{
		{"a", "c", "b", "d", true},  // partial overlap
		{"a", "c", "c", "e", false}, // adjacent (no overlap)
		{"a", "e", "b", "d", true},  // t1 contains t2
		{"b", "d", "a", "e", true},  // t2 contains t1
		{"a", "b", "c", "d", false}, // disjoint
		{"c", "d", "a", "b", false}, // disjoint (reversed)
	}

	for _, tc := range testCases {
		t1 := NewRangeTombstone([]byte(tc.t1Start), []byte(tc.t1End), 1)
		t2 := NewRangeTombstone([]byte(tc.t2Start), []byte(tc.t2End), 1)
		if got := t1.Overlaps(t2); got != tc.overlaps {
			t.Errorf("[%s,%s).Overlaps([%s,%s)) = %v, want %v",
				tc.t1Start, tc.t1End, tc.t2Start, tc.t2End, got, tc.overlaps)
		}
	}
}

func TestRangeTombstoneCompare(t *testing.T) {
	t1 := NewRangeTombstone([]byte("a"), []byte("c"), 100)
	t2 := NewRangeTombstone([]byte("a"), []byte("d"), 50)
	t3 := NewRangeTombstone([]byte("b"), []byte("d"), 100)

	// Same start key, higher seq comes first
	if cmp := t1.Compare(t2); cmp >= 0 {
		t.Errorf("t1.Compare(t2) = %d, want < 0 (higher seq first)", cmp)
	}

	// Different start keys, ordered by start key
	if cmp := t1.Compare(t3); cmp >= 0 {
		t.Errorf("t1.Compare(t3) = %d, want < 0 (a < b)", cmp)
	}
}

func TestRangeTombstoneClone(t *testing.T) {
	original := NewRangeTombstone([]byte("start"), []byte("end"), 42)
	clone := original.Clone()

	// Modify original
	original.StartKey[0] = 'X'
	original.SequenceNum = 999

	// Clone should be unaffected
	if string(clone.StartKey) != "start" {
		t.Errorf("Clone StartKey modified: got %q", clone.StartKey)
	}
	if clone.SequenceNum != 42 {
		t.Errorf("Clone SequenceNum modified: got %d", clone.SequenceNum)
	}
}

func TestRangeTombstoneInternalKey(t *testing.T) {
	tomb := NewRangeTombstone([]byte("key"), []byte("end"), 100)
	ikey := tomb.InternalKey()

	// Parse the internal key
	parsed, err := dbformat.ParseInternalKey(ikey)
	if err != nil {
		t.Fatalf("Failed to parse internal key: %v", err)
	}

	if string(parsed.UserKey) != "key" {
		t.Errorf("UserKey = %q, want %q", parsed.UserKey, "key")
	}
	if parsed.Sequence != 100 {
		t.Errorf("Sequence = %d, want 100", parsed.Sequence)
	}
	if parsed.Type != dbformat.TypeRangeDeletion {
		t.Errorf("Type = %d, want TypeRangeDeletion", parsed.Type)
	}
}

func TestTombstoneListBasic(t *testing.T) {
	list := NewTombstoneList()

	if !list.IsEmpty() {
		t.Error("new list should be empty")
	}
	if list.Len() != 0 {
		t.Errorf("Len = %d, want 0", list.Len())
	}

	// Add tombstones
	list.AddRange([]byte("a"), []byte("c"), 10)
	list.AddRange([]byte("e"), []byte("g"), 20)

	if list.IsEmpty() {
		t.Error("list should not be empty")
	}
	if list.Len() != 2 {
		t.Errorf("Len = %d, want 2", list.Len())
	}

	// Get by index
	if list.Get(0).SequenceNum != 10 {
		t.Error("Get(0) wrong tombstone")
	}
	if list.Get(1).SequenceNum != 20 {
		t.Error("Get(1) wrong tombstone")
	}
	if list.Get(-1) != nil {
		t.Error("Get(-1) should return nil")
	}
	if list.Get(2) != nil {
		t.Error("Get(2) should return nil")
	}
}

func TestTombstoneListContainsKey(t *testing.T) {
	list := NewTombstoneList()
	list.AddRange([]byte("a"), []byte("c"), 10)
	list.AddRange([]byte("e"), []byte("g"), 20)

	testCases := []struct {
		key  string
		want bool
	}{
		{"b", true},  // in first range
		{"f", true},  // in second range
		{"d", false}, // between ranges
		{"z", false}, // after all ranges
	}

	for _, tc := range testCases {
		if got := list.ContainsKey([]byte(tc.key)); got != tc.want {
			t.Errorf("ContainsKey(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestTombstoneListMaxSequenceNum(t *testing.T) {
	list := NewTombstoneList()

	// Empty list
	if list.MaxSequenceNum() != 0 {
		t.Error("empty list should have max seq 0")
	}

	list.AddRange([]byte("a"), []byte("b"), 10)
	list.AddRange([]byte("c"), []byte("d"), 30)
	list.AddRange([]byte("e"), []byte("f"), 20)

	if got := list.MaxSequenceNum(); got != 30 {
		t.Errorf("MaxSequenceNum = %d, want 30", got)
	}
}

func TestTombstoneListClear(t *testing.T) {
	list := NewTombstoneList()
	list.AddRange([]byte("a"), []byte("b"), 10)
	list.AddRange([]byte("c"), []byte("d"), 20)

	list.Clear()

	if !list.IsEmpty() {
		t.Error("list should be empty after Clear")
	}
	if list.Len() != 0 {
		t.Errorf("Len = %d, want 0", list.Len())
	}
}

func TestTombstoneListAll(t *testing.T) {
	list := NewTombstoneList()
	list.AddRange([]byte("a"), []byte("b"), 10)
	list.AddRange([]byte("c"), []byte("d"), 20)

	all := list.All()
	if len(all) != 2 {
		t.Errorf("All() returned %d items, want 2", len(all))
	}
}
