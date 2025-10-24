package rangedel

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

func TestFragmenterEmpty(t *testing.T) {
	f := NewFragmenter()
	result := f.Finish()

	if !result.IsEmpty() {
		t.Error("empty fragmenter should produce empty list")
	}
}

func TestFragmenterSingleTombstone(t *testing.T) {
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("c"), 10)
	result := f.Finish()

	if result.Len() != 1 {
		t.Fatalf("expected 1 fragment, got %d", result.Len())
	}

	frag := result.Get(0)
	if string(frag.StartKey) != "a" || string(frag.EndKey) != "c" {
		t.Errorf("wrong range: got [%s, %s), want [a, c)", frag.StartKey, frag.EndKey)
	}
	if frag.SequenceNum != 10 {
		t.Errorf("SequenceNum = %d, want 10", frag.SequenceNum)
	}
}

func TestFragmenterNonOverlapping(t *testing.T) {
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("c"), 10)
	f.Add([]byte("e"), []byte("g"), 20)
	result := f.Finish()

	if result.Len() != 2 {
		t.Fatalf("expected 2 fragments, got %d", result.Len())
	}

	// First fragment
	if string(result.Get(0).StartKey) != "a" {
		t.Errorf("first fragment start = %q, want a", result.Get(0).StartKey)
	}
	if result.Get(0).SequenceNum != 10 {
		t.Errorf("first fragment seq = %d, want 10", result.Get(0).SequenceNum)
	}

	// Second fragment
	if string(result.Get(1).StartKey) != "e" {
		t.Errorf("second fragment start = %q, want e", result.Get(1).StartKey)
	}
	if result.Get(1).SequenceNum != 20 {
		t.Errorf("second fragment seq = %d, want 20", result.Get(1).SequenceNum)
	}
}

func TestFragmenterOverlapping(t *testing.T) {
	// Overlapping tombstones:
	// T1: [a, d) seq=10
	// T2: [b, e) seq=20
	//
	// After fragmentation:
	// [a, b) seq=10 (only T1)
	// [b, d) seq=20 (max of T1=10, T2=20)
	// [d, e) seq=20 (only T2)
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("d"), 10)
	f.Add([]byte("b"), []byte("e"), 20)
	result := f.Finish()

	if result.Len() != 3 {
		t.Fatalf("expected 3 fragments, got %d", result.Len())
	}

	// Fragment [a, b) - only T1
	frag0 := result.Get(0)
	if string(frag0.StartKey) != "a" || string(frag0.EndKey) != "b" {
		t.Errorf("frag0: got [%s, %s), want [a, b)", frag0.StartKey, frag0.EndKey)
	}
	if frag0.SequenceNum != 10 {
		t.Errorf("frag0 seq = %d, want 10", frag0.SequenceNum)
	}

	// Fragment [b, d) - both T1 and T2, max is 20
	frag1 := result.Get(1)
	if string(frag1.StartKey) != "b" || string(frag1.EndKey) != "d" {
		t.Errorf("frag1: got [%s, %s), want [b, d)", frag1.StartKey, frag1.EndKey)
	}
	if frag1.SequenceNum != 20 {
		t.Errorf("frag1 seq = %d, want 20 (max of overlapping)", frag1.SequenceNum)
	}

	// Fragment [d, e) - only T2
	frag2 := result.Get(2)
	if string(frag2.StartKey) != "d" || string(frag2.EndKey) != "e" {
		t.Errorf("frag2: got [%s, %s), want [d, e)", frag2.StartKey, frag2.EndKey)
	}
	if frag2.SequenceNum != 20 {
		t.Errorf("frag2 seq = %d, want 20", frag2.SequenceNum)
	}
}

func TestFragmenterNestedTombstones(t *testing.T) {
	// Nested tombstones:
	// T1: [a, e) seq=10
	// T2: [b, d) seq=20  (inside T1)
	//
	// After fragmentation:
	// [a, b) seq=10 (only T1)
	// [b, d) seq=20 (max of T1=10, T2=20)
	// [d, e) seq=10 (only T1)
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("e"), 10)
	f.Add([]byte("b"), []byte("d"), 20)
	result := f.Finish()

	if result.Len() != 3 {
		t.Fatalf("expected 3 fragments, got %d", result.Len())
	}

	// Check the middle fragment has the higher seq
	frag1 := result.Get(1)
	if frag1.SequenceNum != 20 {
		t.Errorf("middle fragment seq = %d, want 20", frag1.SequenceNum)
	}
}

func TestFragmenterIgnoresEmptyRanges(t *testing.T) {
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("a"), 10) // empty (start == end)
	f.Add([]byte("c"), []byte("b"), 20) // inverted
	f.Add([]byte("d"), []byte("e"), 30) // valid
	result := f.Finish()

	if result.Len() != 1 {
		t.Fatalf("expected 1 fragment (invalid ranges ignored), got %d", result.Len())
	}
}

func TestFragmentedListShouldDelete(t *testing.T) {
	f := NewFragmenter()
	f.Add([]byte("b"), []byte("e"), 100)
	result := f.Finish()

	testCases := []struct {
		key    string
		seq    dbformat.SequenceNumber
		delete bool
	}{
		{"a", 50, false},  // before range
		{"b", 50, true},   // in range, seq < tombstone
		{"c", 50, true},   // in range, seq < tombstone
		{"c", 99, true},   // in range, seq < tombstone
		{"c", 100, false}, // in range, seq == tombstone (not deleted)
		{"c", 150, false}, // in range, seq > tombstone (not deleted)
		{"e", 50, false},  // at end (exclusive)
		{"f", 50, false},  // after range
	}

	for _, tc := range testCases {
		got := result.ShouldDelete([]byte(tc.key), tc.seq)
		if got != tc.delete {
			t.Errorf("ShouldDelete(%q, %d) = %v, want %v", tc.key, tc.seq, got, tc.delete)
		}
	}
}

func TestFragmentedListShouldDeleteMultipleFragments(t *testing.T) {
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("c"), 100)
	f.Add([]byte("e"), []byte("g"), 200)
	result := f.Finish()

	// Key in first range
	if !result.ShouldDelete([]byte("b"), 50) {
		t.Error("key 'b' seq=50 should be deleted by first tombstone")
	}

	// Key in second range
	if !result.ShouldDelete([]byte("f"), 150) {
		t.Error("key 'f' seq=150 should be deleted by second tombstone")
	}

	// Key between ranges
	if result.ShouldDelete([]byte("d"), 50) {
		t.Error("key 'd' should not be deleted (between ranges)")
	}
}

func TestFragmentedListContainsRange(t *testing.T) {
	f := NewFragmenter()
	f.Add([]byte("c"), []byte("f"), 100)
	result := f.Finish()

	testCases := []struct {
		start, end string
		contains   bool
	}{
		{"a", "b", false}, // entirely before
		{"g", "h", false}, // entirely after
		{"a", "d", true},  // overlaps start
		{"d", "g", true},  // overlaps end
		{"d", "e", true},  // entirely within
		{"a", "z", true},  // encompasses
		{"a", "c", false}, // adjacent at start (no overlap)
		{"f", "g", false}, // adjacent at end (no overlap)
	}

	for _, tc := range testCases {
		got := result.ContainsRange([]byte(tc.start), []byte(tc.end))
		if got != tc.contains {
			t.Errorf("ContainsRange(%s, %s) = %v, want %v",
				tc.start, tc.end, got, tc.contains)
		}
	}
}

func TestFragmentedListMaxSequenceNum(t *testing.T) {
	f := NewFragmenter()
	result := f.Finish()

	// Empty list
	if result.MaxSequenceNum() != 0 {
		t.Error("empty list should have max seq 0")
	}

	f.Add([]byte("a"), []byte("b"), 100)
	f.Add([]byte("c"), []byte("d"), 200)
	f.Add([]byte("e"), []byte("f"), 150)
	result = f.Finish()

	if got := result.MaxSequenceNum(); got != 200 {
		t.Errorf("MaxSequenceNum = %d, want 200", got)
	}
}

func TestFragmenterClear(t *testing.T) {
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("b"), 10)
	f.Add([]byte("c"), []byte("d"), 20)

	if f.Len() != 2 {
		t.Errorf("Len before clear = %d, want 2", f.Len())
	}

	f.Clear()

	if f.Len() != 0 {
		t.Errorf("Len after clear = %d, want 0", f.Len())
	}

	result := f.Finish()
	if !result.IsEmpty() {
		t.Error("result after clear should be empty")
	}
}

func TestFragmenterAddTombstone(t *testing.T) {
	f := NewFragmenter()

	// Add via tombstone object
	tomb := NewRangeTombstone([]byte("a"), []byte("c"), 100)
	f.AddTombstone(tomb)

	// Modify original - should not affect fragmenter
	tomb.SequenceNum = 999

	result := f.Finish()
	if result.Get(0).SequenceNum != 100 {
		t.Errorf("tombstone was not cloned: got seq %d, want 100", result.Get(0).SequenceNum)
	}
}

func TestFragmenterComplexOverlaps(t *testing.T) {
	// Complex overlapping scenario:
	// T1: [a, g) seq=10
	// T2: [b, d) seq=30
	// T3: [c, f) seq=20
	//
	// Boundaries: a, b, c, d, f, g
	// Expected fragments:
	// [a, b) seq=10 (T1 only)
	// [b, c) seq=30 (T1=10, T2=30, max=30)
	// [c, d) seq=30 (T1=10, T2=30, T3=20, max=30)
	// [d, f) seq=20 (T1=10, T3=20, max=20)
	// [f, g) seq=10 (T1 only)
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("g"), 10)
	f.Add([]byte("b"), []byte("d"), 30)
	f.Add([]byte("c"), []byte("f"), 20)
	result := f.Finish()

	if result.Len() != 5 {
		t.Fatalf("expected 5 fragments, got %d", result.Len())
	}

	expectedSeqs := []dbformat.SequenceNumber{10, 30, 30, 20, 10}
	for i, expectedSeq := range expectedSeqs {
		if result.Get(i).SequenceNum != expectedSeq {
			t.Errorf("fragment %d seq = %d, want %d",
				i, result.Get(i).SequenceNum, expectedSeq)
		}
	}
}
