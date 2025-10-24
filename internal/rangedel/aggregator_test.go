package rangedel

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

func TestRangeDelAggregatorEmpty(t *testing.T) {
	agg := NewRangeDelAggregator(1000)

	if !agg.IsEmpty() {
		t.Error("new aggregator should be empty")
	}
	if agg.NumTombstones() != 0 {
		t.Errorf("NumTombstones = %d, want 0", agg.NumTombstones())
	}

	// Should not delete anything
	if agg.ShouldDelete([]byte("any"), 50) {
		t.Error("empty aggregator should not delete anything")
	}
}

func TestRangeDelAggregatorSingleLevel(t *testing.T) {
	agg := NewRangeDelAggregator(1000)

	// Add tombstone [b, e) at seq=100
	f := NewFragmenter()
	f.Add([]byte("b"), []byte("e"), 100)
	agg.AddTombstones(0, f.Finish())

	if agg.IsEmpty() {
		t.Error("aggregator should not be empty")
	}

	testCases := []struct {
		key    string
		seq    dbformat.SequenceNumber
		delete bool
	}{
		{"a", 50, false},  // before range
		{"b", 50, true},   // in range, seq < tombstone
		{"c", 99, true},   // in range, seq < tombstone
		{"c", 100, false}, // in range, seq == tombstone
		{"c", 150, false}, // in range, seq > tombstone
		{"e", 50, false},  // at end (exclusive)
	}

	for _, tc := range testCases {
		got := agg.ShouldDelete([]byte(tc.key), tc.seq)
		if got != tc.delete {
			t.Errorf("ShouldDelete(%q, %d) = %v, want %v",
				tc.key, tc.seq, got, tc.delete)
		}
	}
}

func TestRangeDelAggregatorMultipleLevels(t *testing.T) {
	agg := NewRangeDelAggregator(1000)

	// L0: [a, c) seq=100
	f0 := NewFragmenter()
	f0.Add([]byte("a"), []byte("c"), 100)
	agg.AddTombstones(0, f0.Finish())

	// L1: [d, f) seq=200
	f1 := NewFragmenter()
	f1.Add([]byte("d"), []byte("f"), 200)
	agg.AddTombstones(1, f1.Finish())

	// Check L0 tombstone
	if !agg.ShouldDelete([]byte("b"), 50) {
		t.Error("key 'b' seq=50 should be deleted by L0 tombstone")
	}

	// Check L1 tombstone
	if !agg.ShouldDelete([]byte("e"), 150) {
		t.Error("key 'e' seq=150 should be deleted by L1 tombstone")
	}

	// Between tombstones
	if agg.ShouldDelete([]byte("c"), 50) {
		t.Error("key 'c' should not be deleted (between tombstones)")
	}
}

func TestRangeDelAggregatorSnapshotVisibility(t *testing.T) {
	// Snapshot at seq=150
	agg := NewRangeDelAggregator(150)

	// Tombstone at seq=200 (invisible to this snapshot)
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("c"), 200)
	agg.AddTombstones(0, f.Finish())

	// Tombstone at seq=100 (visible to this snapshot)
	f2 := NewFragmenter()
	f2.Add([]byte("d"), []byte("f"), 100)
	agg.AddTombstones(1, f2.Finish())

	// Key covered by invisible tombstone should NOT be deleted
	if agg.ShouldDelete([]byte("b"), 50) {
		t.Error("key 'b' should not be deleted (tombstone invisible to snapshot)")
	}

	// Key covered by visible tombstone SHOULD be deleted
	if !agg.ShouldDelete([]byte("e"), 50) {
		t.Error("key 'e' should be deleted (tombstone visible to snapshot)")
	}
}

func TestRangeDelAggregatorMemtableLevel(t *testing.T) {
	agg := NewRangeDelAggregator(1000)

	// Add memtable tombstone (level -1)
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("c"), 100)
	agg.AddTombstones(-1, f.Finish())

	if !agg.ShouldDelete([]byte("b"), 50) {
		t.Error("key should be deleted by memtable tombstone")
	}
}

func TestRangeDelAggregatorGetMaxCoveringSeq(t *testing.T) {
	agg := NewRangeDelAggregator(1000)

	// L0: [a, e) seq=100
	f0 := NewFragmenter()
	f0.Add([]byte("a"), []byte("e"), 100)
	agg.AddTombstones(0, f0.Finish())

	// L1: [b, d) seq=200
	f1 := NewFragmenter()
	f1.Add([]byte("b"), []byte("d"), 200)
	agg.AddTombstones(1, f1.Finish())

	// Key 'c' is covered by both, max is 200
	if got := agg.GetMaxCoveringTombstoneSeqNum([]byte("c")); got != 200 {
		t.Errorf("GetMaxCoveringTombstoneSeqNum('c') = %d, want 200", got)
	}

	// Key 'a' is covered only by L0 (seq=100)
	if got := agg.GetMaxCoveringTombstoneSeqNum([]byte("a")); got != 100 {
		t.Errorf("GetMaxCoveringTombstoneSeqNum('a') = %d, want 100", got)
	}

	// Key 'f' is not covered by any tombstone
	if got := agg.GetMaxCoveringTombstoneSeqNum([]byte("f")); got != 0 {
		t.Errorf("GetMaxCoveringTombstoneSeqNum('f') = %d, want 0", got)
	}
}

func TestRangeDelAggregatorShouldDeleteKey(t *testing.T) {
	agg := NewRangeDelAggregator(1000)

	f := NewFragmenter()
	f.Add([]byte("a"), []byte("c"), 100)
	agg.AddTombstones(0, f.Finish())

	// Create an internal key for "b" at seq=50
	ikey := dbformat.NewInternalKey([]byte("b"), 50, dbformat.TypeValue)

	if !agg.ShouldDeleteKey(ikey) {
		t.Error("ShouldDeleteKey should return true for covered internal key")
	}

	// Internal key outside range
	ikey2 := dbformat.NewInternalKey([]byte("d"), 50, dbformat.TypeValue)
	if agg.ShouldDeleteKey(ikey2) {
		t.Error("ShouldDeleteKey should return false for uncovered key")
	}

	// Invalid internal key
	if agg.ShouldDeleteKey([]byte("short")) {
		t.Error("ShouldDeleteKey should return false for invalid key")
	}
}

func TestRangeDelAggregatorAddTombstoneList(t *testing.T) {
	agg := NewRangeDelAggregator(1000)

	// Add via TombstoneList
	list := NewTombstoneList()
	list.AddRange([]byte("a"), []byte("c"), 100)
	list.AddRange([]byte("b"), []byte("d"), 200) // overlaps
	agg.AddTombstoneList(0, list)

	// Should have fragmented the overlapping tombstones
	if agg.IsEmpty() {
		t.Error("aggregator should not be empty")
	}

	// Key in overlap region should see max seq
	if got := agg.GetMaxCoveringTombstoneSeqNum([]byte("b")); got != 200 {
		t.Errorf("overlap region max seq = %d, want 200", got)
	}
}

func TestRangeDelAggregatorClear(t *testing.T) {
	agg := NewRangeDelAggregator(1000)

	f := NewFragmenter()
	f.Add([]byte("a"), []byte("c"), 100)
	agg.AddTombstones(0, f.Finish())

	agg.Clear()

	if !agg.IsEmpty() {
		t.Error("aggregator should be empty after Clear")
	}
	if agg.ShouldDelete([]byte("b"), 50) {
		t.Error("cleared aggregator should not delete anything")
	}
}

func TestRangeDelAggregatorUpperBound(t *testing.T) {
	agg := NewRangeDelAggregator(100)

	if agg.UpperBound() != 100 {
		t.Errorf("UpperBound = %d, want 100", agg.UpperBound())
	}

	agg.SetUpperBound(200)
	if agg.UpperBound() != 200 {
		t.Errorf("UpperBound after Set = %d, want 200", agg.UpperBound())
	}
}

func TestCompactionRangeDelAggregatorShouldDropKey(t *testing.T) {
	// Earliest snapshot at seq=500
	agg := NewCompactionRangeDelAggregator(500)

	// Tombstone at seq=100 (older than earliest snapshot)
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("e"), 100)
	agg.AddTombstones(0, f.Finish())

	testCases := []struct {
		key    string
		seq    dbformat.SequenceNumber
		drop   bool
		reason string
	}{
		{"b", 50, true, "key and tombstone both < earliest snapshot"},
		{"b", 99, true, "key seq < tombstone seq, both < snapshot"},
		{"b", 100, false, "key seq == tombstone seq (not covered)"},
		{"b", 150, false, "key seq > tombstone seq (not covered)"},
		{"b", 600, false, "key seq > snapshot"},
		{"f", 50, false, "key outside tombstone range"},
	}

	for _, tc := range testCases {
		got := agg.ShouldDropKey([]byte(tc.key), tc.seq)
		if got != tc.drop {
			t.Errorf("ShouldDropKey(%q, %d) = %v, want %v (%s)",
				tc.key, tc.seq, got, tc.drop, tc.reason)
		}
	}
}

func TestCompactionRangeDelAggregatorWithActiveSnapshot(t *testing.T) {
	// Earliest snapshot at seq=75
	agg := NewCompactionRangeDelAggregator(75)

	// Tombstone at seq=100 (newer than earliest snapshot)
	f := NewFragmenter()
	f.Add([]byte("a"), []byte("e"), 100)
	agg.AddTombstones(0, f.Finish())

	// Key at seq=50 is covered by tombstone, but tombstone is newer than
	// earliest snapshot, so we can't drop (snapshot might need to see key)
	if agg.ShouldDropKey([]byte("b"), 50) {
		t.Error("shouldn't drop: tombstone newer than earliest snapshot")
	}
}

func TestReadRangeDelAggregator(t *testing.T) {
	agg := NewReadRangeDelAggregator(1000)

	f := NewFragmenter()
	f.Add([]byte("a"), []byte("c"), 100)
	agg.AddTombstones(0, f.Finish())

	if !agg.ShouldDelete([]byte("b"), 50) {
		t.Error("ReadRangeDelAggregator should work like regular aggregator")
	}
}
