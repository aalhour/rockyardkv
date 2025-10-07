package memtable

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

func TestMemTableRangeTombstoneBasic(t *testing.T) {
	mt := NewMemTable(nil)

	// Add some data
	mt.Add(10, dbformat.TypeValue, []byte("a"), []byte("val_a"))
	mt.Add(11, dbformat.TypeValue, []byte("b"), []byte("val_b"))
	mt.Add(12, dbformat.TypeValue, []byte("c"), []byte("val_c"))
	mt.Add(13, dbformat.TypeValue, []byte("d"), []byte("val_d"))

	// Verify data is visible
	val, found, deleted := mt.Get([]byte("b"), 100)
	if !found || deleted || string(val) != "val_b" {
		t.Errorf("Get(b) before deletion: found=%v, deleted=%v, val=%s", found, deleted, val)
	}

	// Add range tombstone [b, d) at seq=50
	mt.AddRangeTombstone(50, []byte("b"), []byte("d"))

	// Keys covered by tombstone should be deleted (if their seq < tombstone seq)
	// Key "a" is outside the range
	_, found, deleted = mt.Get([]byte("a"), 100)
	if !found || deleted {
		t.Errorf("Get(a) after range del: found=%v, deleted=%v, want found=true, deleted=false", found, deleted)
	}

	// Key "b" is in range [b, d), seq=11 < 50, so deleted
	_, found, deleted = mt.Get([]byte("b"), 100)
	if !found || !deleted {
		t.Errorf("Get(b) after range del: found=%v, deleted=%v, want found=true, deleted=true", found, deleted)
	}

	// Key "c" is in range [b, d), seq=12 < 50, so deleted
	_, found, deleted = mt.Get([]byte("c"), 100)
	if !found || !deleted {
		t.Errorf("Get(c) after range del: found=%v, deleted=%v, want found=true, deleted=true", found, deleted)
	}

	// Key "d" is at end of range (exclusive), so NOT deleted
	val, found, deleted = mt.Get([]byte("d"), 100)
	if !found || deleted || string(val) != "val_d" {
		t.Errorf("Get(d) after range del: found=%v, deleted=%v, val=%s", found, deleted, val)
	}
}

func TestMemTableRangeTombstoneWithHigherPointSeq(t *testing.T) {
	mt := NewMemTable(nil)

	// Add range tombstone first at seq=50
	mt.AddRangeTombstone(50, []byte("a"), []byte("z"))

	// Add data at seq=100 (higher than tombstone)
	mt.Add(100, dbformat.TypeValue, []byte("key"), []byte("value"))

	// Key should be visible because point seq (100) > tombstone seq (50)
	val, found, deleted := mt.Get([]byte("key"), 200)
	if !found || deleted || string(val) != "value" {
		t.Errorf("Get: found=%v, deleted=%v, val=%s, want found=true, deleted=false, val=value",
			found, deleted, val)
	}
}

func TestMemTableRangeTombstoneWithLowerPointSeq(t *testing.T) {
	mt := NewMemTable(nil)

	// Add data first at seq=50
	mt.Add(50, dbformat.TypeValue, []byte("key"), []byte("value"))

	// Add range tombstone at seq=100 (higher than point data)
	mt.AddRangeTombstone(100, []byte("a"), []byte("z"))

	// Key should be deleted because tombstone seq (100) > point seq (50)
	_, found, deleted := mt.Get([]byte("key"), 200)
	if !found || !deleted {
		t.Errorf("Get: found=%v, deleted=%v, want found=true, deleted=true", found, deleted)
	}
}

func TestMemTableRangeTombstoneNoPointData(t *testing.T) {
	mt := NewMemTable(nil)

	// Add range tombstone without any point data
	mt.AddRangeTombstone(100, []byte("a"), []byte("z"))

	// Keys in range should report as deleted (even without point data)
	_, found, deleted := mt.Get([]byte("key"), 200)
	if !found || !deleted {
		t.Errorf("Get: found=%v, deleted=%v, want found=true, deleted=true", found, deleted)
	}

	// Keys outside range should not be found
	_, found, _ = mt.Get([]byte("zzz"), 200)
	if found {
		t.Errorf("Get(zzz): found=%v, want false", found)
	}
}

func TestMemTableRangeTombstoneVisibility(t *testing.T) {
	mt := NewMemTable(nil)

	// Add data at seq=50
	mt.Add(50, dbformat.TypeValue, []byte("key"), []byte("value"))

	// Add range tombstone at seq=100
	mt.AddRangeTombstone(100, []byte("a"), []byte("z"))

	// Query at seq=80 (tombstone not visible)
	val, found, deleted := mt.Get([]byte("key"), 80)
	if !found || deleted || string(val) != "value" {
		t.Errorf("Get at seq=80: found=%v, deleted=%v, val=%s, want found=true, deleted=false, val=value",
			found, deleted, val)
	}

	// Query at seq=150 (tombstone visible)
	_, found, deleted = mt.Get([]byte("key"), 150)
	if !found || !deleted {
		t.Errorf("Get at seq=150: found=%v, deleted=%v, want found=true, deleted=true", found, deleted)
	}
}

func TestMemTableRangeTombstoneHelpers(t *testing.T) {
	mt := NewMemTable(nil)

	if mt.HasRangeTombstones() {
		t.Error("HasRangeTombstones should be false initially")
	}
	if mt.RangeTombstoneCount() != 0 {
		t.Errorf("RangeTombstoneCount = %d, want 0", mt.RangeTombstoneCount())
	}

	mt.AddRangeTombstone(100, []byte("a"), []byte("b"))
	mt.AddRangeTombstone(200, []byte("c"), []byte("d"))

	if !mt.HasRangeTombstones() {
		t.Error("HasRangeTombstones should be true after adding")
	}
	if mt.RangeTombstoneCount() != 2 {
		t.Errorf("RangeTombstoneCount = %d, want 2", mt.RangeTombstoneCount())
	}
}

func TestMemTableGetRangeTombstones(t *testing.T) {
	mt := NewMemTable(nil)

	mt.AddRangeTombstone(100, []byte("a"), []byte("c"))
	mt.AddRangeTombstone(200, []byte("e"), []byte("g"))

	list := mt.GetRangeTombstones()
	if list.Len() != 2 {
		t.Errorf("GetRangeTombstones returned %d items, want 2", list.Len())
	}
}

func TestMemTableGetFragmentedRangeTombstones(t *testing.T) {
	mt := NewMemTable(nil)

	// Add overlapping tombstones
	mt.AddRangeTombstone(100, []byte("a"), []byte("d"))
	mt.AddRangeTombstone(200, []byte("b"), []byte("e"))

	fragmented := mt.GetFragmentedRangeTombstones()

	// Should be fragmented into non-overlapping ranges
	// [a, b) seq=100, [b, d) seq=200, [d, e) seq=200
	if fragmented.Len() != 3 {
		t.Errorf("GetFragmentedRangeTombstones returned %d fragments, want 3", fragmented.Len())
	}

	// Check that fragments are in order
	if string(fragmented.Get(0).StartKey) != "a" {
		t.Errorf("First fragment starts at %q, want 'a'", fragmented.Get(0).StartKey)
	}
}

func TestMemTableRangeTombstoneMemoryUsage(t *testing.T) {
	mt := NewMemTable(nil)

	initialUsage := mt.ApproximateMemoryUsage()

	// Add a range tombstone
	mt.AddRangeTombstone(100, []byte("start_key"), []byte("end_key"))

	newUsage := mt.ApproximateMemoryUsage()
	if newUsage <= initialUsage {
		t.Errorf("Memory usage should increase after adding range tombstone: was %d, now %d",
			initialUsage, newUsage)
	}
}

func TestMemTableMultipleRangeTombstones(t *testing.T) {
	mt := NewMemTable(nil)

	// Add data
	mt.Add(10, dbformat.TypeValue, []byte("a"), []byte("val_a"))
	mt.Add(20, dbformat.TypeValue, []byte("b"), []byte("val_b"))
	mt.Add(30, dbformat.TypeValue, []byte("c"), []byte("val_c"))
	mt.Add(40, dbformat.TypeValue, []byte("d"), []byte("val_d"))

	// Multiple non-overlapping tombstones
	mt.AddRangeTombstone(50, []byte("a"), []byte("b")) // covers "a" only (seq 10)
	mt.AddRangeTombstone(35, []byte("c"), []byte("d")) // covers "c" only (seq 30)

	// "a" is deleted (seq 10 < tombstone 50)
	_, found, deleted := mt.Get([]byte("a"), 100)
	if !found || !deleted {
		t.Errorf("Get(a): found=%v, deleted=%v, want found=true, deleted=true", found, deleted)
	}

	// "b" is NOT in first tombstone range (exclusive end)
	val, found, deleted := mt.Get([]byte("b"), 100)
	if !found || deleted || string(val) != "val_b" {
		t.Errorf("Get(b): found=%v, deleted=%v, val=%s", found, deleted, val)
	}

	// "c" is deleted (seq 30 < tombstone 35)
	_, found, deleted = mt.Get([]byte("c"), 100)
	if !found || !deleted {
		t.Errorf("Get(c): found=%v, deleted=%v, want found=true, deleted=true", found, deleted)
	}

	// "d" is NOT in second tombstone range (exclusive end)
	val, found, deleted = mt.Get([]byte("d"), 100)
	if !found || deleted || string(val) != "val_d" {
		t.Errorf("Get(d): found=%v, deleted=%v, val=%s", found, deleted, val)
	}
}
