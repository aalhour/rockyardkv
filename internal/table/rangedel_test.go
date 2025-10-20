package table

import (
	"bytes"
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/rangedel"
)

// memReadableFile implements ReadableFile for testing.
type memReadableFile struct {
	data []byte
}

func newMemReadableFile(data []byte) *memReadableFile {
	return &memReadableFile{data: data}
}

func (f *memReadableFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(f.data)) {
		return 0, nil
	}
	n = copy(p, f.data[off:])
	return n, nil
}

func (f *memReadableFile) Size() int64 {
	return int64(len(f.data))
}

func TestTableBuilderAddRangeTombstone(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 0 // Disable filter for simplicity
	builder := NewTableBuilder(&buf, opts)

	// Add some data
	builder.Add(dbformat.NewInternalKey([]byte("a"), 10, dbformat.TypeValue), []byte("val_a"))
	builder.Add(dbformat.NewInternalKey([]byte("z"), 11, dbformat.TypeValue), []byte("val_z"))

	// Add range tombstones
	if err := builder.AddRangeTombstone([]byte("c"), []byte("f"), 100); err != nil {
		t.Fatalf("AddRangeTombstone failed: %v", err)
	}
	if err := builder.AddRangeTombstone([]byte("m"), []byte("p"), 200); err != nil {
		t.Fatalf("AddRangeTombstone 2 failed: %v", err)
	}

	if !builder.HasRangeTombstones() {
		t.Error("HasRangeTombstones should return true")
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Read back and verify
	file := newMemReadableFile(buf.Bytes())
	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if !reader.HasRangeTombstones() {
		t.Error("Reader should report range tombstones")
	}

	// Get range tombstones
	tombstones, err := reader.GetRangeTombstones()
	if err != nil {
		t.Fatalf("GetRangeTombstones failed: %v", err)
	}

	// Should have 2 tombstones (non-overlapping)
	if tombstones.Len() != 2 {
		t.Errorf("Expected 2 tombstones, got %d", tombstones.Len())
	}

	// Verify the tombstones are correct
	t0 := tombstones.Get(0)
	if string(t0.StartKey) != "c" || string(t0.EndKey) != "f" {
		t.Errorf("Tombstone 0: got [%s, %s), want [c, f)", t0.StartKey, t0.EndKey)
	}
	if t0.SequenceNum != 100 {
		t.Errorf("Tombstone 0 seq: got %d, want 100", t0.SequenceNum)
	}

	t1 := tombstones.Get(1)
	if string(t1.StartKey) != "m" || string(t1.EndKey) != "p" {
		t.Errorf("Tombstone 1: got [%s, %s), want [m, p)", t1.StartKey, t1.EndKey)
	}
	if t1.SequenceNum != 200 {
		t.Errorf("Tombstone 1 seq: got %d, want 200", t1.SequenceNum)
	}
}

func TestTableBuilderNoRangeTombstones(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 0
	builder := NewTableBuilder(&buf, opts)

	// Add only regular data
	builder.Add(dbformat.NewInternalKey([]byte("a"), 10, dbformat.TypeValue), []byte("val_a"))
	builder.Add(dbformat.NewInternalKey([]byte("b"), 11, dbformat.TypeValue), []byte("val_b"))

	if builder.HasRangeTombstones() {
		t.Error("HasRangeTombstones should return false")
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Read back
	file := newMemReadableFile(buf.Bytes())
	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if reader.HasRangeTombstones() {
		t.Error("Reader should report no range tombstones")
	}

	tombstones, err := reader.GetRangeTombstones()
	if err != nil {
		t.Fatalf("GetRangeTombstones failed: %v", err)
	}

	if tombstones.Len() != 0 {
		t.Errorf("Expected 0 tombstones, got %d", tombstones.Len())
	}
}

func TestTableBuilderAddRangeTombstones(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 0
	builder := NewTableBuilder(&buf, opts)

	// Add data
	builder.Add(dbformat.NewInternalKey([]byte("a"), 10, dbformat.TypeValue), []byte("val"))

	// Add multiple tombstones via list
	list := rangedel.NewTombstoneList()
	list.AddRange([]byte("b"), []byte("d"), 100)
	list.AddRange([]byte("e"), []byte("g"), 200)

	if err := builder.AddRangeTombstones(list); err != nil {
		t.Fatalf("AddRangeTombstones failed: %v", err)
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Read back
	file := newMemReadableFile(buf.Bytes())
	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	tombstones, err := reader.GetRangeTombstones()
	if err != nil {
		t.Fatalf("GetRangeTombstones failed: %v", err)
	}

	if tombstones.Len() != 2 {
		t.Errorf("Expected 2 tombstones, got %d", tombstones.Len())
	}
}

func TestTableBuilderAddFragmentedRangeTombstones(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 0
	builder := NewTableBuilder(&buf, opts)

	// Add data
	builder.Add(dbformat.NewInternalKey([]byte("a"), 10, dbformat.TypeValue), []byte("val"))

	// Create fragmented tombstones (overlapping that get fragmented)
	fragmenter := rangedel.NewFragmenter()
	fragmenter.Add([]byte("a"), []byte("d"), 100)
	fragmenter.Add([]byte("b"), []byte("e"), 200)
	fragmented := fragmenter.Finish()

	// Should have 3 fragments: [a,b)@100, [b,d)@200, [d,e)@200
	if fragmented.Len() != 3 {
		t.Errorf("Expected 3 fragments, got %d", fragmented.Len())
	}

	if err := builder.AddFragmentedRangeTombstones(fragmented); err != nil {
		t.Fatalf("AddFragmentedRangeTombstones failed: %v", err)
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Read back
	file := newMemReadableFile(buf.Bytes())
	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	tombstones, err := reader.GetRangeTombstones()
	if err != nil {
		t.Fatalf("GetRangeTombstones failed: %v", err)
	}

	if tombstones.Len() != 3 {
		t.Errorf("Expected 3 tombstones, got %d", tombstones.Len())
	}
}

func TestTableRangeTombstonesProperties(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 0
	builder := NewTableBuilder(&buf, opts)

	// Add data
	builder.Add(dbformat.NewInternalKey([]byte("a"), 10, dbformat.TypeValue), []byte("val"))

	// Add tombstones
	builder.AddRangeTombstone([]byte("b"), []byte("c"), 100)
	builder.AddRangeTombstone([]byte("d"), []byte("e"), 200)
	builder.AddRangeTombstone([]byte("f"), []byte("g"), 300)

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Read back
	file := newMemReadableFile(buf.Bytes())
	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	props, err := reader.Properties()
	if err != nil {
		t.Fatalf("Properties failed: %v", err)
	}

	// Check that num.range-deletions is recorded
	if props.NumRangeDeletions != 3 {
		t.Errorf("NumRangeDeletions = %d, want 3", props.NumRangeDeletions)
	}
}

func TestTableRangeTombstonesShouldDelete(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 0
	builder := NewTableBuilder(&buf, opts)

	// Add data
	builder.Add(dbformat.NewInternalKey([]byte("a"), 10, dbformat.TypeValue), []byte("val"))

	// Add tombstone [b, e) @ seq=100
	builder.AddRangeTombstone([]byte("b"), []byte("e"), 100)

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Read back
	file := newMemReadableFile(buf.Bytes())
	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	tombstones, err := reader.GetRangeTombstones()
	if err != nil {
		t.Fatalf("GetRangeTombstones failed: %v", err)
	}

	// Test ShouldDelete
	testCases := []struct {
		key    string
		seq    dbformat.SequenceNumber
		delete bool
	}{
		{"a", 50, false},  // before range
		{"b", 50, true},   // in range, seq < tombstone
		{"c", 50, true},   // in range
		{"d", 99, true},   // in range, seq < tombstone
		{"d", 100, false}, // in range, seq == tombstone
		{"d", 150, false}, // in range, seq > tombstone
		{"e", 50, false},  // at end (exclusive)
		{"f", 50, false},  // after range
	}

	for _, tc := range testCases {
		got := tombstones.ShouldDelete([]byte(tc.key), tc.seq)
		if got != tc.delete {
			t.Errorf("ShouldDelete(%q, %d) = %v, want %v", tc.key, tc.seq, got, tc.delete)
		}
	}
}

func TestTableGetRangeTombstoneList(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.FilterBitsPerKey = 0
	builder := NewTableBuilder(&buf, opts)

	// Add data
	builder.Add(dbformat.NewInternalKey([]byte("a"), 10, dbformat.TypeValue), []byte("val"))

	// Add overlapping tombstones
	builder.AddRangeTombstone([]byte("a"), []byte("d"), 100)
	builder.AddRangeTombstone([]byte("b"), []byte("e"), 200)

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Read back
	file := newMemReadableFile(buf.Bytes())
	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// GetRangeTombstoneList returns raw (non-fragmented) list
	list, err := reader.GetRangeTombstoneList()
	if err != nil {
		t.Fatalf("GetRangeTombstoneList failed: %v", err)
	}

	if list.Len() != 2 {
		t.Errorf("Expected 2 raw tombstones, got %d", list.Len())
	}

	// GetRangeTombstones returns fragmented list
	fragmented, err := reader.GetRangeTombstones()
	if err != nil {
		t.Fatalf("GetRangeTombstones failed: %v", err)
	}

	// Overlapping tombstones should be fragmented into 3
	if fragmented.Len() != 3 {
		t.Errorf("Expected 3 fragmented tombstones, got %d", fragmented.Len())
	}
}
