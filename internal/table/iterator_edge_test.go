// Iterator edge case tests for the table package.
//
// These tests verify correct behavior in edge cases like:
// - Empty tables
// - Single entry tables
// - Seeking beyond bounds
// - Iterator invalidation
package table

import (
	"bytes"
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

// TestIteratorEmptyTable tests iterator behavior on an empty SST file.
func TestIteratorEdgeCaseEmptyTable(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)

	// Finish without adding any entries
	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	// Open and iterate
	reader, err := Open(&edgeMemFile{data: buf.Bytes()}, ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// SeekToFirst on empty table should be invalid
	iter.SeekToFirst()
	if iter.Valid() {
		t.Error("SeekToFirst on empty table should be invalid")
	}

	// SeekToLast on empty table should be invalid
	iter.SeekToLast()
	if iter.Valid() {
		t.Error("SeekToLast on empty table should be invalid")
	}

	// Seek on empty table should be invalid
	target := makeEdgeTestInternalKey("anything", 100, dbformat.TypeValue)
	iter.Seek(target)
	if iter.Valid() {
		t.Error("Seek on empty table should be invalid")
	}

	// No error expected
	if err := iter.Error(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestIteratorSingleEntry tests iterator behavior with a single entry.
func TestIteratorSingleEntry(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)

	key := makeEdgeTestInternalKey("only_key", 100, dbformat.TypeValue)
	value := []byte("only_value")
	builder.Add(key, value)

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(&edgeMemFile{data: buf.Bytes()}, ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// SeekToFirst should find the single entry
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("SeekToFirst should be valid")
	}
	if !bytes.Equal(iter.Key(), key) {
		t.Errorf("key mismatch: got %q, want %q", iter.Key(), key)
	}
	if !bytes.Equal(iter.Value(), value) {
		t.Errorf("value mismatch: got %q, want %q", iter.Value(), value)
	}

	// Next should invalidate
	iter.Next()
	if iter.Valid() {
		t.Error("Next after single entry should be invalid")
	}

	// SeekToLast should find the single entry
	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatal("SeekToLast should be valid")
	}
	if !bytes.Equal(iter.Key(), key) {
		t.Errorf("SeekToLast key mismatch")
	}

	// Prev should invalidate
	iter.Prev()
	if iter.Valid() {
		t.Error("Prev after single entry should be invalid")
	}
}

// TestIteratorSeekBeyondLast tests seeking past the last key.
func TestIteratorSeekBeyondLast(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)

	// Add keys a, b, c
	for _, k := range []string{"aaa", "bbb", "ccc"} {
		key := makeEdgeTestInternalKey(k, 100, dbformat.TypeValue)
		builder.Add(key, []byte("value_"+k))
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(&edgeMemFile{data: buf.Bytes()}, ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Seek to "zzz" (beyond last key)
	target := makeEdgeTestInternalKey("zzz", 100, dbformat.TypeValue)
	iter.Seek(target)
	if iter.Valid() {
		t.Error("Seek beyond last key should be invalid")
	}
}

// TestIteratorSeekBeforeFirst tests seeking before the first key.
func TestIteratorSeekBeforeFirst(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)

	// Add keys d, e, f
	for _, k := range []string{"ddd", "eee", "fff"} {
		key := makeEdgeTestInternalKey(k, 100, dbformat.TypeValue)
		builder.Add(key, []byte("value_"+k))
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(&edgeMemFile{data: buf.Bytes()}, ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Seek to "aaa" (before first key "ddd")
	target := makeEdgeTestInternalKey("aaa", 100, dbformat.TypeValue)
	iter.Seek(target)
	if !iter.Valid() {
		t.Fatal("Seek before first key should find first key")
	}

	// Should land on "ddd"
	parsed, _ := dbformat.ParseInternalKey(iter.Key())
	if string(parsed.UserKey) != "ddd" {
		t.Errorf("expected to land on 'ddd', got %q", parsed.UserKey)
	}
}

// TestIteratorMultipleDataBlocks tests iteration across multiple data blocks.
func TestIteratorMultipleDataBlocks(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	opts.BlockSize = 100 // Small block size to force multiple blocks

	builder := NewTableBuilder(&buf, opts)

	// Add enough entries to span multiple blocks
	numEntries := 100
	for i := range numEntries {
		key := makeEdgeTestInternalKey(padEdgeKey(i), uint64(1000-i), dbformat.TypeValue)
		value := bytes.Repeat([]byte{'v'}, 50)
		builder.Add(key, value)
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	// The small block size should have created multiple data blocks

	reader, err := Open(&edgeMemFile{data: buf.Bytes()}, ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Forward iteration
	iter := reader.NewIterator()
	count := 0
	var prevKey []byte
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if prevKey != nil {
			if bytes.Compare(prevKey, iter.Key()) >= 0 {
				t.Errorf("keys not in ascending order at %d", count)
			}
		}
		prevKey = append(prevKey[:0], iter.Key()...)
		count++
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("forward iteration error: %v", err)
	}

	if count != numEntries {
		t.Errorf("forward count: got %d, want %d", count, numEntries)
	}

	// Backward iteration
	count = 0
	prevKey = nil
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		if prevKey != nil {
			if bytes.Compare(prevKey, iter.Key()) <= 0 {
				t.Errorf("keys not in descending order at %d", count)
			}
		}
		prevKey = append(prevKey[:0], iter.Key()...)
		count++
	}

	if count != numEntries {
		t.Errorf("backward count: got %d, want %d", count, numEntries)
	}

	t.Logf("Iterated %d entries across multiple data blocks", numEntries)
}

// TestIteratorEarlyTermination tests stopping iteration early.
func TestIteratorEarlyTermination(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)

	// Add 100 entries
	for i := range 100 {
		key := makeEdgeTestInternalKey(padEdgeKey(i), 100, dbformat.TypeValue)
		builder.Add(key, []byte("value"))
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(&edgeMemFile{data: buf.Bytes()}, ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Only iterate first 10 entries
	iter := reader.NewIterator()
	count := 0
	for iter.SeekToFirst(); iter.Valid() && count < 10; iter.Next() {
		count++
	}

	if count != 10 {
		t.Errorf("early termination count: got %d, want 10", count)
	}

	// Iterator should still be valid (we stopped early)
	if !iter.Valid() {
		t.Error("iterator should still be valid after early termination")
	}
}

// TestIteratorReseek tests re-seeking multiple times.
func TestIteratorEdgeCaseReseek(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)

	// Add entries 0-99
	for i := range 100 {
		key := makeEdgeTestInternalKey(padEdgeKey(i), 100, dbformat.TypeValue)
		builder.Add(key, []byte("value"))
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(&edgeMemFile{data: buf.Bytes()}, ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Seek to middle, then first, then last, then middle again
	targets := []int{50, 0, 99, 25}
	for _, idx := range targets {
		target := makeEdgeTestInternalKey(padEdgeKey(idx), 100, dbformat.TypeValue)
		iter.Seek(target)
		if !iter.Valid() {
			t.Errorf("Seek to %d should be valid", idx)
			continue
		}

		parsed, _ := dbformat.ParseInternalKey(iter.Key())
		expected := padEdgeKey(idx)
		if string(parsed.UserKey) != expected {
			t.Errorf("Seek to %d: got %q, want %q", idx, parsed.UserKey, expected)
		}
	}
}

// Helper function for internal key creation
func makeEdgeTestInternalKey(userKey string, seq uint64, typ dbformat.ValueType) []byte {
	return dbformat.AppendInternalKey(nil, &dbformat.ParsedInternalKey{
		UserKey:  []byte(userKey),
		Sequence: dbformat.SequenceNumber(seq),
		Type:     typ,
	})
}

func padEdgeKey(i int) string {
	return string([]byte{byte('a' + i/26), byte('a' + i%26), byte('0' + i%10)})
}

// edgeMemFile implements ReadableFile for in-memory testing
type edgeMemFile struct {
	data []byte
}

func (m *edgeMemFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(m.data)) {
		return 0, nil
	}
	n = copy(p, m.data[off:])
	return n, nil
}

func (m *edgeMemFile) Size() int64 {
	return int64(len(m.data))
}
