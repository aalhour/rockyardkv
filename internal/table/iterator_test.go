package table

import (
	"bytes"
	"testing"
)

// -----------------------------------------------------------------------------
// Iterator Edge Case Tests
// These tests verify iterator behavior in edge cases and corner conditions.
// -----------------------------------------------------------------------------

// memFileForTest implements both WritableFile and ReadableFile for testing.
type memFileForTest struct {
	data []byte
}

func (f *memFileForTest) Write(p []byte) (int, error) {
	f.data = append(f.data, p...)
	return len(p), nil
}

func (f *memFileForTest) Append(p []byte) error {
	f.data = append(f.data, p...)
	return nil
}

func (f *memFileForTest) Close() error { return nil }
func (f *memFileForTest) Sync() error  { return nil }

func (f *memFileForTest) Truncate(size int64) error {
	if size < int64(len(f.data)) {
		f.data = f.data[:size]
	}
	return nil
}

func (f *memFileForTest) Size() (int64, error) {
	return int64(len(f.data)), nil
}

func (f *memFileForTest) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, nil
	}
	n := copy(p, f.data[off:])
	return n, nil
}

func (f *memFileForTest) SizeForRead() int64 {
	return int64(len(f.data))
}

// TestTableIteratorEmptyTable tests iterator behavior on an empty table.
func TestTableIteratorEmptyTable(t *testing.T) {
	// Create an empty SST file
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	// Finish without adding any entries
	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Open and iterate
	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// SeekToFirst on empty table
	iter.SeekToFirst()
	if iter.Valid() {
		t.Error("SeekToFirst on empty table should be invalid")
	}

	// SeekToLast on empty table
	iter.SeekToLast()
	if iter.Valid() {
		t.Error("SeekToLast on empty table should be invalid")
	}

	// Seek on empty table
	iter.Seek([]byte("anykey"))
	if iter.Valid() {
		t.Error("Seek on empty table should be invalid")
	}
}

// readableMemFile wraps memFileForTest to provide Size() int64 for ReadableFile.
type readableMemFile struct {
	*memFileForTest
}

func (f *readableMemFile) Size() int64 {
	return int64(len(f.data))
}

// TestTableIteratorSingleEntry tests iterator with exactly one entry.
func TestTableIteratorSingleEntry(t *testing.T) {
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	key := makeIterTestKey([]byte("only_key"), 100)
	value := []byte("only_value")
	if err := builder.Add(key, value); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// SeekToFirst
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("SeekToFirst should be valid")
	}
	if !bytes.HasPrefix(iter.Key(), []byte("only_key")) {
		t.Errorf("Key = %s, want prefix 'only_key'", iter.Key())
	}
	if !bytes.Equal(iter.Value(), value) {
		t.Errorf("Value = %s, want %s", iter.Value(), value)
	}

	// Next should invalidate
	iter.Next()
	if iter.Valid() {
		t.Error("Next after single entry should be invalid")
	}

	// SeekToLast
	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatal("SeekToLast should be valid")
	}
	if !bytes.HasPrefix(iter.Key(), []byte("only_key")) {
		t.Errorf("Key = %s, want prefix 'only_key'", iter.Key())
	}

	// Seek to exact key
	iter.Seek(key)
	if !iter.Valid() {
		t.Fatal("Seek to exact key should be valid")
	}
}

// TestTableIteratorSeekBeyondLast tests seeking past the last key.
func TestTableIteratorSeekBeyondLast(t *testing.T) {
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	// Add keys a, b, c
	for _, k := range []string{"a", "b", "c"} {
		key := makeIterTestKey([]byte(k), 100)
		if err := builder.Add(key, []byte("val_"+k)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Seek to key beyond last ("z" > "c")
	iter.Seek(makeIterTestKey([]byte("z"), 100))
	if iter.Valid() {
		t.Error("Seek beyond last key should be invalid")
	}
}

// TestTableIteratorSeekBeforeFirst tests seeking before the first key.
func TestTableIteratorSeekBeforeFirst(t *testing.T) {
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	// Add keys m, n, o
	for _, k := range []string{"m", "n", "o"} {
		key := makeIterTestKey([]byte(k), 100)
		if err := builder.Add(key, []byte("val_"+k)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Seek to key before first ("a" < "m")
	iter.Seek(makeIterTestKey([]byte("a"), 100))
	if !iter.Valid() {
		t.Fatal("Seek before first should position at first")
	}
	// Should be at "m"
	if !bytes.HasPrefix(iter.Key(), []byte("m")) {
		t.Errorf("Key = %x, want prefix 'm'", iter.Key())
	}
}

// TestTableIteratorMultipleDataBlocks tests iteration across multiple data blocks.
func TestTableIteratorMultipleDataBlocks(t *testing.T) {
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	opts.BlockSize = 64 // Small blocks to force multiple blocks
	builder := NewTableBuilder(memFile, opts)

	// Add many entries to force multiple blocks
	numEntries := 50
	for i := range numEntries {
		key := makeIterTestKey([]byte{byte('a' + i%26), byte('0' + i/26)}, uint64(100-i))
		value := bytes.Repeat([]byte{byte(i)}, 20)
		if err := builder.Add(key, value); err != nil {
			t.Fatalf("Add %d failed: %v", i, err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// Count entries via iteration
	iter := reader.NewIterator()
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	if count != numEntries {
		t.Errorf("Iterated %d entries, want %d", count, numEntries)
	}
}

// TestTableIteratorLargeKeys tests iteration with large keys.
func TestTableIteratorLargeKeys(t *testing.T) {
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	// Add entries with large keys (1KB each)
	numEntries := 10
	keySize := 1024
	for i := range numEntries {
		largeKey := bytes.Repeat([]byte{byte('a' + i)}, keySize)
		key := makeIterTestKey(largeKey, uint64(100-i))
		value := []byte{byte(i)}
		if err := builder.Add(key, value); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if len(iter.Key()) < keySize {
			t.Errorf("Key %d too short: %d < %d", count, len(iter.Key()), keySize)
		}
		count++
	}

	if count != numEntries {
		t.Errorf("Iterated %d entries, want %d", count, numEntries)
	}
}

// TestTableIteratorLargeValues tests iteration with large values.
func TestTableIteratorLargeValues(t *testing.T) {
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	// Add entries with large values (10KB each)
	numEntries := 5
	valueSize := 10 * 1024
	for i := range numEntries {
		key := makeIterTestKey([]byte{byte('a' + i)}, uint64(100-i))
		largeValue := bytes.Repeat([]byte{byte(i)}, valueSize)
		if err := builder.Add(key, largeValue); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if len(iter.Value()) != valueSize {
			t.Errorf("Value %d wrong size: %d != %d", i, len(iter.Value()), valueSize)
		}
		// Verify value content
		expected := bytes.Repeat([]byte{byte(i)}, valueSize)
		if !bytes.Equal(iter.Value(), expected) {
			t.Errorf("Value %d content mismatch", i)
		}
		i++
	}

	if i != numEntries {
		t.Errorf("Iterated %d entries, want %d", i, numEntries)
	}
}

// TestTableIteratorBinaryKeys tests iteration with binary keys containing null bytes.
func TestTableIteratorBinaryKeys(t *testing.T) {
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	// Keys with embedded null bytes
	binaryKeys := [][]byte{
		{0x00, 0x01, 0x02},
		{0x00, 0x00, 0x00},
		{0x01, 0x00, 0x01},
		{0xFF, 0x00, 0xFF},
	}

	for i, bk := range binaryKeys {
		key := makeIterTestKey(bk, uint64(100-i))
		value := []byte{byte(i)}
		if err := builder.Add(key, value); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	if count != len(binaryKeys) {
		t.Errorf("Iterated %d entries, want %d", count, len(binaryKeys))
	}
}

// TestTableIteratorSeekExact tests seeking to exact keys.
func TestTableIteratorSeekExact(t *testing.T) {
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, k := range keys {
		key := makeIterTestKey([]byte(k), uint64(100-i))
		if err := builder.Add(key, []byte("val_"+k)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Seek to each key exactly
	for i, k := range keys {
		seekKey := makeIterTestKey([]byte(k), uint64(100-i))
		iter.Seek(seekKey)
		if !iter.Valid() {
			t.Errorf("Seek to %q should be valid", k)
			continue
		}
		if !bytes.HasPrefix(iter.Key(), []byte(k)) {
			t.Errorf("Seek to %q found %x", k, iter.Key())
		}
	}
}

// TestTableIteratorSeekBetween tests seeking to keys between existing keys.
func TestTableIteratorSeekBetween(t *testing.T) {
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	// Add keys: aaa, ccc, eee
	keys := []string{"aaa", "ccc", "eee"}
	for i, k := range keys {
		key := makeIterTestKey([]byte(k), uint64(100-i))
		if err := builder.Add(key, []byte("val")); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Seek to "bbb" should find "ccc"
	iter.Seek(makeIterTestKey([]byte("bbb"), 100))
	if !iter.Valid() {
		t.Fatal("Seek to 'bbb' should be valid")
	}
	if !bytes.HasPrefix(iter.Key(), []byte("ccc")) {
		t.Errorf("Seek to 'bbb' found %x, want 'ccc'", iter.Key())
	}

	// Seek to "ddd" should find "eee"
	iter.Seek(makeIterTestKey([]byte("ddd"), 100))
	if !iter.Valid() {
		t.Fatal("Seek to 'ddd' should be valid")
	}
	if !bytes.HasPrefix(iter.Key(), []byte("eee")) {
		t.Errorf("Seek to 'ddd' found %x, want 'eee'", iter.Key())
	}
}

// TestTableIteratorRepeatedSeeks tests multiple seeks on the same iterator.
func TestTableIteratorRepeatedSeeks(t *testing.T) {
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	for i := range 26 {
		key := makeIterTestKey([]byte{byte('a' + i)}, uint64(100-i))
		if err := builder.Add(key, []byte{byte(i)}); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Random seeks
	seeks := []byte{'m', 'a', 'z', 'g', 'p', 'a', 'z'}
	for _, c := range seeks {
		iter.Seek(makeIterTestKey([]byte{c}, 100))
		if c <= 'z' && c >= 'a' {
			if !iter.Valid() {
				t.Errorf("Seek to %c should be valid", c)
			}
		}
	}
}

// TestTableIteratorAfterError verifies iterator state after encountering an error.
func TestTableIteratorAfterError(t *testing.T) {
	// Create a valid SST first
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	key := makeIterTestKey([]byte("key"), 100)
	if err := builder.Add(key, []byte("value")); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("Should be valid initially")
	}

	// Move past end
	iter.Next()
	if iter.Valid() {
		t.Error("Should be invalid after moving past end")
	}

	// Should be able to seek again
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Error("Should be valid after re-seek")
	}
}

// makeIterTestKey creates internal keys for iterator testing.
func makeIterTestKey(userKey []byte, seq uint64) []byte {
	key := make([]byte, len(userKey)+8)
	copy(key, userKey)
	// Type 1 = TypeValue
	trailer := (seq << 8) | 1
	key[len(userKey)] = byte(trailer)
	key[len(userKey)+1] = byte(trailer >> 8)
	key[len(userKey)+2] = byte(trailer >> 16)
	key[len(userKey)+3] = byte(trailer >> 24)
	key[len(userKey)+4] = byte(trailer >> 32)
	key[len(userKey)+5] = byte(trailer >> 40)
	key[len(userKey)+6] = byte(trailer >> 48)
	key[len(userKey)+7] = byte(trailer >> 56)
	return key
}
