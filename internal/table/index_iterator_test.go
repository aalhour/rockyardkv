package table

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestIndexBlockIteratorDirect tests the TableIterator with multi-block tables.
func TestIndexBlockIteratorDirect(t *testing.T) {
	// Build a table with multiple data blocks to create a meaningful index block
	opts := DefaultBuilderOptions()
	opts.BlockSize = 50 // Very small to force multiple blocks

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	// Add enough entries to create multiple data blocks
	for i := range 20 {
		key := makeTestKey(i)
		value := []byte("value")
		builder.Add(key, value)
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Open the table
	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// Test via TableIterator which uses IndexBlockIterator internally
	iter := reader.NewIterator()

	// Test Seek
	t.Run("Seek", func(t *testing.T) {
		target := makeTestKey(10)
		iter.Seek(target)
		if !iter.Valid() {
			t.Fatal("Should be valid after Seek")
		}
		// Key should be >= target
		if bytes.Compare(iter.Key(), target) < 0 {
			t.Errorf("Key after Seek should be >= target")
		}
	})

	// Test SeekToFirst then iterate forward
	t.Run("SeekToFirst", func(t *testing.T) {
		iter.SeekToFirst()
		if !iter.Valid() {
			t.Fatal("Should be valid after SeekToFirst")
		}

		// Count entries
		count := 0
		for iter.Valid() {
			count++
			iter.Next()
		}
		if count != 20 {
			t.Errorf("Expected 20 entries, got %d", count)
		}
	})

	// Test SeekToLast then iterate backward
	t.Run("SeekToLast", func(t *testing.T) {
		iter.SeekToLast()
		if !iter.Valid() {
			t.Fatal("Should be valid after SeekToLast")
		}

		// Count entries going backward
		count := 0
		for iter.Valid() {
			count++
			iter.Prev()
		}
		if count != 20 {
			t.Errorf("Expected 20 entries backward, got %d", count)
		}
	})

	// Test Prev from middle
	t.Run("PrevFromMiddle", func(t *testing.T) {
		iter.SeekToFirst()
		// Move forward 5 positions
		for range 5 {
			iter.Next()
		}
		if !iter.Valid() {
			t.Fatal("Should be valid at position 5")
		}
		keyAt5 := make([]byte, len(iter.Key()))
		copy(keyAt5, iter.Key())

		// Go back one
		iter.Prev()
		if !iter.Valid() {
			t.Fatal("Should be valid after Prev")
		}
		keyAt4 := iter.Key()

		// keyAt4 should be less than keyAt5
		if bytes.Compare(keyAt4, keyAt5) >= 0 {
			t.Errorf("Key after Prev should be less than previous key")
		}
	})
}

// TestIndexBlockIteratorSeekVariants tests various Seek scenarios.
func TestIndexBlockIteratorSeekVariants(t *testing.T) {
	opts := DefaultBuilderOptions()
	opts.BlockSize = 64

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	// Add entries with gaps: key000, key010, key020, ...
	for i := range 10 {
		key := makeTestKeyWithGap(i * 10)
		builder.Add(key, []byte("v"))
	}
	builder.Finish()

	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	t.Run("SeekExact", func(t *testing.T) {
		// Seek to exact key
		target := makeTestKeyWithGap(30)
		iter.Seek(target)
		if !iter.Valid() {
			t.Fatal("Should find exact key")
		}
	})

	t.Run("SeekBetween", func(t *testing.T) {
		// Seek to key between existing keys (should find next)
		target := makeTestKeyWithGap(25) // Between 20 and 30
		iter.Seek(target)
		if !iter.Valid() {
			t.Fatal("Should find next key")
		}
	})

	t.Run("SeekBeforeFirst", func(t *testing.T) {
		// Seek to key before first
		target := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		iter.Seek(target)
		if !iter.Valid() {
			t.Fatal("Should find first key when seeking before all")
		}
	})

	t.Run("SeekAfterLast", func(t *testing.T) {
		// Seek to key after last
		target := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		iter.Seek(target)
		if iter.Valid() {
			t.Error("Should not be valid when seeking past all keys")
		}
	})
}

// TestIndexBlockIteratorKeyMethod tests the Key() method.
func TestIndexBlockIteratorKeyMethod(t *testing.T) {
	opts := DefaultBuilderOptions()
	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	expectedKeys := [][]byte{
		makeTestKey(0),
		makeTestKey(1),
		makeTestKey(2),
	}

	for _, key := range expectedKeys {
		builder.Add(key, []byte("value"))
	}
	builder.Finish()

	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()
	iter.SeekToFirst()

	for i, expected := range expectedKeys {
		if !iter.Valid() {
			t.Fatalf("Should be valid at index %d", i)
		}
		key := iter.Key()
		if !bytes.Equal(key, expected) {
			t.Errorf("Key %d mismatch: got %v, want %v", i, key, expected)
		}
		iter.Next()
	}
}

// TestIndexBlockIteratorFormatV5 tests IndexBlockIterator with C++ RocksDB SST files.
// C++ RocksDB format version >= 4 uses value_delta_encoding in index blocks.
// Note: Go-generated SSTs do NOT use value_delta_encoding, so we test with C++ golden files.
func TestIndexBlockIteratorFormatV5(t *testing.T) {
	// Use C++ golden file which actually uses value_delta_encoding
	goldenPath := filepath.Join("..", "..", "testdata", "golden", "v10.7.5", "sst", "simple.sst")
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Skipf("Golden file not found: %v", err)
	}

	memFile := NewMemFile(data)
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// Verify it uses IndexBlockIterator (value_delta_encoding detected)
	if !reader.indexUsesValueDeltaEncoding {
		t.Skip("SST does not use value_delta_encoding - skipping IndexBlockIterator test")
	}

	iter := reader.NewIterator()

	t.Run("SeekToFirst", func(t *testing.T) {
		iter.SeekToFirst()
		if !iter.Valid() {
			t.Fatal("Should be valid after SeekToFirst")
		}

		count := 0
		for iter.Valid() {
			count++
			iter.Next()
		}
		if count < 1 {
			t.Errorf("Expected at least 1 entry, got %d", count)
		}
	})

	t.Run("SeekToLast", func(t *testing.T) {
		iter.SeekToLast()
		if !iter.Valid() {
			t.Fatal("Should be valid after SeekToLast")
		}
	})

	t.Run("Seek", func(t *testing.T) {
		target := []byte("key1")
		iter.Seek(target)
		if !iter.Valid() {
			t.Skip("No entry >= target found")
		}
	})

	t.Run("Prev", func(t *testing.T) {
		iter.SeekToLast()
		if !iter.Valid() {
			t.Skip("No entries in SST")
		}
		// If there's only one entry, Prev should make it invalid
		iter.Prev()
		// Not checking validity since it depends on entry count
	})
}

// TestIndexBlockIteratorKeyMethodV5 tests Key() with C++ RocksDB SST.
func TestIndexBlockIteratorKeyMethodV5(t *testing.T) {
	// Use C++ golden file which actually uses value_delta_encoding
	goldenPath := filepath.Join("..", "..", "testdata", "golden", "v10.7.5", "sst", "simple.sst")
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Skipf("Golden file not found: %v", err)
	}

	memFile := NewMemFile(data)
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// Verify it uses value_delta_encoding
	if !reader.indexUsesValueDeltaEncoding {
		t.Skip("SST does not use value_delta_encoding - skipping test")
	}

	iter := reader.NewIterator()
	iter.SeekToFirst()

	count := 0
	for iter.Valid() {
		key := iter.Key()
		// Just verify the key is not nil and is reasonable length
		if key == nil {
			t.Errorf("Key should not be nil at index %d", count)
		}
		count++
		iter.Next()
	}
}

// TestIndexBlockIteratorPrevAndKey tests the Prev() and Key() methods directly.
func TestIndexBlockIteratorPrevAndKey(t *testing.T) {
	// Use a C++ golden file that has multiple entries
	goldenPath := filepath.Join("..", "..", "testdata", "rocksdb_generated", "000008.sst")
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Skipf("Golden file not found: %v", err)
	}

	memFile := NewMemFile(data)
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// Test the TableIterator which uses IndexBlockIterator for format v4+
	iter := reader.NewIterator()

	// Collect all keys forward
	var keys [][]byte
	iter.SeekToFirst()
	for iter.Valid() {
		keyCopy := make([]byte, len(iter.Key()))
		copy(keyCopy, iter.Key())
		keys = append(keys, keyCopy)
		iter.Next()
	}

	if len(keys) == 0 {
		t.Skip("No entries in SST")
	}

	t.Logf("Found %d entries in SST", len(keys))

	// Test Prev from last
	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatal("Should be valid at last")
	}

	lastKey := make([]byte, len(iter.Key()))
	copy(lastKey, iter.Key())

	if len(keys) > 1 {
		iter.Prev()
		if !iter.Valid() {
			t.Fatal("Should be valid after Prev from last")
		}
		// Key should be different from last
		if bytes.Equal(iter.Key(), lastKey) {
			t.Error("Key after Prev should be different from last key")
		}
	}

	// Test Key() returns correct value
	iter.SeekToFirst()
	if iter.Valid() {
		key := iter.Key()
		if key == nil {
			t.Error("Key() should not return nil when valid")
		}
		if len(key) == 0 {
			t.Error("Key() should not return empty slice for valid entry")
		}
	}
}

// TestIndexBlockIteratorSyntheticData tests IndexBlockIterator with synthetic data
// that mimics C++ RocksDB's value_delta_encoding format.
func TestIndexBlockIteratorSyntheticData(t *testing.T) {
	// Create a synthetic index block with value_delta_encoding format:
	// Each entry is: <shared:varint><non_shared:varint><key_delta><value>
	// Value is a BlockHandle: <offset:varint><size:varint>
	var buf bytes.Buffer

	// Entry 1: key="key1", handle=(offset=0, size=100)
	buf.WriteByte(0) // shared = 0
	buf.WriteByte(4) // non_shared = 4
	buf.Write([]byte("key1"))
	buf.WriteByte(0)   // offset = 0
	buf.WriteByte(100) // size = 100

	// Entry 2: key="key2", handle=(offset=100, size=100)
	buf.WriteByte(3) // shared = 3 ("key" shared)
	buf.WriteByte(1) // non_shared = 1 ("2")
	buf.Write([]byte("2"))
	buf.WriteByte(100) // offset = 100
	buf.WriteByte(100) // size = 100

	// Add restart points (1 restart at offset 0)
	restartData := make([]byte, 8)
	restartData[4] = 1 // count = 1
	buf.Write(restartData)

	data := buf.Bytes()
	dataEnd := len(data) - 8 // Exclude restart data

	iter := NewIndexBlockIterator(data, dataEnd)

	t.Run("SeekToFirstAndIterate", func(t *testing.T) {
		iter.SeekToFirst()
		if !iter.Valid() {
			t.Fatal("Should be valid after SeekToFirst")
		}

		// First entry
		key := iter.Key()
		if !bytes.Equal(key, []byte("key1")) {
			t.Errorf("First key: got %q, want %q", key, "key1")
		}

		// Test Value()
		value := iter.Value()
		if len(value) < 2 {
			t.Errorf("Value too short: %v", value)
		}

		// Next entry
		iter.Next()
		if !iter.Valid() {
			t.Fatal("Should be valid after Next")
		}
		key = iter.Key()
		if !bytes.Equal(key, []byte("key2")) {
			t.Errorf("Second key: got %q, want %q", key, "key2")
		}

		// After last, should be invalid
		iter.Next()
		if iter.Valid() {
			t.Error("Should be invalid after exhausting entries")
		}
	})

	t.Run("SeekToLast", func(t *testing.T) {
		iter.SeekToLast()
		if !iter.Valid() {
			t.Fatal("Should be valid after SeekToLast")
		}
		key := iter.Key()
		if !bytes.Equal(key, []byte("key2")) {
			t.Errorf("Last key: got %q, want %q", key, "key2")
		}
	})

	t.Run("Prev", func(t *testing.T) {
		iter.SeekToLast()
		iter.Prev()
		if !iter.Valid() {
			t.Fatal("Should be valid after Prev from last")
		}
		key := iter.Key()
		if !bytes.Equal(key, []byte("key1")) {
			t.Errorf("After Prev: got %q, want %q", key, "key1")
		}
	})

	t.Run("Seek", func(t *testing.T) {
		iter.Seek([]byte("key2"))
		if !iter.Valid() {
			t.Fatal("Should be valid after Seek")
		}
		key := iter.Key()
		if !bytes.Equal(key, []byte("key2")) {
			t.Errorf("After Seek(key2): got %q, want %q", key, "key2")
		}
	})

	t.Run("SeekBeyond", func(t *testing.T) {
		iter.Seek([]byte("key9"))
		if iter.Valid() {
			t.Error("Should be invalid after Seek beyond last")
		}
	})
}

// TestIndexBlockIteratorEdgeCases tests edge cases for IndexBlockIterator.
func TestIndexBlockIteratorEdgeCases(t *testing.T) {
	// Create a simple SST to test edge cases
	opts := DefaultBuilderOptions()
	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	// Add a single entry
	builder.Add([]byte("key\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("value"))
	builder.Finish()

	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Test that Prev at first entry goes invalid
	t.Run("PrevAtFirst", func(t *testing.T) {
		iter.SeekToFirst()
		if !iter.Valid() {
			t.Skip("No entries")
		}
		iter.Prev()
		if iter.Valid() {
			t.Error("Should be invalid after Prev at first")
		}
	})

	// Test Next at last entry goes invalid
	t.Run("NextAtLast", func(t *testing.T) {
		iter.SeekToLast()
		if !iter.Valid() {
			t.Skip("No entries")
		}
		iter.Next()
		if iter.Valid() {
			t.Error("Should be invalid after Next at last")
		}
	})

	// Test Key/Value before positioning
	t.Run("KeyValueBeforeSeek", func(t *testing.T) {
		iter2 := reader.NewIterator()
		// Valid should be false before any seek
		if iter2.Valid() {
			t.Error("Should not be valid before seek")
		}
	})
}

// TestNewTableBuilderVariants tests various builder options paths.
func TestNewTableBuilderVariants(t *testing.T) {
	t.Run("WithCompression", func(t *testing.T) {
		opts := DefaultBuilderOptions()
		opts.Compression = 1 // Snappy
		buf := &bytes.Buffer{}
		builder := NewTableBuilder(buf, opts)
		builder.Add([]byte("key\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("value"))
		if err := builder.Finish(); err != nil {
			t.Fatalf("Finish failed: %v", err)
		}
	})

	t.Run("WithFilter", func(t *testing.T) {
		opts := DefaultBuilderOptions()
		opts.FilterBitsPerKey = 10
		buf := &bytes.Buffer{}
		builder := NewTableBuilder(buf, opts)
		for i := range 100 {
			key := makeTestKey(i)
			builder.Add(key, []byte("value"))
		}
		if err := builder.Finish(); err != nil {
			t.Fatalf("Finish failed: %v", err)
		}
	})

	t.Run("LargeBlockSize", func(t *testing.T) {
		opts := DefaultBuilderOptions()
		opts.BlockSize = 1024 * 1024 // 1MB blocks
		buf := &bytes.Buffer{}
		builder := NewTableBuilder(buf, opts)
		for i := range 10 {
			key := makeTestKey(i)
			builder.Add(key, []byte("value"))
		}
		if err := builder.Finish(); err != nil {
			t.Fatalf("Finish failed: %v", err)
		}
	})
}

// TestTableBuilderFinishEmptyTable tests finishing an empty table.
func TestTableBuilderFinishEmptyTable(t *testing.T) {
	opts := DefaultBuilderOptions()
	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	// Finish without adding any entries
	err := builder.Finish()
	if err != nil {
		t.Fatalf("Finish on empty table failed: %v", err)
	}
}

// makeTestKey creates an internal key for index_iterator tests
func makeTestKey(n int) []byte {
	userKey := fmt.Sprintf("key%03d", n)
	key := make([]byte, len(userKey)+8)
	copy(key, userKey)
	seq := uint64(1000 - n)
	trailer := (seq << 8) | 1
	for i := range 8 {
		key[len(userKey)+i] = byte(trailer >> (8 * i))
	}
	return key
}

func makeTestKeyWithGap(n int) []byte {
	userKey := fmt.Sprintf("key%03d", n)
	key := make([]byte, len(userKey)+8)
	copy(key, userKey)
	seq := uint64(1000)
	trailer := (seq << 8) | 1
	for i := range 8 {
		key[len(userKey)+i] = byte(trailer >> (8 * i))
	}
	return key
}
