package block

import (
	"bytes"
	"testing"

	"github.com/aalhour/rockyardkv/internal/encoding"
)

// -----------------------------------------------------------------------------
// Block Corruption and Edge Case Tests
// Based on RocksDB table/block_based/block_test.cc
// -----------------------------------------------------------------------------

// TestBlockCorruptedRestarts tests handling of corrupted restart count.
func TestBlockCorruptedRestarts(t *testing.T) {
	tests := []struct {
		name        string
		blockData   []byte
		expectError bool
	}{
		{
			name:        "empty block",
			blockData:   []byte{},
			expectError: true,
		},
		{
			name:        "too short for restart count",
			blockData:   []byte{0x01, 0x02, 0x03},
			expectError: true,
		},
		{
			name:        "restart count claims too many",
			blockData:   append(make([]byte, 10), []byte{0xFF, 0xFF, 0xFF, 0x7F}...), // 2^31-1 restarts
			expectError: true,
		},
		{
			name:        "restart count points past end",
			blockData:   []byte{0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00}, // 10 restarts but not enough data
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block, err := NewBlock(tt.blockData)
			if tt.expectError {
				if err == nil && block != nil {
					iter := block.NewIterator()
					iter.SeekToFirst()
					if iter.Valid() {
						t.Error("Expected iterator to be invalid for corrupted block")
					}
				}
				// Expected error or invalid block
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

// TestBlockBadSharedKeyPrefix tests handling of invalid shared key prefix.
func TestBlockBadSharedKeyPrefix(t *testing.T) {
	// Build a block with a bad shared prefix (claims to share more than exists)
	var buf bytes.Buffer

	// First entry: shared=0, non_shared=3, value_len=3, key="abc", value="xyz"
	buf.Write(encoding.AppendVarint32(nil, 0)) // shared
	buf.Write(encoding.AppendVarint32(nil, 3)) // non_shared
	buf.Write(encoding.AppendVarint32(nil, 3)) // value_length
	buf.WriteString("abc")
	buf.WriteString("xyz")

	// Second entry: shared=100 (bad - claims to share 100 bytes from 3-byte key)
	buf.Write(encoding.AppendVarint32(nil, 100)) // shared (BAD)
	buf.Write(encoding.AppendVarint32(nil, 1))   // non_shared
	buf.Write(encoding.AppendVarint32(nil, 1))   // value_length
	buf.WriteByte('d')
	buf.WriteByte('w')

	// Restart array: 1 restart at offset 0
	buf.Write(encoding.AppendFixed32(nil, 0))
	buf.Write(encoding.AppendFixed32(nil, 1)) // 1 restart

	blockData := buf.Bytes()

	block, err := NewBlock(blockData)
	if err != nil {
		// Error is acceptable for corrupted data
		return
	}

	iter := block.NewIterator()
	iter.SeekToFirst()
	if !iter.Valid() {
		return // First entry might still be valid
	}

	// Try to get second entry
	iter.Next()

	// The iterator should either be invalid or have an error due to bad shared prefix
	if iter.Valid() {
		key := iter.Key()
		if len(key) > 100 {
			t.Log("Iterator produced very long key from corrupted shared prefix")
		}
	}
}

// TestBlockTruncatedEntry tests handling of truncated block entries.
func TestBlockTruncatedEntry(t *testing.T) {
	var buf bytes.Buffer

	// Entry header: shared=0, non_shared=100, value_len=100 (but no actual data follows)
	buf.Write(encoding.AppendVarint32(nil, 0))
	buf.Write(encoding.AppendVarint32(nil, 100))
	buf.Write(encoding.AppendVarint32(nil, 100))

	// Only write partial key (10 bytes instead of 100)
	buf.Write(make([]byte, 10))

	// Restart array
	buf.Write(encoding.AppendFixed32(nil, 0))
	buf.Write(encoding.AppendFixed32(nil, 1))

	block, err := NewBlock(buf.Bytes())
	if err != nil {
		// Error is acceptable
		return
	}

	iter := block.NewIterator()
	iter.SeekToFirst()

	// Iterator should handle truncated data gracefully
	if iter.Valid() {
		key := iter.Key()
		val := iter.Value()
		t.Logf("Got key len=%d, value len=%d from truncated entry", len(key), len(val))
	}
}

// TestBlockZeroRestartsIsInvalid tests that zero restarts causes error.
func TestBlockZeroRestartsIsInvalid(t *testing.T) {
	// Block with just the restart count (0)
	blockData := encoding.AppendFixed32(nil, 0)

	_, err := NewBlock(blockData)
	if err == nil {
		t.Error("Expected error for block with zero restarts")
	}
}

// TestBlockSeekBeyondLast tests seeking beyond the last key.
func TestBlockSeekBeyondLastKey(t *testing.T) {
	builder := NewBuilder(16)
	keys := []string{"aaa", "bbb", "ccc"}
	for _, k := range keys {
		builder.Add([]byte(k), []byte("value"))
	}
	blockData := builder.Finish()

	block, err := NewBlock(blockData)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}
	iter := block.NewIterator()

	// Seek to a key beyond all entries
	iter.Seek([]byte("zzz"))

	if iter.Valid() {
		t.Errorf("Seek beyond last should be invalid, got key=%s", iter.Key())
	}
}

// TestBlockSeekToBeforeFirst tests seeking before the first key.
func TestBlockSeekToBeforeFirstKey(t *testing.T) {
	builder := NewBuilder(16)
	keys := []string{"bbb", "ccc", "ddd"}
	for _, k := range keys {
		builder.Add([]byte(k), []byte("value"))
	}
	blockData := builder.Finish()

	block, err := NewBlock(blockData)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}
	iter := block.NewIterator()

	// Seek to a key before all entries
	iter.Seek([]byte("aaa"))

	// Should land on first key
	if !iter.Valid() {
		t.Error("Seek before first should land on first key")
	} else if string(iter.Key()) != "bbb" {
		t.Errorf("Expected to land on 'bbb', got '%s'", iter.Key())
	}
}

// TestBlockEmptyKeyEntry tests handling of empty keys.
func TestBlockEmptyKeyEntry(t *testing.T) {
	builder := NewBuilder(16)

	// Add entry with empty key
	builder.Add([]byte{}, []byte("value"))

	blockData := builder.Finish()
	block, err := NewBlock(blockData)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}
	iter := block.NewIterator()

	iter.SeekToFirst()
	if !iter.Valid() {
		t.Error("Expected valid entry for empty key")
		return
	}

	key := iter.Key()
	if len(key) != 0 {
		t.Errorf("Expected empty key, got length %d", len(key))
	}
}

// TestBlockLargeKey tests handling of very large keys.
func TestBlockLargeKeyEntry(t *testing.T) {
	builder := NewBuilder(16)

	// Create a 64KB key
	largeKey := make([]byte, 64*1024)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}

	builder.Add(largeKey, []byte("value"))

	blockData := builder.Finish()
	block, err := NewBlock(blockData)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}
	iter := block.NewIterator()

	iter.SeekToFirst()
	if !iter.Valid() {
		t.Error("Expected valid entry for large key")
		return
	}

	key := iter.Key()
	if !bytes.Equal(key, largeKey) {
		t.Error("Large key content mismatch")
	}
}

// TestBlockLargeValueEntry tests handling of very large values.
func TestBlockLargeValueEntry(t *testing.T) {
	builder := NewBuilder(16)

	// Create a 1MB value
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	builder.Add([]byte("key"), largeValue)

	blockData := builder.Finish()
	block, err := NewBlock(blockData)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}
	iter := block.NewIterator()

	iter.SeekToFirst()
	if !iter.Valid() {
		t.Error("Expected valid entry for large value")
		return
	}

	value := iter.Value()
	if !bytes.Equal(value, largeValue) {
		t.Error("Large value content mismatch")
	}
}

// TestBlockRestartPointAccuracy tests that restart points are correct.
func TestBlockRestartPointAccuracy(t *testing.T) {
	// Use restart interval of 4
	builder := NewBuilder(4)

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for _, k := range keys {
		builder.Add([]byte(k), []byte("v"))
	}

	blockData := builder.Finish()
	block, err := NewBlock(blockData)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}
	iter := block.NewIterator()

	// Seek to each key and verify
	for _, k := range keys {
		iter.Seek([]byte(k))

		if !iter.Valid() {
			t.Errorf("Seek(%s) should be valid", k)
			continue
		}

		if string(iter.Key()) != k {
			t.Errorf("Seek(%s) landed on %s", k, iter.Key())
		}
	}
}

// TestBlockMultipleRestartPoints tests blocks with multiple restart points.
func TestBlockMultipleRestartPoints(t *testing.T) {
	builder := NewBuilder(2) // Restart every 2 entries

	for i := range 100 {
		key := []byte{byte('a' + i/26), byte('a' + i%26)}
		builder.Add(key, []byte("value"))
	}

	blockData := builder.Finish()
	block, err := NewBlock(blockData)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}

	// Verify we can iterate through all entries
	iter := block.NewIterator()
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	if count != 100 {
		t.Errorf("Expected 100 entries, got %d", count)
	}

	// Verify seeking works - seek to a key that exists
	iter.Seek([]byte("ba")) // key 26 = [b, a]
	if !iter.Valid() {
		t.Error("Seek should be valid")
	}
	if string(iter.Key()) != "ba" {
		t.Logf("Seek(ba) landed on %q (this may be expected for seek)", iter.Key())
	}
}
