package block

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// -----------------------------------------------------------------------------
// Handle tests
// -----------------------------------------------------------------------------

func TestHandleEncodeDecode(t *testing.T) {
	tests := []struct {
		offset uint64
		size   uint64
	}{
		{0, 0},
		{1, 1},
		{100, 200},
		{0xFFFFFFFF, 0xFFFFFFFF},
		{1 << 50, 1 << 40},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("offset=%d_size=%d", tt.offset, tt.size), func(t *testing.T) {
			h := Handle{Offset: tt.offset, Size: tt.size}

			// Encode
			encoded := h.EncodeToSlice()

			// Decode
			decoded, remaining, err := DecodeHandle(encoded)
			if err != nil {
				t.Fatalf("DecodeHandle error: %v", err)
			}

			if len(remaining) != 0 {
				t.Errorf("Unexpected remaining bytes: %d", len(remaining))
			}

			if decoded.Offset != tt.offset {
				t.Errorf("Offset = %d, want %d", decoded.Offset, tt.offset)
			}
			if decoded.Size != tt.size {
				t.Errorf("Size = %d, want %d", decoded.Size, tt.size)
			}
		})
	}
}

func TestHandleIsNull(t *testing.T) {
	if !NullHandle.IsNull() {
		t.Error("NullHandle.IsNull() = false, want true")
	}

	h := Handle{Offset: 0, Size: 1}
	if h.IsNull() {
		t.Error("Non-null handle.IsNull() = true")
	}
}

func TestHandleEncodedLength(t *testing.T) {
	tests := []struct {
		h       Handle
		wantLen int
	}{
		{Handle{0, 0}, 2},              // Two 1-byte varints
		{Handle{127, 127}, 2},          // Two 1-byte varints
		{Handle{128, 128}, 4},          // Two 2-byte varints
		{Handle{1 << 28, 1 << 28}, 10}, // Two 5-byte varints
	}

	for _, tt := range tests {
		if got := tt.h.EncodedLength(); got != tt.wantLen {
			t.Errorf("Handle{%d,%d}.EncodedLength() = %d, want %d",
				tt.h.Offset, tt.h.Size, got, tt.wantLen)
		}
	}
}

func TestDecodeHandleError(t *testing.T) {
	// Empty input
	_, _, err := DecodeHandle(nil)
	if !errors.Is(err, ErrBadBlockHandle) {
		t.Errorf("Expected ErrBadBlockHandle for empty input, got %v", err)
	}

	// Truncated input
	_, _, err = DecodeHandle([]byte{0x80}) // Incomplete varint
	if !errors.Is(err, ErrBadBlockHandle) {
		t.Errorf("Expected ErrBadBlockHandle for truncated input, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// Builder tests
// -----------------------------------------------------------------------------

func TestBuilderEmpty(t *testing.T) {
	b := NewBuilder(16)

	if !b.Empty() {
		t.Error("New builder should be empty")
	}

	data := b.Finish()

	// Should have just the restarts array (1 restart) + footer
	// 1 restart (4 bytes) + footer (4 bytes) = 8 bytes
	if len(data) != 8 {
		t.Errorf("Empty block size = %d, want 8", len(data))
	}
}

func TestBuilderSingleEntry(t *testing.T) {
	b := NewBuilder(16)
	b.Add([]byte("key"), []byte("value"))
	data := b.Finish()

	// Parse it back
	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}

	if block.NumRestarts() != 1 {
		t.Errorf("NumRestarts = %d, want 1", block.NumRestarts())
	}

	// Iterate
	it := block.NewIterator()
	it.SeekToFirst()

	if !it.Valid() {
		t.Fatal("Iterator should be valid")
	}

	if !bytes.Equal(it.Key(), []byte("key")) {
		t.Errorf("Key = %q, want %q", it.Key(), "key")
	}
	if !bytes.Equal(it.Value(), []byte("value")) {
		t.Errorf("Value = %q, want %q", it.Value(), "value")
	}

	it.Next()
	if it.Valid() {
		t.Error("Iterator should be invalid after last entry")
	}
}

func TestBuilderMultipleEntries(t *testing.T) {
	b := NewBuilder(16)

	entries := []struct {
		key   string
		value string
	}{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "red"},
		{"date", "brown"},
		{"elderberry", "purple"},
	}

	for _, e := range entries {
		b.Add([]byte(e.key), []byte(e.value))
	}

	data := b.Finish()
	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}

	// Iterate and verify
	it := block.NewIterator()
	it.SeekToFirst()

	for _, e := range entries {
		if !it.Valid() {
			t.Fatalf("Iterator invalid, expected key %q", e.key)
		}
		if string(it.Key()) != e.key {
			t.Errorf("Key = %q, want %q", it.Key(), e.key)
		}
		if string(it.Value()) != e.value {
			t.Errorf("Value = %q, want %q", it.Value(), e.value)
		}
		it.Next()
	}

	if it.Valid() {
		t.Error("Iterator should be invalid after all entries")
	}
}

func TestBuilderRestartPoints(t *testing.T) {
	b := NewBuilder(4) // Restart every 4 entries

	// Add 10 entries
	for i := range 10 {
		key := fmt.Sprintf("key%02d", i)
		value := fmt.Sprintf("value%02d", i)
		b.Add([]byte(key), []byte(value))
	}

	data := b.Finish()
	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}

	// Should have 3 restart points: 0, 4, 8
	if block.NumRestarts() != 3 {
		t.Errorf("NumRestarts = %d, want 3", block.NumRestarts())
	}
}

func TestBuilderPrefixCompression(t *testing.T) {
	b := NewBuilder(16)

	// Add keys with common prefix
	b.Add([]byte("prefix_aaa"), []byte("1"))
	b.Add([]byte("prefix_aab"), []byte("2"))
	b.Add([]byte("prefix_aac"), []byte("3"))

	data := b.Finish()

	// The block should be smaller than without compression
	// Each key is 10 bytes, 3 keys = 30 bytes
	// With compression, only the first key is fully stored
	// The data portion should be less than 30 bytes + 3 values (3 bytes)
	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}

	// Verify iteration still works
	it := block.NewIterator()
	it.SeekToFirst()

	expected := []string{"prefix_aaa", "prefix_aab", "prefix_aac"}
	for _, exp := range expected {
		if !it.Valid() {
			t.Fatalf("Iterator invalid, expected %q", exp)
		}
		if string(it.Key()) != exp {
			t.Errorf("Key = %q, want %q", it.Key(), exp)
		}
		it.Next()
	}
}

func TestBuilderReset(t *testing.T) {
	b := NewBuilder(16)
	b.Add([]byte("key1"), []byte("value1"))
	b.Finish()

	b.Reset()

	if !b.Empty() {
		t.Error("Builder should be empty after Reset")
	}

	b.Add([]byte("key2"), []byte("value2"))
	data := b.Finish()

	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}

	it := block.NewIterator()
	it.SeekToFirst()

	if string(it.Key()) != "key2" {
		t.Errorf("Key = %q, want %q", it.Key(), "key2")
	}
}

func TestBuilderNoDeltaEncoding(t *testing.T) {
	b := NewBuilderWithOptions(16, false) // Disable delta encoding

	b.Add([]byte("aaa"), []byte("1"))
	b.Add([]byte("aab"), []byte("2"))

	data := b.Finish()
	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}

	// Should have multiple restart points since no delta encoding
	if block.NumRestarts() < 2 {
		t.Errorf("Expected multiple restart points without delta encoding")
	}
}

// -----------------------------------------------------------------------------
// Block tests
// -----------------------------------------------------------------------------

func TestBlockSeekToFirst(t *testing.T) {
	b := NewBuilder(16)
	b.Add([]byte("aaa"), []byte("1"))
	b.Add([]byte("bbb"), []byte("2"))
	b.Add([]byte("ccc"), []byte("3"))
	data := b.Finish()

	block, _ := NewBlock(data)
	it := block.NewIterator()
	it.SeekToFirst()

	if !it.Valid() || string(it.Key()) != "aaa" {
		t.Errorf("SeekToFirst: key = %q, want %q", it.Key(), "aaa")
	}
}

func TestBlockSeekToLast(t *testing.T) {
	b := NewBuilder(16)
	b.Add([]byte("aaa"), []byte("1"))
	b.Add([]byte("bbb"), []byte("2"))
	b.Add([]byte("ccc"), []byte("3"))
	data := b.Finish()

	block, _ := NewBlock(data)
	it := block.NewIterator()
	it.SeekToLast()

	if !it.Valid() || string(it.Key()) != "ccc" {
		t.Errorf("SeekToLast: key = %q, want %q", it.Key(), "ccc")
	}
}

func TestBlockSeek(t *testing.T) {
	b := NewBuilder(4)

	// Add many entries to have multiple restart points
	keys := []string{"apple", "banana", "cherry", "date", "elderberry",
		"fig", "grape", "honeydew", "kiwi", "lemon"}

	for _, k := range keys {
		b.Add([]byte(k), []byte("v"))
	}

	data := b.Finish()
	block, _ := NewBlock(data)

	tests := []struct {
		target   string
		expected string
	}{
		{"apple", "apple"},   // Exact match at start
		{"banana", "banana"}, // Exact match
		{"cherry", "cherry"}, // Exact match
		{"aaa", "apple"},     // Before first
		{"cat", "cherry"},    // Between banana and cherry
		{"lemon", "lemon"},   // Exact match at end
		{"zzz", ""},          // After last (invalid)
		{"fig", "fig"},       // Exact match
		{"grape", "grape"},   // Exact match
	}

	for _, tt := range tests {
		it := block.NewIterator()
		it.Seek([]byte(tt.target))

		if tt.expected == "" {
			if it.Valid() {
				t.Errorf("Seek(%q): expected invalid, got key %q", tt.target, it.Key())
			}
		} else {
			if !it.Valid() {
				t.Errorf("Seek(%q): expected %q, got invalid", tt.target, tt.expected)
			} else if string(it.Key()) != tt.expected {
				t.Errorf("Seek(%q): got %q, want %q", tt.target, it.Key(), tt.expected)
			}
		}
	}
}

func TestBlockEmptyValue(t *testing.T) {
	b := NewBuilder(16)
	b.Add([]byte("key"), []byte(""))
	data := b.Finish()

	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}

	it := block.NewIterator()
	it.SeekToFirst()

	if !it.Valid() {
		t.Fatal("Iterator should be valid")
	}
	if len(it.Value()) != 0 {
		t.Errorf("Value length = %d, want 0", len(it.Value()))
	}
}

func TestBlockBinaryData(t *testing.T) {
	b := NewBuilder(16)

	// Binary data with null bytes
	key := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
	value := []byte{0xFF, 0x00, 0xFF, 0x00}

	b.Add(key, value)
	data := b.Finish()

	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock error: %v", err)
	}

	it := block.NewIterator()
	it.SeekToFirst()

	if !bytes.Equal(it.Key(), key) {
		t.Errorf("Key mismatch")
	}
	if !bytes.Equal(it.Value(), value) {
		t.Errorf("Value mismatch")
	}
}

// -----------------------------------------------------------------------------
// Footer tests
// -----------------------------------------------------------------------------

func TestFooterMagicNumbers(t *testing.T) {
	// Verify magic numbers match RocksDB
	if LegacyBlockBasedTableMagicNumber != 0xdb4775248b80fb57 {
		t.Errorf("LegacyBlockBasedTableMagicNumber mismatch")
	}
	if BlockBasedTableMagicNumber != 0x88e241b785f4cff7 {
		t.Errorf("BlockBasedTableMagicNumber mismatch")
	}
}

func TestChecksumTypeConstants(t *testing.T) {
	if ChecksumTypeNone != 0 {
		t.Error("ChecksumTypeNone != 0")
	}
	if ChecksumTypeCRC32C != 1 {
		t.Error("ChecksumTypeCRC32C != 1")
	}
	if ChecksumTypeXXHash != 2 {
		t.Error("ChecksumTypeXXHash != 2")
	}
	if ChecksumTypeXXHash64 != 3 {
		t.Error("ChecksumTypeXXHash64 != 3")
	}
	if ChecksumTypeXXH3 != 4 {
		t.Error("ChecksumTypeXXH3 != 4")
	}
}

func TestPackUnpackIndexTypeAndNumRestarts(t *testing.T) {
	tests := []struct {
		indexType   DataBlockIndexType
		numRestarts uint32
	}{
		{DataBlockBinarySearch, 1},
		{DataBlockBinarySearch, 100},
		{DataBlockBinarySearch, 1000000},
		{DataBlockBinaryAndHash, 1},
		{DataBlockBinaryAndHash, 100},
	}

	for _, tt := range tests {
		packed := PackIndexTypeAndNumRestarts(tt.indexType, tt.numRestarts)
		gotType, gotNum := UnpackIndexTypeAndNumRestarts(packed)

		if gotType != tt.indexType {
			t.Errorf("Index type mismatch: got %d, want %d", gotType, tt.indexType)
		}
		if gotNum != tt.numRestarts {
			t.Errorf("NumRestarts mismatch: got %d, want %d", gotNum, tt.numRestarts)
		}
	}
}

// -----------------------------------------------------------------------------
// Fuzz tests
// -----------------------------------------------------------------------------

func FuzzBlockRoundtrip(f *testing.F) {
	// Seed corpus
	f.Add([]byte("key"), []byte("value"))
	f.Add([]byte(""), []byte(""))
	f.Add([]byte{0, 1, 2}, []byte{3, 4, 5})

	f.Fuzz(func(t *testing.T, key, value []byte) {
		if len(key) == 0 {
			return // Skip empty keys
		}

		b := NewBuilder(16)
		b.Add(key, value)
		data := b.Finish()

		block, err := NewBlock(data)
		if err != nil {
			t.Fatalf("NewBlock error: %v", err)
		}

		it := block.NewIterator()
		it.SeekToFirst()

		if !it.Valid() {
			t.Fatal("Iterator should be valid")
		}

		if !bytes.Equal(it.Key(), key) {
			t.Errorf("Key mismatch")
		}
		if !bytes.Equal(it.Value(), value) {
			t.Errorf("Value mismatch")
		}
	})
}

func FuzzBlockMultipleEntries(f *testing.F) {
	f.Add(3, 10)

	f.Fuzz(func(t *testing.T, numEntries, keyLen int) {
		if numEntries <= 0 || numEntries > 100 || keyLen <= 0 || keyLen > 100 {
			return
		}

		b := NewBuilder(4)

		// Generate sorted keys
		for i := range numEntries {
			key := make([]byte, keyLen)
			for j := range key {
				key[j] = byte('a' + (i % 26))
			}
			key[len(key)-1] = byte('0' + i%10)

			b.Add(key, []byte("value"))
		}

		data := b.Finish()
		block, err := NewBlock(data)
		if err != nil {
			t.Fatalf("NewBlock error: %v", err)
		}

		// Count entries
		count := 0
		it := block.NewIterator()
		it.SeekToFirst()
		for it.Valid() {
			count++
			it.Next()
		}

		if count != numEntries {
			t.Errorf("Entry count = %d, want %d", count, numEntries)
		}
	})
}

// -----------------------------------------------------------------------------
// Benchmark tests
// -----------------------------------------------------------------------------

func BenchmarkBlockBuild(b *testing.B) {
	builder := NewBuilder(16)

	keys := make([][]byte, 1000)
	values := make([][]byte, 1000)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key%06d", i)
		values[i] = fmt.Appendf(nil, "value%06d", i)
	}

	for b.Loop() {
		builder.Reset()
		for j := range keys {
			builder.Add(keys[j], values[j])
		}
		builder.Finish()
	}
}

func BenchmarkBlockIterate(b *testing.B) {
	builder := NewBuilder(16)
	for i := range 1000 {
		key := fmt.Appendf(nil, "key%06d", i)
		value := fmt.Appendf(nil, "value%06d", i)
		builder.Add(key, value)
	}
	data := builder.Finish()
	block, _ := NewBlock(data)

	for b.Loop() {
		it := block.NewIterator()
		it.SeekToFirst()
		for it.Valid() {
			_ = it.Key()
			_ = it.Value()
			it.Next()
		}
	}
}

// -----------------------------------------------------------------------------
// Magic Number Verification Tests
// These tests verify our magic numbers match C++ RocksDB v10.7.5 exactly.
// Source files:
//   - table/block_based/block_based_table_builder.cc
//   - table/plain/plain_table_builder.cc
//   - table/cuckoo/cuckoo_table_builder.cc
// -----------------------------------------------------------------------------

func TestMagicNumbersMatchCpp(t *testing.T) {
	// These values are copied directly from RocksDB v10.7.5 source code.
	// If these fail, we have a compatibility issue.
	tests := []struct {
		name     string
		got      uint64
		expected uint64
		source   string
	}{
		{
			name:     "kBlockBasedTableMagicNumber",
			got:      BlockBasedTableMagicNumber,
			expected: 0x88e241b785f4cff7,
			source:   "table/block_based/block_based_table_builder.cc:123",
		},
		{
			name:     "kLegacyBlockBasedTableMagicNumber",
			got:      LegacyBlockBasedTableMagicNumber,
			expected: 0xdb4775248b80fb57,
			source:   "table/block_based/block_based_table_builder.cc:126",
		},
		{
			name:     "kPlainTableMagicNumber",
			got:      PlainTableMagicNumber,
			expected: 0x8242229663bf9564,
			source:   "table/plain/plain_table_builder.cc:55",
		},
		{
			name:     "kLegacyPlainTableMagicNumber",
			got:      LegacyPlainTableMagicNumber,
			expected: 0x4f3418eb7a8f13b8,
			source:   "table/plain/plain_table_builder.cc:56",
		},
		{
			name:     "kCuckooTableMagicNumber",
			got:      CuckooTableMagicNumber,
			expected: 0x926789d0c5f17873,
			source:   "table/cuckoo/cuckoo_table_builder.cc:47",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s = 0x%x, want 0x%x (from %s)",
					tt.name, tt.got, tt.expected, tt.source)
			}
		})
	}
}

func TestFormatVersionConstants(t *testing.T) {
	// Verify format version constants match C++ table/format.h
	if LatestFormatVersion != 7 {
		t.Errorf("LatestFormatVersion = %d, want 7 (from table/format.h:178)", LatestFormatVersion)
	}

	if BlockTrailerSize != 5 {
		t.Errorf("BlockTrailerSize = %d, want 5", BlockTrailerSize)
	}
}

func TestFormatVersionFunctions(t *testing.T) {
	// Test FormatVersionUsesContextChecksum (version >= 6)
	tests := []struct {
		version  uint32
		expected bool
	}{
		{0, false},
		{1, false},
		{5, false},
		{6, true},
		{7, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("version=%d", tt.version), func(t *testing.T) {
			got := FormatVersionUsesContextChecksum(tt.version)
			if got != tt.expected {
				t.Errorf("FormatVersionUsesContextChecksum(%d) = %v, want %v",
					tt.version, got, tt.expected)
			}
		})
	}

	// Test FormatVersionUsesIndexHandleInFooter (version < 6)
	for _, tt := range tests {
		t.Run(fmt.Sprintf("indexInFooter_version=%d", tt.version), func(t *testing.T) {
			got := FormatVersionUsesIndexHandleInFooter(tt.version)
			expected := !tt.expected // opposite of context checksum
			if got != expected {
				t.Errorf("FormatVersionUsesIndexHandleInFooter(%d) = %v, want %v",
					tt.version, got, expected)
			}
		})
	}
}
