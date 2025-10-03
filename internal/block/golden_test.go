package block

import (
	"testing"
)

// TestGoldenBlockMagicNumbers tests that magic numbers match RocksDB v10.7.5.
// These values are critical for SST file compatibility.
func TestGoldenBlockMagicNumbers(t *testing.T) {
	testCases := []struct {
		name     string
		got      uint64
		expected uint64
	}{
		{
			name:     "BlockBasedTableMagicNumber",
			got:      BlockBasedTableMagicNumber,
			expected: 0x88e241b785f4cff7,
		},
		{
			name:     "LegacyBlockBasedTableMagicNumber",
			got:      LegacyBlockBasedTableMagicNumber,
			expected: 0xdb4775248b80fb57,
		},
		{
			name:     "PlainTableMagicNumber",
			got:      PlainTableMagicNumber,
			expected: 0x8242229663bf9564,
		},
		{
			name:     "LegacyPlainTableMagicNumber",
			got:      LegacyPlainTableMagicNumber,
			expected: 0x4f3418eb7a8f13b8,
		},
		{
			name:     "CuckooTableMagicNumber",
			got:      CuckooTableMagicNumber,
			expected: 0x926789d0c5f17873,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.expected {
				t.Errorf("%s = 0x%016x, want 0x%016x", tc.name, tc.got, tc.expected)
			}
		})
	}
}

// TestGoldenBlockChecksumTypes tests checksum type constants.
func TestGoldenBlockChecksumTypes(t *testing.T) {
	testCases := []struct {
		name     string
		got      ChecksumType
		expected uint8
	}{
		{"ChecksumTypeNone", ChecksumTypeNone, 0},
		{"ChecksumTypeCRC32C", ChecksumTypeCRC32C, 1},
		{"ChecksumTypeXXHash", ChecksumTypeXXHash, 2},
		{"ChecksumTypeXXHash64", ChecksumTypeXXHash64, 3},
		{"ChecksumTypeXXH3", ChecksumTypeXXH3, 4},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if uint8(tc.got) != tc.expected {
				t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.expected)
			}
		})
	}
}

// TestGoldenBlockHandleFormat tests BlockHandle encoding format.
// BlockHandle is encoded as two varints: offset and size.
func TestGoldenBlockHandleFormat(t *testing.T) {
	testCases := []struct {
		name     string
		offset   uint64
		size     uint64
		expected []byte
	}{
		{
			name:     "zero handle",
			offset:   0,
			size:     0,
			expected: []byte{0x00, 0x00},
		},
		{
			name:     "small values",
			offset:   100,
			size:     50,
			expected: []byte{0x64, 0x32},
		},
		{
			name:     "larger values",
			offset:   1000,
			size:     500,
			expected: []byte{0xe8, 0x07, 0xf4, 0x03},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := Handle{Offset: tc.offset, Size: tc.size}
			encoded := h.EncodeToSlice()

			// Verify encoding length
			if len(encoded) != len(tc.expected) {
				t.Errorf("Handle{%d, %d}.EncodeToSlice() length = %d, want %d",
					tc.offset, tc.size, len(encoded), len(tc.expected))
			}

			// Verify decode
			decoded, remaining, err := DecodeHandle(encoded)
			if err != nil {
				t.Fatalf("DecodeHandle failed: %v", err)
			}
			if len(remaining) != 0 {
				t.Errorf("DecodeHandle left %d bytes unconsumed", len(remaining))
			}
			if decoded.Offset != tc.offset || decoded.Size != tc.size {
				t.Errorf("DecodeHandle = {%d, %d}, want {%d, %d}",
					decoded.Offset, decoded.Size, tc.offset, tc.size)
			}
		})
	}
}

// TestGoldenBlockFooterSize tests footer size constants.
func TestGoldenBlockFooterSize(t *testing.T) {
	// Version 0 footer: 48 bytes (2 handles + padding + magic)
	if Version0EncodedLength != 48 {
		t.Errorf("Version0EncodedLength = %d, want 48", Version0EncodedLength)
	}

	// New version footer: 53 bytes (checksum + handles + format_version + magic)
	if NewVersionsEncodedLength != 53 {
		t.Errorf("NewVersionsEncodedLength = %d, want 53", NewVersionsEncodedLength)
	}

	// Magic number is 8 bytes
	if MagicNumberLengthByte != 8 {
		t.Errorf("MagicNumberLengthByte = %d, want 8", MagicNumberLengthByte)
	}

	// Block trailer size (compression type + checksum)
	if BlockTrailerSize != 5 {
		t.Errorf("BlockTrailerSize = %d, want 5", BlockTrailerSize)
	}
}

// TestGoldenBlockPackUnpackIndexType tests the index type packing in block footer.
// The index type is stored in the MSB (bit 31) of the num_restarts field.
func TestGoldenBlockPackUnpackIndexType(t *testing.T) {
	testCases := []struct {
		indexType   DataBlockIndexType
		numRestarts uint32
		expected    uint32
	}{
		{DataBlockBinarySearch, 0, 0x00000000},
		{DataBlockBinarySearch, 1, 0x00000001},
		{DataBlockBinarySearch, 16, 0x00000010},
		{DataBlockBinaryAndHash, 0, 0x80000000},
		{DataBlockBinaryAndHash, 1, 0x80000001},
		{DataBlockBinaryAndHash, 16, 0x80000010},
		{DataBlockBinarySearch, 0x7FFFFFFF, 0x7FFFFFFF},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			packed := PackIndexTypeAndNumRestarts(tc.indexType, tc.numRestarts)
			if packed != tc.expected {
				t.Errorf("PackIndexTypeAndNumRestarts(%d, %d) = 0x%08x, want 0x%08x",
					tc.indexType, tc.numRestarts, packed, tc.expected)
			}

			// Verify unpack
			gotType, gotRestarts := UnpackIndexTypeAndNumRestarts(packed)
			if gotType != tc.indexType {
				t.Errorf("UnpackIndexTypeAndNumRestarts type = %d, want %d", gotType, tc.indexType)
			}
			if gotRestarts != tc.numRestarts {
				t.Errorf("UnpackIndexTypeAndNumRestarts restarts = %d, want %d", gotRestarts, tc.numRestarts)
			}
		})
	}
}

// TestGoldenBlockBuilderFormat tests block builder output format.
func TestGoldenBlockBuilderFormat(t *testing.T) {
	// Build a simple block with known entries
	builder := NewBuilder(2) // restart interval = 2

	builder.Add([]byte("key1"), []byte("val1"))
	builder.Add([]byte("key2"), []byte("val2"))
	builder.Add([]byte("key3"), []byte("val3"))

	data := builder.Finish()

	// Parse and verify the block
	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	iter := block.NewIterator()
	iter.SeekToFirst()

	expected := []struct {
		key   string
		value string
	}{
		{"key1", "val1"},
		{"key2", "val2"},
		{"key3", "val3"},
	}

	for i, exp := range expected {
		if !iter.Valid() {
			t.Fatalf("Iterator not valid at entry %d", i)
		}
		if string(iter.Key()) != exp.key {
			t.Errorf("Entry %d key = %q, want %q", i, iter.Key(), exp.key)
		}
		if string(iter.Value()) != exp.value {
			t.Errorf("Entry %d value = %q, want %q", i, iter.Value(), exp.value)
		}
		iter.Next()
	}

	if iter.Valid() {
		t.Error("Iterator still valid after last entry")
	}
}

// TestGoldenBlockCompressionTypes tests compression type constants.
func TestGoldenBlockCompressionTypes(t *testing.T) {
	testCases := []struct {
		name     string
		got      CompressionType
		expected uint8
	}{
		{"CompressionNone", CompressionNone, 0},
		{"CompressionSnappy", CompressionSnappy, 1},
		{"CompressionZlib", CompressionZlib, 2},
		{"CompressionBZip2", CompressionBZip2, 3},
		{"CompressionLZ4", CompressionLZ4, 4},
		{"CompressionLZ4HC", CompressionLZ4HC, 5},
		{"CompressionXpress", CompressionXpress, 6},
		{"CompressionZstd", CompressionZstd, 7},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if uint8(tc.got) != tc.expected {
				t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.expected)
			}
		})
	}
}
