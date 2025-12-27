// constants_golden_test.go - Tests that verify Go constants match C++ RocksDB.
//
// These tests ensure that magic numbers, property names, footer sizes, etc.
// are bit-for-bit compatible with C++ RocksDB v10.7.5.
package main

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/block"
	"github.com/aalhour/rockyardkv/internal/checksum"
	"github.com/aalhour/rockyardkv/internal/table"
)

// TestGoldenTablePropertiesNames tests that property name constants match RocksDB.
func TestGoldenTablePropertiesNames(t *testing.T) {
	testCases := []struct {
		name     string
		got      string
		expected string
	}{
		{"PropDataSize", table.PropDataSize, "rocksdb.data.size"},
		{"PropIndexSize", table.PropIndexSize, "rocksdb.index.size"},
		{"PropFilterSize", table.PropFilterSize, "rocksdb.filter.size"},
		{"PropRawKeySize", table.PropRawKeySize, "rocksdb.raw.key.size"},
		{"PropRawValueSize", table.PropRawValueSize, "rocksdb.raw.value.size"},
		{"PropNumDataBlocks", table.PropNumDataBlocks, "rocksdb.num.data.blocks"},
		{"PropNumEntries", table.PropNumEntries, "rocksdb.num.entries"},
		{"PropComparator", table.PropComparator, "rocksdb.comparator"},
		{"PropCompression", table.PropCompression, "rocksdb.compression"},
		{"PropColumnFamilyID", table.PropColumnFamilyID, "rocksdb.column.family.id"},
		{"PropColumnFamilyName", table.PropColumnFamilyName, "rocksdb.column.family.name"},
		{"PropCreationTime", table.PropCreationTime, "rocksdb.creation.time"},
		{"PropFormatVersion", table.PropFormatVersion, "rocksdb.format.version"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.expected {
				t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.expected)
			}
		})
	}
}

// TestGoldenTableMagicNumbers tests SST magic numbers match RocksDB.
func TestGoldenTableMagicNumbers(t *testing.T) {
	testCases := []struct {
		name     string
		got      uint64
		expected uint64
	}{
		{"BlockBasedTableMagicNumber", block.BlockBasedTableMagicNumber, 0x88e241b785f4cff7},
		{"LegacyBlockBasedTableMagicNumber", block.LegacyBlockBasedTableMagicNumber, 0xdb4775248b80fb57},
		{"PlainTableMagicNumber", block.PlainTableMagicNumber, 0x8242229663bf9564},
		{"CuckooTableMagicNumber", block.CuckooTableMagicNumber, 0x926789d0c5f17873},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.expected {
				t.Errorf("%s = 0x%016x, want 0x%016x", tc.name, tc.got, tc.expected)
			}
		})
	}
}

// TestGoldenChecksumTypesMatch tests checksum types match between packages.
func TestGoldenChecksumTypesMatch(t *testing.T) {
	// Verify that the checksum type values are consistent
	if uint8(checksum.TypeNoChecksum) != uint8(block.ChecksumTypeNone) {
		t.Errorf("NoChecksum mismatch: checksum.Type=%d, block.ChecksumType=%d",
			checksum.TypeNoChecksum, block.ChecksumTypeNone)
	}
	if uint8(checksum.TypeCRC32C) != uint8(block.ChecksumTypeCRC32C) {
		t.Errorf("CRC32C mismatch: checksum.Type=%d, block.ChecksumType=%d",
			checksum.TypeCRC32C, block.ChecksumTypeCRC32C)
	}
	if uint8(checksum.TypeXXHash) != uint8(block.ChecksumTypeXXHash) {
		t.Errorf("XXHash mismatch: checksum.Type=%d, block.ChecksumType=%d",
			checksum.TypeXXHash, block.ChecksumTypeXXHash)
	}
	if uint8(checksum.TypeXXHash64) != uint8(block.ChecksumTypeXXHash64) {
		t.Errorf("XXHash64 mismatch: checksum.Type=%d, block.ChecksumType=%d",
			checksum.TypeXXHash64, block.ChecksumTypeXXHash64)
	}
	if uint8(checksum.TypeXXH3) != uint8(block.ChecksumTypeXXH3) {
		t.Errorf("XXH3 mismatch: checksum.Type=%d, block.ChecksumType=%d",
			checksum.TypeXXH3, block.ChecksumTypeXXH3)
	}
}

// TestGoldenTableFooterSizes tests footer size constants.
func TestGoldenTableFooterSizes(t *testing.T) {
	if block.Version0EncodedLength != 48 {
		t.Errorf("Version0EncodedLength = %d, want 48", block.Version0EncodedLength)
	}
	if block.NewVersionsEncodedLength != 53 {
		t.Errorf("NewVersionsEncodedLength = %d, want 53", block.NewVersionsEncodedLength)
	}
	if block.MagicNumberLengthByte != 8 {
		t.Errorf("MagicNumberLengthByte = %d, want 8", block.MagicNumberLengthByte)
	}
	if block.BlockTrailerSize != 5 {
		t.Errorf("BlockTrailerSize = %d, want 5", block.BlockTrailerSize)
	}
}

// TestGoldenTableBlockHandleEncoding tests BlockHandle encoding.
func TestGoldenTableBlockHandleEncoding(t *testing.T) {
	testCases := []struct {
		offset uint64
		size   uint64
	}{
		{0, 0},
		{100, 50},
		{1000, 500},
		{0xFFFFFFFF, 0xFFFFFFFF},
	}

	for _, tc := range testCases {
		h := block.Handle{Offset: tc.offset, Size: tc.size}
		encoded := h.EncodeToSlice()

		decoded, remaining, err := block.DecodeHandle(encoded)
		if err != nil {
			t.Fatalf("DecodeHandle failed for {%d, %d}: %v", tc.offset, tc.size, err)
		}
		if len(remaining) != 0 {
			t.Errorf("DecodeHandle left %d bytes unconsumed", len(remaining))
		}
		if decoded.Offset != tc.offset || decoded.Size != tc.size {
			t.Errorf("DecodeHandle({%d, %d}) = {%d, %d}",
				tc.offset, tc.size, decoded.Offset, decoded.Size)
		}
	}
}
