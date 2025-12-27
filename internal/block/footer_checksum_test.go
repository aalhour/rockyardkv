// Footer checksum tests for Format Version 6+
//
// Issue 3: SST footer checksum is missing for Format V6+
// The footer checksum field is written as 0, but RocksDB V6+ requires
// a valid checksum covering the entire footer.
//
// Reference: RocksDB v10.7.5
//   - table/format.cc (FooterBuilder::Build)
//   - table/format.h (ChecksumModifierForContext)
package block

import (
	"encoding/binary"
	"testing"
)

// TestFooterChecksumV6_ZeroIsInvalid verifies that format version 6+
// footer checksum must not be zero (placeholder bug).
// This test should FAIL before the fix and PASS after.
func TestFooterChecksumV6_ZeroIsInvalid(t *testing.T) {
	// Create a format version 6 footer
	footer := &Footer{
		TableMagicNumber:    BlockBasedTableMagicNumber,
		FormatVersion:       6,
		ChecksumType:        ChecksumTypeXXH3,
		MetaindexHandle:     Handle{Offset: 0, Size: 256},
		BaseContextChecksum: 0x12345678,
		BlockTrailerSize:    BlockTrailerSize,
	}

	// Encode the footer
	encoded := footer.EncodeTo()

	// The footer checksum is at offset 5 (after checksum_type + extended_magic)
	// Part1: checksum_type (1 byte)
	// Part2 starts at offset 1:
	//   - extended_magic (4 bytes) at offset 1-4
	//   - footer_checksum (4 bytes) at offset 5-8
	checksumOffset := 5
	footerChecksum := binary.LittleEndian.Uint32(encoded[checksumOffset : checksumOffset+4])

	// The bug: footer checksum is zero (placeholder)
	// After fix: footer checksum should be non-zero
	if footerChecksum == 0 {
		t.Errorf("Footer checksum is zero (placeholder bug). Format V6+ requires a valid checksum. "+
			"Encoded footer hex: %x", encoded)
	}

	t.Logf("Footer checksum: 0x%08x", footerChecksum)
}

// TestFooterChecksumV6_RoundTrip verifies that encode/decode round-trip
// preserves the checksum correctly.
func TestFooterChecksumV6_RoundTrip(t *testing.T) {
	footerOffset := uint64(10000) // Simulating footer at this offset in file

	// Create a format version 6 footer
	footer := &Footer{
		TableMagicNumber:    BlockBasedTableMagicNumber,
		FormatVersion:       6,
		ChecksumType:        ChecksumTypeXXH3,
		MetaindexHandle:     Handle{Offset: 0, Size: 256},
		BaseContextChecksum: 0x12345678,
		BlockTrailerSize:    BlockTrailerSize,
	}

	// Encode the footer - should compute and store checksum
	encoded := footer.EncodeTo()

	// Decode the footer
	decoded, err := DecodeFooter(encoded, footerOffset, 0)
	if err != nil {
		t.Fatalf("DecodeFooter failed: %v", err)
	}

	// Verify the footer was decoded correctly
	if decoded.FormatVersion != 6 {
		t.Errorf("FormatVersion = %d, want 6", decoded.FormatVersion)
	}
	if decoded.BaseContextChecksum != footer.BaseContextChecksum {
		t.Errorf("BaseContextChecksum = 0x%x, want 0x%x",
			decoded.BaseContextChecksum, footer.BaseContextChecksum)
	}
	if decoded.MetaindexHandle.Size != footer.MetaindexHandle.Size {
		t.Errorf("MetaindexHandle.Size = %d, want %d",
			decoded.MetaindexHandle.Size, footer.MetaindexHandle.Size)
	}
}

// TestFooterChecksumV6_ContextModifier verifies that the context modifier
// is applied correctly based on base_context_checksum and footer_offset.
func TestFooterChecksumV6_ContextModifier(t *testing.T) {
	// Different footer offsets should produce different checksums
	// when base_context_checksum is non-zero
	footer := &Footer{
		TableMagicNumber:    BlockBasedTableMagicNumber,
		FormatVersion:       6,
		ChecksumType:        ChecksumTypeCRC32C,
		MetaindexHandle:     Handle{Offset: 0, Size: 256},
		BaseContextChecksum: 0xABCDEF01, // Non-zero to enable context
		BlockTrailerSize:    BlockTrailerSize,
	}

	// Two encodings at different offsets should have different checksums
	// Note: Currently EncodeTo doesn't take footerOffset, this is part of the fix
	encoded1 := footer.EncodeTo()
	encoded2 := footer.EncodeTo()

	// Extract checksums
	checksumOffset := 5
	checksum1 := binary.LittleEndian.Uint32(encoded1[checksumOffset : checksumOffset+4])
	checksum2 := binary.LittleEndian.Uint32(encoded2[checksumOffset : checksumOffset+4])

	t.Logf("Checksum at offset 0: 0x%08x", checksum1)
	t.Logf("Checksum at offset 0: 0x%08x", checksum2)

	// For now they're the same since EncodeTo doesn't use offset.
	// After fix, we'll need to modify the API to accept footerOffset.
}

// TestFooterChecksumV5_NotRequired verifies that format version < 6
// does not require a footer checksum (backward compatibility).
func TestFooterChecksumV5_NotRequired(t *testing.T) {
	footer := &Footer{
		TableMagicNumber: BlockBasedTableMagicNumber,
		FormatVersion:    5,
		ChecksumType:     ChecksumTypeCRC32C,
		MetaindexHandle:  Handle{Offset: 100, Size: 50},
		IndexHandle:      Handle{Offset: 200, Size: 100},
		BlockTrailerSize: BlockTrailerSize,
	}

	encoded := footer.EncodeTo()

	// For version 5, the second part contains block handles, not checksums
	// Verify we can decode it
	decoded, err := DecodeFooter(encoded, 0, 0)
	if err != nil {
		t.Fatalf("DecodeFooter failed: %v", err)
	}

	if decoded.FormatVersion != 5 {
		t.Errorf("FormatVersion = %d, want 5", decoded.FormatVersion)
	}
}
