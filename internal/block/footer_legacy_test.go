// Reference: RocksDB v10.7.5
//
//	table/format.cc (FooterBuilder::Build for format_version == 0)
//
// Tests for legacy (Format Version 0) footer encoding.
package block

import (
	"testing"
)

// TestLegacyFooterEncodingHandlesAreSeparate verifies that both metaindex and
// index handles are correctly encoded in the legacy footer format.
//
// This is a regression test for Issue 4: the original code had a math error
// that caused the second handle to be written at the wrong offset.
func TestLegacyFooterEncodingHandlesAreSeparate(t *testing.T) {
	// Create a footer with distinct handle values
	footer := Footer{
		MetaindexHandle:  Handle{Offset: 100, Size: 50},
		IndexHandle:      Handle{Offset: 200, Size: 75},
		FormatVersion:    0,
		TableMagicNumber: LegacyBlockBasedTableMagicNumber,
		ChecksumType:     ChecksumTypeCRC32C,
	}

	encoded := footer.EncodeTo()

	// Decode it back
	decoded, err := DecodeFooter(encoded, 0, 0)
	if err != nil {
		t.Fatalf("DecodeFooter failed: %v", err)
	}

	// Verify both handles are correctly preserved
	if decoded.MetaindexHandle.Offset != 100 || decoded.MetaindexHandle.Size != 50 {
		t.Errorf("MetaindexHandle mismatch: got {Offset: %d, Size: %d}, want {Offset: 100, Size: 50}",
			decoded.MetaindexHandle.Offset, decoded.MetaindexHandle.Size)
	}

	if decoded.IndexHandle.Offset != 200 || decoded.IndexHandle.Size != 75 {
		t.Errorf("IndexHandle mismatch: got {Offset: %d, Size: %d}, want {Offset: 200, Size: 75}",
			decoded.IndexHandle.Offset, decoded.IndexHandle.Size)
	}
}

// TestLegacyFooterEncodingRoundTrip tests encode/decode round-trip for various handle sizes.
func TestLegacyFooterEncodingRoundTrip(t *testing.T) {
	testCases := []struct {
		name            string
		metaindexOffset uint64
		metaindexSize   uint64
		indexOffset     uint64
		indexSize       uint64
	}{
		{
			name:            "small values",
			metaindexOffset: 0,
			metaindexSize:   100,
			indexOffset:     100,
			indexSize:       200,
		},
		{
			name:            "distinct values",
			metaindexOffset: 1000,
			metaindexSize:   500,
			indexOffset:     2000,
			indexSize:       750,
		},
		{
			name:            "large values",
			metaindexOffset: 1 << 30,
			metaindexSize:   1 << 20,
			indexOffset:     1 << 31,
			indexSize:       1 << 21,
		},
		{
			name:            "max varint values",
			metaindexOffset: 1<<63 - 1,
			metaindexSize:   1<<32 - 1,
			indexOffset:     1<<62 - 1,
			indexSize:       1<<31 - 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			footer := Footer{
				MetaindexHandle:  Handle{Offset: tc.metaindexOffset, Size: tc.metaindexSize},
				IndexHandle:      Handle{Offset: tc.indexOffset, Size: tc.indexSize},
				FormatVersion:    0,
				TableMagicNumber: LegacyBlockBasedTableMagicNumber,
				ChecksumType:     ChecksumTypeCRC32C,
			}

			encoded := footer.EncodeTo()

			decoded, err := DecodeFooter(encoded, 0, 0)
			if err != nil {
				t.Fatalf("DecodeFooter failed: %v", err)
			}

			if decoded.MetaindexHandle.Offset != tc.metaindexOffset ||
				decoded.MetaindexHandle.Size != tc.metaindexSize {
				t.Errorf("MetaindexHandle mismatch: got {%d, %d}, want {%d, %d}",
					decoded.MetaindexHandle.Offset, decoded.MetaindexHandle.Size,
					tc.metaindexOffset, tc.metaindexSize)
			}

			if decoded.IndexHandle.Offset != tc.indexOffset ||
				decoded.IndexHandle.Size != tc.indexSize {
				t.Errorf("IndexHandle mismatch: got {%d, %d}, want {%d, %d}",
					decoded.IndexHandle.Offset, decoded.IndexHandle.Size,
					tc.indexOffset, tc.indexSize)
			}
		})
	}
}
