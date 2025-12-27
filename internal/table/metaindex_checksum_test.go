package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/aalhour/rockyardkv/internal/block"
	"github.com/aalhour/rockyardkv/internal/checksum"
)

// TestMetaindexBlockChecksum verifies that the metaindex block checksum
// is computed correctly for format version 6.
func TestMetaindexBlockChecksum(t *testing.T) {
	var buf bytes.Buffer

	opts := BuilderOptions{
		FormatVersion: 6,
		ChecksumType:  checksum.TypeXXH3,
		BlockSize:     4096,
	}

	builder := NewTableBuilder(&buf, opts)

	// Add a few entries
	for i := range 3 {
		key := fmt.Appendf(nil, "key%03d\x01\x00\x00\x00\x00\x00\x00\x00", i)
		value := fmt.Appendf(nil, "value%03d", i)
		if err := builder.Add(key, value); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	data := buf.Bytes()
	t.Logf("SST size: %d bytes", len(data))

	// Read the footer
	footerData := data[len(data)-block.NewVersionsEncodedLength:]
	footerOffset := uint64(len(data) - block.NewVersionsEncodedLength)
	footer, err := block.DecodeFooter(footerData, footerOffset, 0)
	if err != nil {
		t.Fatalf("DecodeFooter failed: %v", err)
	}

	t.Logf("Footer: FormatVersion=%d, BaseContextChecksum=0x%08x",
		footer.FormatVersion, footer.BaseContextChecksum)
	t.Logf("MetaindexHandle: Offset=%d, Size=%d",
		footer.MetaindexHandle.Offset, footer.MetaindexHandle.Size)

	// Read the metaindex block + trailer
	metaStart := footer.MetaindexHandle.Offset
	metaEnd := metaStart + footer.MetaindexHandle.Size + block.BlockTrailerSize
	metaBlockWithTrailer := data[metaStart:metaEnd]

	t.Logf("Metaindex block+trailer: %d bytes", len(metaBlockWithTrailer))

	// The trailer is the last 5 bytes
	trailer := metaBlockWithTrailer[len(metaBlockWithTrailer)-block.BlockTrailerSize:]
	compressionType := trailer[0]
	storedChecksum := binary.LittleEndian.Uint32(trailer[1:5])

	t.Logf("Compression type: %d", compressionType)
	t.Logf("Stored checksum: 0x%08x", storedChecksum)

	// Compute expected checksum
	blockContent := metaBlockWithTrailer[:len(metaBlockWithTrailer)-block.BlockTrailerSize]
	computed := checksum.ComputeXXH3ChecksumWithLastByte(blockContent, compressionType)
	t.Logf("Computed base checksum: 0x%08x", computed)

	// Add context modifier
	modifier := checksum.ChecksumModifierForContext(footer.BaseContextChecksum, metaStart)
	t.Logf("Context modifier: 0x%08x", modifier)

	computedWithContext := computed + modifier
	t.Logf("Computed checksum with context: 0x%08x", computedWithContext)

	if storedChecksum != computedWithContext {
		t.Errorf("Checksum mismatch: stored=0x%08x, computed=0x%08x",
			storedChecksum, computedWithContext)
	}

	// Also verify what C++ would compute
	// C++ removes the context to verify: stored - modifier == computed_base
	removedContext := storedChecksum - modifier
	t.Logf("Stored - modifier = 0x%08x (should match computed base 0x%08x)",
		removedContext, computed)

	if removedContext != computed {
		t.Errorf("C++ verification would fail: stored-modifier=%08x != computed=%08x",
			removedContext, computed)
	}
}
