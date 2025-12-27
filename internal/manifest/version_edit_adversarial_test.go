// version_edit_adversarial_test.go contains adversarial tests for VersionEdit
// encoding/decoding, particularly around unknown tag preservation.
//
// These tests were inspired by Red Team findings (Dec 2025) that identified
// a critical issue where "safe-to-ignore" tags were being silently dropped
// during decode-encode cycles, causing data loss.
package manifest

import (
	"bytes"
	"errors"
	"testing"

	"github.com/aalhour/rockyardkv/internal/encoding"
)

// TestAdversarial_UnknownTagsPreservedInRoundTrip verifies that unknown
// "safe-to-ignore" tags are preserved when decoding and re-encoding a VersionEdit.
func TestAdversarial_UnknownTagsPreservedInRoundTrip(t *testing.T) {
	// Create a fake "safe-to-ignore" tag (has bit 13 set)
	// This simulates a tag from a future RocksDB version
	futureTag := uint32(TagSafeIgnoreMask) | 99 // Some hypothetical future tag
	futureValue := []byte("future metadata that must not be lost")

	// Build a VersionEdit with known fields plus the unknown tag
	var original []byte

	// Add known fields
	original = encoding.AppendVarint32(original, uint32(TagComparator))
	original = encoding.AppendLengthPrefixedSlice(original, []byte("leveldb.BytewiseComparator"))

	original = encoding.AppendVarint32(original, uint32(TagLogNumber))
	original = encoding.AppendVarint64(original, 42)

	// Add the unknown future tag
	original = encoding.AppendVarint32(original, futureTag)
	original = encoding.AppendLengthPrefixedSlice(original, futureValue)

	original = encoding.AppendVarint32(original, uint32(TagLastSequence))
	original = encoding.AppendVarint64(original, 100)

	// Decode
	ve := NewVersionEdit()
	if err := ve.DecodeFrom(original); err != nil {
		t.Fatalf("DecodeFrom failed: %v", err)
	}

	// Verify known fields were decoded
	if !ve.HasComparator || ve.Comparator != "leveldb.BytewiseComparator" {
		t.Error("Comparator not decoded correctly")
	}
	if !ve.HasLogNumber || ve.LogNumber != 42 {
		t.Error("LogNumber not decoded correctly")
	}
	if !ve.HasLastSequence || ve.LastSequence != 100 {
		t.Error("LastSequence not decoded correctly")
	}

	// Verify unknown tag was preserved
	if len(ve.UnknownTags) != 1 {
		t.Fatalf("Expected 1 unknown tag, got %d", len(ve.UnknownTags))
	}
	if ve.UnknownTags[0].Tag != futureTag {
		t.Errorf("Unknown tag = %d, want %d", ve.UnknownTags[0].Tag, futureTag)
	}
	if !bytes.Equal(ve.UnknownTags[0].Value, futureValue) {
		t.Errorf("Unknown value = %q, want %q", ve.UnknownTags[0].Value, futureValue)
	}

	// Re-encode
	reencoded := ve.EncodeTo()

	// The re-encoded data should contain our unknown tag
	if !bytes.Contains(reencoded, futureValue) {
		t.Error("Re-encoded data lost the unknown tag value")
	}

	// Decode again to verify round-trip
	ve2 := NewVersionEdit()
	if err := ve2.DecodeFrom(reencoded); err != nil {
		t.Fatalf("DecodeFrom (second) failed: %v", err)
	}

	if len(ve2.UnknownTags) != 1 {
		t.Fatalf("After round-trip: expected 1 unknown tag, got %d", len(ve2.UnknownTags))
	}
	if !bytes.Equal(ve2.UnknownTags[0].Value, futureValue) {
		t.Error("Unknown tag value lost after round-trip")
	}
}

// TestAdversarial_MultipleUnknownTags verifies that multiple unknown tags
// are all preserved.
func TestAdversarial_MultipleUnknownTags(t *testing.T) {
	tags := []struct {
		tag   uint32
		value []byte
	}{
		{uint32(TagSafeIgnoreMask) | 50, []byte("future tag 50")},
		{uint32(TagSafeIgnoreMask) | 51, []byte("future tag 51")},
		{uint32(TagSafeIgnoreMask) | 52, []byte("future tag 52 with longer data")},
	}

	var original []byte
	original = encoding.AppendVarint32(original, uint32(TagLogNumber))
	original = encoding.AppendVarint64(original, 1)

	for _, tag := range tags {
		original = encoding.AppendVarint32(original, tag.tag)
		original = encoding.AppendLengthPrefixedSlice(original, tag.value)
	}

	ve := NewVersionEdit()
	if err := ve.DecodeFrom(original); err != nil {
		t.Fatalf("DecodeFrom failed: %v", err)
	}

	if len(ve.UnknownTags) != 3 {
		t.Fatalf("Expected 3 unknown tags, got %d", len(ve.UnknownTags))
	}

	// Re-encode and verify all tags present
	reencoded := ve.EncodeTo()
	for _, tag := range tags {
		if !bytes.Contains(reencoded, tag.value) {
			t.Errorf("Re-encoded data lost tag value %q", tag.value)
		}
	}
}

// TestAdversarial_RequiredUnknownTagRejected verifies that unknown tags
// WITHOUT the safe-to-ignore bit are rejected.
func TestAdversarial_RequiredUnknownTagRejected(t *testing.T) {
	// A tag without bit 13 set that we don't recognize
	unknownRequiredTag := uint32(999) // Not in our enum, no safe-ignore bit

	var data []byte
	data = encoding.AppendVarint32(data, unknownRequiredTag)
	data = encoding.AppendLengthPrefixedSlice(data, []byte("value"))

	ve := NewVersionEdit()
	err := ve.DecodeFrom(data)
	if !errors.Is(err, ErrUnknownRequiredTag) {
		t.Errorf("Expected ErrUnknownRequiredTag, got %v", err)
	}
}

// TestAdversarial_NewFile4UnknownCustomTagsPreserved verifies that unknown
// custom tags in NewFile4 entries are preserved.
func TestAdversarial_NewFile4UnknownCustomTagsPreserved(t *testing.T) {
	// Create a VersionEdit with a NewFile4 entry
	ve := NewVersionEdit()
	meta := NewFileMetaData()
	meta.FD = NewFileDescriptor(123, 0, 4096)
	meta.Smallest = []byte("aaa")
	meta.Largest = []byte("zzz")
	meta.FD.SmallestSeqno = 1
	meta.FD.LargestSeqno = 100

	// Add an unknown custom tag (must NOT have bit 6 set to be safe-to-ignore)
	meta.UnknownCustomTags = append(meta.UnknownCustomTags, UnknownTag{
		Tag:   55, // Hypothetical future custom tag
		Value: []byte("future file metadata"),
	})

	ve.AddFile(0, meta)

	// Encode
	encoded := ve.EncodeTo()

	// Decode
	ve2 := NewVersionEdit()
	if err := ve2.DecodeFrom(encoded); err != nil {
		t.Fatalf("DecodeFrom failed: %v", err)
	}

	if len(ve2.NewFiles) != 1 {
		t.Fatalf("Expected 1 new file, got %d", len(ve2.NewFiles))
	}

	if len(ve2.NewFiles[0].Meta.UnknownCustomTags) != 1 {
		t.Fatalf("Expected 1 unknown custom tag, got %d", len(ve2.NewFiles[0].Meta.UnknownCustomTags))
	}

	ut := ve2.NewFiles[0].Meta.UnknownCustomTags[0]
	if ut.Tag != 55 {
		t.Errorf("Custom tag = %d, want 55", ut.Tag)
	}
	if !bytes.Equal(ut.Value, []byte("future file metadata")) {
		t.Errorf("Custom tag value mismatch")
	}
}

// TestAdversarial_EncodedSizeWithUnknownTags verifies that the encoded
// output is larger (not smaller) when unknown tags are present.
// This catches the original bug where unknown tags were silently dropped.
func TestAdversarial_EncodedSizeWithUnknownTags(t *testing.T) {
	// Create a VersionEdit with just known fields
	veWithoutUnknown := NewVersionEdit()
	veWithoutUnknown.SetLogNumber(1)
	veWithoutUnknown.SetLastSequence(100)
	sizeWithout := len(veWithoutUnknown.EncodeTo())

	// Create same edit but add an unknown tag
	veWithUnknown := NewVersionEdit()
	veWithUnknown.SetLogNumber(1)
	veWithUnknown.SetLastSequence(100)
	veWithUnknown.UnknownTags = append(veWithUnknown.UnknownTags, UnknownTag{
		Tag:   uint32(TagSafeIgnoreMask) | 77,
		Value: []byte("extra data from the future"),
	})
	sizeWith := len(veWithUnknown.EncodeTo())

	if sizeWith <= sizeWithout {
		t.Errorf("Encoded size with unknown tag (%d) should be larger than without (%d)",
			sizeWith, sizeWithout)
	}

	t.Logf("Size without unknown tags: %d bytes", sizeWithout)
	t.Logf("Size with unknown tags: %d bytes", sizeWith)
}
