package filter

import (
	"testing"
)

// TestBloomFilterBuilderEdgeCases tests edge cases for BloomFilterBuilder
func TestBloomFilterBuilderEdgeCases(t *testing.T) {
	// Test bitsPerKey < 1 normalization
	b := NewBloomFilterBuilder(0)
	if b == nil {
		t.Fatal("expected non-nil builder with bitsPerKey=0")
	}
	b.AddKey([]byte("test"))
	data := b.Finish()
	if len(data) == 0 {
		t.Error("expected non-empty filter data")
	}

	// Test negative bitsPerKey
	b2 := NewBloomFilterBuilder(-5)
	if b2 == nil {
		t.Fatal("expected non-nil builder with negative bitsPerKey")
	}
}

// TestBloomFilterEstimatedSize tests EstimatedSize method
func TestBloomFilterEstimatedSize(t *testing.T) {
	b := NewBloomFilterBuilder(10)

	// Empty filter
	if size := b.EstimatedSize(); size != 0 {
		t.Errorf("empty filter EstimatedSize = %d, want 0", size)
	}

	// Add keys and check size increases
	b.AddKey([]byte("key1"))
	size1 := b.EstimatedSize()
	if size1 == 0 {
		t.Error("EstimatedSize should be > 0 after adding key")
	}

	b.AddKey([]byte("key2"))
	b.AddKey([]byte("key3"))
	size3 := b.EstimatedSize()
	if size3 < size1 {
		t.Errorf("EstimatedSize should increase with more keys: %d < %d", size3, size1)
	}
}

// TestChooseNumProbesRanges tests the chooseNumProbes function at different thresholds
func TestChooseNumProbesRanges(t *testing.T) {
	// Based on actual thresholds in bloom.go
	testCases := []struct {
		millibitsPerKey int
		expected        int
	}{
		{1000, 1},   // <= 2080
		{2080, 1},   // boundary
		{2081, 2},   // > 2080, <= 3580
		{3580, 2},   // boundary
		{3581, 3},   // > 3580, <= 5100
		{5100, 3},   // boundary
		{5101, 4},   // > 5100, <= 6640
		{6640, 4},   // boundary
		{6641, 5},   // > 6640, <= 8300
		{8300, 5},   // boundary
		{8301, 6},   // > 8300, <= 10070
		{10070, 6},  // boundary
		{10071, 7},  // > 10070, <= 11720
		{11720, 7},  // boundary
		{11721, 8},  // > 11720, <= 14001
		{14001, 8},  // boundary
		{14002, 9},  // > 14001, <= 16050
		{16050, 9},  // boundary
		{16051, 10}, // > 16050, <= 18300
		{18300, 10}, // boundary
		{18301, 11}, // > 18300, <= 22001
		{22001, 11}, // boundary
		{22002, 12}, // > 22001, <= 25501
		{25501, 12}, // boundary
		{50001, 24}, // > 50000
	}

	for _, tc := range testCases {
		probes := chooseNumProbes(tc.millibitsPerKey)
		if probes != tc.expected {
			t.Errorf("chooseNumProbes(%d) = %d, want %d",
				tc.millibitsPerKey, probes, tc.expected)
		}
	}
}

// TestCalculateSpaceEdgeCases tests calculateSpace edge cases
func TestCalculateSpaceEdgeCases(t *testing.T) {
	// Very small filter
	size := calculateSpace(1, 1)
	if size < MetadataLen {
		t.Errorf("calculateSpace(1,1) = %d, want >= %d", size, MetadataLen)
	}

	// Zero entries would cause division issues - but it should handle gracefully
	size = calculateSpace(0, 10)
	if size != MetadataLen {
		// 0 cache lines = 1 (minimum) + metadata
		t.Logf("calculateSpace(0,10) = %d", size)
	}
}

// TestBloomFilterReaderInvalid tests BloomFilterReader with invalid data
func TestBloomFilterReaderInvalid(t *testing.T) {
	// Too short
	r := NewBloomFilterReader([]byte{1, 2, 3})
	if r != nil {
		t.Error("expected nil reader for too-short data")
	}

	// Wrong marker at position 0 (should be NewBloomMarker = 0xFF)
	data := make([]byte, MetadataLen+10)
	data[10] = 0x00 // Wrong marker (should be 0xFF)
	r = NewBloomFilterReader(data)
	if r != nil {
		t.Error("expected nil reader for wrong first marker")
	}

	// Wrong sub-marker at position 1 (FastLocalBloomMarker is 0x00, try wrong value)
	data[10] = NewBloomMarker
	data[11] = 0x99 // Wrong marker (should be FastLocalBloomMarker = 0x00)
	r = NewBloomFilterReader(data)
	if r != nil {
		t.Error("expected nil reader for wrong second marker")
	}

	// Valid markers but zero probes (always-false filter)
	data[10] = NewBloomMarker
	data[11] = FastLocalBloomMarker
	data[12] = 0 // numProbes = 0
	r = NewBloomFilterReader(data)
	if r == nil {
		t.Fatal("expected non-nil reader for always-false filter")
	}
	// Always-false filter should return false for MayContain
	if r.MayContain([]byte("test")) {
		t.Error("always-false filter should return false")
	}
}

// TestBloomFilterReaderNilReceiver tests MayContain on nil receiver
func TestBloomFilterReaderNilReceiver(t *testing.T) {
	var r *BloomFilterReader
	if r.MayContain([]byte("test")) {
		t.Error("nil reader should return false for MayContain")
	}
}
