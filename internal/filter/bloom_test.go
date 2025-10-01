package filter

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestBloomFilterBasic(t *testing.T) {
	builder := NewBloomFilterBuilder(10) // 10 bits per key

	// Add some keys
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("hello"),
		[]byte("world"),
	}

	for _, key := range keys {
		builder.AddKey(key)
	}

	// Build filter
	data := builder.Finish()
	if len(data) < MetadataLen {
		t.Fatalf("filter data too short: %d bytes", len(data))
	}

	// Check metadata
	filterLen := len(data) - MetadataLen
	if data[filterLen] != NewBloomMarker {
		t.Errorf("expected new bloom marker 0x%02X, got 0x%02X", NewBloomMarker, data[filterLen])
	}
	if data[filterLen+1] != FastLocalBloomMarker {
		t.Errorf("expected FastLocalBloom marker 0x%02X, got 0x%02X", FastLocalBloomMarker, data[filterLen+1])
	}
	numProbes := int(data[filterLen+2])
	if numProbes < 1 || numProbes > 30 {
		t.Errorf("unexpected num_probes: %d", numProbes)
	}
	t.Logf("Filter: %d bytes, %d probes", len(data), numProbes)

	// Create reader
	reader := NewBloomFilterReader(data)
	if reader == nil {
		t.Fatal("failed to create reader")
	}

	// All added keys should be found
	for _, key := range keys {
		if !reader.MayContain(key) {
			t.Errorf("key %q should be in filter", key)
		}
	}

	// Keys not added should mostly not be found (some false positives OK)
	notAddedKeys := [][]byte{
		[]byte("notkey1"),
		[]byte("notkey2"),
		[]byte("missing"),
		[]byte("absent"),
	}

	falsePositives := 0
	for _, key := range notAddedKeys {
		if reader.MayContain(key) {
			falsePositives++
		}
	}
	// With 10 bits/key and 4 test keys, we should have very few FPs
	if falsePositives > 2 {
		t.Logf("Warning: %d false positives in %d tests", falsePositives, len(notAddedKeys))
	}
}

func TestBloomFilterEmpty(t *testing.T) {
	builder := NewBloomFilterBuilder(10)

	// Build empty filter
	data := builder.Finish()

	// Should just be metadata
	if len(data) != MetadataLen {
		t.Errorf("expected %d bytes for empty filter, got %d", MetadataLen, len(data))
	}

	// Empty filter should not match anything
	reader := NewBloomFilterReader(data)
	if reader == nil {
		t.Fatal("failed to create reader for empty filter")
	}

	if reader.MayContain([]byte("anything")) {
		t.Error("empty filter should not match any key")
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	// Test with different bits_per_key settings
	testCases := []struct {
		bitsPerKey int
		maxFPRate  float64
	}{
		{10, 0.02},  // ~1% expected, allow 2%
		{15, 0.005}, // ~0.1% expected, allow 0.5%
		{5, 0.15},   // ~10% expected, allow 15%
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("bits=%d", tc.bitsPerKey), func(t *testing.T) {
			builder := NewBloomFilterBuilder(tc.bitsPerKey)

			// Add 10000 keys
			numKeys := 10000
			for i := range numKeys {
				key := fmt.Sprintf("key%08d", i)
				builder.AddKey([]byte(key))
			}

			data := builder.Finish()
			reader := NewBloomFilterReader(data)
			if reader == nil {
				t.Fatal("failed to create reader")
			}

			// Verify all added keys are found
			for i := range numKeys {
				key := fmt.Sprintf("key%08d", i)
				if !reader.MayContain([]byte(key)) {
					t.Fatalf("key %q should be in filter", key)
				}
			}

			// Test for false positives with different keys
			numTests := 100000
			falsePositives := 0
			for i := range numTests {
				key := fmt.Sprintf("notkey%08d", i)
				if reader.MayContain([]byte(key)) {
					falsePositives++
				}
			}

			fpRate := float64(falsePositives) / float64(numTests)
			t.Logf("bits_per_key=%d: FP rate = %.4f%% (%d/%d)",
				tc.bitsPerKey, fpRate*100, falsePositives, numTests)

			if fpRate > tc.maxFPRate {
				t.Errorf("FP rate %.4f exceeds max %.4f", fpRate, tc.maxFPRate)
			}
		})
	}
}

func TestBloomFilterLargeKeys(t *testing.T) {
	builder := NewBloomFilterBuilder(10)

	// Add keys with various sizes
	sizes := []int{1, 10, 100, 1000, 10000}
	keys := make([][]byte, len(sizes))

	for i, size := range sizes {
		keys[i] = make([]byte, size)
		rand.Read(keys[i])
		builder.AddKey(keys[i])
	}

	data := builder.Finish()
	reader := NewBloomFilterReader(data)
	if reader == nil {
		t.Fatal("failed to create reader")
	}

	for i, key := range keys {
		if !reader.MayContain(key) {
			t.Errorf("large key (size %d) should be in filter", sizes[i])
		}
	}
}

func TestBloomFilterManyKeys(t *testing.T) {
	builder := NewBloomFilterBuilder(10)

	// Add 100k keys
	numKeys := 100000
	for i := range numKeys {
		key := fmt.Sprintf("key%08d", i)
		builder.AddKey([]byte(key))
	}

	data := builder.Finish()
	t.Logf("Filter for %d keys: %d bytes (%.2f bits/key)",
		numKeys, len(data), float64(len(data)*8)/float64(numKeys))

	reader := NewBloomFilterReader(data)
	if reader == nil {
		t.Fatal("failed to create reader")
	}

	// Verify sample of keys
	for i := 0; i < numKeys; i += 1000 {
		key := fmt.Sprintf("key%08d", i)
		if !reader.MayContain([]byte(key)) {
			t.Errorf("key %q should be in filter", key)
		}
	}
}

func TestBloomFilterReaderInvalidData(t *testing.T) {
	// Too short
	if NewBloomFilterReader([]byte{1, 2, 3}) != nil {
		t.Error("should reject data shorter than metadata")
	}

	// Wrong marker
	if NewBloomFilterReader([]byte{0x00, 0x00, 0x06, 0x00, 0x00}) != nil {
		t.Error("should reject wrong marker")
	}

	// Wrong sub-implementation
	if NewBloomFilterReader([]byte{0xFF, 0x01, 0x06, 0x00, 0x00}) != nil {
		t.Error("should reject unknown sub-implementation")
	}

	// Valid but empty (zero probes = always false)
	reader := NewBloomFilterReader([]byte{0xFF, 0x00, 0x00, 0x00, 0x00})
	if reader == nil {
		t.Error("should accept valid empty filter")
	} else if reader.MayContain([]byte("test")) {
		t.Error("empty filter should not match")
	}
}

func TestChooseNumProbes(t *testing.T) {
	// Test the probe selection matches C++ implementation
	testCases := []struct {
		millibitsPerKey int
		expectedProbes  int
	}{
		{1000, 1},  // 1 bit/key
		{5000, 3},  // 5 bits/key
		{10000, 6}, // 10 bits/key
		{15000, 9}, // 15 bits/key
	}

	for _, tc := range testCases {
		probes := chooseNumProbes(tc.millibitsPerKey)
		if probes != tc.expectedProbes {
			t.Errorf("millibits=%d: expected %d probes, got %d",
				tc.millibitsPerKey, tc.expectedProbes, probes)
		}
	}
}

func TestCalculateSpace(t *testing.T) {
	// Space should be cache-line aligned + metadata
	testCases := []struct {
		numEntries int
		bitsPerKey int
		minBytes   int
	}{
		{1, 10, CacheLineSize + MetadataLen},       // 1 key = 1 cache line
		{100, 10, CacheLineSize*2 + MetadataLen},   // 100 keys * 10 bits = 1000 bits < 2 cache lines
		{1000, 10, CacheLineSize*20 + MetadataLen}, // 1000 keys * 10 bits = 10000 bits = ~20 cache lines
	}

	for _, tc := range testCases {
		space := calculateSpace(tc.numEntries, tc.bitsPerKey)
		if space < tc.minBytes {
			t.Errorf("entries=%d, bits=%d: space %d < min %d",
				tc.numEntries, tc.bitsPerKey, space, tc.minBytes)
		}
		// Space minus metadata should be cache-line aligned
		if (space-MetadataLen)%CacheLineSize != 0 {
			t.Errorf("entries=%d, bits=%d: filter size %d not cache-line aligned",
				tc.numEntries, tc.bitsPerKey, space-MetadataLen)
		}
	}
}

func TestBloomFilterBuilderReset(t *testing.T) {
	builder := NewBloomFilterBuilder(10)

	// Add some keys
	builder.AddKey([]byte("key1"))
	builder.AddKey([]byte("key2"))
	if builder.NumKeys() != 2 {
		t.Errorf("expected 2 keys, got %d", builder.NumKeys())
	}

	// Reset
	builder.Reset()
	if builder.NumKeys() != 0 {
		t.Errorf("expected 0 keys after reset, got %d", builder.NumKeys())
	}

	// Add new keys
	builder.AddKey([]byte("key3"))
	if builder.NumKeys() != 1 {
		t.Errorf("expected 1 key, got %d", builder.NumKeys())
	}
}

func BenchmarkBloomFilterAdd(b *testing.B) {
	builder := NewBloomFilterBuilder(10)
	key := []byte("benchmark-key-0123456789")

	for b.Loop() {
		builder.AddKey(key)
	}
}

func BenchmarkBloomFilterBuild(b *testing.B) {
	for b.Loop() {
		builder := NewBloomFilterBuilder(10)
		for j := range 10000 {
			key := fmt.Sprintf("key%08d", j)
			builder.AddKey([]byte(key))
		}
		builder.Finish()
	}
}

func BenchmarkBloomFilterQuery(b *testing.B) {
	builder := NewBloomFilterBuilder(10)
	for i := range 10000 {
		key := fmt.Sprintf("key%08d", i)
		builder.AddKey([]byte(key))
	}
	data := builder.Finish()
	reader := NewBloomFilterReader(data)

	key := []byte("query-key-0123456789")

	for b.Loop() {
		reader.MayContain(key)
	}
}
