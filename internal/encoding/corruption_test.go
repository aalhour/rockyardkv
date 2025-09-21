package encoding

import (
	"errors"
	"testing"
)

// -----------------------------------------------------------------------------
// Corruption and Edge Case Tests
// Based on RocksDB util/coding_test.cc
// -----------------------------------------------------------------------------

// TestVarint32Truncation tests that truncated varint input is handled correctly.
func TestVarint32Truncation(t *testing.T) {
	// Encode a large value that requires 5 bytes
	largeValue := uint32(1<<31) + 100
	encoded := AppendVarint32(nil, largeValue)

	// Try to decode with progressively shorter input
	for length := range len(encoded) - 1 {
		truncated := encoded[:length]
		_, n, err := DecodeVarint32(truncated)
		if err == nil && n > 0 {
			t.Errorf("DecodeVarint32(%d bytes) should fail or return 0 bytes read, got n=%d", length, n)
		}
	}

	// Full input should succeed
	val, n, err := DecodeVarint32(encoded)
	if err != nil {
		t.Errorf("DecodeVarint32(full) failed: %v", err)
	}
	if val != largeValue {
		t.Errorf("DecodeVarint32(full) = %d, want %d", val, largeValue)
	}
	if n != len(encoded) {
		t.Errorf("DecodeVarint32(full) read %d bytes, want %d", n, len(encoded))
	}
}

// TestVarint64Truncation tests that truncated varint64 input is handled correctly.
func TestVarint64Truncation(t *testing.T) {
	// Encode a large value that requires 10 bytes
	largeValue := uint64(1<<63) + 100
	encoded := AppendVarint64(nil, largeValue)

	// Try to decode with progressively shorter input
	for length := range len(encoded) - 1 {
		truncated := encoded[:length]
		_, n, err := DecodeVarint64(truncated)
		if err == nil && n > 0 {
			t.Errorf("DecodeVarint64(%d bytes) should fail or return 0 bytes read, got n=%d", length, n)
		}
	}

	// Full input should succeed
	val, n, err := DecodeVarint64(encoded)
	if err != nil {
		t.Errorf("DecodeVarint64(full) failed: %v", err)
	}
	if val != largeValue {
		t.Errorf("DecodeVarint64(full) = %d, want %d", val, largeValue)
	}
	if n != len(encoded) {
		t.Errorf("DecodeVarint64(full) read %d bytes, want %d", n, len(encoded))
	}
}

// TestVarint32OverflowBits tests that varints with overflow bits are rejected.
func TestVarint32OverflowBits(t *testing.T) {
	// 5 bytes with continuation bit set on all - this is an overflow
	input := []byte{0x81, 0x82, 0x83, 0x84, 0x85, 0x11}

	_, n, err := DecodeVarint32(input)
	if !errors.Is(err, ErrVarintOverflow) {
		t.Errorf("Expected ErrVarintOverflow, got err=%v, n=%d", err, n)
	}
}

// TestVarint64OverflowBits tests that varints with overflow bits are rejected.
func TestVarint64OverflowBits(t *testing.T) {
	// 11 bytes with continuation bit set - this is an overflow for 64-bit
	input := []byte{0x81, 0x82, 0x83, 0x84, 0x85, 0x81, 0x82, 0x83, 0x84, 0x85, 0x11}

	_, n, err := DecodeVarint64(input)
	if !errors.Is(err, ErrVarintOverflow) {
		t.Errorf("Expected ErrVarintOverflow, got err=%v, n=%d", err, n)
	}
}

// TestLengthPrefixedSliceTruncated tests truncated length-prefixed slices.
func TestLengthPrefixedSliceTruncated(t *testing.T) {
	// Create a length-prefixed slice with length 100
	encoded := AppendLengthPrefixedSlice(nil, make([]byte, 100))

	// Try with only the length prefix (no data)
	_, _, err := DecodeLengthPrefixedSlice(encoded[:1])
	if err == nil {
		t.Error("Expected error for truncated length-prefixed slice")
	}

	// Try with partial data
	_, _, err = DecodeLengthPrefixedSlice(encoded[:50])
	if err == nil {
		t.Error("Expected error for truncated data")
	}
}

// TestDecodeEmptyInput tests decoding from empty input.
func TestDecodeEmptyInput(t *testing.T) {
	// Varint32
	_, n, err := DecodeVarint32(nil)
	if n != 0 {
		t.Errorf("DecodeVarint32(nil) should return 0 bytes read")
	}
	_ = err // error may or may not be returned for empty input

	// Varint64
	_, n, _ = DecodeVarint64(nil)
	if n != 0 {
		t.Errorf("DecodeVarint64(nil) should return 0 bytes read")
	}

	// LengthPrefixedSlice
	_, _, err = DecodeLengthPrefixedSlice(nil)
	if err == nil {
		t.Error("DecodeLengthPrefixedSlice(nil) should return error")
	}
}

// TestVarintEdgeValues tests edge values for varints.
func TestVarintEdgeValues(t *testing.T) {
	// Test values at boundaries
	values32 := []uint32{
		0,
		127,        // max 1-byte
		128,        // min 2-byte
		16383,      // max 2-byte
		16384,      // min 3-byte
		2097151,    // max 3-byte
		2097152,    // min 4-byte
		268435455,  // max 4-byte
		268435456,  // min 5-byte
		0xFFFFFFFF, // max uint32
	}

	for _, v := range values32 {
		encoded := AppendVarint32(nil, v)
		decoded, n, err := DecodeVarint32(encoded)
		if err != nil {
			t.Errorf("DecodeVarint32 failed for %d: %v", v, err)
			continue
		}
		if decoded != v {
			t.Errorf("Varint32 roundtrip: got %d, want %d", decoded, v)
		}
		if n != len(encoded) {
			t.Errorf("Varint32 %d: read %d bytes, encoded %d", v, n, len(encoded))
		}
	}

	// Test values at 64-bit boundaries
	values64 := []uint64{
		0,
		127,                // max 1-byte
		128,                // min 2-byte
		1<<14 - 1,          // max 2-byte
		1 << 14,            // min 3-byte
		1<<21 - 1,          // max 3-byte
		1 << 21,            // min 4-byte
		1<<28 - 1,          // max 4-byte
		1 << 28,            // min 5-byte
		1<<35 - 1,          // max 5-byte
		1 << 35,            // min 6-byte
		1<<42 - 1,          // max 6-byte
		1 << 42,            // min 7-byte
		1<<49 - 1,          // max 7-byte
		1 << 49,            // min 8-byte
		1<<56 - 1,          // max 8-byte
		1 << 56,            // min 9-byte
		1<<63 - 1,          // max 9-byte
		1 << 63,            // min 10-byte
		0xFFFFFFFFFFFFFFFF, // max uint64
	}

	for _, v := range values64 {
		encoded := AppendVarint64(nil, v)
		decoded, n, err := DecodeVarint64(encoded)
		if err != nil {
			t.Errorf("DecodeVarint64 failed for %d: %v", v, err)
			continue
		}
		if decoded != v {
			t.Errorf("Varint64 roundtrip: got %d, want %d", decoded, v)
		}
		if n != len(encoded) {
			t.Errorf("Varint64 %d: read %d bytes, encoded %d", v, n, len(encoded))
		}
	}
}

// TestLengthPrefixedSliceEdgeValues tests edge values for length-prefixed slices.
func TestLengthPrefixedSliceEdgeValues(t *testing.T) {
	tests := [][]byte{
		{},                  // empty
		{0x00},              // single null byte
		make([]byte, 127),   // fits in 1-byte varint length
		make([]byte, 128),   // needs 2-byte varint length
		make([]byte, 16383), // max 2-byte varint length
		make([]byte, 16384), // needs 3-byte varint length
	}

	for i, data := range tests {
		encoded := AppendLengthPrefixedSlice(nil, data)
		decoded, n, err := DecodeLengthPrefixedSlice(encoded)
		if err != nil {
			t.Errorf("Test %d: decode error: %v", i, err)
			continue
		}
		if n != len(encoded) {
			t.Errorf("Test %d: read %d bytes, encoded %d", i, n, len(encoded))
		}
		if len(decoded) != len(data) {
			t.Errorf("Test %d: decoded len %d, want %d", i, len(decoded), len(data))
		}
	}
}
