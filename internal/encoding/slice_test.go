package encoding

import (
	"testing"
)

// TestPutVarint64 tests the PutVarint64 function
func TestPutVarint64(t *testing.T) {
	buf := make([]byte, 10)

	testCases := []uint64{
		0,
		127,
		128,
		16383,
		16384,
		1<<21 - 1,
		1 << 21,
		1<<28 - 1,
		1 << 28,
		1<<35 - 1,
		1 << 35,
		1<<63 - 1,
	}

	for _, v := range testCases {
		n := PutVarint64(buf, v)
		if n <= 0 {
			t.Errorf("PutVarint64(%d) returned %d, want > 0", v, n)
		}

		// Verify by decoding
		decoded, bytesRead, err := DecodeVarint64(buf[:n])
		if err != nil {
			t.Errorf("DecodeVarint64 failed: %v", err)
		}
		if bytesRead != n {
			t.Errorf("PutVarint64(%d): encoded %d bytes, decoded %d bytes", v, n, bytesRead)
		}
		if decoded != v {
			t.Errorf("PutVarint64(%d): decoded %d", v, decoded)
		}
	}
}

// TestSliceData tests the Slice.Data method
func TestSliceData(t *testing.T) {
	data := []byte("hello world")
	s := NewSlice(data)

	result := s.Data()
	if string(result) != string(data) {
		t.Errorf("Data() = %q, want %q", result, data)
	}
}

// TestSliceAdvance tests the Slice.Advance method
func TestSliceAdvance(t *testing.T) {
	data := []byte("hello world")
	s := NewSlice(data)

	s.Advance(5)
	remaining := s.Remaining()
	if remaining != len(data)-5 {
		t.Errorf("Remaining after Advance(5) = %d, want %d", remaining, len(data)-5)
	}
}

// TestSliceGetBytes tests the Slice.GetBytes method
func TestSliceGetBytes(t *testing.T) {
	data := []byte("hello world")
	s := NewSlice(data)

	bytes, ok := s.GetBytes(5)
	if !ok {
		t.Fatal("GetBytes(5) returned false")
	}
	if string(bytes) != "hello" {
		t.Errorf("GetBytes(5) = %q, want %q", bytes, "hello")
	}

	// Try to get more than remaining
	_, ok = s.GetBytes(100)
	if ok {
		t.Error("GetBytes(100) should return false for insufficient data")
	}
}

// TestSliceGetMethods tests various Get methods with insufficient data
func TestSliceGetMethods(t *testing.T) {
	// Test with empty slice
	s := NewSlice([]byte{})

	if _, ok := s.GetFixed16(); ok {
		t.Error("GetFixed16 on empty slice should fail")
	}
	if _, ok := s.GetFixed32(); ok {
		t.Error("GetFixed32 on empty slice should fail")
	}
	if _, ok := s.GetFixed64(); ok {
		t.Error("GetFixed64 on empty slice should fail")
	}
	if _, ok := s.GetVarint32(); ok {
		t.Error("GetVarint32 on empty slice should fail")
	}
	if _, ok := s.GetVarint64(); ok {
		t.Error("GetVarint64 on empty slice should fail")
	}
	if _, ok := s.GetVarsignedint64(); ok {
		t.Error("GetVarsignedint64 on empty slice should fail")
	}
	if _, ok := s.GetLengthPrefixedSlice(); ok {
		t.Error("GetLengthPrefixedSlice on empty slice should fail")
	}
}

// TestDecodeVarsignedint64EdgeCases tests DecodeVarsignedint64 edge cases
func TestDecodeVarsignedint64EdgeCases(t *testing.T) {
	// Test with negative value encoding
	negValue := int64(-1234567)
	encoded := AppendVarsignedint64(nil, negValue)

	decoded, n, err := DecodeVarsignedint64(encoded)
	if err != nil {
		t.Errorf("DecodeVarsignedint64 failed: %v", err)
	}
	if n != len(encoded) {
		t.Errorf("DecodeVarsignedint64: expected %d bytes, got %d", len(encoded), n)
	}
	if decoded != negValue {
		t.Errorf("DecodeVarsignedint64: got %d, want %d", decoded, negValue)
	}

	// Test with empty input
	_, n, err = DecodeVarsignedint64(nil)
	if err == nil {
		t.Error("DecodeVarsignedint64(nil) should return an error")
	}
	if n != 0 {
		t.Errorf("DecodeVarsignedint64(nil) should return 0 bytes read, got %d", n)
	}
}

// TestVarintLengthAllRanges tests VarintLength for all ranges
func TestVarintLengthAllRanges(t *testing.T) {
	testCases := []struct {
		value    uint64
		expected int
	}{
		{0, 1},
		{127, 1},
		{128, 2},
		{1<<14 - 1, 2},
		{1 << 14, 3},
		{1<<21 - 1, 3},
		{1 << 21, 4},
		{1<<28 - 1, 4},
		{1 << 28, 5},
		{1<<35 - 1, 5},
		{1 << 35, 6},
		{1<<42 - 1, 6},
		{1 << 42, 7},
		{1<<49 - 1, 7},
		{1 << 49, 8},
		{1<<56 - 1, 8},
		{1 << 56, 9},
		{1<<63 - 1, 9},
		{1 << 63, 10},
	}

	for _, tc := range testCases {
		got := VarintLength(tc.value)
		if got != tc.expected {
			t.Errorf("VarintLength(%d) = %d, want %d", tc.value, got, tc.expected)
		}
	}
}
