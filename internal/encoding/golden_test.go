package encoding

import (
	"bytes"
	"testing"
)

// TestGoldenFixedEncoding tests fixed-width encoding matches RocksDB.
func TestGoldenFixedEncoding(t *testing.T) {
	t.Run("Fixed16", func(t *testing.T) {
		testCases := []struct {
			value    uint16
			expected []byte
		}{
			{0x0000, []byte{0x00, 0x00}},
			{0x0001, []byte{0x01, 0x00}},
			{0x0100, []byte{0x00, 0x01}},
			{0xFFFF, []byte{0xFF, 0xFF}},
			{0x1234, []byte{0x34, 0x12}},
		}

		for _, tc := range testCases {
			buf := make([]byte, 2)
			EncodeFixed16(buf, tc.value)
			if !bytes.Equal(buf, tc.expected) {
				t.Errorf("EncodeFixed16(0x%04x) = %x, want %x", tc.value, buf, tc.expected)
			}
			decoded := DecodeFixed16(tc.expected)
			if decoded != tc.value {
				t.Errorf("DecodeFixed16(%x) = 0x%04x, want 0x%04x", tc.expected, decoded, tc.value)
			}
		}
	})

	t.Run("Fixed32", func(t *testing.T) {
		testCases := []struct {
			value    uint32
			expected []byte
		}{
			{0x00000000, []byte{0x00, 0x00, 0x00, 0x00}},
			{0x00000001, []byte{0x01, 0x00, 0x00, 0x00}},
			{0xFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF}},
			{0x12345678, []byte{0x78, 0x56, 0x34, 0x12}},
		}

		for _, tc := range testCases {
			buf := make([]byte, 4)
			EncodeFixed32(buf, tc.value)
			if !bytes.Equal(buf, tc.expected) {
				t.Errorf("EncodeFixed32(0x%08x) = %x, want %x", tc.value, buf, tc.expected)
			}
			decoded := DecodeFixed32(tc.expected)
			if decoded != tc.value {
				t.Errorf("DecodeFixed32(%x) = 0x%08x, want 0x%08x", tc.expected, decoded, tc.value)
			}
		}
	})

	t.Run("Fixed64", func(t *testing.T) {
		testCases := []struct {
			value    uint64
			expected []byte
		}{
			{0x0000000000000000, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{0x0000000000000001, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{0xFFFFFFFFFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
			{0x0123456789ABCDEF, []byte{0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01}},
		}

		for _, tc := range testCases {
			buf := make([]byte, 8)
			EncodeFixed64(buf, tc.value)
			if !bytes.Equal(buf, tc.expected) {
				t.Errorf("EncodeFixed64(0x%016x) = %x, want %x", tc.value, buf, tc.expected)
			}
			decoded := DecodeFixed64(tc.expected)
			if decoded != tc.value {
				t.Errorf("DecodeFixed64(%x) = 0x%016x, want 0x%016x", tc.expected, decoded, tc.value)
			}
		}
	})
}

// TestGoldenVarint32Encoding tests varint32 encoding matches RocksDB.
func TestGoldenVarint32Encoding(t *testing.T) {
	testCases := []struct {
		value    uint32
		expected []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x01}},
		{255, []byte{0xFF, 0x01}},
		{256, []byte{0x80, 0x02}},
		{16383, []byte{0xFF, 0x7F}},
		{16384, []byte{0x80, 0x80, 0x01}},
		{0xFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			encoded := AppendVarint32(nil, tc.value)
			if !bytes.Equal(encoded, tc.expected) {
				t.Errorf("AppendVarint32(%d) = %x, want %x", tc.value, encoded, tc.expected)
			}

			decoded, n, err := DecodeVarint32(tc.expected)
			if err != nil {
				t.Fatalf("DecodeVarint32(%x) error: %v", tc.expected, err)
			}
			if n != len(tc.expected) {
				t.Errorf("DecodeVarint32(%x) consumed %d bytes, want %d", tc.expected, n, len(tc.expected))
			}
			if decoded != tc.value {
				t.Errorf("DecodeVarint32(%x) = %d, want %d", tc.expected, decoded, tc.value)
			}
		})
	}
}

// TestGoldenVarint64Encoding tests varint64 encoding matches RocksDB.
func TestGoldenVarint64Encoding(t *testing.T) {
	testCases := []struct {
		value    uint64
		expected []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x01}},
		{0xFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},
		{0xFFFFFFFFFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			encoded := AppendVarint64(nil, tc.value)
			if !bytes.Equal(encoded, tc.expected) {
				t.Errorf("AppendVarint64(%d) = %x, want %x", tc.value, encoded, tc.expected)
			}

			decoded, n, err := DecodeVarint64(tc.expected)
			if err != nil {
				t.Fatalf("DecodeVarint64(%x) error: %v", tc.expected, err)
			}
			if n != len(tc.expected) {
				t.Errorf("DecodeVarint64(%x) consumed %d bytes, want %d", tc.expected, n, len(tc.expected))
			}
			if decoded != tc.value {
				t.Errorf("DecodeVarint64(%x) = %d, want %d", tc.expected, decoded, tc.value)
			}
		})
	}
}

// TestGoldenLengthPrefixedSlice tests length-prefixed slice encoding.
func TestGoldenLengthPrefixedSlice(t *testing.T) {
	testCases := []struct {
		name     string
		input    []byte
		expected []byte
	}{
		{
			name:     "empty",
			input:    []byte{},
			expected: []byte{0x00},
		},
		{
			name:     "single byte",
			input:    []byte{0x42},
			expected: []byte{0x01, 0x42},
		},
		{
			name:     "hello",
			input:    []byte("hello"),
			expected: []byte{0x05, 'h', 'e', 'l', 'l', 'o'},
		},
		{
			name:     "128 bytes",
			input:    make([]byte, 128),
			expected: append([]byte{0x80, 0x01}, make([]byte, 128)...),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := AppendLengthPrefixedSlice(nil, tc.input)
			if !bytes.Equal(encoded, tc.expected) {
				t.Errorf("AppendLengthPrefixedSlice(%x) = %x, want %x", tc.input, encoded, tc.expected)
			}

			decoded, n, err := DecodeLengthPrefixedSlice(tc.expected)
			if err != nil {
				t.Fatalf("DecodeLengthPrefixedSlice(%x) error: %v", tc.expected, err)
			}
			if n != len(tc.expected) {
				t.Errorf("DecodeLengthPrefixedSlice consumed %d bytes, want %d", n, len(tc.expected))
			}
			if !bytes.Equal(decoded, tc.input) {
				t.Errorf("DecodeLengthPrefixedSlice(%x) = %x, want %x", tc.expected, decoded, tc.input)
			}
		})
	}
}
