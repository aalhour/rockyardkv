package encoding

import (
	"bytes"
	"testing"
)

// FuzzVarint32Roundtrip tests that encoding then decoding a uint32 produces the original value.
func FuzzVarint32Roundtrip(f *testing.F) {
	// Seed with interesting values
	f.Add(uint32(0))
	f.Add(uint32(1))
	f.Add(uint32(127))
	f.Add(uint32(128))
	f.Add(uint32(255))
	f.Add(uint32(256))
	f.Add(uint32(16383))
	f.Add(uint32(16384))
	f.Add(uint32(0xFFFFFFFF))

	f.Fuzz(func(t *testing.T, value uint32) {
		encoded := AppendVarint32(nil, value)
		decoded, n, err := DecodeVarint32(encoded)
		if err != nil {
			t.Fatalf("DecodeVarint32 error: %v", err)
		}
		if decoded != value {
			t.Fatalf("Roundtrip failed: encoded %d, decoded %d", value, decoded)
		}
		if n != len(encoded) {
			t.Fatalf("Bytes consumed mismatch: %d vs %d", n, len(encoded))
		}
	})
}

// FuzzVarint64Roundtrip tests that encoding then decoding a uint64 produces the original value.
func FuzzVarint64Roundtrip(f *testing.F) {
	// Seed with interesting values
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(127))
	f.Add(uint64(128))
	f.Add(uint64(0xFFFFFFFF))
	f.Add(uint64(0x100000000))
	f.Add(uint64(0xFFFFFFFFFFFFFFFF))

	f.Fuzz(func(t *testing.T, value uint64) {
		encoded := AppendVarint64(nil, value)
		decoded, n, err := DecodeVarint64(encoded)
		if err != nil {
			t.Fatalf("DecodeVarint64 error: %v", err)
		}
		if decoded != value {
			t.Fatalf("Roundtrip failed: encoded %d, decoded %d", value, decoded)
		}
		if n != len(encoded) {
			t.Fatalf("Bytes consumed mismatch: %d vs %d", n, len(encoded))
		}
	})
}

// FuzzVarsignedint64Roundtrip tests zigzag-encoded signed int64.
func FuzzVarsignedint64Roundtrip(f *testing.F) {
	// Seed with interesting values
	f.Add(int64(0))
	f.Add(int64(1))
	f.Add(int64(-1))
	f.Add(int64(127))
	f.Add(int64(-128))
	f.Add(int64(0x7FFFFFFFFFFFFFFF))
	f.Add(int64(-0x8000000000000000))

	f.Fuzz(func(t *testing.T, value int64) {
		encoded := AppendVarsignedint64(nil, value)
		decoded, n, err := DecodeVarsignedint64(encoded)
		if err != nil {
			t.Fatalf("DecodeVarsignedint64 error: %v", err)
		}
		if decoded != value {
			t.Fatalf("Roundtrip failed: encoded %d, decoded %d", value, decoded)
		}
		if n != len(encoded) {
			t.Fatalf("Bytes consumed mismatch: %d vs %d", n, len(encoded))
		}
	})
}

// FuzzLengthPrefixedSliceRoundtrip tests length-prefixed slice encoding.
func FuzzLengthPrefixedSliceRoundtrip(f *testing.F) {
	// Seed with interesting values
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF})
	f.Add([]byte("hello"))
	f.Add([]byte("hello world this is a longer string"))
	f.Add(make([]byte, 1000)) // larger slice

	f.Fuzz(func(t *testing.T, value []byte) {
		encoded := AppendLengthPrefixedSlice(nil, value)
		decoded, n, err := DecodeLengthPrefixedSlice(encoded)
		if err != nil {
			t.Fatalf("DecodeLengthPrefixedSlice error: %v", err)
		}
		if !bytes.Equal(decoded, value) {
			t.Fatalf("Roundtrip failed: len(original)=%d, len(decoded)=%d", len(value), len(decoded))
		}
		if n != len(encoded) {
			t.Fatalf("Bytes consumed mismatch: %d vs %d", n, len(encoded))
		}
	})
}

// FuzzVarint32Decode tests that decoding doesn't panic on arbitrary input.
func FuzzVarint32Decode(f *testing.F) {
	// Seed with valid and invalid encodings
	f.Add([]byte{0x00})
	f.Add([]byte{0x7F})
	f.Add([]byte{0x80, 0x01})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F})
	f.Add([]byte{})
	f.Add([]byte{0x80})
	f.Add([]byte{0x80, 0x80, 0x80, 0x80, 0x80})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Should not panic
		_, _, _ = DecodeVarint32(data)
	})
}

// FuzzVarint64Decode tests that decoding doesn't panic on arbitrary input.
func FuzzVarint64Decode(f *testing.F) {
	// Seed with valid and invalid encodings
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01})
	f.Add([]byte{})
	f.Add([]byte{0x80})
	f.Add(make([]byte, 15)) // all zeros

	f.Fuzz(func(t *testing.T, data []byte) {
		// Should not panic
		_, _, _ = DecodeVarint64(data)
	})
}

// FuzzFixed32Roundtrip tests fixed-width 32-bit encoding.
func FuzzFixed32Roundtrip(f *testing.F) {
	f.Add(uint32(0))
	f.Add(uint32(1))
	f.Add(uint32(0xFFFFFFFF))
	f.Add(uint32(0x12345678))

	f.Fuzz(func(t *testing.T, value uint32) {
		buf := make([]byte, 4)
		EncodeFixed32(buf, value)
		decoded := DecodeFixed32(buf)
		if decoded != value {
			t.Fatalf("Roundtrip failed: encoded %d, decoded %d", value, decoded)
		}
	})
}

// FuzzFixed64Roundtrip tests fixed-width 64-bit encoding.
func FuzzFixed64Roundtrip(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(0xFFFFFFFFFFFFFFFF))
	f.Add(uint64(0x123456789ABCDEF0))

	f.Fuzz(func(t *testing.T, value uint64) {
		buf := make([]byte, 8)
		EncodeFixed64(buf, value)
		decoded := DecodeFixed64(buf)
		if decoded != value {
			t.Fatalf("Roundtrip failed: encoded %d, decoded %d", value, decoded)
		}
	})
}
