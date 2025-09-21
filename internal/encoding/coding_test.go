package encoding

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

// -----------------------------------------------------------------------------
// Fixed-width encoding tests
// -----------------------------------------------------------------------------

func TestFixed16(t *testing.T) {
	tests := []struct {
		name  string
		value uint16
		want  []byte
	}{
		{"zero", 0, []byte{0x00, 0x00}},
		{"one", 1, []byte{0x01, 0x00}},
		{"max", 0xFFFF, []byte{0xFF, 0xFF}},
		{"0x1234", 0x1234, []byte{0x34, 0x12}}, // little-endian
		{"256", 256, []byte{0x00, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test encode
			buf := make([]byte, 2)
			EncodeFixed16(buf, tt.value)
			if !bytes.Equal(buf, tt.want) {
				t.Errorf("EncodeFixed16(%d) = %v, want %v", tt.value, buf, tt.want)
			}

			// Test decode
			got := DecodeFixed16(tt.want)
			if got != tt.value {
				t.Errorf("DecodeFixed16(%v) = %d, want %d", tt.want, got, tt.value)
			}

			// Test append
			appended := AppendFixed16(nil, tt.value)
			if !bytes.Equal(appended, tt.want) {
				t.Errorf("AppendFixed16(%d) = %v, want %v", tt.value, appended, tt.want)
			}
		})
	}
}

func TestFixed32(t *testing.T) {
	tests := []struct {
		name  string
		value uint32
		want  []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00, 0x00}},
		{"one", 1, []byte{0x01, 0x00, 0x00, 0x00}},
		{"max", 0xFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{"0x12345678", 0x12345678, []byte{0x78, 0x56, 0x34, 0x12}},
		{"65536", 65536, []byte{0x00, 0x00, 0x01, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test encode
			buf := make([]byte, 4)
			EncodeFixed32(buf, tt.value)
			if !bytes.Equal(buf, tt.want) {
				t.Errorf("EncodeFixed32(%d) = %v, want %v", tt.value, buf, tt.want)
			}

			// Test decode
			got := DecodeFixed32(tt.want)
			if got != tt.value {
				t.Errorf("DecodeFixed32(%v) = %d, want %d", tt.want, got, tt.value)
			}

			// Test append
			appended := AppendFixed32(nil, tt.value)
			if !bytes.Equal(appended, tt.want) {
				t.Errorf("AppendFixed32(%d) = %v, want %v", tt.value, appended, tt.want)
			}
		})
	}
}

func TestFixed64(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
		want  []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{"one", 1, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{"max", 0xFFFFFFFFFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
		{"0x123456789ABCDEF0", 0x123456789ABCDEF0, []byte{0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test encode
			buf := make([]byte, 8)
			EncodeFixed64(buf, tt.value)
			if !bytes.Equal(buf, tt.want) {
				t.Errorf("EncodeFixed64(%d) = %v, want %v", tt.value, buf, tt.want)
			}

			// Test decode
			got := DecodeFixed64(tt.want)
			if got != tt.value {
				t.Errorf("DecodeFixed64(%v) = %d, want %d", tt.want, got, tt.value)
			}

			// Test append
			appended := AppendFixed64(nil, tt.value)
			if !bytes.Equal(appended, tt.want) {
				t.Errorf("AppendFixed64(%d) = %v, want %v", tt.value, appended, tt.want)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Varint32 tests
// -----------------------------------------------------------------------------

func TestVarint32(t *testing.T) {
	tests := []struct {
		name  string
		value uint32
		want  []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"one", 1, []byte{0x01}},
		{"127", 127, []byte{0x7F}},
		{"128", 128, []byte{0x80, 0x01}},
		{"255", 255, []byte{0xFF, 0x01}},
		{"256", 256, []byte{0x80, 0x02}},
		{"300", 300, []byte{0xAC, 0x02}},
		{"16383", 16383, []byte{0xFF, 0x7F}},
		{"16384", 16384, []byte{0x80, 0x80, 0x01}},
		{"max", 0xFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test encode
			buf := make([]byte, MaxVarint32Length)
			n := EncodeVarint32(buf, tt.value)
			if !bytes.Equal(buf[:n], tt.want) {
				t.Errorf("EncodeVarint32(%d) = %v, want %v", tt.value, buf[:n], tt.want)
			}

			// Test decode
			got, bytesRead, err := DecodeVarint32(tt.want)
			if err != nil {
				t.Errorf("DecodeVarint32(%v) error: %v", tt.want, err)
			}
			if got != tt.value {
				t.Errorf("DecodeVarint32(%v) = %d, want %d", tt.want, got, tt.value)
			}
			if bytesRead != len(tt.want) {
				t.Errorf("DecodeVarint32(%v) bytesRead = %d, want %d", tt.want, bytesRead, len(tt.want))
			}

			// Test append
			appended := AppendVarint32(nil, tt.value)
			if !bytes.Equal(appended, tt.want) {
				t.Errorf("AppendVarint32(%d) = %v, want %v", tt.value, appended, tt.want)
			}
		})
	}
}

func TestVarint32DecodeErrors(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr error
	}{
		{"empty", []byte{}, ErrVarintTermination},
		{"unterminated_1", []byte{0x80}, ErrVarintTermination},
		{"unterminated_2", []byte{0x80, 0x80}, ErrVarintTermination},
		{"unterminated_5", []byte{0x80, 0x80, 0x80, 0x80, 0x80}, ErrVarintOverflow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeVarint32(tt.input)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("DecodeVarint32(%v) error = %v, want %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Varint64 tests
// -----------------------------------------------------------------------------

func TestVarint64(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
		want  []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"one", 1, []byte{0x01}},
		{"127", 127, []byte{0x7F}},
		{"128", 128, []byte{0x80, 0x01}},
		{"max_uint32", math.MaxUint32, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},
		{"max_uint32+1", math.MaxUint32 + 1, []byte{0x80, 0x80, 0x80, 0x80, 0x10}},
		{"max_uint64", math.MaxUint64, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test encode
			buf := make([]byte, MaxVarint64Length)
			n := EncodeVarint64(buf, tt.value)
			if !bytes.Equal(buf[:n], tt.want) {
				t.Errorf("EncodeVarint64(%d) = %v, want %v", tt.value, buf[:n], tt.want)
			}

			// Test decode
			got, bytesRead, err := DecodeVarint64(tt.want)
			if err != nil {
				t.Errorf("DecodeVarint64(%v) error: %v", tt.want, err)
			}
			if got != tt.value {
				t.Errorf("DecodeVarint64(%v) = %d, want %d", tt.want, got, tt.value)
			}
			if bytesRead != len(tt.want) {
				t.Errorf("DecodeVarint64(%v) bytesRead = %d, want %d", tt.want, bytesRead, len(tt.want))
			}

			// Test append
			appended := AppendVarint64(nil, tt.value)
			if !bytes.Equal(appended, tt.want) {
				t.Errorf("AppendVarint64(%d) = %v, want %v", tt.value, appended, tt.want)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// VarintLength tests
// -----------------------------------------------------------------------------

func TestVarintLength(t *testing.T) {
	tests := []struct {
		value uint64
		want  int
	}{
		{0, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
		{math.MaxUint32, 5},
		{math.MaxUint64, 10},
	}

	for _, tt := range tests {
		got := VarintLength(tt.value)
		if got != tt.want {
			t.Errorf("VarintLength(%d) = %d, want %d", tt.value, got, tt.want)
		}
	}
}

// -----------------------------------------------------------------------------
// Zigzag encoding tests
// -----------------------------------------------------------------------------

func TestZigzag(t *testing.T) {
	tests := []struct {
		signed   int64
		unsigned uint64
	}{
		{0, 0},
		{-1, 1},
		{1, 2},
		{-2, 3},
		{2, 4},
		{-128, 255},
		{127, 254},
		{math.MaxInt64, 0xFFFFFFFFFFFFFFFE},
		{math.MinInt64, 0xFFFFFFFFFFFFFFFF},
	}

	for _, tt := range tests {
		// Test I64ToZigzag
		gotUnsigned := I64ToZigzag(tt.signed)
		if gotUnsigned != tt.unsigned {
			t.Errorf("I64ToZigzag(%d) = %d, want %d", tt.signed, gotUnsigned, tt.unsigned)
		}

		// Test ZigzagToI64
		gotSigned := ZigzagToI64(tt.unsigned)
		if gotSigned != tt.signed {
			t.Errorf("ZigzagToI64(%d) = %d, want %d", tt.unsigned, gotSigned, tt.signed)
		}
	}
}

func TestVarsignedint64(t *testing.T) {
	tests := []int64{
		0, 1, -1, 127, -128, 128, -129,
		math.MaxInt32, math.MinInt32,
		math.MaxInt64, math.MinInt64,
	}

	for _, value := range tests {
		encoded := AppendVarsignedint64(nil, value)
		decoded, n, err := DecodeVarsignedint64(encoded)
		if err != nil {
			t.Errorf("DecodeVarsignedint64 error for %d: %v", value, err)
			continue
		}
		if decoded != value {
			t.Errorf("Varsignedint64 roundtrip: got %d, want %d", decoded, value)
		}
		if n != len(encoded) {
			t.Errorf("Varsignedint64 bytes read: got %d, want %d", n, len(encoded))
		}
	}
}

// -----------------------------------------------------------------------------
// Length-prefixed slice tests
// -----------------------------------------------------------------------------

func TestLengthPrefixedSlice(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
	}{
		{"empty", []byte{}},
		{"single", []byte{0x42}},
		{"hello", []byte("hello")},
		{"binary", []byte{0x00, 0x01, 0x02, 0xFF}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := AppendLengthPrefixedSlice(nil, tt.value)

			// Verify length prefix
			length, n, err := DecodeVarint32(encoded)
			if err != nil {
				t.Fatalf("Failed to decode length: %v", err)
			}
			if int(length) != len(tt.value) {
				t.Errorf("Length = %d, want %d", length, len(tt.value))
			}

			// Verify decode
			decoded, bytesRead, err := DecodeLengthPrefixedSlice(encoded)
			if err != nil {
				t.Fatalf("DecodeLengthPrefixedSlice error: %v", err)
			}
			if !bytes.Equal(decoded, tt.value) {
				t.Errorf("Decoded = %v, want %v", decoded, tt.value)
			}
			if bytesRead != n+len(tt.value) {
				t.Errorf("bytesRead = %d, want %d", bytesRead, n+len(tt.value))
			}
		})
	}
}

func TestLengthPrefixedSliceErrors(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr error
	}{
		{"empty", []byte{}, ErrVarintTermination},
		{"length_only", []byte{0x05}, ErrBufferTooSmall},
		{"short_data", []byte{0x05, 0x01, 0x02}, ErrBufferTooSmall},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeLengthPrefixedSlice(tt.input)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("DecodeLengthPrefixedSlice(%v) error = %v, want %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Slice helper tests
// -----------------------------------------------------------------------------

func TestSlice(t *testing.T) {
	// Build a buffer with multiple values
	var buf []byte
	buf = AppendFixed16(buf, 0x1234)
	buf = AppendFixed32(buf, 0x56789ABC)
	buf = AppendFixed64(buf, 0xDEF0123456789ABC)
	buf = AppendVarint32(buf, 300)
	buf = AppendVarint64(buf, math.MaxUint64)
	buf = AppendVarsignedint64(buf, -42)
	buf = AppendLengthPrefixedSlice(buf, []byte("test"))

	s := NewSlice(buf)

	// Read Fixed16
	v16, ok := s.GetFixed16()
	if !ok || v16 != 0x1234 {
		t.Errorf("GetFixed16() = %x, %v; want 0x1234, true", v16, ok)
	}

	// Read Fixed32
	v32, ok := s.GetFixed32()
	if !ok || v32 != 0x56789ABC {
		t.Errorf("GetFixed32() = %x, %v; want 0x56789ABC, true", v32, ok)
	}

	// Read Fixed64
	v64, ok := s.GetFixed64()
	if !ok || v64 != 0xDEF0123456789ABC {
		t.Errorf("GetFixed64() = %x, %v; want 0xDEF0123456789ABC, true", v64, ok)
	}

	// Read Varint32
	vu32, ok := s.GetVarint32()
	if !ok || vu32 != 300 {
		t.Errorf("GetVarint32() = %d, %v; want 300, true", vu32, ok)
	}

	// Read Varint64
	vu64, ok := s.GetVarint64()
	if !ok || vu64 != math.MaxUint64 {
		t.Errorf("GetVarint64() = %d, %v; want MaxUint64, true", vu64, ok)
	}

	// Read Varsignedint64
	vi64, ok := s.GetVarsignedint64()
	if !ok || vi64 != -42 {
		t.Errorf("GetVarsignedint64() = %d, %v; want -42, true", vi64, ok)
	}

	// Read LengthPrefixedSlice
	slice, ok := s.GetLengthPrefixedSlice()
	if !ok || string(slice) != "test" {
		t.Errorf("GetLengthPrefixedSlice() = %q, %v; want \"test\", true", slice, ok)
	}

	// Should have consumed all bytes
	if s.Remaining() != 0 {
		t.Errorf("Remaining() = %d, want 0", s.Remaining())
	}
}

// -----------------------------------------------------------------------------
// Roundtrip property tests
// -----------------------------------------------------------------------------

func TestVarint32Roundtrip(t *testing.T) {
	values := []uint32{0, 1, 127, 128, 255, 256, 16383, 16384, math.MaxUint32}
	for _, v := range values {
		encoded := AppendVarint32(nil, v)
		decoded, n, err := DecodeVarint32(encoded)
		if err != nil {
			t.Errorf("Roundtrip error for %d: %v", v, err)
			continue
		}
		if decoded != v || n != len(encoded) {
			t.Errorf("Roundtrip failed for %d: got %d (n=%d)", v, decoded, n)
		}
	}
}

func TestVarint64Roundtrip(t *testing.T) {
	values := []uint64{0, 1, 127, 128, math.MaxUint32, math.MaxUint32 + 1, math.MaxUint64}
	for _, v := range values {
		encoded := AppendVarint64(nil, v)
		decoded, n, err := DecodeVarint64(encoded)
		if err != nil {
			t.Errorf("Roundtrip error for %d: %v", v, err)
			continue
		}
		if decoded != v || n != len(encoded) {
			t.Errorf("Roundtrip failed for %d: got %d (n=%d)", v, decoded, n)
		}
	}
}
