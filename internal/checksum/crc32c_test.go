package checksum

import (
	"bytes"
	"math/rand"
	"testing"
)

// TestCRC32CBasic tests basic CRC32C computation.
func TestCRC32CBasic(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want uint32
	}{
		{"empty", []byte{}, 0},
		{"zero_byte", []byte{0x00}, 0x527d5351},
		{"one_byte_ff", []byte{0xff}, 0xff000000},
		// Standard test vector for CRC32C
		{"123456789", []byte("123456789"), 0xe3069283},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Value(tt.data)
			if got != tt.want {
				t.Errorf("Value(%v) = 0x%08x, want 0x%08x", tt.data, got, tt.want)
			}
		})
	}
}

// TestCRC32CStandardResults tests RFC3720 test vectors (matching C++ StandardResults test)
func TestCRC32CStandardResults(t *testing.T) {
	// From RFC 3720 section B.4
	buf := make([]byte, 32)

	// All zeros
	for i := range buf {
		buf[i] = 0
	}
	if got := Value(buf); got != 0x8a9136aa {
		t.Errorf("All zeros: got 0x%08x, want 0x8a9136aa", got)
	}

	// All 0xFF
	for i := range buf {
		buf[i] = 0xFF
	}
	if got := Value(buf); got != 0x62a8ab43 {
		t.Errorf("All 0xFF: got 0x%08x, want 0x62a8ab43", got)
	}

	// Ascending bytes
	for i := range buf {
		buf[i] = byte(i)
	}
	if got := Value(buf); got != 0x46dd794e {
		t.Errorf("Ascending: got 0x%08x, want 0x46dd794e", got)
	}

	// Descending bytes
	for i := range buf {
		buf[i] = byte(31 - i)
	}
	if got := Value(buf); got != 0x113fdb5c {
		t.Errorf("Descending: got 0x%08x, want 0x113fdb5c", got)
	}

	// 48-byte test vector from RFC
	data := []byte{
		0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
		0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	if got := Value(data); got != 0xd9963a56 {
		t.Errorf("48-byte vector: got 0x%08x, want 0xd9963a56", got)
	}
}

// TestCRC32CValues tests that different inputs produce different outputs
func TestCRC32CValues(t *testing.T) {
	a := Value([]byte("a"))
	foo := Value([]byte("foo"))
	if a == foo {
		t.Errorf("Value(\"a\") == Value(\"foo\"), both 0x%08x", a)
	}
}

// TestCRC32CExtend tests the Extend function.
func TestCRC32CExtend(t *testing.T) {
	// Compute CRC of "hello world" in two parts (matching C++ Extend test)
	full := Value([]byte("hello world"))
	partial := Value([]byte("hello "))
	extended := Extend(partial, []byte("world"))

	if extended != full {
		t.Errorf("Extend mismatch: got 0x%08x, want 0x%08x", extended, full)
	}
}

// TestCRC32CExtendMultiple tests multiple incremental extensions.
func TestCRC32CExtendMultiple(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog")

	// Compute full CRC
	full := Value(data)

	// Compute incrementally byte by byte
	var crc uint32 = 0
	for i := range data {
		if i == 0 {
			crc = Value(data[0:1])
		} else {
			crc = Extend(crc, data[i:i+1])
		}
	}

	if crc != full {
		t.Errorf("Incremental byte-by-byte mismatch: got 0x%08x, want 0x%08x", crc, full)
	}
}

// TestCRC32CMask tests the masking functions (matching C++ Mask test)
func TestCRC32CMask(t *testing.T) {
	crc := Value([]byte("foo"))

	// Mask should change the value
	masked := Mask(crc)
	if masked == crc {
		t.Errorf("Mask did not change CRC: 0x%08x", crc)
	}

	// Double masking should also change
	doubleMasked := Mask(masked)
	if doubleMasked == crc {
		t.Errorf("Double mask equals original: 0x%08x", crc)
	}

	// Unmask should recover original
	unmasked := Unmask(masked)
	if unmasked != crc {
		t.Errorf("Unmask failed: got 0x%08x, want 0x%08x", unmasked, crc)
	}

	// Double unmask of double mask should recover original
	doubleUnmasked := Unmask(Unmask(Mask(Mask(crc))))
	if doubleUnmasked != crc {
		t.Errorf("Double unmask of double mask failed: got 0x%08x, want 0x%08x", doubleUnmasked, crc)
	}
}

// TestMaskDelta verifies the mask delta constant matches RocksDB.
func TestMaskDelta(t *testing.T) {
	// From RocksDB: static const uint32_t kMaskDelta = 0xa282ead8ul;
	if maskDelta != 0xa282ead8 {
		t.Errorf("maskDelta = 0x%08x, want 0xa282ead8", maskDelta)
	}
}

// TestMaskedValue tests the convenience function.
func TestMaskedValue(t *testing.T) {
	data := []byte("test data")
	expected := Mask(Value(data))
	got := MaskedValue(data)
	if got != expected {
		t.Errorf("MaskedValue mismatch: got 0x%08x, want 0x%08x", got, expected)
	}
}

// TestMaskRoundtrip tests mask/unmask roundtrip for various values
func TestMaskRoundtrip(t *testing.T) {
	tests := []uint32{
		0,
		1,
		0x12345678,
		0xFFFFFFFF,
		0xDEADBEEF,
		0x00000001,
		0x80000000,
	}

	for _, crc := range tests {
		masked := Mask(crc)
		unmasked := Unmask(masked)
		if unmasked != crc {
			t.Errorf("Mask/Unmask roundtrip failed: original=0x%08x, masked=0x%08x, unmasked=0x%08x",
				crc, masked, unmasked)
		}
	}
}

// TestCRC32CEmptyExtend tests extending an empty CRC
func TestCRC32CEmptyExtend(t *testing.T) {
	// Extending empty CRC with data should equal Value of that data
	data := []byte("test")
	fromEmpty := Extend(0, data)
	direct := Value(data)

	if fromEmpty != direct {
		t.Errorf("Extend from 0 mismatch: got 0x%08x, want 0x%08x", fromEmpty, direct)
	}
}

// TestCRC32CLargeBuffer tests CRC on larger buffers
func TestCRC32CLargeBuffer(t *testing.T) {
	sizes := []int{1024, 4096, 32768, 65536}

	for _, size := range sizes {
		buf := make([]byte, size)
		for i := range buf {
			buf[i] = byte(i % 256)
		}

		// Should not panic and should produce a value
		crc := Value(buf)
		if crc == 0 && size > 0 {
			// Zero CRC is unlikely for non-trivial data
			t.Logf("Warning: CRC of %d bytes is zero", size)
		}

		// Verify extend produces same result
		half := size / 2
		crc1 := Value(buf[:half])
		crc2 := Extend(crc1, buf[half:])
		if crc2 != crc {
			t.Errorf("Extend mismatch for size %d: got 0x%08x, want 0x%08x", size, crc2, crc)
		}
	}
}

// TestCRC32CStitching tests stitching two computations together
func TestCRC32CStitching(t *testing.T) {
	// This is the core test from C++ - stitching partial CRCs
	rng := rand.New(rand.NewSource(42))

	for length := range 100 {
		data := make([]byte, length)
		for i := range data {
			data[i] = byte(rng.Intn(256))
		}

		full := Value(data)

		// Test splitting at every point
		for split := 0; split <= length; split++ {
			part1 := data[:split]
			part2 := data[split:]

			crc1 := Value(part1)
			crc2 := Extend(crc1, part2)

			if crc2 != full {
				t.Errorf("Stitching failed at length=%d, split=%d: got 0x%08x, want 0x%08x",
					length, split, crc2, full)
			}
		}
	}
}

// TestCRC32CAlignedInputs tests with various alignments
func TestCRC32CAlignedInputs(t *testing.T) {
	// Test small aligned inputs (matching C++ 3-way CRC tests)
	baseData := make([]byte, 64)
	for i := range baseData {
		baseData[i] = byte(i)
	}

	// Test various offsets and lengths
	for offset := range 16 {
		for length := 1; length <= 16; length++ {
			data := baseData[offset : offset+length]
			crc := Value(data)

			// Verify via extend
			var expected uint32 = 0
			for i := range length {
				if i == 0 {
					expected = Value(data[0:1])
				} else {
					expected = Extend(expected, data[i:i+1])
				}
			}

			if crc != expected {
				t.Errorf("Aligned test offset=%d length=%d: got 0x%08x, want 0x%08x",
					offset, length, crc, expected)
			}
		}
	}
}

// Golden test vectors that should match RocksDB output.
func TestCRC32CGolden(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		unmasked uint32
		masked   uint32
	}{
		// CRC32C("") = 0x00000000
		{"empty", []byte{}, 0x00000000, 0xa282ead8},
		// Verified via computation
		{"foo", []byte("foo"), 0xcfc4ae1d, 0xfebe8a61},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unmasked := Value(tt.data)
			if unmasked != tt.unmasked {
				t.Errorf("Value(%q) = 0x%08x, want 0x%08x", tt.data, unmasked, tt.unmasked)
			}

			masked := Mask(unmasked)
			if masked != tt.masked {
				t.Errorf("Mask(Value(%q)) = 0x%08x, want 0x%08x", tt.data, masked, tt.masked)
			}
		})
	}
}

// Fuzz test for CRC32C
func FuzzCRC32CRoundtrip(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte(""))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})

	f.Fuzz(func(t *testing.T, data []byte) {
		crc := Value(data)

		// Mask/unmask should roundtrip
		masked := Mask(crc)
		unmasked := Unmask(masked)
		if unmasked != crc {
			t.Errorf("Mask/Unmask roundtrip failed for len=%d", len(data))
		}

		// Extend from 0 should equal direct computation
		if len(data) > 0 {
			extended := Extend(0, data)
			if extended != crc {
				t.Errorf("Extend from 0 failed for len=%d", len(data))
			}
		}
	})
}

func FuzzCRC32CExtend(f *testing.F) {
	f.Add([]byte("hello"), []byte("world"))
	f.Add([]byte(""), []byte("test"))

	f.Fuzz(func(t *testing.T, part1, part2 []byte) {
		full := Value(append(part1, part2...))
		crc1 := Value(part1)
		crc2 := Extend(crc1, part2)

		if crc2 != full {
			t.Errorf("Extend mismatch for parts of len %d and %d", len(part1), len(part2))
		}
	})
}

// Benchmark tests
func BenchmarkCRC32C(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 4096)
	b.SetBytes(int64(len(data)))

	for b.Loop() {
		Value(data)
	}
}

func BenchmarkCRC32CMasked(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 4096)
	b.SetBytes(int64(len(data)))

	for b.Loop() {
		MaskedValue(data)
	}
}

func BenchmarkCRC32CExtend(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 4096)
	b.SetBytes(int64(len(data)))

	for b.Loop() {
		crc := Value(data[:2048])
		Extend(crc, data[2048:])
	}
}
