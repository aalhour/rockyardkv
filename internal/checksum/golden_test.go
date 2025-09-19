package checksum

import (
	"testing"
)

// TestGoldenCRC32CDeterminism tests that CRC32C is deterministic.
func TestGoldenCRC32CDeterminism(t *testing.T) {
	testCases := []struct {
		name  string
		input []byte
	}{
		{"empty", []byte{}},
		{"single byte", []byte{0x00}},
		{"hello", []byte("hello")},
		{"123456789", []byte("123456789")},
		{"RocksDB", []byte("RocksDB")},
		{"long string", []byte("The quick brown fox jumps over the lazy dog")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			crc1 := Value(tc.input)
			crc2 := Value(tc.input)
			if crc1 != crc2 {
				t.Errorf("CRC32C not deterministic: got 0x%08x and 0x%08x", crc1, crc2)
			}
		})
	}
}

// TestGoldenCRC32CMaskUnmaskRoundtrip tests mask/unmask roundtrip.
func TestGoldenCRC32CMaskUnmaskRoundtrip(t *testing.T) {
	testCases := []uint32{
		0x00000000,
		0xFFFFFFFF,
		0x12345678,
		0xDEADBEEF,
		Value([]byte("hello")),
		Value([]byte("RocksDB")),
	}

	for _, crc := range testCases {
		masked := Mask(crc)
		unmasked := Unmask(masked)
		if unmasked != crc {
			t.Errorf("Unmask(Mask(0x%08x)) = 0x%08x", crc, unmasked)
		}
	}
}

// TestGoldenCRC32CExtend tests CRC extension.
func TestGoldenCRC32CExtend(t *testing.T) {
	// CRC of "helloworld" should equal extending CRC of "hello" with "world"
	full := Value([]byte("helloworld"))
	extended := Extend(Value([]byte("hello")), []byte("world"))
	if full != extended {
		t.Errorf("CRC(helloworld) = 0x%08x, Extend(CRC(hello), world) = 0x%08x", full, extended)
	}
}

// TestGoldenXXH3ChecksumWithLastByte tests XXH3 checksum with separate last byte.
func TestGoldenXXH3ChecksumWithLastByte(t *testing.T) {
	testCases := []struct {
		name     string
		data     []byte
		lastByte byte
	}{
		{"empty data, zero byte", []byte{}, 0x00},
		{"hello, compression type 1", []byte("hello"), 0x01},
		{"data block, no compression", []byte("test data block"), 0x00},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Verify determinism
			checksum1 := XXH3ChecksumWithLastByte(tc.data, tc.lastByte)
			checksum2 := XXH3ChecksumWithLastByte(tc.data, tc.lastByte)
			if checksum1 != checksum2 {
				t.Errorf("XXH3Checksum not deterministic: got 0x%08x and 0x%08x", checksum1, checksum2)
			}
		})
	}
}

// TestGoldenChecksumTypeString tests string representation of checksum types.
func TestGoldenChecksumTypeString(t *testing.T) {
	testCases := []struct {
		typ      Type
		expected string
	}{
		{TypeNoChecksum, "NoChecksum"},
		{TypeCRC32C, "CRC32C"},
		{TypeXXHash, "XXHash"},
		{TypeXXHash64, "XXHash64"},
		{TypeXXH3, "XXH3"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			got := tc.typ.String()
			if got != tc.expected {
				t.Errorf("Type(%d).String() = %q, want %q", tc.typ, got, tc.expected)
			}
		})
	}
}

// TestGoldenChecksumTypeValues tests that checksum type constants match RocksDB.
func TestGoldenChecksumTypeValues(t *testing.T) {
	// These must match RocksDB's include/rocksdb/table.h
	testCases := []struct {
		name     string
		typ      Type
		expected uint8
	}{
		{"NoChecksum", TypeNoChecksum, 0},
		{"CRC32C", TypeCRC32C, 1},
		{"XXHash", TypeXXHash, 2},
		{"XXHash64", TypeXXHash64, 3},
		{"XXH3", TypeXXH3, 4},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if uint8(tc.typ) != tc.expected {
				t.Errorf("%s = %d, want %d", tc.name, tc.typ, tc.expected)
			}
		})
	}
}

// TestGoldenComputeChecksum tests the ComputeChecksum helper function.
func TestGoldenComputeChecksum(t *testing.T) {
	data := []byte("test block data")
	lastByte := byte(0x00) // no compression

	// CRC32C should be deterministic
	crc1 := ComputeChecksum(TypeCRC32C, data, lastByte)
	crc2 := ComputeChecksum(TypeCRC32C, data, lastByte)
	if crc1 != crc2 {
		t.Errorf("ComputeChecksum(CRC32C) not deterministic: 0x%08x vs 0x%08x", crc1, crc2)
	}

	// XXH3 should be deterministic
	xxh1 := ComputeChecksum(TypeXXH3, data, lastByte)
	xxh2 := ComputeChecksum(TypeXXH3, data, lastByte)
	if xxh1 != xxh2 {
		t.Errorf("ComputeChecksum(XXH3) not deterministic: 0x%08x vs 0x%08x", xxh1, xxh2)
	}

	// NoChecksum should return 0
	none := ComputeChecksum(TypeNoChecksum, data, lastByte)
	if none != 0 {
		t.Errorf("ComputeChecksum(NoChecksum) = 0x%08x, want 0", none)
	}
}
