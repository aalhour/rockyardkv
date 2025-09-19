package checksum

import (
	"testing"
)

// Additional fuzz tests for checksum package.
// Note: FuzzCRC32CRoundtrip and FuzzCRC32CExtend are in crc32c_test.go

// FuzzXXH3Checksum fuzzes the XXH3 checksum implementation.
func FuzzXXH3Checksum(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte("hello world"))
	f.Add(make([]byte, 1024))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Compute checksum
		sum := XXH3Checksum(data)

		// Verify it's consistent
		sum2 := XXH3Checksum(data)
		if sum != sum2 {
			t.Errorf("XXH3Checksum not consistent: %x != %x", sum, sum2)
		}
	})
}

// FuzzXXH3Hash64 fuzzes the full 64-bit XXH3 hash.
func FuzzXXH3Hash64(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte("hello world"))
	f.Add(make([]byte, 1024))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Compute hash
		hash := XXH3_64bits(data)

		// Verify it's consistent
		hash2 := XXH3_64bits(data)
		if hash != hash2 {
			t.Errorf("XXH3_64bits not consistent: %x != %x", hash, hash2)
		}
	})
}

// FuzzComputeChecksumTypes fuzzes the generic ComputeChecksum function.
func FuzzComputeChecksumTypes(f *testing.F) {
	f.Add([]byte{}, byte(0), byte(TypeCRC32C))
	f.Add([]byte("hello"), byte(0x42), byte(TypeXXH3))

	f.Fuzz(func(t *testing.T, data []byte, lastByte byte, checksumType byte) {
		ct := Type(checksumType)

		// Only test valid types
		switch ct {
		case TypeCRC32C, TypeXXH3:
			sum := ComputeChecksum(ct, data, lastByte)

			// Verify consistency
			sum2 := ComputeChecksum(ct, data, lastByte)
			if sum != sum2 {
				t.Errorf("ComputeChecksum not consistent: %x != %x", sum, sum2)
			}
		default:
			// Skip invalid types
		}
	})
}

// FuzzMaskUnmaskRoundtrip fuzzes the mask/unmask functions.
func FuzzMaskUnmaskRoundtrip(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{1, 2, 3, 4})
	f.Add([]byte("test data for CRC"))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}

		// Compute masked CRC
		masked := MaskedExtend(0, data)
		unmasked := Unmask(masked)

		// Verify unmasked gives us back the raw CRC
		rawCRC := Extend(0, data)
		if unmasked != rawCRC {
			t.Errorf("Mask/Unmask roundtrip failed: masked=%x, unmasked=%x, raw=%x",
				masked, unmasked, rawCRC)
		}
	})
}
