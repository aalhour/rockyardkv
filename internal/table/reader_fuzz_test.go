package table

import (
	"testing"
)

// FuzzFooterDecode tests the footer decoder with arbitrary input.
func FuzzFooterDecode(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping fuzz test in short mode")
	}
	// Add some seed corpus
	f.Add(make([]byte, 0))
	f.Add(make([]byte, 10))
	f.Add(make([]byte, 48))
	f.Add(make([]byte, 53))
	f.Add(make([]byte, 100))

	// Add valid footer patterns
	validLegacy := make([]byte, 48)
	// Put legacy magic at end
	validLegacy[40] = 0x00
	validLegacy[41] = 0x77
	validLegacy[42] = 0x00
	validLegacy[43] = 0x77
	validLegacy[44] = 0x00
	validLegacy[45] = 0x77
	validLegacy[46] = 0x00
	validLegacy[47] = 0x77
	f.Add(validLegacy)

	f.Fuzz(func(t *testing.T, data []byte) {
		// This should not panic
		file := &BytesFile{data: data}
		reader, err := Open(file, ReaderOptions{VerifyChecksums: false})
		if err != nil {
			// Expected for invalid data
			return
		}
		defer reader.Close()

		// If we got a reader, try using it
		iter := reader.NewIterator()
		iter.SeekToFirst()
		for iter.Valid() {
			_ = iter.Key()
			_ = iter.Value()
			iter.Next()
		}
	})
}

// FuzzBlockHandleDecode tests BlockHandle decoder with arbitrary input.
func FuzzBlockHandleDecode(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping fuzz test in short mode")
	}
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{0, 0})
	f.Add([]byte{0x80, 0x80, 0x80})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Should not panic
		_, _, _ = decodeBlockHandle(data)
	})
}

// Helper for fuzz testing
func decodeBlockHandle(data []byte) (offset, size uint64, err error) {
	if len(data) < 2 {
		return 0, 0, ErrInvalidSST
	}

	// Try to decode as varint
	offset = 0
	pos := 0
	for i := 0; i < 10 && pos < len(data); i++ {
		b := data[pos]
		offset |= uint64(b&0x7F) << (7 * i)
		pos++
		if b < 0x80 {
			break
		}
	}

	if pos >= len(data) {
		return 0, 0, ErrInvalidSST
	}

	size = 0
	for i := 0; i < 10 && pos < len(data); i++ {
		b := data[pos]
		size |= uint64(b&0x7F) << (7 * i)
		pos++
		if b < 0x80 {
			break
		}
	}

	return offset, size, nil
}
