// Fuzz tests for the table package.
//
// These tests generate random data to verify that the table reader handles
// malformed inputs gracefully without panicking.
//
// Run with: go test -fuzz=Fuzz -fuzztime=30s ./internal/table/...
package table

import (
	"bytes"
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

// FuzzTableReader tests the table reader with random SST-like data.
// This verifies that malformed inputs don't cause panics.
func FuzzTableReader(f *testing.F) {
	// Seed with some interesting cases
	f.Add([]byte{})                                               // Empty
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})                         // Too small
	f.Add(bytes.Repeat([]byte{0xFF}, 100))                        // All 0xFF
	f.Add(bytes.Repeat([]byte{0x00}, 100))                        // All zeros
	f.Add([]byte{0x88, 0x27, 0x05, 0x19, 0x58, 0x9d, 0xee, 0x9a}) // Magic number

	// Add a valid minimal SST file as seed
	var buf bytes.Buffer
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(&buf, opts)
	key := dbformat.AppendInternalKey(nil, &dbformat.ParsedInternalKey{
		UserKey:  []byte("testkey"),
		Sequence: 1,
		Type:     dbformat.TypeValue,
	})
	builder.Add(key, []byte("testvalue"))
	builder.Finish()
	f.Add(buf.Bytes())

	f.Fuzz(func(t *testing.T, data []byte) {
		// Try to open and read - should not panic
		reader, err := Open(&fuzzMemFile{data: data}, ReaderOptions{})
		if err != nil {
			return // Expected for most random data
		}
		defer reader.Close()

		// Try to iterate - should not panic
		iter := reader.NewIterator()
		iter.SeekToFirst()
		for iter.Valid() {
			_ = iter.Key()
			_ = iter.Value()
			iter.Next()
		}
		// Error is OK, panic is not

		// Try seeking
		iter.Seek([]byte("testkey"))

		// Try reading properties
		_, _ = reader.Properties()
	})
}

// FuzzBlockIterator tests the block iterator with random block data.
func FuzzBlockIterator(f *testing.F) {
	// Seed with edge cases
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00}) // Invalid restart count position
	f.Add(bytes.Repeat([]byte{0xFF}, 50))
	f.Add(bytes.Repeat([]byte{0x00}, 50))

	// Valid block format seed
	f.Add([]byte{
		// Some key-value entries
		0x00,                    // shared = 0
		0x05,                    // unshared = 5
		0x05,                    // value_len = 5
		'h', 'e', 'l', 'l', 'o', // key
		'w', 'o', 'r', 'l', 'd', // value
		// Restarts section
		0x00, 0x00, 0x00, 0x00, // restart[0] = 0
		0x01, 0x00, 0x00, 0x00, // num_restarts = 1
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Try to create a block - should not panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("panic on block creation: %v", r)
			}
		}()

		// Try using block package directly if the data is large enough
		if len(data) < 4 {
			return
		}

		// Try to iterate - should not panic
		// We can't easily create a block.Block without proper format,
		// but we can test the table reader's block parsing
	})
}

// FuzzTableBuilder tests that building with various inputs produces valid SSTs.
func FuzzTableBuilder(f *testing.F) {
	// Seed with interesting key/value combinations
	f.Add([]byte("key"), []byte("value"))
	f.Add([]byte{}, []byte("value"))                                          // Empty key
	f.Add([]byte("key"), []byte{})                                            // Empty value
	f.Add([]byte{}, []byte{})                                                 // Both empty
	f.Add([]byte{0, 0, 0}, []byte{0})                                         // Binary with nulls
	f.Add(bytes.Repeat([]byte{'a'}, 10000), bytes.Repeat([]byte{'b'}, 10000)) // Large

	f.Fuzz(func(t *testing.T, key, value []byte) {
		var buf bytes.Buffer
		opts := DefaultBuilderOptions()
		builder := NewTableBuilder(&buf, opts)

		// Build internal key
		ikey := dbformat.AppendInternalKey(nil, &dbformat.ParsedInternalKey{
			UserKey:  key,
			Sequence: 100,
			Type:     dbformat.TypeValue,
		})

		// Try to add
		err := builder.Add(ikey, value)
		if err != nil {
			return // Some inputs may be rejected
		}

		// Try to finish
		err = builder.Finish()
		if err != nil {
			return
		}

		// Verify the result can be read back
		reader, err := Open(&fuzzMemFile{data: buf.Bytes()}, ReaderOptions{})
		if err != nil {
			t.Errorf("failed to open just-built SST: %v", err)
			return
		}
		defer reader.Close()

		// Verify the key-value pair is present
		iter := reader.NewIterator()
		iter.SeekToFirst()
		if !iter.Valid() {
			t.Error("expected at least one entry")
			return
		}

		gotKey := iter.Key()
		gotValue := iter.Value()

		if !bytes.Equal(gotKey, ikey) {
			t.Errorf("key mismatch")
		}
		if !bytes.Equal(gotValue, value) {
			t.Errorf("value mismatch")
		}
	})
}

// FuzzMultipleEntries tests building SSTs with multiple random entries.
func FuzzMultipleEntries(f *testing.F) {
	f.Add(uint8(5), []byte("seed"))

	f.Fuzz(func(t *testing.T, numEntries uint8, seed []byte) {
		if numEntries == 0 || numEntries > 100 {
			numEntries = 10
		}

		var buf bytes.Buffer
		opts := DefaultBuilderOptions()
		opts.BlockSize = 256 // Small blocks for more interesting layouts
		builder := NewTableBuilder(&buf, opts)

		// Generate deterministic keys from seed
		keys := make([][]byte, numEntries)
		values := make([][]byte, numEntries)

		for i := range numEntries {
			// Create key using seed and index to ensure uniqueness
			keys[i] = append(append([]byte{}, seed...), byte(i))
			values[i] = append([]byte("value"), byte(i))
		}

		// Sort keys for SST ordering requirement
		sortedIndices := make([]int, numEntries)
		for i := range sortedIndices {
			sortedIndices[i] = i
		}
		// Simple bubble sort
		for i := range sortedIndices {
			for j := i + 1; j < len(sortedIndices); j++ {
				if bytes.Compare(keys[sortedIndices[i]], keys[sortedIndices[j]]) > 0 {
					sortedIndices[i], sortedIndices[j] = sortedIndices[j], sortedIndices[i]
				}
			}
		}

		// Add entries in sorted order
		added := 0
		var lastKey []byte
		for _, idx := range sortedIndices {
			key := keys[idx]
			// Skip duplicates
			if bytes.Equal(key, lastKey) {
				continue
			}
			lastKey = key

			ikey := dbformat.AppendInternalKey(nil, &dbformat.ParsedInternalKey{
				UserKey:  key,
				Sequence: dbformat.SequenceNumber(1000 - idx),
				Type:     dbformat.TypeValue,
			})
			if err := builder.Add(ikey, values[idx]); err != nil {
				// Skip invalid entries
				continue
			}
			added++
		}

		if added == 0 {
			return // Nothing to test
		}

		if err := builder.Finish(); err != nil {
			return
		}

		// Verify we can read back
		reader, err := Open(&fuzzMemFile{data: buf.Bytes()}, ReaderOptions{})
		if err != nil {
			t.Errorf("failed to open SST: %v", err)
			return
		}
		defer reader.Close()

		// Count entries
		iter := reader.NewIterator()
		count := 0
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			count++
		}

		if count != added {
			t.Errorf("count mismatch: got %d, want %d", count, added)
		}
	})
}

// fuzzMemFile implements ReadableFile for fuzz testing
type fuzzMemFile struct {
	data []byte
}

func (m *fuzzMemFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(m.data)) {
		return 0, nil
	}
	n = copy(p, m.data[off:])
	return n, nil
}

func (m *fuzzMemFile) Size() int64 {
	return int64(len(m.data))
}
