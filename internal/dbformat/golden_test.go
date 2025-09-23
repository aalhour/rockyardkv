package dbformat

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestGoldenInternalKeyFormat tests the internal key format against RocksDB's format.
// Internal key format: user_key + 8-byte trailer
// Trailer format: (sequence_number << 8) | value_type
func TestGoldenInternalKeyFormat(t *testing.T) {
	testCases := []struct {
		name     string
		userKey  []byte
		seq      SequenceNumber
		typ      ValueType
		expected []byte
	}{
		{
			name:    "basic put",
			userKey: []byte("key"),
			seq:     1,
			typ:     TypeValue,
			// Trailer: (1 << 8) | 1 = 0x0000000000000101
			expected: append([]byte("key"), 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00),
		},
		{
			name:    "deletion",
			userKey: []byte("key"),
			seq:     100,
			typ:     TypeDeletion,
			// Trailer: (100 << 8) | 0 = 0x0000000000006400
			expected: append([]byte("key"), 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00),
		},
		{
			name:    "max sequence",
			userKey: []byte("k"),
			seq:     MaxSequenceNumber,
			typ:     TypeValue,
			// Trailer: (MaxSeq << 8) | 1 = 0xFFFFFFFFFFFFFF01 (little-endian)
			expected: append([]byte("k"), 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
		},
		{
			name:    "empty key",
			userKey: []byte{},
			seq:     42,
			typ:     TypeValue,
			// Trailer: (42 << 8) | 1 = 0x0000000000002A01
			expected: []byte{0x01, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Build internal key
			internalKey := make([]byte, len(tc.userKey)+8)
			copy(internalKey, tc.userKey)
			trailer := PackSequenceAndType(tc.seq, tc.typ)
			binary.LittleEndian.PutUint64(internalKey[len(tc.userKey):], trailer)

			if !bytes.Equal(internalKey, tc.expected) {
				t.Errorf("InternalKey = %x, want %x", internalKey, tc.expected)
			}

			// Parse and verify
			if len(internalKey) >= 8 {
				parsedTrailer := binary.LittleEndian.Uint64(internalKey[len(internalKey)-8:])
				parsedSeq, parsedType := UnpackSequenceAndType(parsedTrailer)
				if parsedSeq != tc.seq {
					t.Errorf("Parsed seq = %d, want %d", parsedSeq, tc.seq)
				}
				if parsedType != tc.typ {
					t.Errorf("Parsed type = %d, want %d", parsedType, tc.typ)
				}
			}
		})
	}
}

// TestGoldenValueTypes tests that value type constants match RocksDB v10.7.5.
func TestGoldenValueTypes(t *testing.T) {
	// These values must match RocksDB's db/dbformat.h
	testCases := []struct {
		name     string
		typ      ValueType
		expected uint8
	}{
		{"TypeDeletion", TypeDeletion, 0},
		{"TypeValue", TypeValue, 1},
		{"TypeMerge", TypeMerge, 2},
		{"TypeLogData", TypeLogData, 3},
		{"TypeColumnFamilyDeletion", TypeColumnFamilyDeletion, 4},
		{"TypeColumnFamilyValue", TypeColumnFamilyValue, 5},
		{"TypeColumnFamilyMerge", TypeColumnFamilyMerge, 6},
		{"TypeSingleDeletion", TypeSingleDeletion, 7},
		{"TypeColumnFamilySingleDeletion", TypeColumnFamilySingleDeletion, 8},
		{"TypeBeginPrepareXID", TypeBeginPrepareXID, 9},
		{"TypeEndPrepareXID", TypeEndPrepareXID, 10},
		{"TypeCommitXID", TypeCommitXID, 11},
		{"TypeRollbackXID", TypeRollbackXID, 12},
		{"TypeNoop", TypeNoop, 13},
		{"TypeColumnFamilyRangeDeletion", TypeColumnFamilyRangeDeletion, 14},
		{"TypeRangeDeletion", TypeRangeDeletion, 15},
		{"TypeColumnFamilyBlobIndex", TypeColumnFamilyBlobIndex, 16},
		{"TypeBlobIndex", TypeBlobIndex, 17},
		{"TypeBeginPersistedPrepareXID", TypeBeginPersistedPrepareXID, 18},
		{"TypeBeginUnprepareXID", TypeBeginUnprepareXID, 19},
		{"TypeDeletionWithTimestamp", TypeDeletionWithTimestamp, 20},
		{"TypeCommitXIDAndTimestamp", TypeCommitXIDAndTimestamp, 21},
		{"TypeWideColumnEntity", TypeWideColumnEntity, 22},
		{"TypeColumnFamilyWideColumnEntity", TypeColumnFamilyWideColumnEntity, 23},
		{"TypeValuePreferredSeqno", TypeValuePreferredSeqno, 24},
		{"TypeColumnFamilyValuePreferredSeqno", TypeColumnFamilyValuePreferredSeqno, 25},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if uint8(tc.typ) != tc.expected {
				t.Errorf("%s = %d, want %d", tc.name, tc.typ, tc.expected)
			}
		})
	}
}

// TestGoldenMaxSequenceNumber tests the max sequence number constant.
func TestGoldenMaxSequenceNumber(t *testing.T) {
	// MaxSequenceNumber should be (1 << 56) - 1
	// This is because the trailer uses 8 bits for type and 56 bits for sequence
	expectedMax := SequenceNumber((1 << 56) - 1)
	if MaxSequenceNumber != expectedMax {
		t.Errorf("MaxSequenceNumber = %d, want %d", MaxSequenceNumber, expectedMax)
	}

	// Verify it fits in the trailer format
	trailer := PackSequenceAndType(MaxSequenceNumber, TypeValue)
	seq, typ := UnpackSequenceAndType(trailer)
	if seq != MaxSequenceNumber {
		t.Errorf("PackSequenceAndType(MaxSequenceNumber) roundtrip failed: got %d", seq)
	}
	if typ != TypeValue {
		t.Errorf("PackSequenceAndType type roundtrip failed: got %d", typ)
	}
}

// TestGoldenValueTypeForSeek tests the special seek value type.
func TestGoldenValueTypeForSeek(t *testing.T) {
	// ValueTypeForSeek should be TypeBlobIndex (17) based on RocksDB's dbformat.h
	// This ensures seeks find the most recent entry for a key
	if ValueTypeForSeek != TypeBlobIndex {
		t.Errorf("ValueTypeForSeek = %d, want %d (TypeBlobIndex)",
			ValueTypeForSeek, TypeBlobIndex)
	}
}

// TestGoldenPackSequenceAndType tests the trailer packing/unpacking.
func TestGoldenPackSequenceAndType(t *testing.T) {
	testCases := []struct {
		seq             SequenceNumber
		typ             ValueType
		expectedTrailer uint64
	}{
		{0, TypeDeletion, 0x0000000000000000},
		{0, TypeValue, 0x0000000000000001},
		{1, TypeValue, 0x0000000000000101},
		{100, TypeDeletion, 0x0000000000006400},
		{100, TypeValue, 0x0000000000006401},
		{0xFFFFFFFFFFFF, TypeValue, 0x00FFFFFFFFFFFF01},
		{MaxSequenceNumber, TypeValue, 0xFFFFFFFFFFFFFF01},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			trailer := PackSequenceAndType(tc.seq, tc.typ)
			if trailer != tc.expectedTrailer {
				t.Errorf("PackSequenceAndType(%d, %d) = 0x%016x, want 0x%016x",
					tc.seq, tc.typ, trailer, tc.expectedTrailer)
			}

			// Verify round-trip
			seq, typ := UnpackSequenceAndType(trailer)
			if seq != tc.seq {
				t.Errorf("UnpackSequenceAndType seq = %d, want %d", seq, tc.seq)
			}
			if typ != tc.typ {
				t.Errorf("UnpackSequenceAndType typ = %d, want %d", typ, tc.typ)
			}
		})
	}
}
