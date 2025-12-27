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
// Reference: RocksDB v10.7.5 db/dbformat.cc line 28:
//
//	const ValueType kValueTypeForSeek = kTypeValuePreferredSeqno;
//
// The seek type must be the highest-numbered ValueType so that seeking
// for a key at kMaxSequenceNumber finds all entries for that user key.
func TestGoldenValueTypeForSeek(t *testing.T) {
	// ValueTypeForSeek must be TypeValuePreferredSeqno (24) per RocksDB v10.7.5.
	// Using a lower value (e.g., TypeBlobIndex=17) would cause keys with types
	// 18-24 to sort after the seek target, resulting in incorrect seek behavior.
	if ValueTypeForSeek != TypeValuePreferredSeqno {
		t.Errorf("ValueTypeForSeek = %d, want %d (TypeValuePreferredSeqno)",
			ValueTypeForSeek, TypeValuePreferredSeqno)
	}

	// Also verify ValueTypeForSeekForPrev matches C++.
	// Reference: db/dbformat.cc line 29: const ValueType kValueTypeForSeekForPrev = kTypeDeletion;
	if ValueTypeForSeekForPrev != TypeDeletion {
		t.Errorf("ValueTypeForSeekForPrev = %d, want %d (TypeDeletion)",
			ValueTypeForSeekForPrev, TypeDeletion)
	}
}

// TestGoldenInternalKeyEncodeDecode tests InternalKey encoding/decoding roundtrip
// for all valid ValueTypes across multiple sequence numbers.
// Reference: RocksDB v10.7.5 db/dbformat_test.cc TEST_F(FormatTest, InternalKey_EncodeDecode)
func TestGoldenInternalKeyEncodeDecode(t *testing.T) {
	// Test user keys of various sizes (matching C++ test)
	userKeys := [][]byte{
		{},
		[]byte("k"),
		[]byte("hello"),
		[]byte("longggggggggggggggggggggg"),
	}

	// Test sequence numbers at edge cases (matching C++ test)
	sequences := []SequenceNumber{
		1,
		2,
		3,
		(1 << 8) - 1,
		1 << 8,
		(1 << 8) + 1,
		(1 << 16) - 1,
		1 << 16,
		(1 << 16) + 1,
		(1 << 32) - 1,
		1 << 32,
		(1 << 32) + 1,
		MaxSequenceNumber,
	}

	// All valid inline ValueTypes that can appear in SST data blocks
	// Reference: IsValueType() in dbformat.h
	inlineTypes := []ValueType{
		TypeDeletion,
		TypeValue,
		TypeMerge,
		TypeSingleDeletion,
		TypeBlobIndex,
		TypeDeletionWithTimestamp,
		TypeWideColumnEntity,
		TypeValuePreferredSeqno,
	}

	for _, userKey := range userKeys {
		for _, seq := range sequences {
			for _, vt := range inlineTypes {
				// Build internal key
				pik := &ParsedInternalKey{
					UserKey:  userKey,
					Sequence: seq,
					Type:     vt,
				}
				encoded := AppendInternalKey(nil, pik)

				// Verify length is correct
				expectedLen := len(userKey) + NumInternalBytes
				if len(encoded) != expectedLen {
					t.Errorf("AppendInternalKey len=%d, want %d (userKey=%q, seq=%d, type=%d)",
						len(encoded), expectedLen, userKey, seq, vt)
					continue
				}

				// Parse and verify roundtrip
				decoded, err := ParseInternalKey(encoded)
				if err != nil {
					t.Errorf("ParseInternalKey failed for userKey=%q seq=%d type=%d: %v",
						userKey, seq, vt, err)
					continue
				}

				if !bytes.Equal(decoded.UserKey, userKey) {
					t.Errorf("roundtrip userKey = %q, want %q", decoded.UserKey, userKey)
				}
				if decoded.Sequence != seq {
					t.Errorf("roundtrip seq = %d, want %d", decoded.Sequence, seq)
				}
				if decoded.Type != vt {
					t.Errorf("roundtrip type = %d, want %d", decoded.Type, vt)
				}
			}
		}
	}
}

// TestGoldenExtractFunctions tests the Extract* functions.
func TestGoldenExtractFunctions(t *testing.T) {
	testCases := []struct {
		userKey []byte
		seq     SequenceNumber
		typ     ValueType
	}{
		{[]byte("foo"), 100, TypeValue},
		{[]byte("bar"), MaxSequenceNumber, TypeDeletion},
		{[]byte(""), 1, TypeMerge},
		{[]byte("longkey12345"), 42, TypeSingleDeletion},
	}

	for _, tc := range testCases {
		ik := NewInternalKey(tc.userKey, tc.seq, tc.typ)

		gotUserKey := ExtractUserKey(ik)
		if !bytes.Equal(gotUserKey, tc.userKey) {
			t.Errorf("ExtractUserKey(%q) = %q, want %q", ik, gotUserKey, tc.userKey)
		}

		gotSeq := ExtractSequenceNumber(ik)
		if gotSeq != tc.seq {
			t.Errorf("ExtractSequenceNumber = %d, want %d", gotSeq, tc.seq)
		}

		gotType := ExtractValueType(ik)
		if gotType != tc.typ {
			t.Errorf("ExtractValueType = %d, want %d", gotType, tc.typ)
		}
	}
}

// TestGoldenInternalKeyComparator tests the InternalKeyComparator behavior.
// Reference: RocksDB v10.7.5 db/dbformat.h InternalKeyComparator::Compare
func TestGoldenInternalKeyComparator(t *testing.T) {
	cmp := DefaultInternalKeyComparator

	testCases := []struct {
		name     string
		a, b     InternalKey
		expected int // -1, 0, 1
	}{
		{
			name:     "same key, higher seq first",
			a:        NewInternalKey([]byte("foo"), 100, TypeValue),
			b:        NewInternalKey([]byte("foo"), 99, TypeValue),
			expected: -1, // seq 100 > seq 99 → a comes first (descending)
		},
		{
			name:     "same key and seq, higher type first",
			a:        NewInternalKey([]byte("foo"), 100, TypeValue),
			b:        NewInternalKey([]byte("foo"), 100, TypeDeletion),
			expected: -1, // type 1 > type 0 → a comes first (descending)
		},
		{
			name:     "different user keys, ascending order",
			a:        NewInternalKey([]byte("bar"), 100, TypeValue),
			b:        NewInternalKey([]byte("foo"), 100, TypeValue),
			expected: -1, // "bar" < "foo" (ascending)
		},
		{
			name:     "equal keys",
			a:        NewInternalKey([]byte("foo"), 100, TypeValue),
			b:        NewInternalKey([]byte("foo"), 100, TypeValue),
			expected: 0,
		},
		{
			name:     "same key, lower seq second",
			a:        NewInternalKey([]byte("foo"), 99, TypeValue),
			b:        NewInternalKey([]byte("foo"), 100, TypeValue),
			expected: 1, // seq 99 < seq 100 → a comes after
		},
		{
			name:     "prefix user key",
			a:        NewInternalKey([]byte("foo"), 100, TypeValue),
			b:        NewInternalKey([]byte("foobar"), 100, TypeValue),
			expected: -1, // "foo" < "foobar"
		},
		{
			name:     "empty vs non-empty user key",
			a:        NewInternalKey([]byte(""), 100, TypeValue),
			b:        NewInternalKey([]byte("a"), 100, TypeValue),
			expected: -1, // "" < "a"
		},
		{
			name:     "max sequence",
			a:        NewInternalKey([]byte("foo"), MaxSequenceNumber, TypeValue),
			b:        NewInternalKey([]byte("foo"), 1, TypeValue),
			expected: -1, // max seq comes first
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := cmp.Compare(tc.a, tc.b)
			if got != tc.expected {
				t.Errorf("Compare(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.expected)
			}

			// Also test the convenience function
			got2 := CompareInternalKeys(tc.a, tc.b)
			if got2 != tc.expected {
				t.Errorf("CompareInternalKeys(%v, %v) = %d, want %d", tc.a, tc.b, got2, tc.expected)
			}
		})
	}
}

// TestGoldenInternalKeyComparatorReverse tests a reverse-order comparator.
// This verifies that custom comparators work correctly.
func TestGoldenInternalKeyComparatorReverse(t *testing.T) {
	// Create a reverse bytewise comparator for user keys
	reverseCompare := func(a, b []byte) int {
		return -BytewiseCompare(a, b) // Flip the result
	}
	cmp := NewInternalKeyComparator(reverseCompare)

	testCases := []struct {
		name     string
		a, b     InternalKey
		expected int
	}{
		{
			name: "reverse user key order",
			// With reverse comparator, "foo" > "bar"
			a:        NewInternalKey([]byte("bar"), 100, TypeValue),
			b:        NewInternalKey([]byte("foo"), 100, TypeValue),
			expected: 1, // "bar" > "foo" in reverse order
		},
		{
			name:     "same user key, seq still descending",
			a:        NewInternalKey([]byte("foo"), 100, TypeValue),
			b:        NewInternalKey([]byte("foo"), 99, TypeValue),
			expected: -1, // seq comparison is still descending
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := cmp.Compare(tc.a, tc.b)
			if got != tc.expected {
				t.Errorf("ReverseComparator.Compare(%v, %v) = %d, want %d",
					tc.a, tc.b, got, tc.expected)
			}
		})
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
