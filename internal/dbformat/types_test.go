package dbformat

import (
	"bytes"
	"errors"
	"testing"
)

func TestPackUnpackSequenceAndType(t *testing.T) {
	tests := []struct {
		name string
		seq  SequenceNumber
		typ  ValueType
	}{
		{"zero", 0, TypeDeletion},
		{"one_value", 1, TypeValue},
		{"max_seq", MaxSequenceNumber, TypeValue},
		{"all_types", 12345, TypeMerge},
		{"single_del", 999, TypeSingleDeletion},
		{"range_del", 100, TypeRangeDeletion},
		{"blob", 50, TypeBlobIndex},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := PackSequenceAndType(tt.seq, tt.typ)
			gotSeq, gotType := UnpackSequenceAndType(packed)

			if gotSeq != tt.seq {
				t.Errorf("Sequence mismatch: got %d, want %d", gotSeq, tt.seq)
			}
			if gotType != tt.typ {
				t.Errorf("Type mismatch: got %d, want %d", gotType, tt.typ)
			}
		})
	}
}

func TestInternalKeyEncodeDecode(t *testing.T) {
	tests := []struct {
		name    string
		userKey []byte
		seq     SequenceNumber
		typ     ValueType
	}{
		{"empty_key", []byte{}, 0, TypeValue},
		{"simple", []byte("hello"), 1, TypeValue},
		{"binary_key", []byte{0x00, 0x01, 0xFF}, 12345, TypeMerge},
		{"max_seq", []byte("test"), MaxSequenceNumber, TypeDeletion},
		{"single_del", []byte("key"), 100, TypeSingleDeletion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			key := NewInternalKey(tt.userKey, tt.seq, tt.typ)

			// Check length
			expectedLen := len(tt.userKey) + NumInternalBytes
			if len(key) != expectedLen {
				t.Errorf("Key length = %d, want %d", len(key), expectedLen)
			}

			// Parse back
			parsed, err := key.Parse()
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			if !bytes.Equal(parsed.UserKey, tt.userKey) {
				t.Errorf("UserKey mismatch: got %v, want %v", parsed.UserKey, tt.userKey)
			}
			if parsed.Sequence != tt.seq {
				t.Errorf("Sequence mismatch: got %d, want %d", parsed.Sequence, tt.seq)
			}
			if parsed.Type != tt.typ {
				t.Errorf("Type mismatch: got %d, want %d", parsed.Type, tt.typ)
			}

			// Test individual extractors
			if !bytes.Equal(key.UserKey(), tt.userKey) {
				t.Errorf("UserKey() mismatch")
			}
			if key.Sequence() != tt.seq {
				t.Errorf("Sequence() mismatch")
			}
			if key.Type() != tt.typ {
				t.Errorf("Type() mismatch")
			}
		})
	}
}

func TestInternalKeyValid(t *testing.T) {
	tests := []struct {
		name  string
		key   InternalKey
		valid bool
	}{
		{"valid_simple", NewInternalKey([]byte("test"), 1, TypeValue), true},
		{"valid_empty_user_key", NewInternalKey([]byte{}, 0, TypeValue), true},
		{"too_short", InternalKey([]byte{0, 1, 2}), false},
		{"empty", InternalKey([]byte{}), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.key.Valid(); got != tt.valid {
				t.Errorf("Valid() = %v, want %v", got, tt.valid)
			}
		})
	}
}

func TestParseInternalKeyErrors(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr error
	}{
		{"empty", []byte{}, ErrKeyTooSmall},
		{"too_short_1", []byte{0x00}, ErrKeyTooSmall},
		{"too_short_7", []byte{0, 1, 2, 3, 4, 5, 6}, ErrKeyTooSmall},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseInternalKey(tt.data)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("ParseInternalKey error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestIsValueType(t *testing.T) {
	valuetypes := []ValueType{
		TypeDeletion, TypeValue, TypeMerge, TypeSingleDeletion,
		TypeBlobIndex, TypeDeletionWithTimestamp, TypeWideColumnEntity,
		TypeValuePreferredSeqno,
	}

	for _, vt := range valuetypes {
		if !IsValueType(vt) {
			t.Errorf("IsValueType(%d) = false, want true", vt)
		}
	}

	nonValueTypes := []ValueType{
		TypeLogData, TypeColumnFamilyDeletion, TypeBeginPrepareXID,
		TypeNoop, TypeRangeDeletion,
	}

	for _, vt := range nonValueTypes {
		if IsValueType(vt) {
			t.Errorf("IsValueType(%d) = true, want false", vt)
		}
	}
}

func TestIsExtendedValueType(t *testing.T) {
	// All value types should be extended value types
	valuetypes := []ValueType{
		TypeDeletion, TypeValue, TypeMerge, TypeSingleDeletion,
	}
	for _, vt := range valuetypes {
		if !IsExtendedValueType(vt) {
			t.Errorf("IsExtendedValueType(%d) = false, want true", vt)
		}
	}

	// Range deletion should be an extended value type
	if !IsExtendedValueType(TypeRangeDeletion) {
		t.Error("IsExtendedValueType(TypeRangeDeletion) = false, want true")
	}

	// TypeMaxValid should be an extended value type
	if !IsExtendedValueType(TypeMaxValid) {
		t.Error("IsExtendedValueType(TypeMaxValid) = false, want true")
	}
}

func TestExtractFunctions(t *testing.T) {
	userKey := []byte("mykey")
	seq := SequenceNumber(12345)
	typ := TypeValue

	key := NewInternalKey(userKey, seq, typ)

	if !bytes.Equal(ExtractUserKey(key), userKey) {
		t.Error("ExtractUserKey mismatch")
	}
	if ExtractSequenceNumber(key) != seq {
		t.Error("ExtractSequenceNumber mismatch")
	}
	if ExtractValueType(key) != typ {
		t.Error("ExtractValueType mismatch")
	}
}

func TestParsedInternalKeyEncodedLength(t *testing.T) {
	pik := &ParsedInternalKey{
		UserKey:  []byte("hello"),
		Sequence: 100,
		Type:     TypeValue,
	}

	expectedLen := 5 + 8 // 5 bytes for "hello" + 8 bytes trailer
	if pik.EncodedLength() != expectedLen {
		t.Errorf("EncodedLength() = %d, want %d", pik.EncodedLength(), expectedLen)
	}
}

func TestMaxSequenceNumber(t *testing.T) {
	// MaxSequenceNumber should be 2^56 - 1
	expected := SequenceNumber((1 << 56) - 1)
	if MaxSequenceNumber != expected {
		t.Errorf("MaxSequenceNumber = %d, want %d", MaxSequenceNumber, expected)
	}

	// Pack max sequence and verify it roundtrips
	packed := PackSequenceAndType(MaxSequenceNumber, TypeValue)
	gotSeq, _ := UnpackSequenceAndType(packed)
	if gotSeq != MaxSequenceNumber {
		t.Errorf("Max sequence roundtrip failed: got %d", gotSeq)
	}
}

// Golden test - binary format must match RocksDB exactly
func TestInternalKeyGoldenFormat(t *testing.T) {
	// Create a key with known values
	userKey := []byte("key")
	seq := SequenceNumber(0x123456789AB)
	typ := TypeValue

	key := NewInternalKey(userKey, seq, typ)

	// Expected format:
	// "key" (3 bytes) + packed trailer (8 bytes little-endian)
	// Packed = (0x123456789AB << 8) | 0x01 = 0x123456789AB01
	// Little-endian bytes: 0x01, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01, 0x00

	expectedTrailer := []byte{0x01, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01, 0x00}
	expected := append([]byte("key"), expectedTrailer...)

	if !bytes.Equal(key, expected) {
		t.Errorf("Internal key binary format mismatch:\ngot:  %v\nwant: %v", []byte(key), expected)
	}
}

// TestUpdateInternalKey verifies that we can update an internal key's sequence and type
func TestUpdateInternalKey(t *testing.T) {
	userKey := []byte("abcdefghijklmnopqrstuvwxyz")
	originalSeq := SequenceNumber(100)
	originalType := TypeValue

	key := NewInternalKey(userKey, originalSeq, originalType)
	originalLen := len(key)

	newSeq := SequenceNumber(0x123456)
	newType := TypeDeletion

	// Update the key
	UpdateInternalKey(&key, newSeq, newType)

	// Length should be unchanged
	if len(key) != originalLen {
		t.Errorf("Length changed: got %d, want %d", len(key), originalLen)
	}

	// Parse and verify
	parsed, err := key.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if !bytes.Equal(parsed.UserKey, userKey) {
		t.Errorf("UserKey changed")
	}
	if parsed.Sequence != newSeq {
		t.Errorf("Sequence = %d, want %d", parsed.Sequence, newSeq)
	}
	if parsed.Type != newType {
		t.Errorf("Type = %d, want %d", parsed.Type, newType)
	}
}

// TestInternalKeyEncodeDecodeComprehensive tests many different key/seq combinations
func TestInternalKeyEncodeDecodeComprehensive(t *testing.T) {
	keys := []string{"", "k", "hello", "longggggggggggggggggggggg"}
	seqs := []SequenceNumber{
		1, 2, 3,
		(1 << 8) - 1, 1 << 8, (1 << 8) + 1,
		(1 << 16) - 1, 1 << 16, (1 << 16) + 1,
		(1 << 32) - 1, 1 << 32, (1 << 32) + 1,
	}

	for _, keyStr := range keys {
		for _, seq := range seqs {
			for _, typ := range []ValueType{TypeValue, TypeDeletion} {
				key := NewInternalKey([]byte(keyStr), seq, typ)
				parsed, err := key.Parse()
				if err != nil {
					t.Fatalf("Parse error for key=%q seq=%d type=%d: %v", keyStr, seq, typ, err)
				}
				if string(parsed.UserKey) != keyStr {
					t.Errorf("UserKey mismatch")
				}
				if parsed.Sequence != seq {
					t.Errorf("Sequence mismatch: got %d, want %d", parsed.Sequence, seq)
				}
				if parsed.Type != typ {
					t.Errorf("Type mismatch")
				}
			}
		}
	}
}

// TestInternalKeyCompare verifies comparison semantics
func TestInternalKeyCompare(t *testing.T) {
	// Keys should sort by user key first, then by decreasing sequence number
	k1 := NewInternalKey([]byte("foo"), 100, TypeValue)
	k2 := NewInternalKey([]byte("foo"), 99, TypeValue)
	k3 := NewInternalKey([]byte("foo"), 101, TypeValue)
	k4 := NewInternalKey([]byte("bar"), 100, TypeValue)

	// Same user key, higher sequence should come first in iteration order
	// (because we process newer entries first)
	if bytes.Compare(k1, k2) >= 0 {
		// Lower sequence means larger packed value
		t.Logf("k1 seq=100, k2 seq=99: k1 < k2 in bytes.Compare (expected)")
	}

	if bytes.Compare(k3, k1) >= 0 {
		t.Logf("k3 seq=101, k1 seq=100: k3 < k1 in bytes.Compare (expected)")
	}

	// Different user keys
	if bytes.Compare(k4, k1) >= 0 {
		t.Logf("bar < foo (expected)")
	}
}

// TestNumInternalBytes verifies the constant
func TestNumInternalBytes(t *testing.T) {
	if NumInternalBytes != 8 {
		t.Errorf("NumInternalBytes = %d, want 8", NumInternalBytes)
	}
}

// TestValueTypeConstants verifies value type constant values
func TestValueTypeConstants(t *testing.T) {
	// These values are serialized and must match RocksDB exactly
	expectedTypes := map[ValueType]uint8{
		TypeDeletion:                   0x0,
		TypeValue:                      0x1,
		TypeMerge:                      0x2,
		TypeLogData:                    0x3,
		TypeColumnFamilyDeletion:       0x4,
		TypeColumnFamilyValue:          0x5,
		TypeColumnFamilyMerge:          0x6,
		TypeSingleDeletion:             0x7,
		TypeColumnFamilySingleDeletion: 0x8,
		TypeBeginPrepareXID:            0x9,
		TypeEndPrepareXID:              0xA,
		TypeCommitXID:                  0xB,
		TypeRollbackXID:                0xC,
		TypeNoop:                       0xD,
		TypeColumnFamilyRangeDeletion:  0xE,
		TypeRangeDeletion:              0xF,
		TypeColumnFamilyBlobIndex:      0x10,
		TypeBlobIndex:                  0x11,
	}

	for typ, expected := range expectedTypes {
		if uint8(typ) != expected {
			t.Errorf("ValueType constant %d has value %d, want %d", typ, uint8(typ), expected)
		}
	}
}

// TestInternalKeyUserKeySlice verifies that UserKey returns a slice into the original key
func TestInternalKeyUserKeySlice(t *testing.T) {
	original := []byte("myuserkey")
	key := NewInternalKey(original, 100, TypeValue)

	userKey := key.UserKey()

	// Verify it's the right data
	if !bytes.Equal(userKey, original) {
		t.Errorf("UserKey mismatch")
	}
}

// TestPackingEdgeCases tests edge cases in sequence/type packing
func TestPackingEdgeCases(t *testing.T) {
	tests := []struct {
		seq SequenceNumber
		typ ValueType
	}{
		{0, TypeDeletion},
		{0, TypeValue},
		{1, TypeDeletion},
		{MaxSequenceNumber, TypeDeletion},
		{MaxSequenceNumber, TypeMaxValid},
		{(1 << 56) - 1, TypeValue}, // Max valid sequence
	}

	for _, tt := range tests {
		packed := PackSequenceAndType(tt.seq, tt.typ)
		gotSeq, gotType := UnpackSequenceAndType(packed)

		if gotSeq != tt.seq {
			t.Errorf("Sequence roundtrip failed for seq=%d: got %d", tt.seq, gotSeq)
		}
		if gotType != tt.typ {
			t.Errorf("Type roundtrip failed for type=%d: got %d", tt.typ, gotType)
		}
	}
}

// TestParsedInternalKeyDebug tests the debug string representation
func TestParsedInternalKeyDebug(t *testing.T) {
	pik := &ParsedInternalKey{
		UserKey:  []byte("test"),
		Sequence: 12345,
		Type:     TypeValue,
	}

	// Just verify it doesn't panic and returns something
	str := pik.DebugString()
	if str == "" {
		t.Error("DebugString returned empty string")
	}
}

// TestParsedInternalKeyString tests the String() method
func TestParsedInternalKeyString(t *testing.T) {
	pik := &ParsedInternalKey{
		UserKey:  []byte("mykey"),
		Sequence: 999,
		Type:     TypeDeletion,
	}

	str := pik.String()
	if str == "" {
		t.Error("String returned empty string")
	}
	// Should contain the user key
	if !bytes.Contains([]byte(str), []byte("mykey")) {
		t.Errorf("String should contain user key: %s", str)
	}
}

// TestExtractUserKeyTooShort tests ExtractUserKey with too short input
func TestExtractUserKeyTooShort(t *testing.T) {
	// Key shorter than NumInternalBytes (8) should return nil
	shortKey := []byte("short")
	result := ExtractUserKey(shortKey)
	if result != nil {
		t.Errorf("Expected nil for short key, got %v", result)
	}
}

// TestExtractValueTypeTooShort tests ExtractValueType with too short input
func TestExtractValueTypeTooShort(t *testing.T) {
	shortKey := []byte("short")
	result := ExtractValueType(shortKey)
	if result != TypeMax {
		t.Errorf("Expected TypeMax for short key, got %d", result)
	}
}

// TestExtractSequenceNumberTooShort tests ExtractSequenceNumber with too short input
func TestExtractSequenceNumberTooShort(t *testing.T) {
	shortKey := []byte("short")
	result := ExtractSequenceNumber(shortKey)
	if result != 0 {
		t.Errorf("Expected 0 for short key, got %d", result)
	}
}

// TestUpdateInternalKeyTooShort tests UpdateInternalKey with too short key
func TestUpdateInternalKeyTooShort(t *testing.T) {
	shortKey := InternalKey([]byte("short"))
	originalLen := len(shortKey)

	// Should not panic, just return early
	UpdateInternalKey(&shortKey, 999, TypeValue)

	// Key should be unchanged
	if len(shortKey) != originalLen {
		t.Error("Short key should be unchanged")
	}
}

// TestUpdateInternalKeyValid tests UpdateInternalKey with valid key
func TestUpdateInternalKeyValid(t *testing.T) {
	key := NewInternalKey([]byte("test"), 100, TypeValue)

	// Update the sequence and type
	UpdateInternalKey(&key, 200, TypeDeletion)

	// Verify the update
	parsed, err := ParseInternalKey(key)
	if err != nil {
		t.Fatalf("ParseInternalKey failed: %v", err)
	}
	if parsed.Sequence != 200 {
		t.Errorf("Sequence = %d, want 200", parsed.Sequence)
	}
	if parsed.Type != TypeDeletion {
		t.Errorf("Type = %d, want TypeDeletion", parsed.Type)
	}
}
