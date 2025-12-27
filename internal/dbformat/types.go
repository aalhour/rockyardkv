// Package dbformat provides the internal key format used by RocksDB.
//
// This package implements the InternalKey encoding which consists of:
// - User key (arbitrary bytes)
// - 8-byte trailer: (sequence_number << 8) | value_type
//
// The format is bit-compatible with RocksDB's db/dbformat.h.
//
// Reference: RocksDB v10.7.5
//   - db/dbformat.h
//   - db/dbformat.cc
package dbformat

import (
	"errors"
	"fmt"

	"github.com/aalhour/rockyardkv/internal/encoding"
)

// SequenceNumber is a 56-bit sequence number (stored in upper 56 bits of 64-bit trailer).
type SequenceNumber uint64

// MaxSequenceNumber is the maximum valid sequence number (2^56 - 1).
const MaxSequenceNumber SequenceNumber = (1 << 56) - 1

// DisableGlobalSequenceNumber is a special value indicating no global sequence override.
const DisableGlobalSequenceNumber SequenceNumber = ^SequenceNumber(0)

// NumInternalBytes is the size of the internal key trailer (sequence + type).
const NumInternalBytes = 8

// ValueType represents the type of a key-value record.
// These values are embedded in the on-disk format and MUST NOT change.
type ValueType uint8

// Value types - these are embedded in the on-disk format and must match RocksDB exactly.
const (
	TypeDeletion                        ValueType = 0x00
	TypeValue                           ValueType = 0x01
	TypeMerge                           ValueType = 0x02
	TypeLogData                         ValueType = 0x03 // WAL only
	TypeColumnFamilyDeletion            ValueType = 0x04 // WAL only
	TypeColumnFamilyValue               ValueType = 0x05 // WAL only
	TypeColumnFamilyMerge               ValueType = 0x06 // WAL only
	TypeSingleDeletion                  ValueType = 0x07
	TypeColumnFamilySingleDeletion      ValueType = 0x08 // WAL only
	TypeBeginPrepareXID                 ValueType = 0x09 // WAL only
	TypeEndPrepareXID                   ValueType = 0x0A // WAL only
	TypeCommitXID                       ValueType = 0x0B // WAL only
	TypeRollbackXID                     ValueType = 0x0C // WAL only
	TypeNoop                            ValueType = 0x0D // WAL only
	TypeColumnFamilyRangeDeletion       ValueType = 0x0E // WAL only
	TypeRangeDeletion                   ValueType = 0x0F // meta block
	TypeColumnFamilyBlobIndex           ValueType = 0x10 // Blob DB only
	TypeBlobIndex                       ValueType = 0x11 // Blob DB only
	TypeBeginPersistedPrepareXID        ValueType = 0x12 // WAL only
	TypeBeginUnprepareXID               ValueType = 0x13 // WAL only
	TypeDeletionWithTimestamp           ValueType = 0x14
	TypeCommitXIDAndTimestamp           ValueType = 0x15 // WAL only
	TypeWideColumnEntity                ValueType = 0x16
	TypeColumnFamilyWideColumnEntity    ValueType = 0x17 // WAL only
	TypeValuePreferredSeqno             ValueType = 0x18
	TypeColumnFamilyValuePreferredSeqno ValueType = 0x19 // WAL only
	TypeMaxValid                        ValueType = 0x1A // Should be after the last valid type
	TypeMax                             ValueType = 0x7F // Not used for storing records
)

// ValueTypeForSeek is used when seeking to find a specific user key.
// We seek to the key with the largest possible sequence number and value type.
// Reference: RocksDB v10.7.5 db/dbformat.cc line 28:
//
//	const ValueType kValueTypeForSeek = kTypeValuePreferredSeqno;
const ValueTypeForSeek = TypeValuePreferredSeqno

// ValueTypeForSeekForPrev is similar but for reverse seeking.
const ValueTypeForSeekForPrev = TypeDeletion

var (
	// ErrCorruptedKey is returned when an internal key is malformed.
	ErrCorruptedKey = errors.New("dbformat: corrupted internal key")

	// ErrKeyTooSmall is returned when an internal key is smaller than the trailer.
	ErrKeyTooSmall = errors.New("dbformat: internal key too small")

	// ErrInvalidValueType is returned when the value type is not recognized.
	ErrInvalidValueType = errors.New("dbformat: invalid value type")
)

// IsValueType returns true if t is a valid inline value type
// (i.e., a type used in memtable skiplist and SST file datablock).
func IsValueType(t ValueType) bool {
	return t <= TypeMerge ||
		t == TypeSingleDeletion ||
		t == TypeBlobIndex ||
		t == TypeDeletionWithTimestamp ||
		t == TypeWideColumnEntity ||
		t == TypeValuePreferredSeqno
}

// IsExtendedValueType includes types from user operations plus range deletion.
func IsExtendedValueType(t ValueType) bool {
	return IsValueType(t) || t == TypeRangeDeletion || t == TypeMaxValid
}

// PackSequenceAndType packs a sequence number and value type into a 64-bit value.
// The sequence number occupies the upper 56 bits, the type occupies the lower 8 bits.
func PackSequenceAndType(seq SequenceNumber, t ValueType) uint64 {
	return (uint64(seq) << 8) | uint64(t)
}

// UnpackSequenceAndType extracts the sequence number and value type from a packed 64-bit value.
func UnpackSequenceAndType(packed uint64) (SequenceNumber, ValueType) {
	return SequenceNumber(packed >> 8), ValueType(packed & 0xFF)
}

// ParsedInternalKey represents a parsed internal key.
type ParsedInternalKey struct {
	UserKey  []byte
	Sequence SequenceNumber
	Type     ValueType
}

// String returns a human-readable representation.
func (p *ParsedInternalKey) String() string {
	return fmt.Sprintf("{UserKey: %q, Seq: %d, Type: %d}", p.UserKey, p.Sequence, p.Type)
}

// EncodedLength returns the length of the encoded internal key.
func (p *ParsedInternalKey) EncodedLength() int {
	return len(p.UserKey) + NumInternalBytes
}

// AppendInternalKey appends the serialization of key to dst.
func AppendInternalKey(dst []byte, key *ParsedInternalKey) []byte {
	dst = append(dst, key.UserKey...)
	packed := PackSequenceAndType(key.Sequence, key.Type)
	return encoding.AppendFixed64(dst, packed)
}

// ParseInternalKey parses an internal key from data.
// Returns an error if the key is corrupted.
func ParseInternalKey(data []byte) (*ParsedInternalKey, error) {
	n := len(data)
	if n < NumInternalBytes {
		return nil, ErrKeyTooSmall
	}

	packed := encoding.DecodeFixed64(data[n-NumInternalBytes:])
	seq, t := UnpackSequenceAndType(packed)

	result := &ParsedInternalKey{
		UserKey:  data[:n-NumInternalBytes],
		Sequence: seq,
		Type:     t,
	}

	if !IsExtendedValueType(t) {
		return result, ErrInvalidValueType
	}

	return result, nil
}

// ExtractUserKey returns the user key portion of an internal key.
// REQUIRES: len(internalKey) >= NumInternalBytes
func ExtractUserKey(internalKey []byte) []byte {
	if len(internalKey) < NumInternalBytes {
		return nil
	}
	return internalKey[:len(internalKey)-NumInternalBytes]
}

// ExtractValueType returns the value type from an internal key.
// REQUIRES: len(internalKey) >= NumInternalBytes
func ExtractValueType(internalKey []byte) ValueType {
	if len(internalKey) < NumInternalBytes {
		return TypeMax
	}
	n := len(internalKey)
	packed := encoding.DecodeFixed64(internalKey[n-NumInternalBytes:])
	return ValueType(packed & 0xFF)
}

// ExtractSequenceNumber returns the sequence number from an internal key.
// REQUIRES: len(internalKey) >= NumInternalBytes
func ExtractSequenceNumber(internalKey []byte) SequenceNumber {
	if len(internalKey) < NumInternalBytes {
		return 0
	}
	n := len(internalKey)
	packed := encoding.DecodeFixed64(internalKey[n-NumInternalBytes:])
	return SequenceNumber(packed >> 8)
}

// InternalKey is an encoded internal key stored as a byte slice.
type InternalKey []byte

// NewInternalKey creates a new internal key from user key, sequence, and type.
func NewInternalKey(userKey []byte, seq SequenceNumber, t ValueType) InternalKey {
	return AppendInternalKey(nil, &ParsedInternalKey{
		UserKey:  userKey,
		Sequence: seq,
		Type:     t,
	})
}

// UserKey returns the user key portion.
func (k InternalKey) UserKey() []byte {
	return ExtractUserKey(k)
}

// Sequence returns the sequence number.
func (k InternalKey) Sequence() SequenceNumber {
	return ExtractSequenceNumber(k)
}

// Type returns the value type.
func (k InternalKey) Type() ValueType {
	return ExtractValueType(k)
}

// Valid returns true if this is a valid internal key.
func (k InternalKey) Valid() bool {
	if len(k) < NumInternalBytes {
		return false
	}
	_, err := ParseInternalKey(k)
	return err == nil
}

// Parse returns the parsed internal key.
func (k InternalKey) Parse() (*ParsedInternalKey, error) {
	return ParseInternalKey(k)
}

// UpdateInternalKey updates an internal key's sequence number and type in place.
// REQUIRES: the key must be valid and have space for the trailer.
func UpdateInternalKey(key *InternalKey, seq SequenceNumber, t ValueType) {
	if len(*key) < NumInternalBytes {
		return
	}
	n := len(*key)
	packed := PackSequenceAndType(seq, t)
	encoding.EncodeFixed64((*key)[n-NumInternalBytes:], packed)
}

// DebugString returns a debug string representation of the parsed internal key.
func (p *ParsedInternalKey) DebugString() string {
	return fmt.Sprintf("'%s' @ %d : %d", p.UserKey, p.Sequence, p.Type)
}

// =============================================================================
// InternalKeyComparator
// =============================================================================

// UserKeyComparer is a function that compares two user keys.
// Returns negative if a < b, positive if a > b, zero if equal.
type UserKeyComparer func(a, b []byte) int

// BytewiseCompare is the default user key comparer (lexicographic ordering).
func BytewiseCompare(a, b []byte) int {
	minLen := min(len(a), len(b))
	for i := range minLen {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// InternalKeyComparator compares internal keys.
//
// Internal key format: user_key + 8-byte trailer (sequence << 8 | type)
//
// Comparison order (matching C++ RocksDB):
//  1. User key (ascending, using the wrapped user comparator)
//  2. Sequence number (descending - higher comes first)
//  3. Value type (descending - higher comes first)
//
// Since sequence and type are packed as (seq << 8 | type), comparing
// the packed trailer in descending order handles both.
//
// Reference: RocksDB v10.7.5 db/dbformat.h InternalKeyComparator::Compare
type InternalKeyComparator struct {
	userCompare UserKeyComparer
}

// NewInternalKeyComparator creates a new InternalKeyComparator with the given
// user key comparison function.
func NewInternalKeyComparator(userCompare UserKeyComparer) *InternalKeyComparator {
	if userCompare == nil {
		userCompare = BytewiseCompare
	}
	return &InternalKeyComparator{userCompare: userCompare}
}

// DefaultInternalKeyComparator is the default comparator using bytewise user key ordering.
var DefaultInternalKeyComparator = NewInternalKeyComparator(BytewiseCompare)

// Compare compares two internal keys.
// Returns negative if a < b, positive if a > b, zero if equal.
func (c *InternalKeyComparator) Compare(a, b []byte) int {
	// Extract user keys
	userKeyA := ExtractUserKey(a)
	userKeyB := ExtractUserKey(b)

	// Handle nil/empty cases from ExtractUserKey when key is too short
	if userKeyA == nil {
		userKeyA = a
	}
	if userKeyB == nil {
		userKeyB = b
	}

	// Compare user keys (ascending)
	cmp := c.userCompare(userKeyA, userKeyB)
	if cmp != 0 {
		return cmp
	}

	// User keys are equal, compare trailers (descending)
	// Higher trailer (higher seq/type) should come first
	if len(a) >= NumInternalBytes && len(b) >= NumInternalBytes {
		trailerA := encoding.DecodeFixed64(a[len(a)-NumInternalBytes:])
		trailerB := encoding.DecodeFixed64(b[len(b)-NumInternalBytes:])
		if trailerA > trailerB {
			return -1
		}
		if trailerA < trailerB {
			return 1
		}
	}
	return 0
}

// CompareUserKey compares just the user key portion of two internal keys.
func (c *InternalKeyComparator) CompareUserKey(a, b []byte) int {
	userKeyA := ExtractUserKey(a)
	userKeyB := ExtractUserKey(b)
	if userKeyA == nil {
		userKeyA = a
	}
	if userKeyB == nil {
		userKeyB = b
	}
	return c.userCompare(userKeyA, userKeyB)
}

// UserCompare returns the user key comparison function.
func (c *InternalKeyComparator) UserCompare() UserKeyComparer {
	return c.userCompare
}

// CompareInternalKeys is a convenience function using the default bytewise comparator.
// This is the most common case and maintains backward compatibility.
func CompareInternalKeys(a, b []byte) int {
	return DefaultInternalKeyComparator.Compare(a, b)
}
