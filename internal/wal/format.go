// Package wal provides Write-Ahead Log (WAL) reader and writer implementations
// that are bit-compatible with RocksDB's log format.
//
// File Format:
// A log file is divided into fixed-size blocks (32KB). Records are written
// sequentially and may span multiple blocks. Each physical record has a header
// containing a checksum, length, and type.
//
// Record Format (Legacy):
//
//	+----------+---------+------+---------+
//	| CRC (4B) | Len(2B) | Type | Payload |
//	+----------+---------+------+---------+
//
// Record Format (Recyclable):
//
// Reference: RocksDB v10.7.5
//
//   - db/log_format.h (RecordType enum, constants)
//
//     +----------+---------+------+----------+---------+
//     | CRC (4B) | Len(2B) | Type | LogNo(4B)| Payload |
//     +----------+---------+------+----------+---------+
//
// CRC is computed over Type + [LogNo] + Payload and masked using crc32c.Mask().
package wal

import (
	"fmt"

	"github.com/aalhour/rockyardkv/internal/encoding"
)

// BlockSize is the size of each block in the log file.
// Records are written within these blocks, with padding at the end if needed.
const BlockSize = 32768

// HeaderSize is the size of the legacy record header.
// Header: checksum (4) + length (2) + type (1) = 7 bytes
const HeaderSize = 7

// RecyclableHeaderSize is the size of the recyclable record header.
// Header: checksum (4) + length (2) + type (1) + log_number (4) = 11 bytes
const RecyclableHeaderSize = 11

// MaxRecordPayload is the maximum payload size for a single physical record
// in a legacy (non-recyclable) log.
const MaxRecordPayload = BlockSize - HeaderSize

// MaxRecyclableRecordPayload is the maximum payload for recyclable logs.
const MaxRecyclableRecordPayload = BlockSize - RecyclableHeaderSize

// RecordType represents the type of a log record.
// These values are embedded in the on-disk format and MUST NOT change.
type RecordType uint8

const (
	// ZeroType is reserved for preallocated files (all zeros).
	ZeroType RecordType = 0

	// FullType indicates a complete record that fits within a single fragment.
	FullType RecordType = 1

	// FirstType indicates the first fragment of a record that spans multiple blocks.
	FirstType RecordType = 2

	// MiddleType indicates a middle fragment of a record.
	MiddleType RecordType = 3

	// LastType indicates the final fragment of a record.
	LastType RecordType = 4

	// RecyclableFullType is like FullType but for recycled log files.
	RecyclableFullType RecordType = 5

	// RecyclableFirstType is like FirstType but for recycled log files.
	RecyclableFirstType RecordType = 6

	// RecyclableMiddleType is like MiddleType but for recycled log files.
	RecyclableMiddleType RecordType = 7

	// RecyclableLastType is like LastType but for recycled log files.
	RecyclableLastType RecordType = 8

	// SetCompressionType indicates a compression type record.
	SetCompressionType RecordType = 9

	// UserDefinedTimestampSizeType indicates a user-defined timestamp size record.
	UserDefinedTimestampSizeType RecordType = 10

	// RecyclableUserDefinedTimestampSizeType is the recyclable variant.
	RecyclableUserDefinedTimestampSizeType RecordType = 11

	// PredecessorWALInfoType indicates predecessor WAL information for verification.
	PredecessorWALInfoType RecordType = 130

	// RecyclePredecessorWALInfoType is the recyclable variant.
	RecyclePredecessorWALInfoType RecordType = 131

	// MaxRecordType is the maximum valid record type value.
	MaxRecordType = RecyclePredecessorWALInfoType
)

// RecordTypeSafeIgnoreMask indicates unknown record types with bit 7 set can be ignored.
const RecordTypeSafeIgnoreMask = 1 << 7

// IsRecyclableType returns true if the record type is a recyclable variant.
func IsRecyclableType(t RecordType) bool {
	return t >= RecyclableFullType && t <= RecyclableLastType ||
		t == RecyclableUserDefinedTimestampSizeType ||
		t == RecyclePredecessorWALInfoType
}

// IsFragmentType returns true if the record type is a fragment type (Full, First, Middle, Last).
func IsFragmentType(t RecordType) bool {
	return (t >= FullType && t <= LastType) ||
		(t >= RecyclableFullType && t <= RecyclableLastType)
}

// ToRecyclable converts a legacy record type to its recyclable equivalent.
// Returns the same type if already recyclable or not a fragment type.
func ToRecyclable(t RecordType) RecordType {
	switch t {
	case FullType:
		return RecyclableFullType
	case FirstType:
		return RecyclableFirstType
	case MiddleType:
		return RecyclableMiddleType
	case LastType:
		return RecyclableLastType
	default:
		return t
	}
}

// ToLegacy converts a recyclable record type to its legacy equivalent.
// Returns the same type if already legacy or not a convertible type.
func ToLegacy(t RecordType) RecordType {
	switch t {
	case RecyclableFullType:
		return FullType
	case RecyclableFirstType:
		return FirstType
	case RecyclableMiddleType:
		return MiddleType
	case RecyclableLastType:
		return LastType
	case RecyclePredecessorWALInfoType:
		return PredecessorWALInfoType
	default:
		return t
	}
}

// IsPredecessorWALInfoType returns true if the record type is a predecessor WAL info type.
func IsPredecessorWALInfoType(t RecordType) bool {
	return t == PredecessorWALInfoType || t == RecyclePredecessorWALInfoType
}

// =============================================================================
// PredecessorWALInfo
// =============================================================================

// PredecessorWALInfo contains information about the predecessor WAL file.
// This is used to verify WAL sequence continuity during recovery.
//
// The record format is:
//   - log_number: fixed64 (predecessor WAL file number)
//   - size_bytes: fixed64 (size of predecessor WAL file)
//   - last_seqno_recorded: fixed64 (last sequence number in predecessor WAL)
//
// Reference: RocksDB v10.7.5 db/dbformat.h class PredecessorWALInfo
type PredecessorWALInfo struct {
	LogNumber         uint64 // Predecessor WAL file number
	SizeBytes         uint64 // Size of predecessor WAL in bytes
	LastSeqnoRecorded uint64 // Last sequence number recorded in predecessor WAL
}

// PredecessorWALInfoSize is the size of an encoded PredecessorWALInfo record.
const PredecessorWALInfoSize = 24 // 3 * 8 bytes

// DecodePredecessorWALInfo decodes a PredecessorWALInfo from a byte slice.
// Returns an error if the slice is too short.
func DecodePredecessorWALInfo(data []byte) (*PredecessorWALInfo, error) {
	if len(data) < PredecessorWALInfoSize {
		return nil, fmt.Errorf("PredecessorWALInfo too short: got %d, want %d", len(data), PredecessorWALInfoSize)
	}

	info := &PredecessorWALInfo{
		LogNumber:         encoding.DecodeFixed64(data[0:8]),
		SizeBytes:         encoding.DecodeFixed64(data[8:16]),
		LastSeqnoRecorded: encoding.DecodeFixed64(data[16:24]),
	}
	return info, nil
}

// EncodeTo encodes the PredecessorWALInfo to a byte slice.
func (p *PredecessorWALInfo) EncodeTo(dst []byte) []byte {
	dst = encoding.AppendFixed64(dst, p.LogNumber)
	dst = encoding.AppendFixed64(dst, p.SizeBytes)
	dst = encoding.AppendFixed64(dst, p.LastSeqnoRecorded)
	return dst
}

// String returns a string representation of PredecessorWALInfo.
func (p *PredecessorWALInfo) String() string {
	return fmt.Sprintf("PredecessorWALInfo{LogNumber: %d, SizeBytes: %d, LastSeqno: %d}",
		p.LogNumber, p.SizeBytes, p.LastSeqnoRecorded)
}

// String returns the string representation of a RecordType.
func (t RecordType) String() string {
	switch t {
	case ZeroType:
		return "ZeroType"
	case FullType:
		return "FullType"
	case FirstType:
		return "FirstType"
	case MiddleType:
		return "MiddleType"
	case LastType:
		return "LastType"
	case RecyclableFullType:
		return "RecyclableFullType"
	case RecyclableFirstType:
		return "RecyclableFirstType"
	case RecyclableMiddleType:
		return "RecyclableMiddleType"
	case RecyclableLastType:
		return "RecyclableLastType"
	case SetCompressionType:
		return "SetCompressionType"
	case UserDefinedTimestampSizeType:
		return "UserDefinedTimestampSizeType"
	case RecyclableUserDefinedTimestampSizeType:
		return "RecyclableUserDefinedTimestampSizeType"
	case PredecessorWALInfoType:
		return "PredecessorWALInfoType"
	case RecyclePredecessorWALInfoType:
		return "RecyclePredecessorWALInfoType"
	default:
		return "UnknownType"
	}
}
