// Package encoding provides binary encoding/decoding primitives that are
// bit-compatible with RocksDB's util/coding.h implementation.
//
// All multi-byte integers are encoded in little-endian format.
// Variable-length integers (varints) use 7-bit encoding with MSB continuation.
//
// Reference: RocksDB v10.7.5
//   - util/coding.h
//   - util/coding.cc
package encoding

import (
	"encoding/binary"
	"errors"
)

// MaxVarint32Length is the maximum number of bytes a varint32 can occupy.
const MaxVarint32Length = 5

// MaxVarint64Length is the maximum number of bytes a varint64 can occupy.
const MaxVarint64Length = 10

// MaxVarintLen64 is an alias for MaxVarint64Length for compatibility.
const MaxVarintLen64 = MaxVarint64Length

var (
	// ErrBufferTooSmall is returned when the buffer doesn't have enough space.
	ErrBufferTooSmall = errors.New("encoding: buffer too small")

	// ErrVarintOverflow is returned when a varint exceeds the maximum value.
	ErrVarintOverflow = errors.New("encoding: varint overflow")

	// ErrVarintTermination is returned when varint doesn't terminate properly.
	ErrVarintTermination = errors.New("encoding: varint not terminated")
)

// -----------------------------------------------------------------------------
// Fixed-width encoding (little-endian)
// -----------------------------------------------------------------------------

// EncodeFixed16 encodes a uint16 into a 2-byte little-endian buffer.
// REQUIRES: dst has at least 2 bytes.
func EncodeFixed16(dst []byte, value uint16) {
	binary.LittleEndian.PutUint16(dst, value)
}

// DecodeFixed16 decodes a uint16 from a 2-byte little-endian buffer.
// REQUIRES: src has at least 2 bytes.
func DecodeFixed16(src []byte) uint16 {
	return binary.LittleEndian.Uint16(src)
}

// EncodeFixed32 encodes a uint32 into a 4-byte little-endian buffer.
// REQUIRES: dst has at least 4 bytes.
func EncodeFixed32(dst []byte, value uint32) {
	binary.LittleEndian.PutUint32(dst, value)
}

// DecodeFixed32 decodes a uint32 from a 4-byte little-endian buffer.
// REQUIRES: src has at least 4 bytes.
func DecodeFixed32(src []byte) uint32 {
	return binary.LittleEndian.Uint32(src)
}

// EncodeFixed64 encodes a uint64 into an 8-byte little-endian buffer.
// REQUIRES: dst has at least 8 bytes.
func EncodeFixed64(dst []byte, value uint64) {
	binary.LittleEndian.PutUint64(dst, value)
}

// DecodeFixed64 decodes a uint64 from an 8-byte little-endian buffer.
// REQUIRES: src has at least 8 bytes.
func DecodeFixed64(src []byte) uint64 {
	return binary.LittleEndian.Uint64(src)
}

// -----------------------------------------------------------------------------
// Appending variants (for building strings/slices)
// -----------------------------------------------------------------------------

// AppendFixed16 appends a little-endian uint16 to dst and returns the extended slice.
func AppendFixed16(dst []byte, value uint16) []byte {
	return binary.LittleEndian.AppendUint16(dst, value)
}

// AppendFixed32 appends a little-endian uint32 to dst and returns the extended slice.
func AppendFixed32(dst []byte, value uint32) []byte {
	return binary.LittleEndian.AppendUint32(dst, value)
}

// AppendFixed64 appends a little-endian uint64 to dst and returns the extended slice.
func AppendFixed64(dst []byte, value uint64) []byte {
	return binary.LittleEndian.AppendUint64(dst, value)
}

// -----------------------------------------------------------------------------
// Variable-length encoding (7-bit with MSB continuation)
// -----------------------------------------------------------------------------

// EncodeVarint32 encodes a uint32 as a varint into dst.
// Returns the number of bytes written.
// REQUIRES: dst has at least MaxVarint32Length bytes.
func EncodeVarint32(dst []byte, value uint32) int {
	const B = 128
	i := 0
	for value >= B {
		dst[i] = byte(value&(B-1)) | B
		value >>= 7
		i++
	}
	dst[i] = byte(value)
	return i + 1
}

// AppendVarint32 appends a uint32 as a varint to dst and returns the extended slice.
func AppendVarint32(dst []byte, value uint32) []byte {
	var buf [MaxVarint32Length]byte
	n := EncodeVarint32(buf[:], value)
	return append(dst, buf[:n]...)
}

// DecodeVarint32 decodes a varint32 from src.
// Returns the decoded value and the number of bytes consumed.
// Returns (0, 0, error) on error.
func DecodeVarint32(src []byte) (value uint32, bytesRead int, err error) {
	var result uint32
	for shift := uint(0); shift < 32; shift += 7 {
		if bytesRead >= len(src) {
			return 0, 0, ErrVarintTermination
		}
		b := src[bytesRead]
		bytesRead++
		if b < 128 {
			// Last byte
			result |= uint32(b) << shift
			return result, bytesRead, nil
		}
		result |= uint32(b&0x7f) << shift
	}
	return 0, 0, ErrVarintOverflow
}

// EncodeVarint64 encodes a uint64 as a varint into dst.
// Returns the number of bytes written.
// REQUIRES: dst has at least MaxVarint64Length bytes.
func EncodeVarint64(dst []byte, value uint64) int {
	const B = 128
	i := 0
	for value >= B {
		dst[i] = byte(value&(B-1)) | B
		value >>= 7
		i++
	}
	dst[i] = byte(value)
	return i + 1
}

// AppendVarint64 appends a uint64 as a varint to dst and returns the extended slice.
func AppendVarint64(dst []byte, value uint64) []byte {
	var buf [MaxVarint64Length]byte
	n := EncodeVarint64(buf[:], value)
	return append(dst, buf[:n]...)
}

// PutVarint64 encodes a uint64 as a varint into dst and returns the number of bytes written.
// This is equivalent to EncodeVarint64.
// REQUIRES: dst has at least MaxVarint64Length bytes.
func PutVarint64(dst []byte, value uint64) int {
	return EncodeVarint64(dst, value)
}

// DecodeVarint64 decodes a varint64 from src.
// Returns the decoded value and the number of bytes consumed.
// Returns (0, 0, error) on error.
func DecodeVarint64(src []byte) (value uint64, bytesRead int, err error) {
	var result uint64
	for shift := uint(0); shift < 64; shift += 7 {
		if bytesRead >= len(src) {
			return 0, 0, ErrVarintTermination
		}
		b := src[bytesRead]
		bytesRead++
		if b < 128 {
			// Last byte
			result |= uint64(b) << shift
			return result, bytesRead, nil
		}
		result |= uint64(b&0x7f) << shift
	}
	return 0, 0, ErrVarintOverflow
}

// VarintLength returns the number of bytes needed to encode v as a varint.
func VarintLength(v uint64) int {
	length := 1
	for v >= 128 {
		v >>= 7
		length++
	}
	return length
}

// -----------------------------------------------------------------------------
// Signed varint (zigzag encoding)
// -----------------------------------------------------------------------------

// I64ToZigzag converts a signed int64 to an unsigned uint64 using zigzag encoding.
// This allows negative numbers to be encoded efficiently as varints.
func I64ToZigzag(v int64) uint64 {
	return (uint64(v) << 1) ^ uint64(v>>63)
}

// ZigzagToI64 converts a zigzag-encoded uint64 back to a signed int64.
func ZigzagToI64(n uint64) int64 {
	return int64(n>>1) ^ -int64(n&1)
}

// AppendVarsignedint64 appends a signed int64 using zigzag + varint encoding.
func AppendVarsignedint64(dst []byte, v int64) []byte {
	return AppendVarint64(dst, I64ToZigzag(v))
}

// DecodeVarsignedint64 decodes a zigzag-encoded varint64 as a signed int64.
func DecodeVarsignedint64(src []byte) (value int64, bytesRead int, err error) {
	u, n, err := DecodeVarint64(src)
	if err != nil {
		return 0, 0, err
	}
	return ZigzagToI64(u), n, nil
}

// -----------------------------------------------------------------------------
// Length-prefixed slices
// -----------------------------------------------------------------------------

// AppendLengthPrefixedSlice appends a length-prefixed slice to dst.
// Format: [varint32 length][bytes]
func AppendLengthPrefixedSlice(dst []byte, value []byte) []byte {
	dst = AppendVarint32(dst, uint32(len(value)))
	return append(dst, value...)
}

// DecodeLengthPrefixedSlice decodes a length-prefixed slice from src.
// Returns the slice (pointing into src), bytes consumed, and any error.
func DecodeLengthPrefixedSlice(src []byte) (value []byte, bytesRead int, err error) {
	length, n, err := DecodeVarint32(src)
	if err != nil {
		return nil, 0, err
	}
	bytesRead = n
	if bytesRead+int(length) > len(src) {
		return nil, 0, ErrBufferTooSmall
	}
	value = src[bytesRead : bytesRead+int(length)]
	bytesRead += int(length)
	return value, bytesRead, nil
}

// -----------------------------------------------------------------------------
// Slice-based decoding (similar to RocksDB Slice-based Get* functions)
// -----------------------------------------------------------------------------

// Slice is a helper for reading from a byte slice, similar to RocksDB's Slice.
// It tracks the current position and allows sequential reads.
type Slice struct {
	data []byte
	pos  int
}

// NewSlice creates a new Slice from a byte slice.
func NewSlice(data []byte) *Slice {
	return &Slice{data: data, pos: 0}
}

// Remaining returns the number of bytes remaining.
func (s *Slice) Remaining() int {
	return len(s.data) - s.pos
}

// Data returns the remaining data.
func (s *Slice) Data() []byte {
	return s.data[s.pos:]
}

// Advance advances the position by n bytes.
func (s *Slice) Advance(n int) {
	s.pos += n
}

// GetFixed16 reads a fixed 16-bit value.
func (s *Slice) GetFixed16() (uint16, bool) {
	if s.Remaining() < 2 {
		return 0, false
	}
	v := DecodeFixed16(s.data[s.pos:])
	s.pos += 2
	return v, true
}

// GetFixed32 reads a fixed 32-bit value.
func (s *Slice) GetFixed32() (uint32, bool) {
	if s.Remaining() < 4 {
		return 0, false
	}
	v := DecodeFixed32(s.data[s.pos:])
	s.pos += 4
	return v, true
}

// GetFixed64 reads a fixed 64-bit value.
func (s *Slice) GetFixed64() (uint64, bool) {
	if s.Remaining() < 8 {
		return 0, false
	}
	v := DecodeFixed64(s.data[s.pos:])
	s.pos += 8
	return v, true
}

// GetVarint32 reads a varint32.
func (s *Slice) GetVarint32() (uint32, bool) {
	v, n, err := DecodeVarint32(s.data[s.pos:])
	if err != nil {
		return 0, false
	}
	s.pos += n
	return v, true
}

// GetVarint64 reads a varint64.
func (s *Slice) GetVarint64() (uint64, bool) {
	v, n, err := DecodeVarint64(s.data[s.pos:])
	if err != nil {
		return 0, false
	}
	s.pos += n
	return v, true
}

// GetVarsignedint64 reads a zigzag-encoded signed int64.
func (s *Slice) GetVarsignedint64() (int64, bool) {
	v, n, err := DecodeVarsignedint64(s.data[s.pos:])
	if err != nil {
		return 0, false
	}
	s.pos += n
	return v, true
}

// GetLengthPrefixedSlice reads a length-prefixed slice.
func (s *Slice) GetLengthPrefixedSlice() ([]byte, bool) {
	v, n, err := DecodeLengthPrefixedSlice(s.data[s.pos:])
	if err != nil {
		return nil, false
	}
	s.pos += n
	return v, true
}

// GetBytes reads exactly n bytes.
func (s *Slice) GetBytes(n int) ([]byte, bool) {
	if s.Remaining() < n {
		return nil, false
	}
	v := s.data[s.pos : s.pos+n]
	s.pos += n
	return v, true
}
