// Package block implements the block format for RocksDB SST files.
//
// A Block contains a sequence of key-value pairs with prefix compression.
// The format is:
//
//	entries: key-value pairs with prefix compression
//	restarts: uint32[num_restarts] - offsets of restart points
//	num_restarts: uint32
//
// Each entry has the format:
//
//	shared_bytes: varint32 (shared prefix with previous key)
//	unshared_bytes: varint32 (unshared key suffix length)
//	value_length: varint32
//	key_delta: char[unshared_bytes]
//
// Reference: RocksDB v10.7.5
//   - table/format.h (BlockHandle class)
//   - table/format.cc
//     value: char[value_length]
package block

import (
	"errors"

	"github.com/aalhour/rockyardkv/internal/encoding"
)

// MaxVarint64Length is the maximum length of a varint64 encoding.
const MaxVarint64Length = 10

var (
	// ErrBadBlockHandle is returned when a block handle is corrupted.
	ErrBadBlockHandle = errors.New("block: bad block handle")

	// ErrBadBlockFooter is returned when a block footer is corrupted.
	ErrBadBlockFooter = errors.New("block: bad block footer")

	// ErrBadBlock is returned when a block is corrupted.
	ErrBadBlock = errors.New("block: corrupted block")
)

// Handle is a pointer to the extent of a file that stores a data block or a meta block.
// It consists of an offset and a size. This structure is bit-compatible with RocksDB.
type Handle struct {
	Offset uint64
	Size   uint64
}

// NullHandle is a block handle with offset=0 and size=0, representing "no block".
var NullHandle = Handle{Offset: 0, Size: 0}

// MaxEncodedLength is the maximum encoding length of a BlockHandle.
// Two varint64s, each up to 10 bytes.
const MaxEncodedLength = 2 * MaxVarint64Length

// IsNull returns true if this is a null block handle.
func (h Handle) IsNull() bool {
	return h.Offset == 0 && h.Size == 0
}

// EncodeTo appends the encoding of h to dst.
func (h Handle) EncodeTo(dst []byte) []byte {
	dst = encoding.AppendVarint64(dst, h.Offset)
	dst = encoding.AppendVarint64(dst, h.Size)
	return dst
}

// EncodeToSlice encodes the handle into a new slice.
func (h Handle) EncodeToSlice() []byte {
	return h.EncodeTo(nil)
}

// EncodedLength returns the encoded length of this handle.
func (h Handle) EncodedLength() int {
	return encoding.VarintLength(h.Offset) + encoding.VarintLength(h.Size)
}

// DecodeHandle decodes a block handle from data and returns the remaining bytes.
// Returns an error if the data is corrupted.
func DecodeHandle(data []byte) (Handle, []byte, error) {
	var h Handle

	offset, n1, err := encoding.DecodeVarint64(data)
	if err != nil {
		return Handle{}, nil, ErrBadBlockHandle
	}
	h.Offset = offset
	data = data[n1:]

	size, n2, err := encoding.DecodeVarint64(data)
	if err != nil {
		return Handle{}, nil, ErrBadBlockHandle
	}
	h.Size = size
	data = data[n2:]

	return h, data, nil
}

// DecodeHandleFrom decodes a block handle from data without returning remaining bytes.
func DecodeHandleFrom(data []byte) (Handle, error) {
	h, _, err := DecodeHandle(data)
	return h, err
}
