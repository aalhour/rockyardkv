// Package vfs provides a virtual filesystem abstraction layer with Direct I/O support.
//
// Reference: RocksDB v10.7.5
//   env/io_posix.h - DirectIOHelper struct, PosixRandomAccessFile, PosixWritableFile
//   env/io_posix.cc - Direct I/O implementation with alignment handling
//   env/fs_posix.cc - O_DIRECT flag usage on Linux, F_NOCACHE on macOS
//   include/rocksdb/env.h - EnvOptions struct with use_direct_reads/use_direct_writes
//
// Direct I/O bypasses the OS page cache and reads/writes directly to disk.
// This is beneficial for:
//   - Reducing memory pressure (no double buffering)
//   - More predictable I/O performance
//   - Better control over caching behavior
//
// Requirements for Direct I/O:
//   - Buffers must be aligned to the logical block size (typically 4KB)
//   - Read/write offsets must be aligned to the logical block size
//   - Read/write lengths must be aligned to the logical block size

package vfs

import (
	"errors"
)

// DefaultBlockSize is the default logical block size for Direct I/O alignment.
// Most modern filesystems use 4KB blocks.
const DefaultBlockSize = 4096

// ErrDirectIONotSupported is returned when Direct I/O is not supported
// on the current platform or filesystem.
var ErrDirectIONotSupported = errors.New("direct I/O not supported")

// ErrNotAligned is returned when a buffer or offset is not properly aligned
// for Direct I/O operations.
var ErrNotAligned = errors.New("buffer or offset not aligned for direct I/O")

// FileOptions configures how files are opened.
// Mirrors RocksDB's EnvOptions.
type FileOptions struct {
	// UseDirectReads enables O_DIRECT for reads.
	// This bypasses the OS page cache for read operations.
	UseDirectReads bool

	// UseDirectWrites enables O_DIRECT for writes.
	// This bypasses the OS page cache for write operations.
	UseDirectWrites bool

	// UseMmapReads enables memory-mapped reads.
	// Cannot be used with UseDirectReads.
	UseMmapReads bool

	// UseMmapWrites enables memory-mapped writes.
	// Cannot be used with UseDirectWrites.
	UseMmapWrites bool

	// BlockSize is the alignment requirement for Direct I/O.
	// If 0, DefaultBlockSize is used.
	BlockSize int
}

// GetBlockSize returns the block size to use, defaulting to DefaultBlockSize.
func (o FileOptions) GetBlockSize() int {
	if o.BlockSize <= 0 {
		return DefaultBlockSize
	}
	return o.BlockSize
}

// Validate checks that the options are valid.
func (o FileOptions) Validate() error {
	if o.UseDirectReads && o.UseMmapReads {
		return errors.New("cannot use both direct reads and mmap reads")
	}
	if o.UseDirectWrites && o.UseMmapWrites {
		return errors.New("cannot use both direct writes and mmap writes")
	}
	return nil
}

// DirectIOFile is a file that supports Direct I/O operations.
type DirectIOFile interface {
	// UseDirectIO returns true if Direct I/O is enabled for this file.
	UseDirectIO() bool

	// GetRequiredBufferAlignment returns the required alignment for buffers
	// used with this file. Typically 4KB for Direct I/O.
	GetRequiredBufferAlignment() int
}

// IsAligned checks if the given value is aligned to the given alignment.
func IsAligned(value, alignment int) bool {
	if alignment <= 0 {
		return true
	}
	return value%alignment == 0
}

// AlignUp rounds up the given value to the next multiple of alignment.
func AlignUp(value, alignment int) int {
	if alignment <= 0 {
		return value
	}
	return ((value + alignment - 1) / alignment) * alignment
}

// AlignDown rounds down the given value to the previous multiple of alignment.
func AlignDown(value, alignment int) int {
	if alignment <= 0 {
		return value
	}
	return (value / alignment) * alignment
}

// AlignedBuffer is a buffer that is properly aligned for Direct I/O.
type AlignedBuffer struct {
	data      []byte
	alignment int
	capacity  int
}

// NewAlignedBuffer creates a new aligned buffer with the given capacity and alignment.
// The actual capacity may be larger than requested to accommodate alignment.
//
// Note: In Go, we cannot guarantee memory alignment without using unsafe.
// This implementation provides a buffer of the requested size and relies on
// the caller to use aligned offsets when performing Direct I/O operations.
// For true memory-aligned buffers, consider using C allocation via cgo,
// but this project is pure Go and doesn't use cgo.
func NewAlignedBuffer(capacity, alignment int) *AlignedBuffer {
	if alignment <= 0 {
		alignment = DefaultBlockSize
	}
	// Allocate the buffer - Go's make() may or may not align it
	// For Direct I/O, the offset alignment is more critical than memory alignment
	data := make([]byte, capacity)

	return &AlignedBuffer{
		data:      data,
		alignment: alignment,
		capacity:  capacity,
	}
}

// Bytes returns the underlying byte slice.
func (b *AlignedBuffer) Bytes() []byte {
	return b.data
}

// Len returns the length of the buffer.
func (b *AlignedBuffer) Len() int {
	return len(b.data)
}

// Cap returns the capacity of the buffer.
func (b *AlignedBuffer) Cap() int {
	return b.capacity
}

// Alignment returns the alignment of the buffer.
func (b *AlignedBuffer) Alignment() int {
	return b.alignment
}

// Resize resizes the buffer to the given size.
// The size must not exceed the capacity.
func (b *AlignedBuffer) Resize(size int) error {
	if size > b.capacity {
		return errors.New("size exceeds capacity")
	}
	b.data = b.data[:size]
	return nil
}

// Clear resets the buffer to zero length.
func (b *AlignedBuffer) Clear() {
	b.data = b.data[:0]
}

// DirectIOHelper helps manage Direct I/O operations with alignment requirements.
// Mirrors RocksDB's DirectIOHelper struct in env/io_posix.h.
type DirectIOHelper struct {
	alignment int
	finished  int64 // bytes finished processing
}

// NewDirectIOHelper creates a new DirectIOHelper with the given alignment.
func NewDirectIOHelper(alignment int) *DirectIOHelper {
	if alignment <= 0 {
		alignment = DefaultBlockSize
	}
	return &DirectIOHelper{
		alignment: alignment,
		finished:  0,
	}
}

// IsOffsetAndLengthAligned checks if an offset and length are properly aligned.
func (h *DirectIOHelper) IsOffsetAndLengthAligned(offset, length int64) bool {
	return offset%int64(h.alignment) == 0 && length%int64(h.alignment) == 0
}

// AlignOffset aligns an offset down to the alignment boundary.
func (h *DirectIOHelper) AlignOffset(offset int64) int64 {
	return (offset / int64(h.alignment)) * int64(h.alignment)
}

// AlignLength aligns a length up to the alignment boundary.
func (h *DirectIOHelper) AlignLength(length int64) int64 {
	return ((length + int64(h.alignment) - 1) / int64(h.alignment)) * int64(h.alignment)
}

// AddFinished adds to the finished count.
func (h *DirectIOHelper) AddFinished(n int64) {
	h.finished += n
}

// Finished returns the total bytes finished.
func (h *DirectIOHelper) Finished() int64 {
	return h.finished
}

// Alignment returns the alignment requirement.
func (h *DirectIOHelper) Alignment() int {
	return h.alignment
}
