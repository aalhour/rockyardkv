// builder.go implements block building with prefix compression.
//
// BlockBuilder generates blocks where keys are prefix-compressed
// with periodic restart points for efficient random access.
//
// Reference: RocksDB v10.7.5
//   - table/block_based/block_builder.h
//   - table/block_based/block_builder.cc
package block

import (
	"github.com/aalhour/rockyardkv/internal/encoding"
)

// Builder generates blocks where keys are prefix-compressed.
//
// When we store a key, we drop the prefix shared with the previous key.
// This helps reduce the space requirement significantly. Furthermore,
// once every K keys, we do not apply the prefix compression and store
// the entire key. We call this a "restart point".
//
// Format (single entry):
//
//	shared_bytes:    varint32
//	unshared_bytes:  varint32
//	value_length:    varint32
//	key_delta:       char[unshared_bytes]
//	value:           char[value_length]
//
// Format (overall block):
//
//	[entry 1]
//	[entry 2]
//	...
//	[entry N]
//	[restart point 1: uint32]
//	...
//	[restart point M: uint32]
//	[footer: uint32]  // PackIndexTypeAndNumRestarts(type, M)
type Builder struct {
	buffer           []byte   // Serialized block data
	restarts         []uint32 // Restart points (offsets into buffer)
	counter          int      // Entries since last restart
	restartInterval  int      // Restart interval
	lastKey          []byte   // Last key added
	useDeltaEncoding bool     // Whether to use delta encoding for keys
	finished         bool     // Whether Finish() has been called
}

// NewBuilder creates a new block builder.
// restartInterval controls how often restart points are created.
// A restart point is created every restartInterval entries.
// Set to 1 for no compression, 16 is a typical value.
func NewBuilder(restartInterval int) *Builder {
	return NewBuilderWithOptions(restartInterval, true)
}

// NewBuilderWithOptions creates a new block builder with configurable delta encoding.
func NewBuilderWithOptions(restartInterval int, useDeltaEncoding bool) *Builder {
	if restartInterval < 1 {
		restartInterval = 1
	}
	return &Builder{
		buffer:           make([]byte, 0, 4096),
		restartInterval:  restartInterval,
		useDeltaEncoding: useDeltaEncoding,
		restarts:         []uint32{0},
		counter:          0,
		finished:         false,
	}
}

// Reset resets the builder for reuse.
func (b *Builder) Reset() {
	b.buffer = b.buffer[:0]
	b.restarts = b.restarts[:1]
	b.restarts[0] = 0
	b.counter = 0
	b.lastKey = b.lastKey[:0]
	b.finished = false
}

// Add adds a key-value pair to the block.
// REQUIRES: Finish() has not been called since the last Reset().
// REQUIRES: key is larger than any previously added key (for data blocks).
func (b *Builder) Add(key, value []byte) {
	if b.finished {
		// Invariant violation: calling Add after Finish is a programmer error.
		// This matches RocksDB DCHECK behavior for internal consistency.
		panic("block: Add called after Finish") //nolint:forbidigo // intentional panic for invariant violation
	}

	shared := 0
	if b.useDeltaEncoding && b.counter < b.restartInterval {
		// Calculate shared prefix with previous key
		shared = sharedPrefixLength(b.lastKey, key)
	} else {
		// Restart point - store offset
		b.restarts = append(b.restarts, uint32(len(b.buffer)))
		b.counter = 0
	}

	unshared := len(key) - shared

	// Add entry: shared_bytes, unshared_bytes, value_length, key_delta, value
	b.buffer = encoding.AppendVarint32(b.buffer, uint32(shared))
	b.buffer = encoding.AppendVarint32(b.buffer, uint32(unshared))
	b.buffer = encoding.AppendVarint32(b.buffer, uint32(len(value)))
	b.buffer = append(b.buffer, key[shared:]...)
	b.buffer = append(b.buffer, value...)

	// Update state
	b.lastKey = append(b.lastKey[:0], key...)
	b.counter++
}

// CurrentSizeEstimate returns an estimate of the current block size.
func (b *Builder) CurrentSizeEstimate() int {
	// buffer + restarts array (4 bytes each) + footer (4 bytes)
	return len(b.buffer) + len(b.restarts)*4 + 4
}

// EstimateSizeAfterKV estimates the block size after adding a key-value pair.
func (b *Builder) EstimateSizeAfterKV(key, value []byte) int {
	estimate := b.CurrentSizeEstimate()

	// Key and value
	estimate += len(key) + len(value)

	// Varint headers (approximate)
	estimate += 3 * 5 // 3 varints, up to 5 bytes each

	// Possible new restart point
	if b.counter >= b.restartInterval {
		estimate += 4
	}

	return estimate
}

// EstimatedSize returns the current estimated size of the block.
// This is an alias for CurrentSizeEstimate for compatibility.
func (b *Builder) EstimatedSize() int {
	return b.CurrentSizeEstimate()
}

// Empty returns true if no entries have been added.
func (b *Builder) Empty() bool {
	return len(b.buffer) == 0
}

// Finish finishes building the block and returns the block data.
// The returned slice is valid until Reset() is called.
func (b *Builder) Finish() []byte {
	// Append restart array
	for _, restart := range b.restarts {
		b.buffer = encoding.AppendFixed32(b.buffer, restart)
	}

	// Append footer (num_restarts with index type)
	footer := PackIndexTypeAndNumRestarts(DataBlockBinarySearch, uint32(len(b.restarts)))
	b.buffer = encoding.AppendFixed32(b.buffer, footer)

	b.finished = true
	return b.buffer
}

// sharedPrefixLength returns the length of the shared prefix between a and b.
func sharedPrefixLength(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
