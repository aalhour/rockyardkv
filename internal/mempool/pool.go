// Package mempool provides memory pooling utilities.
//
// This package is internal and not part of the public API.
//
// Reference: RocksDB v10.7.5
//   - memory/arena.h - Arena allocator for efficient memory management
//   - memory/allocator.h - Allocator interface
//
// The buffer pool provides reusable byte slices to reduce allocations
// for temporary buffers used in encoding/decoding operations.
package mempool

import "sync"

// Pool manages reusable byte slices of various sizes.
// This reduces allocations for temporary buffers used in encoding/decoding.
type Pool struct {
	// Size buckets: 256B, 1KB, 4KB, 16KB, 64KB
	pools [5]sync.Pool
}

// BucketSizes defines the buffer size buckets.
var BucketSizes = [5]int{
	256,       // 256 bytes
	1024,      // 1KB
	4 * 1024,  // 4KB
	16 * 1024, // 16KB
	64 * 1024, // 64KB
}

// NewPool creates a new Pool.
func NewPool() *Pool {
	bp := &Pool{}
	for i := range bp.pools {
		size := BucketSizes[i]
		bp.pools[i] = sync.Pool{
			New: func() any {
				buf := make([]byte, 0, size)
				return &buf
			},
		}
	}
	return bp
}

// Get retrieves a byte slice with at least the specified capacity.
func (bp *Pool) Get(minSize int) []byte {
	bucket := bp.getBucket(minSize)
	if bucket < 0 {
		// Too large for pool
		return make([]byte, 0, minSize)
	}

	bufPtr, ok := bp.pools[bucket].Get().(*[]byte)
	if !ok {
		return make([]byte, 0, minSize)
	}
	buf := *bufPtr
	return buf[:0]
}

// Put returns a byte slice to the pool.
func (bp *Pool) Put(buf []byte) {
	if buf == nil {
		return
	}

	bucket := bp.getBucket(cap(buf))
	if bucket < 0 || cap(buf) > BucketSizes[len(BucketSizes)-1]*2 {
		// Too large - don't pool
		return
	}

	// Clear the slice before returning
	buf = buf[:0]
	bp.pools[bucket].Put(&buf)
}

func (bp *Pool) getBucket(size int) int {
	for i, bucketSize := range BucketSizes {
		if size <= bucketSize {
			return i
		}
	}
	return -1
}

// GlobalPool is the default global buffer pool.
var GlobalPool = NewPool()
