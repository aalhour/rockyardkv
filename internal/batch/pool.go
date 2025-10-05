// Package batch implements the WriteBatch format for atomic writes.
//
// This file implements write batch pooling for reduced memory allocations.
// Reference: RocksDB's WriteBufferManager and memory arena concepts.
package batch

import (
	"sync"
)

// WriteBatchPool manages a pool of WriteBatch objects for reuse.
// This significantly reduces GC pressure in high-throughput scenarios.
//
// Usage:
//
//	pool := batch.NewWriteBatchPool()
//	wb := pool.Get()
//	defer pool.Put(wb)
//	wb.Put(key, value)
//	db.Write(nil, wb)
type WriteBatchPool struct {
	pool sync.Pool

	// Stats for monitoring (optional)
	stats PoolStats
	mu    sync.Mutex
}

// PoolStats tracks pool usage statistics.
type PoolStats struct {
	Gets       uint64 // Total Get() calls
	Hits       uint64 // Reused from pool
	Misses     uint64 // Newly allocated
	Puts       uint64 // Returned to pool
	Discarded  uint64 // Too large, discarded
	TotalBytes uint64 // Total bytes allocated
}

// DefaultMaxBatchSize is the maximum size batch we'll return to the pool.
// Larger batches are discarded to prevent memory bloat.
const DefaultMaxBatchSize = 4 * 1024 * 1024 // 4MB

// NewWriteBatchPool creates a new WriteBatchPool.
func NewWriteBatchPool() *WriteBatchPool {
	return &WriteBatchPool{
		pool: sync.Pool{
			New: func() any {
				return New()
			},
		},
	}
}

// Get retrieves a WriteBatch from the pool.
// The batch is cleared and ready for use.
func (p *WriteBatchPool) Get() *WriteBatch {
	p.mu.Lock()
	p.stats.Gets++
	p.mu.Unlock()

	wb, ok := p.pool.Get().(*WriteBatch)
	if !ok {
		// Shouldn't happen - pool only stores *WriteBatch
		wb = New()
	}
	wb.Clear()

	// Track hit vs miss based on capacity
	p.mu.Lock()
	if cap(wb.data) > HeaderSize {
		p.stats.Hits++
	} else {
		p.stats.Misses++
	}
	p.mu.Unlock()

	return wb
}

// Put returns a WriteBatch to the pool for reuse.
// Very large batches are discarded to prevent memory bloat.
func (p *WriteBatchPool) Put(wb *WriteBatch) {
	if wb == nil {
		return
	}

	p.mu.Lock()
	p.stats.Puts++
	p.stats.TotalBytes += uint64(len(wb.data))
	p.mu.Unlock()

	// Don't pool very large batches - let GC reclaim them
	if cap(wb.data) > DefaultMaxBatchSize {
		p.mu.Lock()
		p.stats.Discarded++
		p.mu.Unlock()
		return
	}

	// Clear before returning to pool
	wb.Clear()
	p.pool.Put(wb)
}

// Stats returns a copy of the pool statistics.
func (p *WriteBatchPool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// ResetStats resets the pool statistics.
func (p *WriteBatchPool) ResetStats() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats = PoolStats{}
}

// HitRate returns the cache hit rate (0.0 to 1.0).
func (s *PoolStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// ---------------------------------------------------------------------------
// Pre-sized batch pool (multiple buckets for different sizes)
// ---------------------------------------------------------------------------

// SizedWriteBatchPool provides pools for different batch sizes.
// This reduces memory fragmentation for workloads with varying batch sizes.
type SizedWriteBatchPool struct {
	// Size buckets: 4KB, 16KB, 64KB, 256KB, 1MB
	pools [5]sync.Pool

	// Stats
	stats SizedPoolStats
	mu    sync.Mutex
}

// SizedPoolStats tracks statistics for sized pools.
type SizedPoolStats struct {
	BucketGets   [5]uint64
	BucketPuts   [5]uint64
	BucketMisses [5]uint64
	Oversized    uint64
}

// Bucket sizes in bytes
var bucketSizes = [5]int{
	4 * 1024,    // 4KB
	16 * 1024,   // 16KB
	64 * 1024,   // 64KB
	256 * 1024,  // 256KB
	1024 * 1024, // 1MB
}

// NewSizedWriteBatchPool creates a pool with multiple size buckets.
func NewSizedWriteBatchPool() *SizedWriteBatchPool {
	p := &SizedWriteBatchPool{}
	for i := range p.pools {
		size := bucketSizes[i]
		p.pools[i] = sync.Pool{
			New: func() any {
				wb := &WriteBatch{
					data: make([]byte, HeaderSize, size),
				}
				return wb
			},
		}
	}
	return p
}

// getBucket returns the appropriate bucket index for a given size.
func getBucket(size int) int {
	for i, bucketSize := range bucketSizes {
		if size <= bucketSize {
			return i
		}
	}
	return -1 // Oversized
}

// Get retrieves a WriteBatch sized appropriately for the expected size.
// If expectedSize is 0, the smallest bucket is used.
func (p *SizedWriteBatchPool) Get(expectedSize int) *WriteBatch {
	bucket := getBucket(expectedSize)

	p.mu.Lock()
	if bucket >= 0 {
		p.stats.BucketGets[bucket]++
	} else {
		p.stats.Oversized++
	}
	p.mu.Unlock()

	if bucket < 0 {
		// Oversized - allocate directly
		wb := &WriteBatch{
			data: make([]byte, HeaderSize, expectedSize),
		}
		return wb
	}

	wb, ok := p.pools[bucket].Get().(*WriteBatch)
	if !ok {
		// Shouldn't happen - pool only stores *WriteBatch
		wb = &WriteBatch{
			data: make([]byte, HeaderSize, expectedSize),
		}
	}
	wb.Clear()
	return wb
}

// Put returns a WriteBatch to the appropriate pool.
func (p *SizedWriteBatchPool) Put(wb *WriteBatch) {
	if wb == nil {
		return
	}

	bucket := getBucket(cap(wb.data))
	if bucket < 0 {
		// Oversized - don't pool
		return
	}

	p.mu.Lock()
	p.stats.BucketPuts[bucket]++
	p.mu.Unlock()

	wb.Clear()
	p.pools[bucket].Put(wb)
}

// Stats returns pool statistics.
func (p *SizedWriteBatchPool) Stats() SizedPoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// ---------------------------------------------------------------------------
// Global default pool
// ---------------------------------------------------------------------------

var defaultPool = NewWriteBatchPool()

// GlobalPool returns the global default WriteBatch pool.
func GlobalPool() *WriteBatchPool {
	return defaultPool
}

// GetFromPool retrieves a WriteBatch from the global pool.
func GetFromPool() *WriteBatch {
	return defaultPool.Get()
}

// ReturnToPool returns a WriteBatch to the global pool.
func ReturnToPool(wb *WriteBatch) {
	defaultPool.Put(wb)
}
