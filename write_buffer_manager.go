package rockyardkv

// write_buffer_manager.go implements write buffer manager.
//
// This file implements WriteBufferManager for controlling memory usage
// across multiple memtables and/or DB instances.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/write_buffer_manager.h
//   - memtable/write_buffer_manager.cc

import (
	"sync"
	"sync/atomic"

	"github.com/aalhour/rockyardkv/internal/mempool"
)

// WriteBufferManager controls memory usage across memtables.
// It can be shared across multiple DBs or column families.
//
// When memory usage exceeds the buffer limit, it triggers a flush
// in the DB that receives the next write operation.
//
// Reference: RocksDB's WriteBufferManager class.
type WriteBufferManager struct {
	// Buffer size limit (0 = unlimited)
	bufferSize uint64

	// Current memory usage
	memoryUsed   atomic.Uint64
	memoryActive atomic.Uint64

	// Allow stalling when memory pressure is high
	allowStall bool

	// Stall condition signaling
	stallCond *sync.Cond
	stallMu   sync.Mutex
	isStalled atomic.Bool

	// Stats
	stats WriteBufferStats
	mu    sync.Mutex
}

// WriteBufferStats tracks memory management statistics.
type WriteBufferStats struct {
	TotalReserved   uint64 // Total memory reserved
	TotalFreed      uint64 // Total memory freed
	PeakUsage       uint64 // Peak memory usage
	FlushTriggers   uint64 // Number of times flush was triggered
	StallEvents     uint64 // Number of stall events
	StallDurationNs uint64 // Total stall duration in nanoseconds
}

// NewWriteBufferManager creates a new WriteBufferManager.
//
// Parameters:
//   - bufferSize: Maximum memory to use (0 = unlimited)
//   - allowStall: If true, stall writes when memory usage exceeds limit
//
// Example:
//
//	// 256MB limit, stall when exceeded
//	wbm := NewWriteBufferManager(256*1024*1024, true)
//
//	// Unlimited memory, no stalling
//	wbm := NewWriteBufferManager(0, false)
func NewWriteBufferManager(bufferSize uint64, allowStall bool) *WriteBufferManager {
	wbm := &WriteBufferManager{
		bufferSize: bufferSize,
		allowStall: allowStall,
	}
	wbm.stallCond = sync.NewCond(&wbm.stallMu)
	return wbm
}

// Enabled returns true if memory limiting is enabled.
func (wbm *WriteBufferManager) Enabled() bool {
	return wbm.bufferSize > 0
}

// BufferSize returns the configured buffer size limit.
func (wbm *WriteBufferManager) BufferSize() uint64 {
	return wbm.bufferSize
}

// MemoryUsage returns the current total memory usage.
func (wbm *WriteBufferManager) MemoryUsage() uint64 {
	return wbm.memoryUsed.Load()
}

// MutableMemtableMemoryUsage returns the memory used by active (mutable) memtables.
func (wbm *WriteBufferManager) MutableMemtableMemoryUsage() uint64 {
	return wbm.memoryActive.Load()
}

// ShouldFlush returns true if a flush should be triggered.
// This happens when memory usage exceeds 7/8 of the buffer limit.
func (wbm *WriteBufferManager) ShouldFlush() bool {
	if !wbm.Enabled() {
		return false
	}
	usage := wbm.memoryUsed.Load()
	// Trigger flush at 7/8 (87.5%) of buffer limit
	// This matches RocksDB's behavior
	threshold := wbm.bufferSize * 7 / 8
	return usage >= threshold
}

// ReserveMem reserves memory for a new allocation.
// This should be called when memory is allocated for a memtable.
func (wbm *WriteBufferManager) ReserveMem(mem uint64) {
	if !wbm.Enabled() && !wbm.allowStall {
		return
	}

	newUsed := wbm.memoryUsed.Add(mem)
	wbm.memoryActive.Add(mem)

	// Update stats
	wbm.mu.Lock()
	wbm.stats.TotalReserved += mem
	if newUsed > wbm.stats.PeakUsage {
		wbm.stats.PeakUsage = newUsed
	}
	wbm.mu.Unlock()
}

// ScheduleFreeMem marks memory as no longer active (scheduled for free).
// This is called when a memtable becomes immutable and is waiting for flush.
func (wbm *WriteBufferManager) ScheduleFreeMem(mem uint64) {
	if wbm.Enabled() {
		wbm.memoryActive.Add(^(mem - 1)) // Atomic subtract
	}
}

// FreeMem frees previously reserved memory.
// This should be called when a memtable is flushed and freed.
func (wbm *WriteBufferManager) FreeMem(mem uint64) {
	if !wbm.Enabled() {
		return
	}

	wbm.memoryUsed.Add(^(mem - 1)) // Atomic subtract

	// Update stats
	wbm.mu.Lock()
	wbm.stats.TotalFreed += mem
	wbm.mu.Unlock()

	// Check if we can end stall
	wbm.maybeEndWriteStall()
}

// WaitIfStalled blocks until memory pressure is relieved.
// This implements write stalling based on memory usage.
// Returns true if the caller was stalled.
func (wbm *WriteBufferManager) WaitIfStalled() bool {
	if !wbm.allowStall || !wbm.Enabled() {
		return false
	}

	if wbm.memoryUsed.Load() < wbm.bufferSize {
		return false
	}

	// Need to stall
	wbm.stallMu.Lock()
	defer wbm.stallMu.Unlock()

	wbm.isStalled.Store(true)
	wbm.mu.Lock()
	wbm.stats.StallEvents++
	wbm.mu.Unlock()

	// Wait for signal that memory has been freed
	for wbm.memoryUsed.Load() >= wbm.bufferSize {
		wbm.stallCond.Wait()
	}

	return true
}

// IsStalled returns true if writes are currently stalled.
func (wbm *WriteBufferManager) IsStalled() bool {
	return wbm.isStalled.Load()
}

// maybeEndWriteStall signals waiting writers if memory pressure is relieved.
func (wbm *WriteBufferManager) maybeEndWriteStall() {
	if !wbm.allowStall || !wbm.isStalled.Load() {
		return
	}

	// End stall if we're below 7/8 of limit
	threshold := wbm.bufferSize * 7 / 8
	if wbm.memoryUsed.Load() < threshold {
		wbm.stallMu.Lock()
		wbm.isStalled.Store(false)
		wbm.stallCond.Broadcast()
		wbm.stallMu.Unlock()
	}
}

// Stats returns a copy of the statistics.
func (wbm *WriteBufferManager) Stats() WriteBufferStats {
	wbm.mu.Lock()
	defer wbm.mu.Unlock()
	return wbm.stats
}

// ResetStats resets the statistics.
func (wbm *WriteBufferManager) ResetStats() {
	wbm.mu.Lock()
	defer wbm.mu.Unlock()
	wbm.stats = WriteBufferStats{}
}

// UsageRatio returns the current memory usage as a ratio (0.0 to 1.0+).
func (wbm *WriteBufferManager) UsageRatio() float64 {
	if !wbm.Enabled() {
		return 0
	}
	return float64(wbm.memoryUsed.Load()) / float64(wbm.bufferSize)
}

// ---------------------------------------------------------------------------
// Global buffer pool (delegates to internal/mempool)
// ---------------------------------------------------------------------------

// GetBuffer retrieves a byte slice from the global buffer pool.
func GetBuffer(minSize int) []byte {
	return mempool.GlobalPool.Get(minSize)
}

// PutBuffer returns a byte slice to the global buffer pool.
func PutBuffer(buf []byte) {
	mempool.GlobalPool.Put(buf)
}
