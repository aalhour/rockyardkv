package rockyardkv

// write_controller.go implements writeController for managing write stalling.
//
// Write stalling prevents the database from being overwhelmed when compaction
// cannot keep up with writes. It has three states:
//   - Normal: Writes proceed at full speed
//   - Delayed: Writes are slowed down (backpressure)
//   - Stopped: Writes are blocked until compaction catches up
//
// Reference: RocksDB v10.7.5 db/write_controller.h

import (
	"sync"
	"time"
)

// WriteStallCause indicates why writes are being stalled.
type WriteStallCause int

const (
	// WriteStallCauseNone means no stall.
	WriteStallCauseNone WriteStallCause = iota
	// WriteStallCauseMemtableLimit means too many unflushed memtables.
	WriteStallCauseMemtableLimit
	// WriteStallCauseL0FileCountLimit means too many L0 files.
	WriteStallCauseL0FileCountLimit
	// WriteStallCausePendingCompactionBytes means too many pending compaction bytes.
	WriteStallCausePendingCompactionBytes
)

// writeController manages write stalling to prevent compaction from falling behind.
type writeController struct {
	mu sync.Mutex

	// Current stall state
	condition WriteStallCondition
	cause     WriteStallCause

	// Condition variable for stopped writes
	stallCond *sync.Cond

	// Delayed write rate (bytes/sec), 0 means use default
	delayedWriteRate uint64

	// closed indicates shutdown has been requested.
	// When true, MaybeStallWrite returns immediately instead of blocking.
	closed bool

	// Statistics
	totalStopped uint64
	totalDelayed uint64
}

// newWriteController creates a new write controller.
func newWriteController() *writeController {
	wc := &writeController{
		condition:        WriteStallConditionNormal,
		cause:            WriteStallCauseNone,
		delayedWriteRate: 16 * 1024 * 1024, // 16 MB/s default
	}
	wc.stallCond = sync.NewCond(&wc.mu)
	return wc
}

// GetStallCondition returns the current stall condition and cause.
func (wc *writeController) getStallCondition() (WriteStallCondition, WriteStallCause) {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	return wc.condition, wc.cause
}

// SetStallCondition updates the stall condition.
func (wc *writeController) setStallCondition(condition WriteStallCondition, cause WriteStallCause) {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	prevCondition := wc.condition
	wc.condition = condition
	wc.cause = cause

	// If transitioning from stopped to non-stopped, wake up blocked writers
	if prevCondition == WriteStallConditionStopped && condition != WriteStallConditionStopped {
		wc.stallCond.Broadcast()
	}

	// Update statistics
	switch condition {
	case WriteStallConditionStopped:
		wc.totalStopped++
	case WriteStallConditionDelayed:
		wc.totalDelayed++
	}
}

// maybeStallWrite checks the stall condition and blocks or delays if needed.
// If the controller is closed (via releaseWriteStall), returns immediately.
func (wc *writeController) maybeStallWrite(writeSize int) {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	// Handle stopped condition - block until released or closed
	for wc.condition == WriteStallConditionStopped && !wc.closed {
		wc.stallCond.Wait()
	}

	// If closed, return immediately without delay
	if wc.closed {
		return
	}

	// Handle delayed condition - sleep based on write rate
	if wc.condition == WriteStallConditionDelayed && wc.delayedWriteRate > 0 {
		// Calculate delay: (writeSize / rate) seconds
		delayNs := int64(writeSize) * int64(time.Second) / int64(wc.delayedWriteRate)
		if delayNs > 0 {
			// Release lock during sleep to not block other operations
			wc.mu.Unlock()
			time.Sleep(time.Duration(delayNs))
			wc.mu.Lock()
		}
	}
}

// SetDelayedWriteRate sets the delayed write rate.
func (wc *writeController) setDelayedWriteRate(rate uint64) {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	wc.delayedWriteRate = rate
}

// GetStats returns statistics about write stalls.
func (wc *writeController) getStats() (stopped, delayed uint64) {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	return wc.totalStopped, wc.totalDelayed
}

// ReleaseWriteStall marks the controller as closed and wakes up all waiting writers.
// After calling this, MaybeStallWrite returns immediately instead of blocking.
// Use this during graceful shutdown to unblock workers stuck in MaybeStallWrite.
func (wc *writeController) releaseWriteStall() {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	wc.closed = true
	wc.stallCond.Broadcast()
}

// recalculateWriteStallCondition determines the write stall condition based on current state.
func recalculateWriteStallCondition(
	numUnflushedMemtables int,
	numL0Files int,
	maxWriteBufferNumber int,
	level0SlowdownTrigger int,
	level0StopTrigger int,
	disableAutoCompactions bool,
) (WriteStallCondition, WriteStallCause) {
	// Check memtable limit first
	if numUnflushedMemtables >= maxWriteBufferNumber {
		return WriteStallConditionStopped, WriteStallCauseMemtableLimit
	}

	// Check L0 file count (unless auto compactions disabled)
	if !disableAutoCompactions {
		if numL0Files >= level0StopTrigger {
			return WriteStallConditionStopped, WriteStallCauseL0FileCountLimit
		}
		if numL0Files >= level0SlowdownTrigger {
			return WriteStallConditionDelayed, WriteStallCauseL0FileCountLimit
		}
	}

	// Check memtable near-limit for delay
	if maxWriteBufferNumber > 3 && numUnflushedMemtables >= maxWriteBufferNumber-1 {
		return WriteStallConditionDelayed, WriteStallCauseMemtableLimit
	}

	return WriteStallConditionNormal, WriteStallCauseNone
}

// String returns a human-readable description of the stall cause.
func (c WriteStallCause) String() string {
	switch c {
	case WriteStallCauseNone:
		return "none"
	case WriteStallCauseMemtableLimit:
		return "memtable_limit"
	case WriteStallCauseL0FileCountLimit:
		return "l0_file_count_limit"
	case WriteStallCausePendingCompactionBytes:
		return "pending_compaction_bytes"
	default:
		return "unknown"
	}
}
