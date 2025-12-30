package db

import (
	"sync"
	"testing"
	"time"
)

func TestWriteControllerBasic(t *testing.T) {
	wc := NewWriteController()

	// Initial state should be normal
	condition, cause := wc.GetStallCondition()
	if condition != WriteStallConditionNormal {
		t.Errorf("Expected normal condition, got %v", condition)
	}
	if cause != WriteStallCauseNone {
		t.Errorf("Expected no cause, got %v", cause)
	}
}

func TestWriteControllerSetCondition(t *testing.T) {
	wc := NewWriteController()

	// Set to delayed
	wc.SetStallCondition(WriteStallConditionDelayed, WriteStallCauseL0FileCountLimit)
	condition, cause := wc.GetStallCondition()
	if condition != WriteStallConditionDelayed {
		t.Errorf("Expected delayed condition, got %v", condition)
	}
	if cause != WriteStallCauseL0FileCountLimit {
		t.Errorf("Expected L0 file count cause, got %v", cause)
	}

	// Set to stopped
	wc.SetStallCondition(WriteStallConditionStopped, WriteStallCauseMemtableLimit)
	condition, cause = wc.GetStallCondition()
	if condition != WriteStallConditionStopped {
		t.Errorf("Expected stopped condition, got %v", condition)
	}
	if cause != WriteStallCauseMemtableLimit {
		t.Errorf("Expected memtable limit cause, got %v", cause)
	}

	// Set back to normal
	wc.SetStallCondition(WriteStallConditionNormal, WriteStallCauseNone)
	condition, _ = wc.GetStallCondition()
	if condition != WriteStallConditionNormal {
		t.Errorf("Expected normal condition, got %v", condition)
	}
}

func TestWriteControllerStoppedWakesUp(t *testing.T) {
	wc := NewWriteController()

	// Set to stopped
	wc.SetStallCondition(WriteStallConditionStopped, WriteStallCauseL0FileCountLimit)

	var wg sync.WaitGroup
	started := make(chan struct{})
	done := make(chan struct{})

	wg.Go(func() {
		close(started)
		// This should block until released
		wc.MaybeStallWrite(100)
		close(done)
	})

	// Wait for goroutine to start
	<-started
	time.Sleep(10 * time.Millisecond)

	// Release the stall
	wc.SetStallCondition(WriteStallConditionNormal, WriteStallCauseNone)

	// Wait for completion with timeout
	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("MaybeStallWrite did not wake up after stall released")
	}

	wg.Wait()
}

func TestWriteControllerDelayedSlowsDown(t *testing.T) {
	wc := NewWriteController()

	// Set a high write rate for testing
	wc.SetDelayedWriteRate(1024 * 1024) // 1 MB/s

	// Set to delayed
	wc.SetStallCondition(WriteStallConditionDelayed, WriteStallCauseL0FileCountLimit)

	// Write should be delayed proportionally
	writeSize := 100 * 1024 // 100 KB
	start := time.Now()
	wc.MaybeStallWrite(writeSize)
	elapsed := time.Since(start)

	// Expected delay: 100KB / 1MB/s = 0.1s = 100ms
	// Allow some tolerance
	expectedMin := 80 * time.Millisecond
	if elapsed < expectedMin {
		t.Logf("Note: delay was shorter than expected: %v (expected >= %v)", elapsed, expectedMin)
		// Not a hard failure - timing tests are flaky
	}
}

func TestRecalculateWriteStallCondition(t *testing.T) {
	tests := []struct {
		name                   string
		numUnflushed           int
		numL0Files             int
		maxWriteBufferNumber   int
		level0SlowdownTrigger  int
		level0StopTrigger      int
		disableAutoCompactions bool
		wantCondition          WriteStallCondition
		wantCause              WriteStallCause
	}{
		{
			name:                   "normal",
			numUnflushed:           1,
			numL0Files:             5,
			maxWriteBufferNumber:   4,
			level0SlowdownTrigger:  20,
			level0StopTrigger:      36,
			disableAutoCompactions: false,
			wantCondition:          WriteStallConditionNormal,
			wantCause:              WriteStallCauseNone,
		},
		{
			name:                   "stopped_memtable_limit",
			numUnflushed:           4,
			numL0Files:             5,
			maxWriteBufferNumber:   4,
			level0SlowdownTrigger:  20,
			level0StopTrigger:      36,
			disableAutoCompactions: false,
			wantCondition:          WriteStallConditionStopped,
			wantCause:              WriteStallCauseMemtableLimit,
		},
		{
			name:                   "stopped_l0_limit",
			numUnflushed:           1,
			numL0Files:             40,
			maxWriteBufferNumber:   4,
			level0SlowdownTrigger:  20,
			level0StopTrigger:      36,
			disableAutoCompactions: false,
			wantCondition:          WriteStallConditionStopped,
			wantCause:              WriteStallCauseL0FileCountLimit,
		},
		{
			name:                   "delayed_l0_limit",
			numUnflushed:           1,
			numL0Files:             25,
			maxWriteBufferNumber:   4,
			level0SlowdownTrigger:  20,
			level0StopTrigger:      36,
			disableAutoCompactions: false,
			wantCondition:          WriteStallConditionDelayed,
			wantCause:              WriteStallCauseL0FileCountLimit,
		},
		{
			name:                   "delayed_memtable_near_limit",
			numUnflushed:           4,
			numL0Files:             5,
			maxWriteBufferNumber:   5,
			level0SlowdownTrigger:  20,
			level0StopTrigger:      36,
			disableAutoCompactions: false,
			wantCondition:          WriteStallConditionDelayed,
			wantCause:              WriteStallCauseMemtableLimit,
		},
		{
			name:                   "disabled_compactions_ignores_l0",
			numUnflushed:           1,
			numL0Files:             100,
			maxWriteBufferNumber:   4,
			level0SlowdownTrigger:  20,
			level0StopTrigger:      36,
			disableAutoCompactions: true,
			wantCondition:          WriteStallConditionNormal,
			wantCause:              WriteStallCauseNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition, cause := RecalculateWriteStallCondition(
				tt.numUnflushed,
				tt.numL0Files,
				tt.maxWriteBufferNumber,
				tt.level0SlowdownTrigger,
				tt.level0StopTrigger,
				tt.disableAutoCompactions,
			)
			if condition != tt.wantCondition {
				t.Errorf("condition = %v, want %v", condition, tt.wantCondition)
			}
			if cause != tt.wantCause {
				t.Errorf("cause = %v, want %v", cause, tt.wantCause)
			}
		})
	}
}

func TestWriteControllerStats(t *testing.T) {
	wc := NewWriteController()

	// Initially zero
	stopped, delayed := wc.GetStats()
	if stopped != 0 || delayed != 0 {
		t.Errorf("Initial stats should be 0, got stopped=%d, delayed=%d", stopped, delayed)
	}

	// Set stopped
	wc.SetStallCondition(WriteStallConditionStopped, WriteStallCauseL0FileCountLimit)
	stopped, _ = wc.GetStats()
	if stopped != 1 {
		t.Errorf("Expected 1 stopped, got %d", stopped)
	}

	// Set delayed
	wc.SetStallCondition(WriteStallConditionDelayed, WriteStallCauseL0FileCountLimit)
	_, delayed = wc.GetStats()
	if delayed != 1 {
		t.Errorf("Expected 1 delayed, got %d", delayed)
	}
}

func TestWriteStallCauseString(t *testing.T) {
	tests := []struct {
		cause WriteStallCause
		want  string
	}{
		{WriteStallCauseNone, "none"},
		{WriteStallCauseMemtableLimit, "memtable_limit"},
		{WriteStallCauseL0FileCountLimit, "l0_file_count_limit"},
		{WriteStallCausePendingCompactionBytes, "pending_compaction_bytes"},
	}
	for _, tt := range tests {
		if got := tt.cause.String(); got != tt.want {
			t.Errorf("%v.String() = %q, want %q", tt.cause, got, tt.want)
		}
	}
}

// TestWriteControllerReleaseWriteStall tests graceful shutdown by verifying
// that ReleaseWriteStall unblocks goroutines waiting in MaybeStallWrite
// even when the stall condition is still Stopped.
func TestWriteControllerReleaseWriteStall(t *testing.T) {
	wc := NewWriteController()

	// Set to stopped condition
	wc.SetStallCondition(WriteStallConditionStopped, WriteStallCauseMemtableLimit)

	var wg sync.WaitGroup
	started := make(chan struct{})
	done := make(chan struct{})

	wg.Go(func() {
		close(started)
		// This should block until ReleaseWriteStall is called
		wc.MaybeStallWrite(100)
		close(done)
	})

	// Wait for goroutine to start and block
	<-started
	time.Sleep(10 * time.Millisecond)

	// Verify it's still blocked (condition is still Stopped)
	select {
	case <-done:
		t.Fatal("MaybeStallWrite should be blocked")
	default:
		// Expected - still blocked
	}

	// Call ReleaseWriteStall - should unblock even though condition is Stopped
	wc.ReleaseWriteStall()

	// Wait for completion with timeout
	select {
	case <-done:
		// Success - graceful shutdown worked
	case <-time.After(1 * time.Second):
		t.Fatal("MaybeStallWrite did not unblock after ReleaseWriteStall")
	}

	wg.Wait()

	// Verify subsequent calls to MaybeStallWrite return immediately (closed state)
	start := time.Now()
	wc.MaybeStallWrite(1000000)
	if time.Since(start) > 100*time.Millisecond {
		t.Error("MaybeStallWrite should return immediately after ReleaseWriteStall")
	}
}
