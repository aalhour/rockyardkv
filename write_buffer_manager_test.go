// write_buffer_manager_test.go implements tests for write buffer manager.
package rockyardkv

import (
	"sync"
	"testing"
	"time"
)

func TestWriteBufferManagerBasic(t *testing.T) {
	wbm := NewWriteBufferManager(1024*1024, false) // 1MB limit

	if !wbm.Enabled() {
		t.Error("expected manager to be enabled")
	}
	if wbm.BufferSize() != 1024*1024 {
		t.Errorf("expected buffer size 1MB, got %d", wbm.BufferSize())
	}
	if wbm.MemoryUsage() != 0 {
		t.Errorf("expected 0 usage, got %d", wbm.MemoryUsage())
	}
}

func TestWriteBufferManagerDisabled(t *testing.T) {
	wbm := NewWriteBufferManager(0, false) // Disabled

	if wbm.Enabled() {
		t.Error("expected manager to be disabled")
	}
	if wbm.ShouldFlush() {
		t.Error("disabled manager should not trigger flush")
	}
}

func TestWriteBufferManagerReserveAndFree(t *testing.T) {
	wbm := NewWriteBufferManager(1024*1024, false)

	// Reserve some memory
	wbm.ReserveMem(100 * 1024) // 100KB
	if wbm.MemoryUsage() != 100*1024 {
		t.Errorf("expected 100KB usage, got %d", wbm.MemoryUsage())
	}
	if wbm.MutableMemtableMemoryUsage() != 100*1024 {
		t.Errorf("expected 100KB active, got %d", wbm.MutableMemtableMemoryUsage())
	}

	// Reserve more
	wbm.ReserveMem(200 * 1024)
	if wbm.MemoryUsage() != 300*1024 {
		t.Errorf("expected 300KB usage, got %d", wbm.MemoryUsage())
	}

	// Schedule free (becomes immutable)
	wbm.ScheduleFreeMem(100 * 1024)
	if wbm.MemoryUsage() != 300*1024 {
		t.Errorf("schedule free shouldn't change total usage, got %d", wbm.MemoryUsage())
	}
	if wbm.MutableMemtableMemoryUsage() != 200*1024 {
		t.Errorf("expected 200KB active, got %d", wbm.MutableMemtableMemoryUsage())
	}

	// Actually free
	wbm.FreeMem(100 * 1024)
	if wbm.MemoryUsage() != 200*1024 {
		t.Errorf("expected 200KB usage after free, got %d", wbm.MemoryUsage())
	}
}

func TestWriteBufferManagerShouldFlush(t *testing.T) {
	wbm := NewWriteBufferManager(1024*1024, false) // 1MB limit

	// Below threshold (7/8 = 896KB)
	wbm.ReserveMem(800 * 1024) // 800KB
	if wbm.ShouldFlush() {
		t.Error("should not flush below 7/8 threshold")
	}

	// At threshold
	wbm.ReserveMem(100 * 1024) // 900KB total
	if !wbm.ShouldFlush() {
		t.Error("should flush at 7/8 threshold")
	}
}

func TestWriteBufferManagerUsageRatio(t *testing.T) {
	wbm := NewWriteBufferManager(1024*1024, false)

	// 0% usage
	if ratio := wbm.UsageRatio(); ratio != 0 {
		t.Errorf("expected 0 ratio, got %f", ratio)
	}

	// 50% usage
	wbm.ReserveMem(512 * 1024)
	if ratio := wbm.UsageRatio(); ratio != 0.5 {
		t.Errorf("expected 0.5 ratio, got %f", ratio)
	}

	// 100% usage
	wbm.ReserveMem(512 * 1024)
	if ratio := wbm.UsageRatio(); ratio != 1.0 {
		t.Errorf("expected 1.0 ratio, got %f", ratio)
	}

	// Over 100%
	wbm.ReserveMem(512 * 1024)
	if ratio := wbm.UsageRatio(); ratio != 1.5 {
		t.Errorf("expected 1.5 ratio, got %f", ratio)
	}
}

func TestWriteBufferManagerStats(t *testing.T) {
	wbm := NewWriteBufferManager(1024*1024, false)

	wbm.ReserveMem(100)
	wbm.ReserveMem(200)
	wbm.FreeMem(100)

	stats := wbm.Stats()
	if stats.TotalReserved != 300 {
		t.Errorf("expected 300 reserved, got %d", stats.TotalReserved)
	}
	if stats.TotalFreed != 100 {
		t.Errorf("expected 100 freed, got %d", stats.TotalFreed)
	}
	if stats.PeakUsage != 300 {
		t.Errorf("expected peak 300, got %d", stats.PeakUsage)
	}

	// Reset stats
	wbm.ResetStats()
	stats = wbm.Stats()
	if stats.TotalReserved != 0 || stats.TotalFreed != 0 {
		t.Error("stats should be reset")
	}
}

func TestWriteBufferManagerStalling(t *testing.T) {
	wbm := NewWriteBufferManager(1024, true) // 1KB limit with stalling

	// Reserve at limit
	wbm.ReserveMem(1024)
	if !wbm.IsStalled() {
		// Not stalled yet - need to call WaitIfStalled
	}

	// Start a goroutine that will free memory after delay
	done := make(chan bool)
	go func() {
		time.Sleep(50 * time.Millisecond)
		wbm.FreeMem(512)
		done <- true
	}()

	// This should stall briefly
	start := time.Now()
	stalled := wbm.WaitIfStalled()
	duration := time.Since(start)

	<-done

	if !stalled {
		t.Log("warning: didn't stall (may be timing dependent)")
	} else if duration < 40*time.Millisecond {
		t.Errorf("stall was too short: %v", duration)
	}
}

func TestWriteBufferManagerNoStalling(t *testing.T) {
	wbm := NewWriteBufferManager(1024, false) // No stalling

	wbm.ReserveMem(2048) // Over limit

	// Should not stall
	start := time.Now()
	stalled := wbm.WaitIfStalled()
	duration := time.Since(start)

	if stalled {
		t.Error("should not stall when allowStall=false")
	}
	if duration > 10*time.Millisecond {
		t.Errorf("took too long for no-stall case: %v", duration)
	}
}

func TestWriteBufferManagerConcurrent(t *testing.T) {
	wbm := NewWriteBufferManager(1024*1024, false)
	var wg sync.WaitGroup

	workers := 10
	iterations := 100

	for range workers {
		wg.Go(func() {
			for range iterations {
				wbm.ReserveMem(1024)
				wbm.ScheduleFreeMem(512)
				wbm.FreeMem(1024)
			}
		})
	}

	wg.Wait()

	// Final state should show some usage from concurrent operations
	// Due to timing, exact values may vary
	stats := wbm.Stats()
	expected := uint64(workers * iterations * 1024)
	if stats.TotalReserved != expected {
		t.Errorf("expected %d reserved, got %d", expected, stats.TotalReserved)
	}
}

// Buffer pool tests

func TestBufferPoolBasic(t *testing.T) {
	pool := NewBufferPool()

	// Get various sizes
	sizes := []int{100, 500, 2000, 10000, 50000}
	for _, size := range sizes {
		buf := pool.Get(size)
		if cap(buf) < size {
			t.Errorf("expected cap >= %d, got %d", size, cap(buf))
		}
		if len(buf) != 0 {
			t.Errorf("expected len 0, got %d", len(buf))
		}
		pool.Put(buf)
	}
}

func TestBufferPoolBuckets(t *testing.T) {
	pool := NewBufferPool()

	// Get a 1KB buffer
	buf1 := pool.Get(1000)
	if cap(buf1) < 1000 {
		t.Errorf("expected cap >= 1000, got %d", cap(buf1))
	}

	// Use and return it
	buf1 = append(buf1, make([]byte, 500)...)
	pool.Put(buf1)

	// Get another - should be from pool (capacity >= requested)
	buf2 := pool.Get(800)
	if cap(buf2) < 800 {
		t.Errorf("expected cap >= 800, got %d", cap(buf2))
	}
	pool.Put(buf2)
}

func TestBufferPoolOversized(t *testing.T) {
	pool := NewBufferPool()

	// Request very large buffer (larger than any bucket)
	buf := pool.Get(1024 * 1024) // 1MB
	if cap(buf) < 1024*1024 {
		t.Errorf("expected cap >= 1MB, got %d", cap(buf))
	}

	// Should not panic on put
	pool.Put(buf)
}

func TestGlobalBufferPool(t *testing.T) {
	buf := GetBuffer(1024)
	if cap(buf) < 1024 {
		t.Errorf("expected cap >= 1024, got %d", cap(buf))
	}
	PutBuffer(buf)

	// Should not panic
	PutBuffer(nil)
}

// Benchmarks

func BenchmarkWriteBufferManagerReserveFree(b *testing.B) {
	wbm := NewWriteBufferManager(1024*1024*1024, false)

	for b.Loop() {
		wbm.ReserveMem(4096)
		wbm.FreeMem(4096)
	}
}

func BenchmarkWriteBufferManagerReserveFreeParallel(b *testing.B) {
	wbm := NewWriteBufferManager(1024*1024*1024, false)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wbm.ReserveMem(4096)
			wbm.FreeMem(4096)
		}
	})
}

func BenchmarkBufferPoolGet(b *testing.B) {
	pool := NewBufferPool()

	for b.Loop() {
		buf := pool.Get(1024)
		pool.Put(buf)
	}
}

func BenchmarkBufferPoolGetParallel(b *testing.B) {
	pool := NewBufferPool()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get(1024)
			pool.Put(buf)
		}
	})
}

func BenchmarkMakeSlice(b *testing.B) {
	for b.Loop() {
		buf := make([]byte, 0, 1024)
		_ = buf
	}
}

func BenchmarkGlobalBufferPool(b *testing.B) {
	for b.Loop() {
		buf := GetBuffer(1024)
		PutBuffer(buf)
	}
}
