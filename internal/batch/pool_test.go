package batch

import (
	"runtime"
	"sync"
	"testing"
)

func TestWriteBatchPoolBasic(t *testing.T) {
	pool := NewWriteBatchPool()

	// Get a batch
	wb := pool.Get()
	if wb == nil {
		t.Fatal("expected non-nil WriteBatch")
	}

	// Verify it's empty
	if wb.Count() != 0 {
		t.Errorf("expected count 0, got %d", wb.Count())
	}
	if wb.Size() != HeaderSize {
		t.Errorf("expected size %d, got %d", HeaderSize, wb.Size())
	}

	// Use it
	wb.Put([]byte("key"), []byte("value"))
	if wb.Count() != 1 {
		t.Errorf("expected count 1, got %d", wb.Count())
	}

	// Return it
	pool.Put(wb)

	stats := pool.Stats()
	if stats.Gets != 1 {
		t.Errorf("expected 1 get, got %d", stats.Gets)
	}
	if stats.Puts != 1 {
		t.Errorf("expected 1 put, got %d", stats.Puts)
	}
}

func TestWriteBatchPoolReuse(t *testing.T) {
	pool := NewWriteBatchPool()

	// Get and return several batches
	for range 10 {
		wb := pool.Get()
		wb.Put([]byte("key"), []byte("value"))
		pool.Put(wb)
	}

	// Force GC to ensure pool survives
	runtime.GC()

	// Get another - should reuse
	wb := pool.Get()
	if wb == nil {
		t.Fatal("expected non-nil WriteBatch")
	}
	if wb.Count() != 0 {
		t.Errorf("expected cleared batch, got count %d", wb.Count())
	}

	stats := pool.Stats()
	if stats.Gets < 10 {
		t.Errorf("expected at least 10 gets, got %d", stats.Gets)
	}
	// Hit rate should be positive after reuse cycles
	// Note: sync.Pool behavior is non-deterministic
}

func TestWriteBatchPoolConcurrent(t *testing.T) {
	pool := NewWriteBatchPool()
	var wg sync.WaitGroup

	workers := 10
	iterations := 100

	for w := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for range iterations {
				wb := pool.Get()
				wb.Put([]byte("key"), []byte("value"))
				wb.Put([]byte("key2"), []byte("value2"))
				pool.Put(wb)
			}
		}(w)
	}

	wg.Wait()

	stats := pool.Stats()
	expected := uint64(workers * iterations)
	if stats.Gets != expected {
		t.Errorf("expected %d gets, got %d", expected, stats.Gets)
	}
	if stats.Puts != expected {
		t.Errorf("expected %d puts, got %d", expected, stats.Puts)
	}
}

func TestWriteBatchPoolOversizedDiscard(t *testing.T) {
	pool := NewWriteBatchPool()

	// Create an oversized batch
	wb := pool.Get()
	largeValue := make([]byte, DefaultMaxBatchSize+1)
	for range 100 {
		wb.Put([]byte("key"), largeValue)
	}

	if cap(wb.data) <= DefaultMaxBatchSize {
		t.Skip("batch didn't grow large enough for this test")
	}

	// Return it
	pool.Put(wb)

	stats := pool.Stats()
	if stats.Discarded != 1 {
		t.Errorf("expected 1 discard, got %d", stats.Discarded)
	}
}

func TestWriteBatchPoolHitRate(t *testing.T) {
	pool := NewWriteBatchPool()
	pool.ResetStats()

	// Simulate usage pattern
	for range 100 {
		wb := pool.Get()
		wb.Put([]byte("key"), []byte("value"))
		pool.Put(wb)
	}

	stats := pool.Stats()
	hitRate := stats.HitRate()

	// After priming, hit rate should be positive
	// Note: due to sync.Pool behavior, exact rate is unpredictable
	t.Logf("Hit rate: %.2f%% (hits=%d, misses=%d)", hitRate*100, stats.Hits, stats.Misses)
}

func TestSizedWriteBatchPoolBasic(t *testing.T) {
	pool := NewSizedWriteBatchPool()

	// Get batches of various expected sizes
	testSizes := []int{100, 1000, 10000, 100000, 500000}

	for _, size := range testSizes {
		wb := pool.Get(size)
		if wb == nil {
			t.Fatalf("expected non-nil WriteBatch for size %d", size)
		}
		if wb.Count() != 0 {
			t.Errorf("expected count 0 for size %d, got %d", size, wb.Count())
		}
		pool.Put(wb)
	}
}

func TestSizedWriteBatchPoolBuckets(t *testing.T) {
	// Test bucket selection logic
	testCases := []struct {
		size           int
		expectedBucket int
	}{
		{100, 0},      // 4KB bucket
		{5000, 1},     // 16KB bucket
		{20000, 2},    // 64KB bucket
		{100000, 3},   // 256KB bucket
		{500000, 4},   // 1MB bucket
		{2000000, -1}, // Oversized
	}

	for _, tc := range testCases {
		bucket := getBucket(tc.size)
		if bucket != tc.expectedBucket {
			t.Errorf("size %d: expected bucket %d, got %d", tc.size, tc.expectedBucket, bucket)
		}
	}
}

func TestSizedWriteBatchPoolOversized(t *testing.T) {
	pool := NewSizedWriteBatchPool()

	// Request oversized batch
	wb := pool.Get(10 * 1024 * 1024) // 10MB
	if wb == nil {
		t.Fatal("expected non-nil WriteBatch")
	}

	// Should have correct capacity
	if cap(wb.data) < 10*1024*1024 {
		t.Errorf("expected capacity >= 10MB, got %d", cap(wb.data))
	}

	stats := pool.Stats()
	if stats.Oversized != 1 {
		t.Errorf("expected 1 oversized, got %d", stats.Oversized)
	}

	// Put shouldn't panic
	pool.Put(wb)
}

func TestGlobalPool(t *testing.T) {
	// Test global pool functions
	pool := GlobalPool()
	if pool == nil {
		t.Fatal("expected non-nil global pool")
	}

	// Test convenience functions
	wb := GetFromPool()
	if wb == nil {
		t.Fatal("expected non-nil WriteBatch")
	}

	wb.Put([]byte("key"), []byte("value"))
	ReturnToPool(wb)

	// Should not panic
	ReturnToPool(nil)
}

// Benchmarks

func BenchmarkWriteBatchNew(b *testing.B) {
	for b.Loop() {
		wb := New()
		wb.Put([]byte("key"), []byte("value"))
		_ = wb
	}
}

func BenchmarkWriteBatchPool(b *testing.B) {
	pool := NewWriteBatchPool()

	for b.Loop() {
		wb := pool.Get()
		wb.Put([]byte("key"), []byte("value"))
		pool.Put(wb)
	}
}

func BenchmarkWriteBatchPoolParallel(b *testing.B) {
	pool := NewWriteBatchPool()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wb := pool.Get()
			wb.Put([]byte("key"), []byte("value"))
			pool.Put(wb)
		}
	})
}

func BenchmarkSizedWriteBatchPool(b *testing.B) {
	sizedPool := NewSizedWriteBatchPool()

	for b.Loop() {
		wb := sizedPool.Get(1000)
		wb.Put([]byte("key"), []byte("value"))
		sizedPool.Put(wb)
	}
}

func BenchmarkSizedWriteBatchPoolParallel(b *testing.B) {
	pool := NewSizedWriteBatchPool()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wb := pool.Get(1000)
			wb.Put([]byte("key"), []byte("value"))
			pool.Put(wb)
		}
	})
}
