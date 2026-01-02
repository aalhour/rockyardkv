package mempool

// pool_test.go tests the buffer pool implementation.

import "testing"

func TestPoolBasic(t *testing.T) {
	pool := NewPool()

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

func TestPoolBuckets(t *testing.T) {
	pool := NewPool()

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

func TestPoolOversized(t *testing.T) {
	pool := NewPool()

	// Request very large buffer (larger than any bucket)
	buf := pool.Get(1024 * 1024) // 1MB
	if cap(buf) < 1024*1024 {
		t.Errorf("expected cap >= 1MB, got %d", cap(buf))
	}

	// Should not panic on put
	pool.Put(buf)
}

func TestPoolNilPut(t *testing.T) {
	pool := NewPool()

	// Should not panic
	pool.Put(nil)
}

func BenchmarkPoolGet(b *testing.B) {
	pool := NewPool()

	for b.Loop() {
		buf := pool.Get(1024)
		pool.Put(buf)
	}
}

func BenchmarkPoolGetParallel(b *testing.B) {
	pool := NewPool()
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
