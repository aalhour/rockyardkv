package cache

import (
	"bytes"
	"sync"
	"testing"
)

// =============================================================================
// Basic LRU Cache Tests
// =============================================================================

func TestNewLRUCache(t *testing.T) {
	c := NewLRUCache(1024)
	if c == nil {
		t.Fatal("NewLRUCache returned nil")
	}
	if c.GetCapacity() != 1024 {
		t.Errorf("Capacity = %d, want 1024", c.GetCapacity())
	}
	if c.GetUsage() != 0 {
		t.Errorf("Usage = %d, want 0", c.GetUsage())
	}
	if c.GetOccupancyCount() != 0 {
		t.Errorf("OccupancyCount = %d, want 0", c.GetOccupancyCount())
	}
}

func TestLRUCacheInsertLookup(t *testing.T) {
	c := NewLRUCache(1024)

	key := CacheKey{FileNumber: 1, BlockOffset: 0}
	value := []byte("hello world")
	charge := uint64(len(value))

	h := c.Insert(key, value, charge)
	if h == nil {
		t.Fatal("Insert returned nil handle")
	}
	if !bytes.Equal(h.Value(), value) {
		t.Errorf("Handle value = %s, want %s", h.Value(), value)
	}

	c.Release(h)

	// Lookup should find it
	h2 := c.Lookup(key)
	if h2 == nil {
		t.Fatal("Lookup returned nil")
	}
	if !bytes.Equal(h2.Value(), value) {
		t.Errorf("Lookup value = %s, want %s", h2.Value(), value)
	}
	c.Release(h2)
}

func TestLRUCacheLookupMiss(t *testing.T) {
	c := NewLRUCache(1024)

	key := CacheKey{FileNumber: 999, BlockOffset: 999}
	h := c.Lookup(key)
	if h != nil {
		t.Error("Lookup should return nil for missing key")
	}
}

func TestLRUCacheErase(t *testing.T) {
	c := NewLRUCache(1024)

	key := CacheKey{FileNumber: 1, BlockOffset: 0}
	value := []byte("to be erased")
	charge := uint64(len(value))

	h := c.Insert(key, value, charge)
	c.Release(h)

	c.Erase(key)

	h2 := c.Lookup(key)
	if h2 != nil {
		t.Error("Lookup should return nil after Erase")
	}
}

func TestLRUCacheEviction(t *testing.T) {
	c := NewLRUCache(100) // Small capacity

	// Insert entries that exceed capacity
	for i := range uint64(10) {
		key := CacheKey{FileNumber: i, BlockOffset: 0}
		value := bytes.Repeat([]byte("x"), 20) // 20 bytes each
		h := c.Insert(key, value, 20)
		c.Release(h)
	}

	// Capacity is 100, each entry is 20 bytes
	// Should have at most 5 entries
	if c.GetUsage() > 100 {
		t.Errorf("Usage = %d, should be <= 100", c.GetUsage())
	}
	if c.GetOccupancyCount() > 5 {
		t.Errorf("OccupancyCount = %d, should be <= 5", c.GetOccupancyCount())
	}

	// Early entries should be evicted
	key0 := CacheKey{FileNumber: 0, BlockOffset: 0}
	h := c.Lookup(key0)
	if h != nil {
		t.Error("Entry 0 should have been evicted")
		c.Release(h)
	}

	// Later entries should still be there
	key9 := CacheKey{FileNumber: 9, BlockOffset: 0}
	h = c.Lookup(key9)
	if h == nil {
		t.Error("Entry 9 should still be in cache")
	} else {
		c.Release(h)
	}
}

func TestLRUCacheEvictionOrder(t *testing.T) {
	c := NewLRUCache(60) // Fits 3 entries of 20 bytes

	// Insert 3 entries
	for i := range uint64(3) {
		key := CacheKey{FileNumber: i, BlockOffset: 0}
		value := bytes.Repeat([]byte{byte(i)}, 20)
		h := c.Insert(key, value, 20)
		c.Release(h)
	}

	// Access entry 0 to make it recently used
	key0 := CacheKey{FileNumber: 0, BlockOffset: 0}
	h := c.Lookup(key0)
	if h == nil {
		t.Fatal("Entry 0 should be in cache")
	}
	c.Release(h)

	// Insert a new entry, should evict entry 1 (least recently used)
	key3 := CacheKey{FileNumber: 3, BlockOffset: 0}
	h = c.Insert(key3, bytes.Repeat([]byte("x"), 20), 20)
	c.Release(h)

	// Entry 0 should still be there (recently accessed)
	h = c.Lookup(key0)
	if h == nil {
		t.Error("Entry 0 should still be in cache")
	} else {
		c.Release(h)
	}

	// Entry 1 should be evicted
	key1 := CacheKey{FileNumber: 1, BlockOffset: 0}
	h = c.Lookup(key1)
	if h != nil {
		t.Error("Entry 1 should have been evicted")
		c.Release(h)
	}
}

func TestLRUCachePinnedNotEvicted(t *testing.T) {
	c := NewLRUCache(40) // Fits 2 entries of 20 bytes

	// Insert and keep pinned
	key0 := CacheKey{FileNumber: 0, BlockOffset: 0}
	h0 := c.Insert(key0, bytes.Repeat([]byte("0"), 20), 20)
	// Don't release h0

	// Insert another entry
	key1 := CacheKey{FileNumber: 1, BlockOffset: 0}
	h1 := c.Insert(key1, bytes.Repeat([]byte("1"), 20), 20)
	c.Release(h1)

	// Insert a third entry, should try to evict
	key2 := CacheKey{FileNumber: 2, BlockOffset: 0}
	h2 := c.Insert(key2, bytes.Repeat([]byte("2"), 20), 20)
	c.Release(h2)

	// Pinned entry should still be there
	if h0.deleted {
		t.Error("Pinned entry should not be deleted")
	}

	// Release the pinned entry
	c.Release(h0)
}

func TestLRUCacheUpdateExisting(t *testing.T) {
	c := NewLRUCache(1024)

	key := CacheKey{FileNumber: 1, BlockOffset: 0}

	// Insert initial value
	h1 := c.Insert(key, []byte("initial"), 7)
	c.Release(h1)

	// Update with new value
	h2 := c.Insert(key, []byte("updated"), 7)
	if !bytes.Equal(h2.Value(), []byte("updated")) {
		t.Errorf("Updated value = %s, want 'updated'", h2.Value())
	}
	c.Release(h2)

	// Lookup should return updated value
	h3 := c.Lookup(key)
	if !bytes.Equal(h3.Value(), []byte("updated")) {
		t.Errorf("Lookup after update = %s, want 'updated'", h3.Value())
	}
	c.Release(h3)

	// Should still have only 1 entry
	if c.GetOccupancyCount() != 1 {
		t.Errorf("OccupancyCount = %d, want 1", c.GetOccupancyCount())
	}
}

func TestLRUCacheSetCapacity(t *testing.T) {
	c := NewLRUCache(1000)

	// Insert some entries
	for i := range uint64(10) {
		key := CacheKey{FileNumber: i, BlockOffset: 0}
		h := c.Insert(key, bytes.Repeat([]byte("x"), 50), 50)
		c.Release(h)
	}

	// Reduce capacity
	c.SetCapacity(200)

	if c.GetCapacity() != 200 {
		t.Errorf("Capacity = %d, want 200", c.GetCapacity())
	}
	if c.GetUsage() > 200 {
		t.Errorf("Usage = %d, should be <= 200", c.GetUsage())
	}
}

func TestLRUCacheClose(t *testing.T) {
	c := NewLRUCache(1024)

	// Insert some entries
	for i := range uint64(5) {
		key := CacheKey{FileNumber: i, BlockOffset: 0}
		h := c.Insert(key, []byte("value"), 5)
		c.Release(h)
	}

	c.Close()

	if c.GetUsage() != 0 {
		t.Errorf("Usage after Close = %d, want 0", c.GetUsage())
	}
	if c.GetOccupancyCount() != 0 {
		t.Errorf("OccupancyCount after Close = %d, want 0", c.GetOccupancyCount())
	}
}

func TestLRUCacheHitMissStats(t *testing.T) {
	c := NewLRUCache(1024)

	// Lookup miss
	_ = c.Lookup(CacheKey{FileNumber: 1, BlockOffset: 0})
	if c.GetMissCount() != 1 {
		t.Errorf("MissCount = %d, want 1", c.GetMissCount())
	}

	// Insert and lookup hit
	key := CacheKey{FileNumber: 2, BlockOffset: 0}
	h := c.Insert(key, []byte("value"), 5)
	c.Release(h)

	h = c.Lookup(key)
	if h != nil {
		c.Release(h)
	}

	if c.GetHitCount() != 1 {
		t.Errorf("HitCount = %d, want 1", c.GetHitCount())
	}

	// Another hit
	h = c.Lookup(key)
	if h != nil {
		c.Release(h)
	}

	if c.GetHitCount() != 2 {
		t.Errorf("HitCount = %d, want 2", c.GetHitCount())
	}

	// Hit rate should be 2/3 = 0.666...
	rate := c.GetHitRate()
	if rate < 0.66 || rate > 0.67 {
		t.Errorf("HitRate = %f, want ~0.666", rate)
	}
}

func TestLRUCacheGetPinnedUsage(t *testing.T) {
	c := NewLRUCache(1024)

	// Insert and don't release
	key1 := CacheKey{FileNumber: 1, BlockOffset: 0}
	h1 := c.Insert(key1, []byte("pinned1"), 7)

	key2 := CacheKey{FileNumber: 2, BlockOffset: 0}
	h2 := c.Insert(key2, []byte("pinned2"), 7)

	// Insert and release
	key3 := CacheKey{FileNumber: 3, BlockOffset: 0}
	h3 := c.Insert(key3, []byte("unpinned"), 8)
	c.Release(h3)

	// Pinned usage should be 14 (7 + 7)
	if c.GetPinnedUsage() != 14 {
		t.Errorf("PinnedUsage = %d, want 14", c.GetPinnedUsage())
	}

	// Release one
	c.Release(h1)
	if c.GetPinnedUsage() != 7 {
		t.Errorf("PinnedUsage = %d, want 7", c.GetPinnedUsage())
	}

	// Release the other
	c.Release(h2)
	if c.GetPinnedUsage() != 0 {
		t.Errorf("PinnedUsage = %d, want 0", c.GetPinnedUsage())
	}
}

func TestLRUCacheEraseWhilePinned(t *testing.T) {
	c := NewLRUCache(1024)

	key := CacheKey{FileNumber: 1, BlockOffset: 0}
	h := c.Insert(key, []byte("value"), 5)

	// Erase while pinned
	c.Erase(key)

	// Should still have access through handle
	if h.Value() == nil {
		t.Error("Pinned handle should still have value")
	}

	// Lookup should fail
	h2 := c.Lookup(key)
	if h2 != nil {
		t.Error("Lookup should return nil after Erase")
	}

	// Release the pinned handle
	c.Release(h)
}

func TestLRUCacheZeroCapacity(t *testing.T) {
	c := NewLRUCache(0)

	key := CacheKey{FileNumber: 1, BlockOffset: 0}
	h := c.Insert(key, []byte("value"), 5)

	// Should still work - insert works even with zero capacity
	if h == nil {
		t.Error("Insert should return handle even with zero capacity")
	}

	c.Release(h)

	// With zero capacity, the entry may or may not be evicted immediately
	// (depends on implementation details). The key behavior is that Insert works.
	// Just verify no panic and the cache is consistent.
	_ = c.GetUsage() // Should not panic
}

func TestLRUCacheLargeEntry(t *testing.T) {
	c := NewLRUCache(1024)

	// Insert entry larger than capacity
	key := CacheKey{FileNumber: 1, BlockOffset: 0}
	value := bytes.Repeat([]byte("x"), 2048) // 2KB, larger than 1KB capacity
	h := c.Insert(key, value, 2048)

	// Should still work
	if h == nil {
		t.Error("Insert should return handle for large entry")
	}
	c.Release(h)

	// Usage might exceed capacity temporarily
	// But after release, it should try to evict
}

// =============================================================================
// Concurrent Tests
// =============================================================================

func TestLRUCacheConcurrentInsert(t *testing.T) {
	c := NewLRUCache(10000)
	var wg sync.WaitGroup

	numGoroutines := 10
	numInserts := 100

	for g := range numGoroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range numInserts {
				key := CacheKey{FileNumber: uint64(gid*1000 + i), BlockOffset: 0}
				h := c.Insert(key, []byte("value"), 10)
				c.Release(h)
			}
		}(g)
	}

	wg.Wait()

	// Should have some entries
	if c.GetOccupancyCount() == 0 {
		t.Error("Should have entries after concurrent inserts")
	}
}

func TestLRUCacheConcurrentLookup(t *testing.T) {
	c := NewLRUCache(10000)

	// Pre-populate
	for i := range uint64(100) {
		key := CacheKey{FileNumber: i, BlockOffset: 0}
		h := c.Insert(key, []byte("value"), 10)
		c.Release(h)
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	numLookups := 100

	for range numGoroutines {
		wg.Go(func() {
			for i := range numLookups {
				key := CacheKey{FileNumber: uint64(i % 100), BlockOffset: 0}
				h := c.Lookup(key)
				if h != nil {
					c.Release(h)
				}
			}
		})
	}

	wg.Wait()

	// Should have many hits
	if c.GetHitCount() == 0 {
		t.Error("Should have hits after concurrent lookups")
	}
}

func TestLRUCacheConcurrentMixed(t *testing.T) {
	c := NewLRUCache(5000)
	var wg sync.WaitGroup

	numGoroutines := 5

	// Inserters
	for g := range numGoroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range 50 {
				key := CacheKey{FileNumber: uint64(gid*100 + i), BlockOffset: 0}
				h := c.Insert(key, []byte("value"), 10)
				c.Release(h)
			}
		}(g)
	}

	// Readers
	for range numGoroutines {
		wg.Go(func() {
			for i := range 50 {
				key := CacheKey{FileNumber: uint64(i), BlockOffset: 0}
				h := c.Lookup(key)
				if h != nil {
					c.Release(h)
				}
			}
		})
	}

	// Erasers
	wg.Go(func() {
		for i := range 20 {
			key := CacheKey{FileNumber: uint64(i), BlockOffset: 0}
			c.Erase(key)
		}
	})

	wg.Wait()
}

// =============================================================================
// Sharded LRU Cache Tests
// =============================================================================

func TestNewShardedLRUCache(t *testing.T) {
	c := NewShardedLRUCache(1024, 4)
	if c == nil {
		t.Fatal("NewShardedLRUCache returned nil")
	}
	// Capacity per shard = 1024 / 4 = 256
	if c.GetCapacity() != 1024 {
		t.Errorf("Capacity = %d, want 1024", c.GetCapacity())
	}
}

func TestShardedLRUCacheInsertLookup(t *testing.T) {
	c := NewShardedLRUCache(1024, 4)

	key := CacheKey{FileNumber: 1, BlockOffset: 100}
	value := []byte("sharded value")
	charge := uint64(len(value))

	h := c.Insert(key, value, charge)
	if h == nil {
		t.Fatal("Insert returned nil")
	}
	c.Release(h)

	h2 := c.Lookup(key)
	if h2 == nil {
		t.Fatal("Lookup returned nil")
	}
	if !bytes.Equal(h2.Value(), value) {
		t.Errorf("Lookup value = %s, want %s", h2.Value(), value)
	}
	c.Release(h2)
}

func TestShardedLRUCacheErase(t *testing.T) {
	c := NewShardedLRUCache(1024, 4)

	key := CacheKey{FileNumber: 5, BlockOffset: 200}
	h := c.Insert(key, []byte("to erase"), 8)
	c.Release(h)

	c.Erase(key)

	h2 := c.Lookup(key)
	if h2 != nil {
		t.Error("Lookup should return nil after Erase")
	}
}

func TestShardedLRUCacheDistribution(t *testing.T) {
	c := NewShardedLRUCache(4000, 4)

	// Insert entries that should distribute across shards
	for i := range uint64(100) {
		key := CacheKey{FileNumber: i, BlockOffset: i * 100}
		h := c.Insert(key, []byte("value"), 10)
		c.Release(h)
	}

	// Check that entries are distributed (usage > 0)
	if c.GetUsage() == 0 {
		t.Error("Should have usage after inserts")
	}
	if c.GetOccupancyCount() == 0 {
		t.Error("Should have entries after inserts")
	}
}

func TestShardedLRUCacheSetCapacity(t *testing.T) {
	c := NewShardedLRUCache(1000, 4)

	// Insert some entries
	for i := range uint64(20) {
		key := CacheKey{FileNumber: i, BlockOffset: 0}
		h := c.Insert(key, bytes.Repeat([]byte("x"), 30), 30)
		c.Release(h)
	}

	c.SetCapacity(200)

	if c.GetUsage() > 200 {
		t.Errorf("Usage = %d, should be <= 200", c.GetUsage())
	}
}

func TestShardedLRUCacheClose(t *testing.T) {
	c := NewShardedLRUCache(1024, 4)

	for i := range uint64(10) {
		key := CacheKey{FileNumber: i, BlockOffset: 0}
		h := c.Insert(key, []byte("value"), 5)
		c.Release(h)
	}

	c.Close()

	if c.GetUsage() != 0 {
		t.Errorf("Usage after Close = %d, want 0", c.GetUsage())
	}
}

func TestShardedLRUCacheHitRate(t *testing.T) {
	c := NewShardedLRUCache(10000, 4)

	// Insert entries
	for i := range uint64(50) {
		key := CacheKey{FileNumber: i, BlockOffset: 0}
		h := c.Insert(key, []byte("value"), 10)
		c.Release(h)
	}

	// Lookup hits
	for i := range uint64(50) {
		key := CacheKey{FileNumber: i, BlockOffset: 0}
		h := c.Lookup(key)
		if h != nil {
			c.Release(h)
		}
	}

	// Lookup misses
	for i := uint64(100); i < 150; i++ {
		key := CacheKey{FileNumber: i, BlockOffset: 0}
		_ = c.Lookup(key)
	}

	// Hit rate should be 50/100 = 0.5
	rate := c.GetHitRate()
	if rate < 0.49 || rate > 0.51 {
		t.Errorf("HitRate = %f, want ~0.5", rate)
	}
}

func TestShardedLRUCacheConcurrent(t *testing.T) {
	c := NewShardedLRUCache(50000, 16)
	var wg sync.WaitGroup

	numGoroutines := 20
	numOps := 100

	for g := range numGoroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range numOps {
				key := CacheKey{FileNumber: uint64(gid*1000 + i), BlockOffset: uint64(i * 100)}
				h := c.Insert(key, []byte("concurrent value"), 15)
				c.Release(h)

				// Lookup
				h2 := c.Lookup(key)
				if h2 != nil {
					c.Release(h2)
				}
			}
		}(g)
	}

	wg.Wait()

	if c.GetOccupancyCount() == 0 {
		t.Error("Should have entries after concurrent operations")
	}
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func TestLRUCacheEmptyValue(t *testing.T) {
	c := NewLRUCache(1024)

	key := CacheKey{FileNumber: 1, BlockOffset: 0}
	h := c.Insert(key, []byte{}, 0)
	if h == nil {
		t.Fatal("Insert with empty value returned nil")
	}
	c.Release(h)

	h2 := c.Lookup(key)
	if h2 == nil {
		t.Fatal("Lookup returned nil for empty value")
	}
	if len(h2.Value()) != 0 {
		t.Errorf("Value length = %d, want 0", len(h2.Value()))
	}
	c.Release(h2)
}

func TestLRUCacheNilRelease(t *testing.T) {
	c := NewLRUCache(1024)
	// Should not panic
	c.Release(nil)
}

func TestCacheKeyEquality(t *testing.T) {
	key1 := CacheKey{FileNumber: 1, BlockOffset: 100}
	key2 := CacheKey{FileNumber: 1, BlockOffset: 100}
	key3 := CacheKey{FileNumber: 1, BlockOffset: 200}
	key4 := CacheKey{FileNumber: 2, BlockOffset: 100}

	if key1 != key2 {
		t.Error("key1 should equal key2")
	}
	if key1 == key3 {
		t.Error("key1 should not equal key3")
	}
	if key1 == key4 {
		t.Error("key1 should not equal key4")
	}
}

func TestHandleCharge(t *testing.T) {
	c := NewLRUCache(1024)

	key := CacheKey{FileNumber: 1, BlockOffset: 0}
	value := []byte("test value")
	h := c.Insert(key, value, 42)

	if h.Charge() != 42 {
		t.Errorf("Charge = %d, want 42", h.Charge())
	}

	c.Release(h)
}

func TestNextPowerOf2(t *testing.T) {
	tests := []struct {
		input int
		want  int
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{15, 16},
		{16, 16},
		{17, 32},
	}

	for _, tt := range tests {
		got := nextPowerOf2(tt.input)
		if got != tt.want {
			t.Errorf("nextPowerOf2(%d) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestShardedCacheDefaultShards(t *testing.T) {
	// Test with 0 shards (should default to 16)
	c := NewShardedLRUCache(1600, 0)
	if len(c.shards) != 16 {
		t.Errorf("Expected 16 shards, got %d", len(c.shards))
	}

	// Test with negative shards
	c = NewShardedLRUCache(1600, -5)
	if len(c.shards) != 16 {
		t.Errorf("Expected 16 shards for negative input, got %d", len(c.shards))
	}
}
