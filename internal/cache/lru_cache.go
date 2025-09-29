// Package cache provides caching implementations for RockyardKV.
//
// This package includes an LRU (Least Recently Used) block cache that is used
// to cache SST file data blocks and index blocks, reducing disk I/O and
// improving read performance.
//
// Reference: RocksDB v10.7.5
//   - cache/lru_cache.h
//   - cache/lru_cache.cc
package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// Cache is the interface for all cache implementations.
type Cache interface {
	// Insert adds a block to the cache. If the key already exists, it updates the value.
	// Returns the handle to the cached block.
	Insert(key CacheKey, value []byte, charge uint64) *Handle

	// Lookup retrieves a block from the cache.
	// Returns nil if not found.
	Lookup(key CacheKey) *Handle

	// Release releases a handle obtained from Insert or Lookup.
	// The caller must call Release when done using the handle.
	Release(handle *Handle)

	// Erase removes a key from the cache.
	Erase(key CacheKey)

	// SetCapacity sets the maximum capacity of the cache.
	SetCapacity(capacity uint64)

	// GetCapacity returns the maximum capacity of the cache.
	GetCapacity() uint64

	// GetUsage returns the current usage of the cache.
	GetUsage() uint64

	// GetPinnedUsage returns the usage of currently pinned entries.
	GetPinnedUsage() uint64

	// GetOccupancyCount returns the number of entries in the cache.
	GetOccupancyCount() uint64

	// Close releases all resources associated with the cache.
	Close()
}

// CacheKey uniquely identifies a cached block.
type CacheKey struct {
	FileNumber  uint64
	BlockOffset uint64
}

// Handle represents a reference to a cached block.
type Handle struct {
	key     CacheKey
	value   []byte
	charge  uint64
	refs    int32
	deleted bool
}

// Value returns the cached block data.
func (h *Handle) Value() []byte {
	return h.value
}

// Charge returns the memory charge of this entry.
func (h *Handle) Charge() uint64 {
	return h.charge
}

// =============================================================================
// LRU Cache Implementation
// =============================================================================

// LRUCache is a thread-safe LRU cache with a fixed capacity.
type LRUCache struct {
	mu       sync.RWMutex
	capacity uint64
	usage    uint64
	table    map[CacheKey]*list.Element
	lru      *list.List // For eviction ordering

	// Statistics
	hits   atomic.Uint64
	misses atomic.Uint64
}

// lruEntry is the entry stored in the LRU list.
type lruEntry struct {
	handle *Handle
}

// getEntry safely extracts an lruEntry from a list element.
// The type assertion is safe because the list only ever stores *lruEntry.
func getEntry(elem *list.Element) *lruEntry {
	entry, _ := elem.Value.(*lruEntry)
	return entry
}

// NewLRUCache creates a new LRU cache with the given capacity in bytes.
func NewLRUCache(capacity uint64) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		table:    make(map[CacheKey]*list.Element),
		lru:      list.New(),
	}
}

// Insert adds a block to the cache.
func (c *LRUCache) Insert(key CacheKey, value []byte, charge uint64) *Handle {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if key already exists
	if elem, ok := c.table[key]; ok {
		entry := getEntry(elem)
		// Update the value and move to front
		c.usage -= entry.handle.charge
		entry.handle.value = value
		entry.handle.charge = charge
		c.usage += charge
		c.lru.MoveToFront(elem)
		entry.handle.refs++
		return entry.handle
	}

	// Create new handle
	handle := &Handle{
		key:    key,
		value:  value,
		charge: charge,
		refs:   1,
	}

	// Evict entries if needed
	for c.usage+charge > c.capacity && c.lru.Len() > 0 {
		c.evictOne()
	}

	// Insert new entry
	entry := &lruEntry{handle: handle}
	elem := c.lru.PushFront(entry)
	c.table[key] = elem
	c.usage += charge

	return handle
}

// Lookup retrieves a block from the cache.
func (c *LRUCache) Lookup(key CacheKey) *Handle {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.table[key]; ok {
		entry := getEntry(elem)
		if !entry.handle.deleted {
			// Move to front (recently used)
			c.lru.MoveToFront(elem)
			entry.handle.refs++
			c.hits.Add(1)
			return entry.handle
		}
	}

	c.misses.Add(1)
	return nil
}

// Release releases a handle.
func (c *LRUCache) Release(handle *Handle) {
	if handle == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	handle.refs--
	if handle.refs == 0 && handle.deleted {
		// Actually remove it now
		c.removeHandle(handle)
	}
}

// Erase removes a key from the cache.
func (c *LRUCache) Erase(key CacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.table[key]; ok {
		entry := getEntry(elem)
		entry.handle.deleted = true

		if entry.handle.refs == 0 {
			c.removeHandle(entry.handle)
		}
	}
}

// SetCapacity sets the maximum capacity.
func (c *LRUCache) SetCapacity(capacity uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.capacity = capacity

	// Evict if over capacity
	for c.usage > c.capacity && c.lru.Len() > 0 {
		c.evictOne()
	}
}

// GetCapacity returns the maximum capacity.
func (c *LRUCache) GetCapacity() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.capacity
}

// GetUsage returns the current usage.
func (c *LRUCache) GetUsage() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.usage
}

// GetPinnedUsage returns the usage of currently pinned entries.
func (c *LRUCache) GetPinnedUsage() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var pinned uint64
	for _, elem := range c.table {
		entry := getEntry(elem)
		if entry.handle.refs > 0 {
			pinned += entry.handle.charge
		}
	}
	return pinned
}

// GetOccupancyCount returns the number of entries.
func (c *LRUCache) GetOccupancyCount() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return uint64(len(c.table))
}

// Close releases all resources.
func (c *LRUCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.table = make(map[CacheKey]*list.Element)
	c.lru.Init()
	c.usage = 0
}

// GetHitCount returns the number of cache hits.
func (c *LRUCache) GetHitCount() uint64 {
	return c.hits.Load()
}

// GetMissCount returns the number of cache misses.
func (c *LRUCache) GetMissCount() uint64 {
	return c.misses.Load()
}

// GetHitRate returns the cache hit rate (0.0 to 1.0).
func (c *LRUCache) GetHitRate() float64 {
	hits := c.hits.Load()
	misses := c.misses.Load()
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return float64(hits) / float64(total)
}

// evictOne evicts the least recently used entry that is not pinned.
// Must be called with mu held.
func (c *LRUCache) evictOne() {
	// Start from the back (least recently used)
	for e := c.lru.Back(); e != nil; e = e.Prev() {
		entry := getEntry(e)
		if entry.handle.refs == 0 && !entry.handle.deleted {
			c.removeEntry(e)
			return
		}
	}
}

// removeEntry removes an entry from the cache.
// Must be called with mu held.
func (c *LRUCache) removeEntry(elem *list.Element) {
	entry := getEntry(elem)
	delete(c.table, entry.handle.key)
	c.lru.Remove(elem)
	c.usage -= entry.handle.charge
}

// removeHandle removes a handle that has been marked deleted.
// Must be called with mu held.
func (c *LRUCache) removeHandle(handle *Handle) {
	if elem, ok := c.table[handle.key]; ok {
		c.removeEntry(elem)
	}
}

// =============================================================================
// Sharded LRU Cache (for better concurrency)
// =============================================================================

// ShardedLRUCache is an LRU cache with multiple shards for reduced lock contention.
type ShardedLRUCache struct {
	shards    []*LRUCache
	numShards uint64
}

// NewShardedLRUCache creates a new sharded LRU cache.
// numShards should be a power of 2 for best performance.
func NewShardedLRUCache(capacity uint64, numShards int) *ShardedLRUCache {
	if numShards <= 0 {
		numShards = 16 // Default
	}

	// Round up to power of 2
	numShards = nextPowerOf2(numShards)

	shardCapacity := capacity / uint64(numShards)
	if shardCapacity == 0 {
		shardCapacity = 1
	}

	c := &ShardedLRUCache{
		shards:    make([]*LRUCache, numShards),
		numShards: uint64(numShards),
	}

	for i := range numShards {
		c.shards[i] = NewLRUCache(shardCapacity)
	}

	return c
}

func nextPowerOf2(n int) int {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

func (c *ShardedLRUCache) shard(key CacheKey) *LRUCache {
	// Simple hash based on file number and offset
	h := key.FileNumber ^ (key.BlockOffset * 0x9E3779B9)
	return c.shards[h%c.numShards]
}

// Insert adds a block to the cache.
func (c *ShardedLRUCache) Insert(key CacheKey, value []byte, charge uint64) *Handle {
	return c.shard(key).Insert(key, value, charge)
}

// Lookup retrieves a block from the cache.
func (c *ShardedLRUCache) Lookup(key CacheKey) *Handle {
	return c.shard(key).Lookup(key)
}

// Release releases a handle.
func (c *ShardedLRUCache) Release(handle *Handle) {
	if handle == nil {
		return
	}
	c.shard(handle.key).Release(handle)
}

// Erase removes a key from the cache.
func (c *ShardedLRUCache) Erase(key CacheKey) {
	c.shard(key).Erase(key)
}

// SetCapacity sets the maximum capacity.
func (c *ShardedLRUCache) SetCapacity(capacity uint64) {
	shardCapacity := capacity / c.numShards
	if shardCapacity == 0 {
		shardCapacity = 1
	}
	for _, s := range c.shards {
		s.SetCapacity(shardCapacity)
	}
}

// GetCapacity returns the maximum capacity.
func (c *ShardedLRUCache) GetCapacity() uint64 {
	var total uint64
	for _, s := range c.shards {
		total += s.GetCapacity()
	}
	return total
}

// GetUsage returns the current usage.
func (c *ShardedLRUCache) GetUsage() uint64 {
	var total uint64
	for _, s := range c.shards {
		total += s.GetUsage()
	}
	return total
}

// GetPinnedUsage returns the usage of currently pinned entries.
func (c *ShardedLRUCache) GetPinnedUsage() uint64 {
	var total uint64
	for _, s := range c.shards {
		total += s.GetPinnedUsage()
	}
	return total
}

// GetOccupancyCount returns the number of entries.
func (c *ShardedLRUCache) GetOccupancyCount() uint64 {
	var total uint64
	for _, s := range c.shards {
		total += s.GetOccupancyCount()
	}
	return total
}

// Close releases all resources.
func (c *ShardedLRUCache) Close() {
	for _, s := range c.shards {
		s.Close()
	}
}

// GetHitCount returns the total number of cache hits.
func (c *ShardedLRUCache) GetHitCount() uint64 {
	var total uint64
	for _, s := range c.shards {
		total += s.GetHitCount()
	}
	return total
}

// GetMissCount returns the total number of cache misses.
func (c *ShardedLRUCache) GetMissCount() uint64 {
	var total uint64
	for _, s := range c.shards {
		total += s.GetMissCount()
	}
	return total
}

// GetHitRate returns the overall cache hit rate.
func (c *ShardedLRUCache) GetHitRate() float64 {
	hits := c.GetHitCount()
	misses := c.GetMissCount()
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return float64(hits) / float64(total)
}
