// Package table provides SST file reading and writing.
// This file implements TableCache for caching open SST file readers.
//
// Reference: RocksDB v10.7.5
//   - table/table_cache.h
//   - table/table_cache.cc

package table

import (
	"sync"

	"github.com/aalhour/rockyardkv/internal/vfs"
)

// TableCache caches open SST file readers to avoid repeatedly opening files.
// It uses an LRU-style eviction policy when the cache is full.
type TableCache struct {
	mu sync.RWMutex

	// Filesystem for opening files
	fs vfs.FS

	// Cache of open readers, keyed by file number
	cache map[uint64]*cachedReader

	// LRU list for eviction (most recently used at front)
	lruHead *cachedReader
	lruTail *cachedReader

	// Maximum number of open readers to cache
	maxSize int

	// Current number of cached readers
	size int

	// Reader options
	opts ReaderOptions
}

// cachedReader is a wrapper around a Reader with LRU tracking.
type cachedReader struct {
	fileNum uint64
	reader  *Reader

	// LRU list pointers
	prev *cachedReader
	next *cachedReader

	// Reference count (how many active users)
	refs int
}

// TableCacheOptions configures the TableCache.
type TableCacheOptions struct {
	// MaxOpenFiles is the maximum number of SST files to keep open.
	MaxOpenFiles int

	// VerifyChecksums enables checksum verification when reading blocks.
	VerifyChecksums bool
}

// DefaultTableCacheOptions returns default options.
func DefaultTableCacheOptions() TableCacheOptions {
	return TableCacheOptions{
		MaxOpenFiles:    1000,
		VerifyChecksums: true,
	}
}

// NewTableCache creates a new TableCache.
func NewTableCache(fs vfs.FS, opts TableCacheOptions) *TableCache {
	return &TableCache{
		fs:      fs,
		cache:   make(map[uint64]*cachedReader),
		maxSize: opts.MaxOpenFiles,
		opts: ReaderOptions{
			VerifyChecksums: opts.VerifyChecksums,
		},
	}
}

// Get returns a Reader for the given file. The caller must call Release()
// when done with the reader.
func (tc *TableCache) Get(fileNum uint64, path string) (*Reader, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Check if already cached
	if cr, ok := tc.cache[fileNum]; ok {
		cr.refs++
		tc.moveToFront(cr)
		return cr.reader, nil
	}

	// Not cached, open the file
	file, err := tc.fs.OpenRandomAccess(path)
	if err != nil {
		return nil, err
	}

	reader, err := Open(file, tc.opts)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	// Create cache entry
	cr := &cachedReader{
		fileNum: fileNum,
		reader:  reader,
		refs:    1,
	}

	// Add to cache
	tc.cache[fileNum] = cr
	tc.addToFront(cr)
	tc.size++

	// Evict if necessary
	tc.evictIfNeeded()

	return reader, nil
}

// Release decrements the reference count for a reader.
// The reader may be evicted from the cache if it has no more references.
func (tc *TableCache) Release(fileNum uint64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if cr, ok := tc.cache[fileNum]; ok {
		cr.refs--
	}
}

// Evict removes a specific file from the cache.
func (tc *TableCache) Evict(fileNum uint64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if cr, ok := tc.cache[fileNum]; ok {
		tc.remove(cr)
	}
}

// Close closes all cached readers and clears the cache.
func (tc *TableCache) Close() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for _, cr := range tc.cache {
		_ = cr.reader.Close()
	}
	tc.cache = make(map[uint64]*cachedReader)
	tc.lruHead = nil
	tc.lruTail = nil
	tc.size = 0

	return nil
}

// Size returns the current number of cached readers.
func (tc *TableCache) Size() int {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.size
}

// addToFront adds a cached reader to the front of the LRU list.
func (tc *TableCache) addToFront(cr *cachedReader) {
	cr.prev = nil
	cr.next = tc.lruHead

	if tc.lruHead != nil {
		tc.lruHead.prev = cr
	}
	tc.lruHead = cr

	if tc.lruTail == nil {
		tc.lruTail = cr
	}
}

// moveToFront moves a cached reader to the front of the LRU list.
func (tc *TableCache) moveToFront(cr *cachedReader) {
	if cr == tc.lruHead {
		return // Already at front
	}

	// Remove from current position
	if cr.prev != nil {
		cr.prev.next = cr.next
	}
	if cr.next != nil {
		cr.next.prev = cr.prev
	}
	if cr == tc.lruTail {
		tc.lruTail = cr.prev
	}

	// Add to front
	cr.prev = nil
	cr.next = tc.lruHead
	if tc.lruHead != nil {
		tc.lruHead.prev = cr
	}
	tc.lruHead = cr
}

// remove removes a cached reader from the cache and LRU list.
func (tc *TableCache) remove(cr *cachedReader) {
	// Remove from LRU list
	if cr.prev != nil {
		cr.prev.next = cr.next
	} else {
		tc.lruHead = cr.next
	}
	if cr.next != nil {
		cr.next.prev = cr.prev
	} else {
		tc.lruTail = cr.prev
	}

	// Remove from cache map
	delete(tc.cache, cr.fileNum)
	tc.size--

	// Close the reader
	_ = cr.reader.Close()
}

// evictIfNeeded evicts the least recently used entries if the cache is full.
func (tc *TableCache) evictIfNeeded() {
	for tc.size > tc.maxSize && tc.lruTail != nil {
		// Don't evict if still in use
		if tc.lruTail.refs > 0 {
			break
		}
		tc.remove(tc.lruTail)
	}
}

// NewIterator creates an iterator over an SST file.
func (tc *TableCache) NewIterator(fileNum uint64, path string) (*TableIterator, error) {
	reader, err := tc.Get(fileNum, path)
	if err != nil {
		return nil, err
	}

	iter := reader.NewIterator()
	// Note: The caller should release the reader when done with the iterator
	return iter, nil
}
