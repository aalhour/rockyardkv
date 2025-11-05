// cache.go implements blob file caching for BlobDB.
//
// BlobFileCache in RocksDB caches open blob file readers to avoid
// repeated file opens for blob reads.
//
// Reference: RocksDB v10.7.5
//   - db/blob/blob_file_cache.h
//   - db/blob/blob_file_cache.cc
package blob

import (
	"sync"

	"github.com/aalhour/rockyardkv/internal/vfs"
)

// Cache caches blob file readers for efficient blob access.
type Cache struct {
	mu      sync.RWMutex
	fs      vfs.FS
	dbPath  string
	readers map[uint64]*Reader
	maxSize int
}

// CacheOptions configures the blob cache
type CacheOptions struct {
	MaxOpenFiles int
}

// DefaultCacheOptions returns default cache options
func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		MaxOpenFiles: 64,
	}
}

// NewCache creates a new blob cache
func NewCache(fs vfs.FS, dbPath string, opts CacheOptions) *Cache {
	return &Cache{
		fs:      fs,
		dbPath:  dbPath,
		readers: make(map[uint64]*Reader),
		maxSize: opts.MaxOpenFiles,
	}
}

// Get retrieves a blob from the cache, opening the blob file if necessary
func (c *Cache) Get(idx *BlobIndex) (*BlobRecord, error) {
	c.mu.RLock()
	reader, ok := c.readers[idx.FileNumber]
	c.mu.RUnlock()

	if !ok {
		var err error
		reader, err = c.openReader(idx.FileNumber)
		if err != nil {
			return nil, err
		}
	}

	return reader.GetBlob(idx)
}

// openReader opens a blob file reader and adds it to the cache
func (c *Cache) openReader(fileNumber uint64) (*Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if reader, ok := c.readers[fileNumber]; ok {
		return reader, nil
	}

	// Evict if at capacity
	if len(c.readers) >= c.maxSize {
		c.evictOne()
	}

	// Open the blob file
	path := c.blobFilePath(fileNumber)
	file, err := c.fs.OpenRandomAccess(path)
	if err != nil {
		return nil, err
	}

	reader, err := NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	c.readers[fileNumber] = reader
	return reader, nil
}

// blobFilePath returns the path to a blob file
func (c *Cache) blobFilePath(fileNumber uint64) string {
	return c.dbPath + "/" + blobFileName(fileNumber)
}

// blobFileName returns the name of a blob file
func blobFileName(fileNumber uint64) string {
	return formatFileNumber(fileNumber) + ".blob"
}

// formatFileNumber formats a file number as a 6-digit string
func formatFileNumber(n uint64) string {
	s := make([]byte, 6)
	for i := 5; i >= 0; i-- {
		s[i] = byte('0' + n%10)
		n /= 10
	}
	return string(s)
}

// evictOne evicts one reader from the cache (simple LRU-like)
func (c *Cache) evictOne() {
	// Simple eviction: just pick the first one
	for fileNumber, reader := range c.readers {
		_ = reader.Close()
		delete(c.readers, fileNumber)
		return
	}
}

// Evict removes a specific file from the cache
func (c *Cache) Evict(fileNumber uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if reader, ok := c.readers[fileNumber]; ok {
		_ = reader.Close()
		delete(c.readers, fileNumber)
	}
}

// Close closes all cached readers
func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, reader := range c.readers {
		_ = reader.Close()
	}
	c.readers = make(map[uint64]*Reader)
	return nil
}
