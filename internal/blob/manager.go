package blob

// manager.go manages blob files for a database.
// Internal machinery - not part of the public API.
//
// Reference: RocksDB v10.7.5
//   - db/blob/blob_file_builder.h - BlobFileBuilder for writing blob files
//   - db/blob/blob_file_builder.cc - Implementation
//   - db/blob/blob_source.h - BlobSource for reading blob files

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/vfs"
)

// ManagerOptions configures the blob file manager.
type ManagerOptions struct {
	Enable              bool
	MinBlobSize         int
	BlobFileSize        int64
	BlobCompressionType compression.Type
	EnableBlobGC        bool
	BlobGCAgeCutoff     float64
}

// FileManager manages blob files for a database.
// This is internal machinery configured by the DB implementation.
type FileManager struct {
	mu sync.Mutex

	fs     vfs.FS
	dbPath string
	opts   ManagerOptions

	// Current blob file being written
	currentWriter  *Writer
	currentFile    vfs.WritableFile
	currentFileNum uint64

	// Blob cache for reading
	cache *Cache

	// File number generator
	nextFileNum func() uint64

	// Statistics
	totalBlobsWritten uint64
	totalBytesWritten uint64
}

// NewFileManager creates a new blob file manager.
func NewFileManager(fs vfs.FS, dbPath string, opts ManagerOptions, nextFileNum func() uint64) *FileManager {
	return &FileManager{
		fs:          fs,
		dbPath:      dbPath,
		opts:        opts,
		nextFileNum: nextFileNum,
		cache:       NewCache(fs, dbPath, DefaultCacheOptions()),
	}
}

// ShouldStoreInBlob returns true if the value should be stored in a blob file.
func (m *FileManager) ShouldStoreInBlob(value []byte) bool {
	return m.opts.Enable && len(value) >= m.opts.MinBlobSize
}

// StoreBlob stores a value in a blob file and returns the blob index.
func (m *FileManager) StoreBlob(key, value []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create new blob file if needed
	if m.currentWriter == nil || m.shouldRollFile() {
		if err := m.rollFile(); err != nil {
			return nil, err
		}
	}

	// Write the blob
	idx, err := m.currentWriter.AddBlob(key, value)
	if err != nil {
		return nil, err
	}
	idx.FileNumber = m.currentFileNum

	atomic.AddUint64(&m.totalBlobsWritten, 1)
	atomic.AddUint64(&m.totalBytesWritten, uint64(len(value)))

	// Return encoded blob index as the value to store in LSM
	return idx.Encode(), nil
}

// GetBlob retrieves a blob value given its index.
func (m *FileManager) GetBlob(indexData []byte) ([]byte, error) {
	idx, err := DecodeBlobIndex(indexData)
	if err != nil {
		return nil, err
	}

	record, err := m.cache.Get(idx)
	if err != nil {
		return nil, err
	}

	return record.Value, nil
}

// shouldRollFile returns true if we should start a new blob file.
func (m *FileManager) shouldRollFile() bool {
	if m.currentWriter == nil {
		return true
	}
	return int64(m.currentWriter.FileSize()) >= m.opts.BlobFileSize
}

// rollFile creates a new blob file.
func (m *FileManager) rollFile() error {
	// Close current file if any
	if m.currentWriter != nil {
		if err := m.currentWriter.Close(); err != nil {
			return err
		}
	}

	// Create new file
	fileNum := m.nextFileNum()
	path := m.blobFilePath(fileNum)

	file, err := m.fs.Create(path)
	if err != nil {
		return err
	}

	writer, err := NewWriter(file, WriterOptions{
		CompressionType: m.opts.BlobCompressionType,
	})
	if err != nil {
		_ = file.Close()
		return err
	}

	m.currentWriter = writer
	m.currentFile = file
	m.currentFileNum = fileNum

	return nil
}

// blobFilePath returns the path to a blob file.
func (m *FileManager) blobFilePath(fileNum uint64) string {
	return fmt.Sprintf("%s/%06d.blob", m.dbPath, fileNum)
}

// Flush ensures all pending blobs are written to disk by finishing the current file.
func (m *FileManager) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.currentWriter != nil {
		// Finish the current file so it can be read
		if err := m.currentWriter.Close(); err != nil {
			return err
		}
		m.currentWriter = nil
		m.currentFile = nil
	}
	return nil
}

// Close closes the blob file manager.
func (m *FileManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.currentWriter != nil {
		if err := m.currentWriter.Close(); err != nil {
			return err
		}
		m.currentWriter = nil
		m.currentFile = nil
	}

	return m.cache.Close()
}

// Stats returns blob file manager statistics.
func (m *FileManager) Stats() (blobsWritten, bytesWritten uint64) {
	return atomic.LoadUint64(&m.totalBlobsWritten), atomic.LoadUint64(&m.totalBytesWritten)
}
