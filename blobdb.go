package rockyardkv

// blobdb.go implements BlobDB - integrated blob storage for large values.
//
// When a value exceeds the configured threshold (MinBlobSize), it's stored
// in a separate blob file and only a blob reference is stored in the LSM tree.
// This reduces write amplification for workloads with large values.
//
// Reference: RocksDB v10.7.5
//   - db/blob/blob_file_builder.h
//   - db/blob/blob_source.h
//   - include/rocksdb/advanced_options.h (blob_options)


import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aalhour/rockyardkv/internal/blob"
	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// BlobDBOptions configures BlobDB behavior
type BlobDBOptions struct {
	// Enable enables BlobDB (storing large values in separate files)
	Enable bool

	// MinBlobSize is the minimum value size to store in blob files
	// Values smaller than this are stored inline in the LSM tree
	MinBlobSize int

	// BlobFileSize is the target size for blob files
	BlobFileSize int64

	// BlobCompressionType is the compression algorithm for blob files
	BlobCompressionType compression.Type

	// EnableBlobGC enables garbage collection of unreferenced blobs
	EnableBlobGC bool

	// BlobGCAgeCutoff is the age cutoff for blob GC (0.0 to 1.0)
	// Blobs older than this fraction of the oldest file are candidates for GC
	BlobGCAgeCutoff float64
}

// DefaultBlobDBOptions returns default BlobDB options
func DefaultBlobDBOptions() BlobDBOptions {
	return BlobDBOptions{
		Enable:              false,
		MinBlobSize:         blob.DefaultMinBlobSize,
		BlobFileSize:        256 * 1024 * 1024, // 256MB
		BlobCompressionType: compression.NoCompression,
		EnableBlobGC:        true,
		BlobGCAgeCutoff:     0.25,
	}
}

// BlobFileManager manages blob files for a database
type BlobFileManager struct {
	mu sync.Mutex

	fs     vfs.FS
	dbPath string
	opts   BlobDBOptions

	// Current blob file being written
	currentWriter  *blob.Writer
	currentFile    vfs.WritableFile
	currentFileNum uint64

	// Blob cache for reading
	cache *blob.Cache

	// File number generator
	nextFileNum func() uint64

	// Statistics
	totalBlobsWritten uint64
	totalBytesWritten uint64
}

// NewBlobFileManager creates a new blob file manager
func NewBlobFileManager(fs vfs.FS, dbPath string, opts BlobDBOptions, nextFileNum func() uint64) *BlobFileManager {
	return &BlobFileManager{
		fs:          fs,
		dbPath:      dbPath,
		opts:        opts,
		nextFileNum: nextFileNum,
		cache:       blob.NewCache(fs, dbPath, blob.DefaultCacheOptions()),
	}
}

// ShouldStoreInBlob returns true if the value should be stored in a blob file
func (m *BlobFileManager) ShouldStoreInBlob(value []byte) bool {
	return m.opts.Enable && len(value) >= m.opts.MinBlobSize
}

// StoreBlob stores a value in a blob file and returns the blob index
func (m *BlobFileManager) StoreBlob(key, value []byte) ([]byte, error) {
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

// GetBlob retrieves a blob value given its index
func (m *BlobFileManager) GetBlob(indexData []byte) ([]byte, error) {
	idx, err := blob.DecodeBlobIndex(indexData)
	if err != nil {
		return nil, err
	}

	record, err := m.cache.Get(idx)
	if err != nil {
		return nil, err
	}

	return record.Value, nil
}

// shouldRollFile returns true if we should start a new blob file
func (m *BlobFileManager) shouldRollFile() bool {
	if m.currentWriter == nil {
		return true
	}
	return int64(m.currentWriter.FileSize()) >= m.opts.BlobFileSize
}

// rollFile creates a new blob file
func (m *BlobFileManager) rollFile() error {
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

	writer, err := blob.NewWriter(file, blob.WriterOptions{
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

// blobFilePath returns the path to a blob file
func (m *BlobFileManager) blobFilePath(fileNum uint64) string {
	return fmt.Sprintf("%s/%06d.blob", m.dbPath, fileNum)
}

// Flush ensures all pending blobs are written to disk by finishing the current file
func (m *BlobFileManager) Flush() error {
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

// Close closes the blob file manager
func (m *BlobFileManager) Close() error {
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

// Stats returns blob file manager statistics
func (m *BlobFileManager) Stats() (blobsWritten, bytesWritten uint64) {
	return atomic.LoadUint64(&m.totalBlobsWritten), atomic.LoadUint64(&m.totalBytesWritten)
}

// IsBlobValue checks if a value is a blob index (reference to blob file)
func IsBlobValue(value []byte) bool {
	return blob.IsBlobIndex(value)
}

// BlobGarbageCollector handles garbage collection of unreferenced blobs.
// Reference: RocksDB v10.7.5
//
//	db/blob/blob_garbage_meter.h - BlobGarbageMeter for tracking garbage
//	db/blob/blob_file_garbage.h - BlobFileGarbage for garbage metadata
//	db/compaction/compaction_job.cc - Auto-GC during compaction
type BlobGarbageCollector struct {
	mu     sync.Mutex
	fs     vfs.FS
	dbPath string

	// Set of blob file numbers that are referenced
	referencedFiles map[uint64]bool

	// Garbage tracking per file
	garbageBytes map[uint64]uint64 // file number -> garbage bytes
	garbageCount map[uint64]uint64 // file number -> garbage blob count
	totalBytes   map[uint64]uint64 // file number -> total bytes

	// Statistics
	totalGCRuns     uint64
	totalFilesFreed uint64
	totalBytesFreed uint64

	// Configuration
	ageCutoff       float64 // Fraction of oldest file age to use as cutoff
	garbageRatio    float64 // Ratio of garbage that triggers GC (0.0 to 1.0)
	enableAutoGC    bool
	minFilesToCheck int
}

// NewBlobGarbageCollector creates a new blob garbage collector.
func NewBlobGarbageCollector(fs vfs.FS, dbPath string) *BlobGarbageCollector {
	return &BlobGarbageCollector{
		fs:              fs,
		dbPath:          dbPath,
		referencedFiles: make(map[uint64]bool),
		garbageBytes:    make(map[uint64]uint64),
		garbageCount:    make(map[uint64]uint64),
		totalBytes:      make(map[uint64]uint64),
		ageCutoff:       0.25,
		garbageRatio:    0.5,
		enableAutoGC:    true,
		minFilesToCheck: 1,
	}
}

// SetOptions configures the garbage collector.
func (gc *BlobGarbageCollector) SetOptions(enableAutoGC bool, ageCutoff, garbageRatio float64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.enableAutoGC = enableAutoGC
	gc.ageCutoff = ageCutoff
	gc.garbageRatio = garbageRatio
}

// MarkReferenced marks a blob file as referenced.
func (gc *BlobGarbageCollector) MarkReferenced(indexData []byte) {
	idx, err := blob.DecodeBlobIndex(indexData)
	if err != nil {
		return
	}
	gc.mu.Lock()
	gc.referencedFiles[idx.FileNumber] = true
	gc.mu.Unlock()
}

// AddFileMetadata registers a blob file with its total size.
func (gc *BlobGarbageCollector) AddFileMetadata(fileNum uint64, totalBytes uint64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.totalBytes[fileNum] = totalBytes
}

// RecordGarbage records garbage (deleted/overwritten blobs) for a file.
// This is called during compaction when a blob reference is dropped.
func (gc *BlobGarbageCollector) RecordGarbage(fileNum uint64, blobSize uint64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.garbageBytes[fileNum] += blobSize
	gc.garbageCount[fileNum]++
}

// ShouldRunAutoGC returns true if auto-GC should run.
// It checks if any file exceeds the garbage ratio threshold.
func (gc *BlobGarbageCollector) ShouldRunAutoGC() bool {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	if !gc.enableAutoGC {
		return false
	}

	for fileNum, garbageSize := range gc.garbageBytes {
		totalSize := gc.totalBytes[fileNum]
		if totalSize == 0 {
			continue
		}
		ratio := float64(garbageSize) / float64(totalSize)
		if ratio >= gc.garbageRatio {
			return true
		}
	}
	return false
}

// GetGarbageRatio returns the garbage ratio for a specific file.
func (gc *BlobGarbageCollector) GetGarbageRatio(fileNum uint64) float64 {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	totalSize := gc.totalBytes[fileNum]
	if totalSize == 0 {
		return 0
	}
	return float64(gc.garbageBytes[fileNum]) / float64(totalSize)
}

// GetStatistics returns GC statistics.
func (gc *BlobGarbageCollector) GetStatistics() (runs, filesFreed, bytesFreed uint64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.totalGCRuns, gc.totalFilesFreed, gc.totalBytesFreed
}

// ResetReferences clears the referenced files map.
// This should be called before scanning all references in a compaction.
func (gc *BlobGarbageCollector) ResetReferences() {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.referencedFiles = make(map[uint64]bool)
}

// CollectGarbage removes unreferenced blob files.
// Returns the number of files deleted and total bytes freed.
// Reference: RocksDB v10.7.5 db/blob/blob_file_garbage.cc
func (gc *BlobGarbageCollector) CollectGarbage() (filesDeleted int, bytesFreed int64, err error) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	// List all blob files
	entries, err := gc.fs.ListDir(gc.dbPath)
	if err != nil {
		return 0, 0, err
	}

	for _, entry := range entries {
		// Check if it's a blob file
		if len(entry) < 5 || entry[len(entry)-5:] != ".blob" {
			continue
		}

		// Extract file number
		fileNum := parseFileNumber(entry[:len(entry)-5])
		if fileNum == 0 {
			continue
		}

		// Check if referenced
		if gc.referencedFiles[fileNum] {
			continue
		}

		// Delete unreferenced blob file
		path := gc.dbPath + "/" + entry
		info, err := gc.fs.Stat(path)
		if err != nil {
			continue
		}
		size := info.Size()

		if err := gc.fs.Remove(path); err != nil {
			continue
		}

		filesDeleted++
		bytesFreed += size

		// Clean up garbage tracking for deleted file
		delete(gc.garbageBytes, fileNum)
		delete(gc.garbageCount, fileNum)
		delete(gc.totalBytes, fileNum)
	}

	// Update statistics
	gc.totalGCRuns++
	gc.totalFilesFreed += uint64(filesDeleted)
	gc.totalBytesFreed += uint64(bytesFreed)

	return filesDeleted, bytesFreed, nil
}

// parseFileNumber parses a file number from a string like "000001"
func parseFileNumber(s string) uint64 {
	var n uint64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + uint64(c-'0')
	}
	return n
}
