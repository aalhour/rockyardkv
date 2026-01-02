package blob

// gc.go handles garbage collection of unreferenced blobs.
// Internal machinery - not part of the public API.
//
// Reference: RocksDB v10.7.5
//   - db/blob/blob_garbage_meter.h - BlobGarbageMeter for tracking garbage
//   - db/blob/blob_file_garbage.h - BlobFileGarbage for garbage metadata
//   - db/blob/blob_counting_iterator.h - Iterator with garbage tracking
//   - db/compaction/compaction_job.cc - Auto-GC during compaction

import (
	"sync"

	"github.com/aalhour/rockyardkv/vfs"
)

// GarbageCollector handles garbage collection of unreferenced blobs.
type GarbageCollector struct {
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

// NewGarbageCollector creates a new blob garbage collector.
func NewGarbageCollector(fs vfs.FS, dbPath string) *GarbageCollector {
	return &GarbageCollector{
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
func (gc *GarbageCollector) SetOptions(enableAutoGC bool, ageCutoff, garbageRatio float64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.enableAutoGC = enableAutoGC
	gc.ageCutoff = ageCutoff
	gc.garbageRatio = garbageRatio
}

// MarkReferenced marks a blob file as referenced.
func (gc *GarbageCollector) MarkReferenced(indexData []byte) {
	idx, err := DecodeBlobIndex(indexData)
	if err != nil {
		return
	}
	gc.mu.Lock()
	gc.referencedFiles[idx.FileNumber] = true
	gc.mu.Unlock()
}

// AddFileMetadata registers a blob file with its total size.
func (gc *GarbageCollector) AddFileMetadata(fileNum uint64, totalBytes uint64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.totalBytes[fileNum] = totalBytes
}

// RecordGarbage records garbage (deleted/overwritten blobs) for a file.
// This is called during compaction when a blob reference is dropped.
func (gc *GarbageCollector) RecordGarbage(fileNum uint64, blobSize uint64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.garbageBytes[fileNum] += blobSize
	gc.garbageCount[fileNum]++
}

// ShouldRunAutoGC returns true if auto-GC should run.
// It checks if any file exceeds the garbage ratio threshold.
func (gc *GarbageCollector) ShouldRunAutoGC() bool {
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
func (gc *GarbageCollector) GetGarbageRatio(fileNum uint64) float64 {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	totalSize := gc.totalBytes[fileNum]
	if totalSize == 0 {
		return 0
	}
	return float64(gc.garbageBytes[fileNum]) / float64(totalSize)
}

// GetStatistics returns GC statistics.
func (gc *GarbageCollector) GetStatistics() (runs, filesFreed, bytesFreed uint64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.totalGCRuns, gc.totalFilesFreed, gc.totalBytesFreed
}

// ResetReferences clears the referenced files map.
// This should be called before scanning all references in a compaction.
func (gc *GarbageCollector) ResetReferences() {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.referencedFiles = make(map[uint64]bool)
}

// CollectGarbage removes unreferenced blob files.
// Returns the number of files deleted and total bytes freed.
// Reference: RocksDB v10.7.5 db/blob/blob_file_garbage.cc
func (gc *GarbageCollector) CollectGarbage() (filesDeleted int, bytesFreed int64, err error) {
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

// parseFileNumber parses a file number from a string like "000001".
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
