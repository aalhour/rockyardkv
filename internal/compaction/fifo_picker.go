// Package compaction implements compaction strategies for the LSM-tree.
//
// FIFO compaction is designed for time-series workloads where old data
// can be dropped. It simply deletes the oldest SST files when the database
// exceeds a configured size or when files exceed a TTL.
//
// Reference: RocksDB v10.7.5
//   - db/compaction/compaction_picker_fifo.cc
//   - include/rocksdb/options.h (FIFOCompactionOptions)
package compaction

import (
	"sort"
	"time"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/version"
)

// FIFOCompactionOptions contains options for FIFO compaction.
type FIFOCompactionOptions struct {
	// MaxTableFilesSize is the maximum total size of all SST files.
	// Once exceeded, the oldest files are deleted.
	// Default: 1GB
	MaxTableFilesSize uint64

	// TTL is the time-to-live for SST files. Files older than this
	// are deleted regardless of total size.
	// Default: 0 (disabled)
	TTL time.Duration

	// AllowCompaction allows FIFO to also do intra-L0 compaction
	// to reduce the number of L0 files.
	// Default: false
	AllowCompaction bool
}

// DefaultFIFOCompactionOptions returns default FIFO compaction options.
func DefaultFIFOCompactionOptions() *FIFOCompactionOptions {
	return &FIFOCompactionOptions{
		MaxTableFilesSize: 1 << 30, // 1GB
		TTL:               0,       // Disabled
		AllowCompaction:   false,
	}
}

// FIFOCompactionPicker implements FIFO compaction strategy.
type FIFOCompactionPicker struct {
	opts *FIFOCompactionOptions
	now  func() time.Time // For testing
}

// NewFIFOCompactionPicker creates a new FIFO compaction picker.
func NewFIFOCompactionPicker(opts *FIFOCompactionOptions) *FIFOCompactionPicker {
	if opts == nil {
		opts = DefaultFIFOCompactionOptions()
	}
	return &FIFOCompactionPicker{
		opts: opts,
		now:  time.Now,
	}
}

// NeedsCompaction returns true if files should be dropped.
func (p *FIFOCompactionPicker) NeedsCompaction(v *version.Version) bool {
	totalSize := p.getTotalSize(v)

	// Check size limit
	if totalSize > p.opts.MaxTableFilesSize {
		return true
	}

	// Check TTL
	if p.opts.TTL > 0 {
		if p.findExpiredFiles(v) != nil {
			return true
		}
	}

	return false
}

// PickCompaction selects files to delete (represented as a "delete" compaction).
func (p *FIFOCompactionPicker) PickCompaction(v *version.Version) *Compaction {
	// Priority 1: Delete expired files (TTL)
	if p.opts.TTL > 0 {
		if expired := p.findExpiredFiles(v); len(expired) > 0 {
			return p.createDeleteCompaction(expired)
		}
	}

	// Priority 2: Delete oldest files if over size limit
	totalSize := p.getTotalSize(v)
	if totalSize > p.opts.MaxTableFilesSize {
		return p.pickSizeCompaction(v, totalSize)
	}

	// Priority 3: Intra-L0 compaction (if enabled)
	if p.opts.AllowCompaction {
		return p.pickIntraL0Compaction(v)
	}

	return nil
}

// getTotalSize returns the total size of all SST files.
func (p *FIFOCompactionPicker) getTotalSize(v *version.Version) uint64 {
	var total uint64
	for level := range version.MaxNumLevels {
		for _, f := range v.Files(level) {
			total += f.FD.FileSize
		}
	}
	return total
}

// sortedFile wraps a file with its creation time for sorting.
type sortedFile struct {
	file        *manifest.FileMetaData
	level       int
	createdTime uint64 // Unix timestamp in seconds
}

// getAllFilesSortedByAge returns all files sorted by creation time (oldest first).
func (p *FIFOCompactionPicker) getAllFilesSortedByAge(v *version.Version) []*sortedFile {
	var files []*sortedFile

	for level := range version.MaxNumLevels {
		for _, f := range v.Files(level) {
			if f.BeingCompacted {
				continue
			}
			// Use file creation time if available, otherwise estimate from seqno
			createdTime := f.FileCreationTime
			if createdTime == 0 || createdTime == manifest.UnknownFileCreationTime {
				// Fallback: older sequence numbers = older files
				// Use seqno as approximation (not ideal but functional)
				createdTime = uint64(f.FD.SmallestSeqno)
			}
			files = append(files, &sortedFile{
				file:        f,
				level:       level,
				createdTime: createdTime,
			})
		}
	}

	// Sort oldest first
	sort.Slice(files, func(i, j int) bool {
		return files[i].createdTime < files[j].createdTime
	})

	return files
}

// findExpiredFiles returns files that have exceeded the TTL.
func (p *FIFOCompactionPicker) findExpiredFiles(v *version.Version) []*sortedFile {
	if p.opts.TTL <= 0 {
		return nil
	}

	cutoff := uint64(p.now().Add(-p.opts.TTL).Unix())
	files := p.getAllFilesSortedByAge(v)

	var expired []*sortedFile
	for _, f := range files {
		if f.createdTime < cutoff {
			expired = append(expired, f)
		}
	}

	return expired
}

// pickSizeCompaction picks oldest files to delete to get under size limit.
func (p *FIFOCompactionPicker) pickSizeCompaction(v *version.Version, totalSize uint64) *Compaction {
	files := p.getAllFilesSortedByAge(v)
	if len(files) == 0 {
		return nil
	}

	// Delete oldest files until we're under the limit
	var toDelete []*sortedFile
	currentSize := totalSize

	for _, f := range files {
		if currentSize <= p.opts.MaxTableFilesSize {
			break
		}
		toDelete = append(toDelete, f)
		currentSize -= f.file.FD.FileSize
	}

	if len(toDelete) == 0 {
		return nil
	}

	return p.createDeleteCompaction(toDelete)
}

// createDeleteCompaction creates a "delete" compaction.
// In FIFO, compaction output is empty - files are simply deleted.
func (p *FIFOCompactionPicker) createDeleteCompaction(files []*sortedFile) *Compaction {
	if len(files) == 0 {
		return nil
	}

	// Group by level
	filesByLevel := make(map[int][]*manifest.FileMetaData)
	maxLevel := 0
	for _, f := range files {
		filesByLevel[f.level] = append(filesByLevel[f.level], f.file)
		if f.level > maxLevel {
			maxLevel = f.level
		}
	}

	var inputs []*CompactionInputFiles
	for level := 0; level <= maxLevel; level++ {
		if files, ok := filesByLevel[level]; ok && len(files) > 0 {
			inputs = append(inputs, &CompactionInputFiles{
				Level: level,
				Files: files,
			})
		}
	}

	if len(inputs) == 0 {
		return nil
	}

	// Output level is special: -1 means delete (no output)
	c := NewCompaction(inputs, -1)
	c.Reason = CompactionReasonFIFOMaxSize
	c.IsDeletionCompaction = true
	return c
}

// pickIntraL0Compaction picks L0 files for intra-L0 compaction.
// This is only used when AllowCompaction is true.
func (p *FIFOCompactionPicker) pickIntraL0Compaction(v *version.Version) *Compaction {
	l0Files := v.Files(0)
	if len(l0Files) < 2 {
		return nil
	}

	// Filter out files being compacted
	var available []*manifest.FileMetaData
	for _, f := range l0Files {
		if !f.BeingCompacted {
			available = append(available, f)
		}
	}

	if len(available) < 2 {
		return nil
	}

	// Sort by sequence number (oldest first)
	sort.Slice(available, func(i, j int) bool {
		return available[i].FD.SmallestSeqno < available[j].FD.SmallestSeqno
	})

	// Compact oldest files together (up to a limit)
	maxFiles := min(len(available), 10)

	input := &CompactionInputFiles{
		Level: 0,
		Files: available[:maxFiles],
	}

	c := NewCompaction([]*CompactionInputFiles{input}, 0)
	c.Reason = CompactionReasonFIFOReduceNumFiles
	return c
}
