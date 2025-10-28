// Package compaction implements the compaction logic for RockyardKV.
//
// Compaction merges and reorganizes SST files to optimize read performance
// and reclaim space from deleted/overwritten keys.
//
// Reference: RocksDB v10.7.5
//   - db/compaction/compaction.h
//   - db/compaction/compaction.cc
package compaction

import (
	"github.com/aalhour/rockyardkv/internal/manifest"
)

// Compaction represents a single compaction operation.
// It describes which files to read from (inputs) and where to write to (output level).
type Compaction struct {
	// Input files organized by level
	Inputs []*CompactionInputFiles

	// The output level
	OutputLevel int

	// Maximum output file size
	MaxOutputFileSize uint64

	// Smallest and largest keys across all input files
	SmallestKey []byte
	LargestKey  []byte

	// Edit to record changes to the version
	Edit *manifest.VersionEdit

	// Whether this is a trivial move (no merging needed)
	IsTrivialMove bool

	// Whether this is a deletion-only compaction (FIFO)
	IsDeletionCompaction bool

	// The score that triggered this compaction
	Score float64

	// The reason for this compaction
	Reason CompactionReason
}

// CompactionInputFiles represents input files from a single level.
type CompactionInputFiles struct {
	Level int
	Files []*manifest.FileMetaData
}

// CompactionReason indicates why a compaction was triggered.
type CompactionReason int

const (
	CompactionReasonUnknown CompactionReason = iota
	CompactionReasonLevelL0FileNumTrigger
	CompactionReasonLevelMaxLevelSize
	CompactionReasonManualCompaction
	CompactionReasonFlush
	// Universal compaction reasons
	CompactionReasonUniversalSizeAmplification
	CompactionReasonUniversalSizeRatio
	CompactionReasonUniversalSortedRunNum
	// FIFO compaction reasons
	CompactionReasonFIFOMaxSize
	CompactionReasonFIFOTTL
	CompactionReasonFIFOReduceNumFiles
)

func (r CompactionReason) String() string {
	switch r {
	case CompactionReasonLevelL0FileNumTrigger:
		return "L0 file count"
	case CompactionReasonLevelMaxLevelSize:
		return "Level size"
	case CompactionReasonManualCompaction:
		return "Manual"
	case CompactionReasonFlush:
		return "Flush"
	case CompactionReasonUniversalSizeAmplification:
		return "Universal size amplification"
	case CompactionReasonUniversalSizeRatio:
		return "Universal size ratio"
	case CompactionReasonUniversalSortedRunNum:
		return "Universal sorted run count"
	case CompactionReasonFIFOMaxSize:
		return "FIFO max size"
	case CompactionReasonFIFOTTL:
		return "FIFO TTL"
	case CompactionReasonFIFOReduceNumFiles:
		return "FIFO reduce file count"
	default:
		return "Unknown"
	}
}

// NewCompaction creates a new Compaction with the given inputs and output level.
func NewCompaction(inputs []*CompactionInputFiles, outputLevel int) *Compaction {
	c := &Compaction{
		Inputs:            inputs,
		OutputLevel:       outputLevel,
		MaxOutputFileSize: 64 * 1024 * 1024, // 64MB default
		Edit:              manifest.NewVersionEdit(),
	}
	c.computeKeyRange()
	return c
}

// NumInputFiles returns the total number of input files.
func (c *Compaction) NumInputFiles() int {
	total := 0
	for _, in := range c.Inputs {
		total += len(in.Files)
	}
	return total
}

// StartLevel returns the start level of this compaction.
func (c *Compaction) StartLevel() int {
	if len(c.Inputs) == 0 {
		return -1
	}
	return c.Inputs[0].Level
}

// computeKeyRange computes the smallest and largest keys across all input files.
func (c *Compaction) computeKeyRange() {
	for i, in := range c.Inputs {
		for j, f := range in.Files {
			if i == 0 && j == 0 {
				c.SmallestKey = f.Smallest
				c.LargestKey = f.Largest
			} else {
				// Update smallest
				if len(f.Smallest) > 0 {
					if len(c.SmallestKey) == 0 || compareKeys(f.Smallest, c.SmallestKey) < 0 {
						c.SmallestKey = f.Smallest
					}
				}
				// Update largest
				if len(f.Largest) > 0 {
					if len(c.LargestKey) == 0 || compareKeys(f.Largest, c.LargestKey) > 0 {
						c.LargestKey = f.Largest
					}
				}
			}
		}
	}
}

// compareKeys performs a simple bytewise comparison of keys.
// For internal keys, this should use the internal key comparator.
func compareKeys(a, b []byte) int {
	minLen := min(len(b), len(a))
	for i := range minLen {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// AddInputDeletions adds delete operations for all input files to the edit.
func (c *Compaction) AddInputDeletions() {
	for _, in := range c.Inputs {
		for _, f := range in.Files {
			c.Edit.DeleteFile(in.Level, f.FD.GetNumber())
		}
	}
}

// DeletedFiles returns the deleted files in the edit.
func (c *Compaction) DeletedFiles() []manifest.DeletedFileEntry {
	return c.Edit.DeletedFiles
}

// MarkFilesBeingCompacted marks all input files as being compacted.
func (c *Compaction) MarkFilesBeingCompacted(beingCompacted bool) {
	for _, in := range c.Inputs {
		for _, f := range in.Files {
			f.BeingCompacted = beingCompacted
		}
	}
}

// HasSufficientKeyRangeForSubcompaction checks if the compaction has enough key range
// diversity to benefit from parallel subcompaction.
// Returns true if the key range is large enough and there are distinct file boundaries.
func (c *Compaction) HasSufficientKeyRangeForSubcompaction() bool {
	// Must have smallest and largest keys
	if len(c.SmallestKey) == 0 || len(c.LargestKey) == 0 {
		return false
	}

	// Key range must be substantial (not empty)
	if compareKeys(c.SmallestKey, c.LargestKey) >= 0 {
		return false
	}

	// Count distinct file boundaries
	boundaries := make(map[string]bool)
	for _, input := range c.Inputs {
		for _, f := range input.Files {
			if len(f.Smallest) > 0 {
				boundaries[string(f.Smallest)] = true
			}
			if len(f.Largest) > 0 {
				boundaries[string(f.Largest)] = true
			}
		}
	}

	// Need at least 4 distinct boundaries to split into meaningful subranges
	return len(boundaries) >= 4
}
