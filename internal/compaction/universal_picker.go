// Package compaction implements compaction strategies for the LSM-tree.
//
// Universal compaction (also known as "tiered" or "size-tiered") is optimized
// for write-heavy workloads with lower write amplification than leveled compaction.
//
// Reference: RocksDB v10.7.5
//   - db/compaction/compaction_picker_universal.cc
//   - include/rocksdb/universal_compaction.h
package compaction

import (
	"sort"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/version"
)

// UniversalCompactionOptions contains options for universal compaction.
type UniversalCompactionOptions struct {
	// SizeRatio is the percentage trigger for size ratio compaction.
	// A run is picked for compaction if its size is <= (100 + SizeRatio) / 100
	// times the size of the next run.
	// Default: 1 (i.e., runs within 1% size difference are compacted together)
	SizeRatio int

	// MinMergeWidth is the minimum number of files to merge at once.
	// Default: 2
	MinMergeWidth int

	// MaxMergeWidth is the maximum number of files to merge at once.
	// Default: unlimited (MaxInt)
	MaxMergeWidth int

	// MaxSizeAmplificationPercent triggers compaction when the total size
	// of all files exceeds this percent of the sum of newest files.
	// Default: 200 (i.e., 2x amplification triggers full compaction)
	MaxSizeAmplificationPercent int

	// StopStyle determines when to stop including files in a compaction.
	StopStyle UniversalCompactionStopStyle

	// AllowTrivialMove allows trivial move when possible.
	AllowTrivialMove bool
}

// UniversalCompactionStopStyle determines when to stop adding files to compaction.
type UniversalCompactionStopStyle int

const (
	// StopStyleTotalSize stops when adding more files would exceed the size limit.
	StopStyleTotalSize UniversalCompactionStopStyle = iota
	// StopStyleSimilarSize stops when encountering a file much smaller than others.
	StopStyleSimilarSize
)

// DefaultUniversalCompactionOptions returns default universal compaction options.
func DefaultUniversalCompactionOptions() *UniversalCompactionOptions {
	return &UniversalCompactionOptions{
		SizeRatio:                   1,
		MinMergeWidth:               2,
		MaxMergeWidth:               1<<31 - 1, // MaxInt
		MaxSizeAmplificationPercent: 200,
		StopStyle:                   StopStyleTotalSize,
		AllowTrivialMove:            false,
	}
}

// UniversalCompactionPicker implements universal (size-tiered) compaction.
type UniversalCompactionPicker struct {
	opts *UniversalCompactionOptions
}

// NewUniversalCompactionPicker creates a new universal compaction picker.
func NewUniversalCompactionPicker(opts *UniversalCompactionOptions) *UniversalCompactionPicker {
	if opts == nil {
		opts = DefaultUniversalCompactionOptions()
	}
	return &UniversalCompactionPicker{opts: opts}
}

// sortedRun represents a sorted run (either a single L0 file or an entire level).
type sortedRun struct {
	level    int
	files    []*manifest.FileMetaData
	size     uint64
	earliest uint64 // Earliest sequence number
}

// NeedsCompaction returns true if compaction is needed.
func (p *UniversalCompactionPicker) NeedsCompaction(v *version.Version) bool {
	runs := p.getSortedRuns(v)
	if len(runs) < p.opts.MinMergeWidth {
		return false
	}

	// Check for size amplification
	if p.calculateSizeAmplification(runs) > p.opts.MaxSizeAmplificationPercent {
		return true
	}

	// Check for size ratio trigger
	if p.findSizeRatioCompaction(runs) != nil {
		return true
	}

	return false
}

// PickCompaction selects files for compaction.
func (p *UniversalCompactionPicker) PickCompaction(v *version.Version) *Compaction {
	runs := p.getSortedRuns(v)
	if len(runs) < p.opts.MinMergeWidth {
		return nil
	}

	// Priority 1: Size amplification compaction (compact all)
	amp := p.calculateSizeAmplification(runs)
	if amp > p.opts.MaxSizeAmplificationPercent {
		return p.pickAmplificationCompaction(runs)
	}

	// Priority 2: Size ratio compaction
	return p.findSizeRatioCompaction(runs)
}

// getSortedRuns extracts sorted runs from the version.
// In universal compaction:
// - Each L0 file is a separate sorted run
// - Each level > 0 is a single sorted run
func (p *UniversalCompactionPicker) getSortedRuns(v *version.Version) []*sortedRun {
	var runs []*sortedRun

	// L0 files: each is a separate sorted run (newest first)
	l0Files := v.Files(0)
	// Sort L0 files by sequence number (newest first)
	sortedL0 := make([]*manifest.FileMetaData, len(l0Files))
	copy(sortedL0, l0Files)
	sort.Slice(sortedL0, func(i, j int) bool {
		return sortedL0[i].FD.LargestSeqno > sortedL0[j].FD.LargestSeqno
	})

	for _, f := range sortedL0 {
		if !f.BeingCompacted {
			runs = append(runs, &sortedRun{
				level:    0,
				files:    []*manifest.FileMetaData{f},
				size:     f.FD.FileSize,
				earliest: uint64(f.FD.SmallestSeqno),
			})
		}
	}

	// Levels 1-6: each level is a single sorted run
	for level := 1; level < version.MaxNumLevels; level++ {
		files := v.Files(level)
		if len(files) == 0 {
			continue
		}

		// Check if any file is being compacted
		allAvailable := true
		var totalSize uint64
		var earliestSeq = ^uint64(0)
		for _, f := range files {
			if f.BeingCompacted {
				allAvailable = false
				break
			}
			totalSize += f.FD.FileSize
			if uint64(f.FD.SmallestSeqno) < earliestSeq {
				earliestSeq = uint64(f.FD.SmallestSeqno)
			}
		}

		if allAvailable && len(files) > 0 {
			runs = append(runs, &sortedRun{
				level:    level,
				files:    files,
				size:     totalSize,
				earliest: earliestSeq,
			})
		}
	}

	return runs
}

// calculateSizeAmplification calculates the size amplification factor.
// Amplification = total_size / size_of_newest_run * 100
func (p *UniversalCompactionPicker) calculateSizeAmplification(runs []*sortedRun) int {
	if len(runs) < 2 {
		return 0
	}

	// Total size of all runs except the newest
	var totalOldSize uint64
	for i := 1; i < len(runs); i++ {
		totalOldSize += runs[i].size
	}

	newestSize := runs[0].size
	if newestSize == 0 {
		return 0
	}

	return int((totalOldSize * 100) / newestSize)
}

// pickAmplificationCompaction creates a compaction of all runs.
func (p *UniversalCompactionPicker) pickAmplificationCompaction(runs []*sortedRun) *Compaction {
	if len(runs) == 0 {
		return nil
	}

	// Find max level
	maxLevel := 0
	for _, run := range runs {
		if run.level > maxLevel {
			maxLevel = run.level
		}
	}

	// Group files by level
	filesByLevel := make(map[int][]*manifest.FileMetaData)
	for _, run := range runs {
		filesByLevel[run.level] = append(filesByLevel[run.level], run.files...)
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

	// Output goes to the last level with files, or level 1 if only L0
	outputLevel := maxLevel
	if outputLevel == 0 {
		outputLevel = 1
	}

	c := NewCompaction(inputs, outputLevel)
	c.Reason = CompactionReasonUniversalSizeAmplification
	return c
}

// findSizeRatioCompaction finds runs that should be compacted based on size ratio.
func (p *UniversalCompactionPicker) findSizeRatioCompaction(runs []*sortedRun) *Compaction {
	if len(runs) < p.opts.MinMergeWidth {
		return nil
	}

	// Find a contiguous sequence of runs where each run's size is within
	// SizeRatio% of the next run's size
	threshold := 100 + p.opts.SizeRatio

	for start := range len(runs) - 1 {
		// Try to extend from this starting point
		end := start + 1

		for end < len(runs) && end-start < p.opts.MaxMergeWidth {
			prevSize := runs[end-1].size
			currSize := runs[end].size

			// Check if sizes are within ratio
			if currSize == 0 || (prevSize*100)/currSize > uint64(threshold) {
				break
			}
			end++
		}

		// Check if we have enough files to compact
		if end-start >= p.opts.MinMergeWidth {
			return p.createCompactionFromRuns(runs[start:end])
		}
	}

	return nil
}

// createCompactionFromRuns creates a compaction from the given sorted runs.
func (p *UniversalCompactionPicker) createCompactionFromRuns(runs []*sortedRun) *Compaction {
	if len(runs) == 0 {
		return nil
	}

	// Group files by level
	filesByLevel := make(map[int][]*manifest.FileMetaData)
	maxLevel := 0
	for _, run := range runs {
		filesByLevel[run.level] = append(filesByLevel[run.level], run.files...)
		if run.level > maxLevel {
			maxLevel = run.level
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

	// Output to the max level or level 1 if only L0
	outputLevel := maxLevel
	if outputLevel == 0 {
		outputLevel = 1
	}

	c := NewCompaction(inputs, outputLevel)
	c.Reason = CompactionReasonUniversalSizeRatio
	return c
}
