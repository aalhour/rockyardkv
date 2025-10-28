// Package compaction implements compaction strategies for the LSM-tree.
//
// This file implements Subcompactions - parallel compaction within a single job.
//
// When a compaction job has a large key range, it can be split into multiple
// subcompactions that run in parallel, significantly improving compaction
// throughput on multi-core systems.
//
// Reference: RocksDB v10.7.5
//   - db/compaction/compaction_job.cc (SubcompactionState)
//   - db/compaction/subcompaction_state.cc
package compaction

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aalhour/rockyardkv/internal/block"
	"github.com/aalhour/rockyardkv/internal/iterator"
	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// SubcompactionState represents the state of a single subcompaction.
type SubcompactionState struct {
	// The parent compaction
	compaction *Compaction

	// Key range for this subcompaction [startKey, endKey)
	startKey []byte
	endKey   []byte

	// Output files produced by this subcompaction
	outputs []*manifest.FileMetaData

	// Statistics
	stats SubcompactionStats

	// Status
	status error
}

// SubcompactionStats tracks statistics for a subcompaction.
type SubcompactionStats struct {
	// Number of input records
	NumInputRecords uint64

	// Number of output records
	NumOutputRecords uint64

	// Bytes read
	BytesRead uint64

	// Bytes written
	BytesWritten uint64

	// Number of output files
	NumOutputFiles int
}

// ParallelCompactionJob runs a compaction job with subcompactions.
type ParallelCompactionJob struct {
	compaction  *Compaction
	dbPath      string
	fs          vfs.FS
	tableCache  *table.TableCache
	nextFileNum func() uint64

	// Number of parallel subcompactions
	numSubcompactions int

	// Results
	subcompactions []*SubcompactionState
	outputFiles    []*manifest.FileMetaData

	// Aggregate statistics
	stats SubcompactionStats
}

// NewParallelCompactionJob creates a new parallel compaction job.
func NewParallelCompactionJob(
	c *Compaction,
	dbPath string,
	fs vfs.FS,
	tableCache *table.TableCache,
	nextFileNum func() uint64,
	numSubcompactions int,
) *ParallelCompactionJob {
	if numSubcompactions <= 0 {
		numSubcompactions = 1
	}
	// Cap at reasonable maximum
	if numSubcompactions > 16 {
		numSubcompactions = 16
	}

	return &ParallelCompactionJob{
		compaction:        c,
		dbPath:            dbPath,
		fs:                fs,
		tableCache:        tableCache,
		nextFileNum:       nextFileNum,
		numSubcompactions: numSubcompactions,
	}
}

// Run executes the parallel compaction job.
func (job *ParallelCompactionJob) Run() ([]*manifest.FileMetaData, error) {
	// Partition the key range
	boundaries := job.computeKeyBoundaries()

	if len(boundaries) <= 2 {
		// Not enough range to parallelize, use single compaction
		singleJob := NewCompactionJob(job.compaction, job.dbPath, job.fs, job.tableCache, job.nextFileNum)
		return singleJob.Run()
	}

	// Create subcompactions
	// Each subcompaction handles range [startKey, endKey)
	// - First subcompaction: startKey = nil (include all keys < boundaries[1])
	// - Last subcompaction: endKey = nil (include all keys >= boundaries[n-2])
	// This ensures no keys are lost at boundaries
	job.subcompactions = make([]*SubcompactionState, len(boundaries)-1)
	for i := range len(boundaries) - 1 {
		var startKey, endKey []byte

		// First subcompaction has no start boundary (include everything from the beginning)
		if i > 0 {
			startKey = boundaries[i]
		}

		// Last subcompaction has no end boundary (include everything to the end)
		if i < len(boundaries)-2 {
			endKey = boundaries[i+1]
		}

		job.subcompactions[i] = &SubcompactionState{
			compaction: job.compaction,
			startKey:   startKey,
			endKey:     endKey,
		}
	}

	// Run subcompactions in parallel
	var wg sync.WaitGroup
	var firstError atomic.Pointer[error]

	for i := range job.subcompactions {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			sub := job.subcompactions[idx]
			err := job.runSubcompaction(sub)
			if err != nil {
				sub.status = err
				// Store first error
				firstError.CompareAndSwap(nil, &err)
			}
		}(i)
	}

	wg.Wait()

	// Check for errors
	if errPtr := firstError.Load(); errPtr != nil {
		// Cleanup any output files from successful subcompactions
		for _, sub := range job.subcompactions {
			for _, f := range sub.outputs {
				path := fmt.Sprintf("%s/%06d.sst", job.dbPath, f.FD.GetNumber())
				_ = job.fs.Remove(path)
			}
		}
		return nil, *errPtr
	}

	// Collect all output files in order
	var allOutputs []*manifest.FileMetaData
	for _, sub := range job.subcompactions {
		allOutputs = append(allOutputs, sub.outputs...)
		// Aggregate stats
		job.stats.NumInputRecords += sub.stats.NumInputRecords
		job.stats.NumOutputRecords += sub.stats.NumOutputRecords
		job.stats.BytesRead += sub.stats.BytesRead
		job.stats.BytesWritten += sub.stats.BytesWritten
		job.stats.NumOutputFiles += sub.stats.NumOutputFiles
	}

	// Add output files to the compaction's version edit
	for _, f := range allOutputs {
		job.compaction.Edit.AddFile(job.compaction.OutputLevel, f)
	}

	job.outputFiles = allOutputs
	return allOutputs, nil
}

// computeKeyBoundaries divides the key range into numSubcompactions partitions.
// Returns USER KEYS (not internal keys) as boundaries.
func (job *ParallelCompactionJob) computeKeyBoundaries() [][]byte {
	// Collect all file boundaries as USER KEYS
	var boundaries [][]byte
	seen := make(map[string]bool)

	// Helper to add a user key boundary
	addBoundary := func(internalKey []byte) {
		if len(internalKey) == 0 {
			return
		}
		userKey := extractUserKey(internalKey)
		if len(userKey) == 0 {
			return
		}
		keyStr := string(userKey)
		if !seen[keyStr] {
			// Make a copy of the user key
			boundaries = append(boundaries, append([]byte(nil), userKey...))
			seen[keyStr] = true
		}
	}

	// Add the overall smallest and largest keys
	addBoundary(job.compaction.SmallestKey)
	addBoundary(job.compaction.LargestKey)

	// Add boundaries from all input files
	for _, input := range job.compaction.Inputs {
		for _, f := range input.Files {
			addBoundary(f.Smallest)
			addBoundary(f.Largest)
		}
	}

	// Sort boundaries (user keys use simple bytes.Compare)
	sortBoundaries(boundaries)

	// If we have more boundaries than needed, reduce them
	if len(boundaries) > job.numSubcompactions+1 {
		// Take evenly spaced boundaries
		step := len(boundaries) / job.numSubcompactions
		var reduced [][]byte
		for i := 0; i < len(boundaries); i += step {
			reduced = append(reduced, boundaries[i])
		}
		// Ensure we include the last boundary
		if !bytes.Equal(reduced[len(reduced)-1], boundaries[len(boundaries)-1]) {
			reduced = append(reduced, boundaries[len(boundaries)-1])
		}
		boundaries = reduced
	}

	return boundaries
}

// sortBoundaries sorts key boundaries in ascending order.
func sortBoundaries(boundaries [][]byte) {
	// Simple bubble sort for small slices
	n := len(boundaries)
	for i := range n - 1 {
		for j := range n - i - 1 {
			if bytes.Compare(boundaries[j], boundaries[j+1]) > 0 {
				boundaries[j], boundaries[j+1] = boundaries[j+1], boundaries[j]
			}
		}
	}
}

// runSubcompaction runs a single subcompaction.
func (job *ParallelCompactionJob) runSubcompaction(sub *SubcompactionState) error {
	// Create a filtered version of the compaction for this key range
	filteredInputs := job.filterInputsForRange(sub.startKey, sub.endKey)
	if len(filteredInputs) == 0 {
		return nil // No work for this range
	}

	// Create iterators for the filtered inputs
	var iters []iterator.Iterator
	for _, input := range filteredInputs {
		for _, f := range input.Files {
			path := fmt.Sprintf("%s/%06d.sst", job.dbPath, f.FD.GetNumber())
			reader, err := job.tableCache.Get(f.FD.GetNumber(), path)
			if err != nil {
				return fmt.Errorf("failed to open SST %d: %w", f.FD.GetNumber(), err)
			}
			iter := reader.NewIterator()
			iters = append(iters, iter)
			sub.stats.BytesRead += f.FD.FileSize
		}
	}

	if len(iters) == 0 {
		return nil
	}

	// Create merging iterator
	merged := iterator.NewMergingIterator(iters, block.CompareInternalKeys)

	// Create output file builder
	var currentBuilder *table.TableBuilder
	var currentFile *manifest.FileMetaData
	var currentPath string
	var entriesInCurrentFile uint64

	// Target entries per file (estimate based on max output size and average entry size)
	const avgEntrySize = 100 // bytes
	targetEntriesPerFile := job.compaction.MaxOutputFileSize / avgEntrySize

	finishCurrentFile := func() error {
		if currentBuilder == nil {
			return nil
		}

		if err := currentBuilder.Finish(); err != nil {
			return err
		}

		// Get file size
		info, err := job.fs.Stat(currentPath)
		if err != nil {
			return err
		}

		currentFile.FD.FileSize = uint64(info.Size())
		sub.outputs = append(sub.outputs, currentFile)
		sub.stats.NumOutputFiles++
		sub.stats.BytesWritten += currentFile.FD.FileSize

		currentBuilder = nil
		currentFile = nil
		entriesInCurrentFile = 0
		return nil
	}

	startNewFile := func() error {
		fileNum := job.nextFileNum()
		currentPath = fmt.Sprintf("%s/%06d.sst", job.dbPath, fileNum)

		file, err := job.fs.Create(currentPath)
		if err != nil {
			return err
		}

		currentBuilder = table.NewTableBuilder(file, table.DefaultBuilderOptions())
		currentFile = manifest.NewFileMetaData()
		currentFile.FD = manifest.NewFileDescriptor(fileNum, 0, 0)
		entriesInCurrentFile = 0
		return nil
	}

	// Iterate through the merged data
	// Note: sub.startKey and sub.endKey are USER KEYS (not internal keys)
	for merged.SeekToFirst(); merged.Valid(); merged.Next() {
		key := merged.Key()
		value := merged.Value()

		// Extract user key from internal key for boundary comparison
		userKey := extractUserKey(key)

		// Skip if key is before our range
		if len(sub.startKey) > 0 && bytes.Compare(userKey, sub.startKey) < 0 {
			continue
		}
		// Break if key is at or past our range end (keys are in sorted order)
		if len(sub.endKey) > 0 && bytes.Compare(userKey, sub.endKey) >= 0 {
			break
		}

		sub.stats.NumInputRecords++

		// Start a new file if needed
		if currentBuilder == nil {
			if err := startNewFile(); err != nil {
				return err
			}
		}

		// Track key range
		if currentFile.Smallest == nil {
			currentFile.Smallest = append([]byte(nil), key...)
		}
		currentFile.Largest = append(currentFile.Largest[:0], key...)

		// Add to current file
		if err := currentBuilder.Add(key, value); err != nil {
			return err
		}
		sub.stats.NumOutputRecords++
		entriesInCurrentFile++

		// Check if we need to finish the current file (based on entry count estimate)
		if entriesInCurrentFile >= targetEntriesPerFile {
			if err := finishCurrentFile(); err != nil {
				return err
			}
		}
	}

	if err := merged.Error(); err != nil {
		return err
	}

	// Finish the last file
	return finishCurrentFile()
}

// filterInputsForRange filters input files to only those overlapping the key range.
// startKey and endKey are USER KEYS (not internal keys).
func (job *ParallelCompactionJob) filterInputsForRange(startKey, endKey []byte) []*CompactionInputFiles {
	var result []*CompactionInputFiles

	for _, input := range job.compaction.Inputs {
		var filteredFiles []*manifest.FileMetaData

		for _, f := range input.Files {
			// Extract user keys from file boundaries
			fileSmallestUser := extractUserKey(f.Smallest)
			fileLargestUser := extractUserKey(f.Largest)

			// Check if file overlaps the range [startKey, endKey)
			overlaps := true

			// File is entirely before startKey if fileLargest < startKey
			if len(startKey) > 0 && len(fileLargestUser) > 0 {
				if bytes.Compare(fileLargestUser, startKey) < 0 {
					overlaps = false
				}
			}

			// File is entirely at or after endKey if fileSmallest >= endKey
			if len(endKey) > 0 && len(fileSmallestUser) > 0 {
				if bytes.Compare(fileSmallestUser, endKey) >= 0 {
					overlaps = false
				}
			}

			if overlaps {
				filteredFiles = append(filteredFiles, f)
			}
		}

		if len(filteredFiles) > 0 {
			result = append(result, &CompactionInputFiles{
				Level: input.Level,
				Files: filteredFiles,
			})
		}
	}

	return result
}

// GetStats returns the aggregate statistics for the parallel compaction.
func (job *ParallelCompactionJob) GetStats() SubcompactionStats {
	return job.stats
}

// extractUserKey extracts the user key portion from an internal key.
// Internal keys have format: user_key + 8 bytes (sequence number + type)
func extractUserKey(internalKey []byte) []byte {
	if len(internalKey) < 8 {
		return internalKey // Not an internal key, return as-is
	}
	return internalKey[:len(internalKey)-8]
}
