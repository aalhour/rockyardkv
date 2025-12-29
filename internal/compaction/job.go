// job.go implements CompactionJob which executes a single compaction.
//
// CompactionJob runs the compaction process: reading input files,
// merging keys, filtering deleted entries, and writing output files.
//
// Reference: RocksDB v10.7.5
//   - db/compaction/compaction_job.h
//   - db/compaction/compaction_job.cc
//
// # Whitebox Testing Hooks
//
// This file contains sync points (requires -tags synctest) for whitebox testing.
// In production builds, these compile to no-ops with zero overhead.
// See docs/testing.md for usage.
package compaction

import (
	"fmt"
	"path/filepath"

	"github.com/aalhour/rockyardkv/internal/block"
	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/iterator"
	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/rangedel"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/testutil"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// RateLimiter is an interface for rate limiting I/O operations.
type RateLimiter interface {
	Request(bytes int64, priority int)
}

// IOPriority constants for rate limiting.
const (
	IOPriorityLow  = 0 // Background operations (compaction, flush)
	IOPriorityHigh = 1 // User reads/writes
)

// FilterDecision represents the decision made by a compaction filter.
type FilterDecision int

const (
	// FilterKeep keeps the key-value pair unchanged.
	FilterKeep FilterDecision = iota

	// FilterRemove removes the key-value pair from the database.
	FilterRemove

	// FilterChange changes the value of the key-value pair.
	FilterChange
)

// Filter is the interface for compaction filters.
// During compaction, Filter is called for each key-value pair,
// allowing the user to decide whether to keep, remove, or modify the entry.
// Reference: RocksDB include/rocksdb/compaction_filter.h
type Filter interface {
	// Name returns the name of the compaction filter.
	Name() string

	// Filter is called for each key-value pair during compaction.
	// Parameters:
	//   - level: The compaction output level
	//   - key: The user key (not internal key)
	//   - value: The current value
	// Returns:
	//   - decision: Whether to keep, remove, or change the entry
	//   - newValue: If decision is FilterChange, this is the new value
	Filter(level int, key, value []byte) (decision FilterDecision, newValue []byte)
}

// MergeOperator is the interface for user-defined merge operations during compaction.
// When multiple merge operands exist for the same key, they are combined using FullMerge.
type MergeOperator interface {
	// FullMerge performs a merge operation.
	// Parameters:
	// - key: The key associated with this merge operation
	// - existingValue: The existing value (nil if key doesn't exist)
	// - operands: List of merge operands to apply, oldest first
	// Returns:
	// - newValue: The result of the merge
	// - ok: Whether the merge succeeded
	FullMerge(key []byte, existingValue []byte, operands [][]byte) (newValue []byte, ok bool)
}

// CompactionJob performs a single compaction operation.
// It reads from input files, merges them, and writes to new output files.
type CompactionJob struct {
	compaction *Compaction
	dbPath     string
	fs         vfs.FS
	tableCache *table.TableCache

	// File number generator
	nextFileNum func() uint64

	// Output files created by this job
	outputFiles []*manifest.FileMetaData

	// Range deletion aggregator for dropping keys covered by range tombstones
	rangeDelAgg *rangedel.CompactionRangeDelAggregator

	// Earliest snapshot sequence number (for garbage collection decisions)
	earliestSnapshot dbformat.SequenceNumber

	// Rate limiter for controlling I/O rate (optional)
	rateLimiter RateLimiter

	// Compaction filter for custom filtering/transformation during compaction
	filter Filter

	// Merge operator for combining merge operands during compaction
	mergeOperator MergeOperator

	// Statistics about filtered entries
	filteredRecords uint64
	changedRecords  uint64
	mergedRecords   uint64
}

// NewCompactionJob creates a new compaction job.
func NewCompactionJob(
	c *Compaction,
	dbPath string,
	fs vfs.FS,
	tableCache *table.TableCache,
	nextFileNum func() uint64,
) *CompactionJob {
	return NewCompactionJobWithSnapshot(c, dbPath, fs, tableCache, nextFileNum, 0)
}

// NewCompactionJobWithSnapshot creates a new compaction job with an earliest snapshot.
// Keys covered by range tombstones with sequence numbers <= earliestSnapshot can be dropped.
func NewCompactionJobWithSnapshot(
	c *Compaction,
	dbPath string,
	fs vfs.FS,
	tableCache *table.TableCache,
	nextFileNum func() uint64,
	earliestSnapshot dbformat.SequenceNumber,
) *CompactionJob {
	return &CompactionJob{
		compaction:       c,
		dbPath:           dbPath,
		fs:               fs,
		tableCache:       tableCache,
		nextFileNum:      nextFileNum,
		rangeDelAgg:      rangedel.NewCompactionRangeDelAggregator(earliestSnapshot),
		earliestSnapshot: earliestSnapshot,
	}
}

// NewCompactionJobWithRateLimiter creates a new compaction job with a rate limiter.
func NewCompactionJobWithRateLimiter(
	c *Compaction,
	dbPath string,
	fs vfs.FS,
	tableCache *table.TableCache,
	nextFileNum func() uint64,
	earliestSnapshot dbformat.SequenceNumber,
	rateLimiter RateLimiter,
) *CompactionJob {
	return &CompactionJob{
		compaction:       c,
		dbPath:           dbPath,
		fs:               fs,
		tableCache:       tableCache,
		nextFileNum:      nextFileNum,
		rangeDelAgg:      rangedel.NewCompactionRangeDelAggregator(earliestSnapshot),
		earliestSnapshot: earliestSnapshot,
		rateLimiter:      rateLimiter,
	}
}

// SetFilter sets the compaction filter for this job.
// The filter will be called for each key-value pair during compaction.
func (j *CompactionJob) SetFilter(f Filter) {
	j.filter = f
}

// SetMergeOperator sets the merge operator for this job.
// When set, merge operands for the same key will be combined during compaction.
func (j *CompactionJob) SetMergeOperator(m MergeOperator) {
	j.mergeOperator = m
}

// FilterStats returns statistics about filtered entries.
// Returns the count of removed records and changed records.
func (j *CompactionJob) FilterStats() (removed, changed uint64) {
	return j.filteredRecords, j.changedRecords
}

// Run executes the compaction.
// Returns the list of output files created.
func (j *CompactionJob) Run() ([]*manifest.FileMetaData, error) {
	// Whitebox [synctest]: barrier at compaction job start
	_ = testutil.SP(testutil.SPCompactionStart)

	// Check for trivial move
	if j.compaction.IsTrivialMove {
		return j.doTrivialMove()
	}

	// Whitebox [synctest]: barrier before opening input files
	_ = testutil.SP(testutil.SPCompactionOpenInputs)

	// Create iterators for all input files
	iters, err := j.createInputIterators()
	if err != nil {
		return nil, fmt.Errorf("create input iterators: %w", err)
	}

	// Create merging iterator
	mergingIter := iterator.NewMergingIterator(iters, block.CompareInternalKeys)

	// Whitebox [synctest]: barrier during entry processing
	_ = testutil.SP(testutil.SPCompactionProcessing)

	// Process all entries
	err = j.processEntries(mergingIter)
	if err != nil {
		return nil, fmt.Errorf("process entries: %w", err)
	}

	// Whitebox [synctest]: barrier after output files written
	_ = testutil.SP(testutil.SPCompactionFinishOutput)

	// Whitebox [synctest]: barrier at compaction job complete
	_ = testutil.SP(testutil.SPCompactionComplete)

	return j.outputFiles, nil
}

// doTrivialMove handles trivial move compactions (just update metadata).
func (j *CompactionJob) doTrivialMove() ([]*manifest.FileMetaData, error) {
	// For trivial move, we just update the level in the edit
	// The file itself doesn't need to be rewritten
	for _, input := range j.compaction.Inputs {
		for _, f := range input.Files {
			// Add the file to the output level
			outputMeta := manifest.NewFileMetaData()
			outputMeta.FD = f.FD
			outputMeta.Smallest = f.Smallest
			outputMeta.Largest = f.Largest
			j.compaction.Edit.AddFile(j.compaction.OutputLevel, outputMeta)

			// Delete from the input level
			j.compaction.Edit.DeleteFile(input.Level, f.FD.GetNumber())
		}
	}
	return nil, nil
}

// createInputIterators creates iterators for all input files.
// It also loads range tombstones from the input files into the aggregator.
func (j *CompactionJob) createInputIterators() ([]iterator.Iterator, error) {
	var iters []iterator.Iterator
	var openedFiles []uint64 // Track opened files for cleanup on error

	for _, input := range j.compaction.Inputs {
		for _, f := range input.Files {
			// Construct the file path
			filePath := j.sstPath(f.FD.GetNumber())

			// Verify file exists before opening
			if !j.fs.Exists(filePath) {
				// Clean up already opened files
				for _, fileNum := range openedFiles {
					j.tableCache.Release(fileNum)
				}
				return nil, fmt.Errorf("input file %d does not exist: %s", f.FD.GetNumber(), filePath)
			}

			reader, err := j.tableCache.Get(f.FD.GetNumber(), filePath)
			if err != nil {
				// Clean up already opened files
				for _, fileNum := range openedFiles {
					j.tableCache.Release(fileNum)
				}
				return nil, fmt.Errorf("get table reader %d: %w", f.FD.GetNumber(), err)
			}
			openedFiles = append(openedFiles, f.FD.GetNumber())

			// Load range tombstones from this file into the aggregator
			if j.rangeDelAgg != nil {
				tombstoneList, err := reader.GetRangeTombstoneList()
				if err == nil && !tombstoneList.IsEmpty() {
					j.rangeDelAgg.AddTombstoneList(input.Level, tombstoneList)
				}
			}

			// Wrap the table iterator
			iters = append(iters, &tableIteratorWrapper{
				iter:       reader.NewIterator(),
				fileNumber: f.FD.GetNumber(),
			})
		}
	}

	return iters, nil
}

// sstPath returns the path to an SST file.
func (j *CompactionJob) sstPath(fileNum uint64) string {
	return filepath.Join(j.dbPath, fmt.Sprintf("%06d.sst", fileNum))
}

// processEntries iterates through all entries and writes them to output files.
// When a merge operator is configured, merge operands for the same key are combined.
func (j *CompactionJob) processEntries(iter *iterator.MergingIterator) error {
	proc := newCompactionProcessor(j)

	iter.SeekToFirst()

	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()

		// Check if this key should be dropped (covered by a range tombstone)
		if j.shouldDropKey(key) {
			iter.Next()
			continue
		}

		// If no merge operator, write entries as-is (original behavior)
		if j.mergeOperator == nil {
			if err := proc.writeRawEntry(key, value); err != nil {
				return err
			}
			iter.Next()
			continue
		}

		// Merge operator is configured - handle merge operand accumulation
		if err := proc.processEntryWithMerge(key, value); err != nil {
			return err
		}

		iter.Next()
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	// Flush any remaining accumulated merge state
	if err := proc.flushMerge(); err != nil {
		return err
	}

	// Finish the last file
	return proc.finish()
}

// =============================================================================
// compactionProcessor: Helper for processEntries
// =============================================================================
//
// compactionProcessor manages the mutable state during entry processing.
// It handles output file management and merge operand accumulation.

// compactionProcessor holds the mutable state for processing compaction entries.
type compactionProcessor struct {
	job         *CompactionJob
	builder     *table.TableBuilder
	currentFile *compactionOutputFile

	// Merge accumulator state (only used when merge operator is configured)
	currentUserKey []byte
	mergeOperands  [][]byte                // Collected in newest-first order
	baseValue      []byte                  // Base value (from Put) if found
	hasBaseValue   bool                    // Whether we found a Put for this key
	baseSeqNum     dbformat.SequenceNumber // Sequence number for output key
	isDeleted      bool                    // Whether key is deleted
}

// newCompactionProcessor creates a new processor for the given job.
func newCompactionProcessor(job *CompactionJob) *compactionProcessor {
	return &compactionProcessor{job: job}
}

// writeRawEntry writes an entry using its original internal key.
// This is the fast path when no merge operator is configured.
func (p *compactionProcessor) writeRawEntry(internalKey, value []byte) error {
	userKey := dbformat.ExtractUserKey(internalKey)

	// Apply compaction filter if configured
	if p.job.filter != nil {
		decision, newValue := p.job.filter.Filter(p.job.compaction.OutputLevel, userKey, value)
		switch decision {
		case FilterRemove:
			p.job.filteredRecords++
			return nil
		case FilterChange:
			value = newValue
			p.job.changedRecords++
		}
	}

	return p.addToOutput(internalKey, value)
}

// writeEntry writes an entry by constructing a new internal key.
// Used when emitting merged results.
func (p *compactionProcessor) writeEntry(userKey, value []byte, seqNum dbformat.SequenceNumber, valueType dbformat.ValueType) error {
	internalKey := dbformat.NewInternalKey(userKey, seqNum, valueType)

	// Apply compaction filter if configured
	if p.job.filter != nil {
		decision, newValue := p.job.filter.Filter(p.job.compaction.OutputLevel, userKey, value)
		switch decision {
		case FilterRemove:
			p.job.filteredRecords++
			return nil
		case FilterChange:
			value = newValue
			p.job.changedRecords++
		}
	}

	return p.addToOutput(internalKey, value)
}

// addToOutput adds a key-value pair to the current output file.
// Creates a new file if needed.
func (p *compactionProcessor) addToOutput(internalKey, value []byte) error {
	// Check if we should start a new output file
	if p.builder == nil || p.job.shouldFinishFile(p.currentFile, internalKey) {
		if p.builder != nil {
			if err := p.job.finishOutputFile(p.builder, p.currentFile); err != nil {
				return err
			}
		}
		var err error
		p.currentFile, p.builder, err = p.job.startOutputFile()
		if err != nil {
			return err
		}
	}

	// Add the key-value pair
	if err := p.builder.Add(internalKey, value); err != nil {
		return fmt.Errorf("add to builder: %w", err)
	}

	// Track key range
	if p.currentFile.smallest == nil {
		p.currentFile.smallest = append([]byte{}, internalKey...)
	}
	p.currentFile.largest = append(p.currentFile.largest[:0], internalKey...)

	return nil
}

// processEntryWithMerge handles an entry when merge operator is configured.
// Accumulates merge operands and flushes when user key changes.
func (p *compactionProcessor) processEntryWithMerge(key, value []byte) error {
	userKey := dbformat.ExtractUserKey(key)
	seqNum := dbformat.ExtractSequenceNumber(key)
	valueType := dbformat.ExtractValueType(key)

	// Check if we're starting a new user key
	if p.currentUserKey == nil || !bytesEqual(userKey, p.currentUserKey) {
		// Flush previous key's accumulated state
		if err := p.flushMerge(); err != nil {
			return err
		}

		// Start new accumulation
		p.currentUserKey = append(p.currentUserKey[:0], userKey...)
		p.baseSeqNum = seqNum // Use highest seqnum (first seen) for output
		p.mergeOperands = nil
		p.baseValue = nil
		p.hasBaseValue = false
		p.isDeleted = false
	}

	// Process based on value type
	switch valueType {
	case dbformat.TypeValue:
		// Found a Put - this is the base value
		p.baseValue = append([]byte{}, value...)
		p.hasBaseValue = true

	case dbformat.TypeMerge:
		// Accumulate merge operand (newest first)
		p.mergeOperands = append(p.mergeOperands, append([]byte{}, value...))

	case dbformat.TypeDeletion, dbformat.TypeSingleDeletion:
		// Delete wins - discard any accumulated operands
		p.isDeleted = true

	default:
		// For other types (range delete, etc.), write directly
		if err := p.writeEntry(userKey, value, seqNum, valueType); err != nil {
			return err
		}
	}

	return nil
}

// flushMerge flushes any accumulated merge operands for the current user key.
func (p *compactionProcessor) flushMerge() error {
	if p.currentUserKey == nil {
		return nil
	}

	// If deleted, skip (delete wins over merges)
	if p.isDeleted {
		p.resetMergeState()
		return nil
	}

	// If no merge operands, write the base value directly
	if len(p.mergeOperands) == 0 {
		if p.hasBaseValue {
			err := p.writeEntry(p.currentUserKey, p.baseValue, p.baseSeqNum, dbformat.TypeValue)
			p.resetMergeState()
			return err
		}
		p.resetMergeState()
		return nil
	}

	// Combine operands using merge operator
	if p.job.mergeOperator != nil {
		// Reverse operands to get oldest-first order for FullMerge
		reversed := make([][]byte, len(p.mergeOperands))
		for i, op := range p.mergeOperands {
			reversed[len(p.mergeOperands)-1-i] = op
		}

		var existingValue []byte
		if p.hasBaseValue {
			existingValue = p.baseValue
		}

		mergedValue, ok := p.job.mergeOperator.FullMerge(p.currentUserKey, existingValue, reversed)
		if !ok {
			return fmt.Errorf("merge operator failed for key %q", p.currentUserKey)
		}

		p.job.mergedRecords++
		err := p.writeEntry(p.currentUserKey, mergedValue, p.baseSeqNum, dbformat.TypeValue)
		p.resetMergeState()
		return err
	}

	// No merge operator configured - write entries as-is (fallback)
	if p.hasBaseValue {
		if err := p.writeEntry(p.currentUserKey, p.baseValue, p.baseSeqNum, dbformat.TypeValue); err != nil {
			return err
		}
	}
	for _, op := range p.mergeOperands {
		if err := p.writeEntry(p.currentUserKey, op, p.baseSeqNum, dbformat.TypeMerge); err != nil {
			return err
		}
	}

	p.resetMergeState()
	return nil
}

// resetMergeState clears the merge accumulator.
func (p *compactionProcessor) resetMergeState() {
	p.currentUserKey = nil
	p.mergeOperands = nil
	p.baseValue = nil
	p.hasBaseValue = false
	p.isDeleted = false
}

// finish completes the current output file if any.
func (p *compactionProcessor) finish() error {
	if p.builder != nil {
		return p.job.finishOutputFile(p.builder, p.currentFile)
	}
	return nil
}

// =============================================================================
// End of compactionProcessor helpers
// =============================================================================

// bytesEqual compares two byte slices for equality.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// shouldDropKey checks if a key should be dropped during compaction.
// A key is dropped if:
// 1. It's covered by a range tombstone with a higher sequence number
// 2. Both the key and tombstone are older than the earliest snapshot
func (j *CompactionJob) shouldDropKey(internalKey []byte) bool {
	if j.rangeDelAgg == nil || j.rangeDelAgg.IsEmpty() {
		return false
	}

	// Extract user key and sequence number from internal key
	if len(internalKey) < dbformat.NumInternalBytes {
		return false
	}

	userKey := dbformat.ExtractUserKey(internalKey)
	seqNum := dbformat.ExtractSequenceNumber(internalKey)

	return j.rangeDelAgg.ShouldDropKey(userKey, seqNum)
}

type compactionOutputFile struct {
	fileNumber uint64
	file       vfs.WritableFile
	path       string
	smallest   []byte
	largest    []byte
}

// startOutputFile creates a new output file.
func (j *CompactionJob) startOutputFile() (*compactionOutputFile, *table.TableBuilder, error) {
	fileNum := j.nextFileNum()
	fileName := fmt.Sprintf("%06d.sst", fileNum)
	filePath := filepath.Join(j.dbPath, fileName)

	file, err := j.fs.Create(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("create file %s: %w", filePath, err)
	}

	opts := table.DefaultBuilderOptions()
	builder := table.NewTableBuilder(file, opts)

	output := &compactionOutputFile{
		fileNumber: fileNum,
		file:       file,
		path:       filePath,
	}

	return output, builder, nil
}

// finishOutputFile completes an output file and records its metadata.
func (j *CompactionJob) finishOutputFile(builder *table.TableBuilder, output *compactionOutputFile) error {
	err := builder.Finish()
	if err != nil {
		_ = output.file.Close()
		return fmt.Errorf("finish builder: %w", err)
	}

	fileSize := builder.FileSize()

	// Apply rate limiting for the I/O if configured
	if j.rateLimiter != nil {
		j.rateLimiter.Request(int64(fileSize), IOPriorityLow)
	}

	err = output.file.Sync()
	if err != nil {
		_ = output.file.Close()
		return fmt.Errorf("sync file: %w", err)
	}

	err = output.file.Close()
	if err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	// Sync directory to make SST file entry durable.
	// This is required before updating MANIFEST to reference this SST.
	// Without this, a crash could leave MANIFEST referencing a non-existent SST.
	if err := j.fs.SyncDir(j.dbPath); err != nil {
		return fmt.Errorf("sync directory after compaction SST write: %w", err)
	}

	// Record the output file metadata
	fileMeta := manifest.NewFileMetaData()
	fileMeta.FD = manifest.NewFileDescriptor(output.fileNumber, 0, fileSize)
	fileMeta.Smallest = output.smallest
	fileMeta.Largest = output.largest

	j.outputFiles = append(j.outputFiles, fileMeta)

	// Add to the edit
	j.compaction.Edit.AddFile(j.compaction.OutputLevel, fileMeta)

	return nil
}

// shouldFinishFile returns true if we should start a new output file.
func (j *CompactionJob) shouldFinishFile(current *compactionOutputFile, _ []byte) bool {
	if current == nil {
		return true
	}

	// Check file size
	// A full implementation would track the builder's current size
	// For now, we rely on the builder to handle file size limits

	return false
}

// tableIteratorWrapper wraps a table.TableIterator to implement iterator.Iterator.
type tableIteratorWrapper struct {
	iter       *table.TableIterator
	fileNumber uint64
}

func (w *tableIteratorWrapper) Valid() bool {
	return w.iter.Valid()
}

func (w *tableIteratorWrapper) Key() []byte {
	return w.iter.Key()
}

func (w *tableIteratorWrapper) Value() []byte {
	return w.iter.Value()
}

func (w *tableIteratorWrapper) SeekToFirst() {
	w.iter.SeekToFirst()
}

func (w *tableIteratorWrapper) SeekToLast() {
	w.iter.SeekToLast()
}

func (w *tableIteratorWrapper) Seek(target []byte) {
	w.iter.Seek(target)
}

func (w *tableIteratorWrapper) Next() {
	w.iter.Next()
}

func (w *tableIteratorWrapper) Prev() {
	w.iter.Prev()
}

func (w *tableIteratorWrapper) Error() error {
	return w.iter.Error()
}
