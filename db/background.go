// background.go implements background tasks like flush and compaction.
//
// BackgroundWork handles scheduling and execution of background tasks
// including memtable flushes and L0→L1→... compactions.
//
// Reference: RocksDB v10.7.5
//   - db/db_impl/db_impl_compaction_flush.cc
//   - db/db_impl/db_impl_bg.cc
//
// # Whitebox Testing Hooks
//
// This file contains sync points (requires -tags synctest) and kill points
// (requires -tags crashtest) for whitebox testing. In production builds,
// these compile to no-ops with zero overhead. See docs/testing.md for usage.
package db

import (
	"fmt"
	"sync"

	"github.com/aalhour/rockyardkv/internal/compaction"
	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/testutil"
)

// BackgroundWork handles background tasks like compaction.
type BackgroundWork struct {
	db *DBImpl

	// Compaction picker
	picker compaction.CompactionPicker

	// Max subcompactions per job
	maxSubcompactions int

	// Rate limiter for background I/O (optional)
	rateLimiter RateLimiter

	// Channels for coordination
	compactionCh   chan struct{}
	flushCh        chan struct{}
	shutdownCh     chan struct{}
	backgroundDone sync.WaitGroup

	// State
	mu                sync.Mutex
	compactionRunning bool
	flushRunning      bool
	backgroundErrors  int
	paused            bool
	pauseCond         *sync.Cond
}

// newBackgroundWork creates a new background work handler.
func newBackgroundWork(db *DBImpl, opts *Options) *BackgroundWork {
	picker := createCompactionPicker(opts)
	maxSub := opts.MaxSubcompactions
	if maxSub <= 0 {
		maxSub = 1
	}
	bg := &BackgroundWork{
		db:                db,
		picker:            picker,
		maxSubcompactions: maxSub,
		rateLimiter:       opts.RateLimiter,
		compactionCh:      make(chan struct{}, 1),
		flushCh:           make(chan struct{}, 1),
		shutdownCh:        make(chan struct{}),
	}
	bg.pauseCond = sync.NewCond(&bg.mu)
	return bg
}

// compactionFilterAdapter adapts db.CompactionFilter to compaction.Filter.
type compactionFilterAdapter struct {
	filter CompactionFilter
}

func (a *compactionFilterAdapter) Name() string {
	return a.filter.Name()
}

func (a *compactionFilterAdapter) Filter(level int, key, value []byte) (compaction.FilterDecision, []byte) {
	decision, newValue := a.filter.Filter(level, key, value)
	// Map db.CompactionFilterDecision to compaction.FilterDecision
	switch decision {
	case FilterRemove:
		return compaction.FilterRemove, nil
	case FilterChange:
		return compaction.FilterChange, newValue
	default:
		return compaction.FilterKeep, nil
	}
}

// mergeOperatorAdapter adapts db.MergeOperator to compaction.MergeOperator.
type mergeOperatorAdapter struct {
	op MergeOperator
}

func (a *mergeOperatorAdapter) FullMerge(key []byte, existingValue []byte, operands [][]byte) ([]byte, bool) {
	return a.op.FullMerge(key, existingValue, operands)
}

// createCompactionPicker creates the appropriate picker based on options.
func createCompactionPicker(opts *Options) compaction.CompactionPicker {
	switch opts.CompactionStyle {
	case CompactionStyleUniversal:
		var uopts *compaction.UniversalCompactionOptions
		if opts.UniversalCompactionOptions != nil {
			uopts = &compaction.UniversalCompactionOptions{
				SizeRatio:                   opts.UniversalCompactionOptions.SizeRatio,
				MinMergeWidth:               opts.UniversalCompactionOptions.MinMergeWidth,
				MaxMergeWidth:               opts.UniversalCompactionOptions.MaxMergeWidth,
				MaxSizeAmplificationPercent: opts.UniversalCompactionOptions.MaxSizeAmplificationPercent,
				AllowTrivialMove:            opts.UniversalCompactionOptions.AllowTrivialMove,
			}
		}
		return compaction.NewUniversalCompactionPicker(uopts)

	case CompactionStyleFIFO:
		var fopts *compaction.FIFOCompactionOptions
		if opts.FIFOCompactionOptions != nil {
			fopts = &compaction.FIFOCompactionOptions{
				MaxTableFilesSize: opts.FIFOCompactionOptions.MaxTableFilesSize,
				TTL:               opts.FIFOCompactionOptions.TTL,
				AllowCompaction:   opts.FIFOCompactionOptions.AllowCompaction,
			}
		}
		return compaction.NewFIFOCompactionPicker(fopts)

	default:
		// Default to leveled compaction
		picker := compaction.DefaultLeveledCompactionPicker()
		if opts.Level0FileNumCompactionTrigger > 0 {
			picker.L0CompactionTrigger = opts.Level0FileNumCompactionTrigger
		}
		if opts.MaxBytesForLevelBase > 0 {
			picker.MaxBytesForLevelBase = uint64(opts.MaxBytesForLevelBase)
		}
		return picker
	}
}

// Start starts the background workers.
func (bg *BackgroundWork) Start() {
	bg.backgroundDone.Add(1)
	go bg.backgroundLoop()
}

// Stop stops the background workers and waits for them to finish.
func (bg *BackgroundWork) Stop() {
	close(bg.shutdownCh)
	bg.backgroundDone.Wait()
}

// Pause pauses all background work.
// Reference: RocksDB v10.7.5 db/db_impl/db_impl.cc PauseBackgroundWork()
func (bg *BackgroundWork) Pause() {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	bg.paused = true
}

// Continue resumes background work after Pause.
// Reference: RocksDB v10.7.5 db/db_impl/db_impl.cc ContinueBackgroundWork()
func (bg *BackgroundWork) Continue() {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	bg.paused = false
	bg.pauseCond.Broadcast()
}

// IsPaused returns true if background work is paused.
func (bg *BackgroundWork) IsPaused() bool {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	return bg.paused
}

// WaitIfPaused waits if background work is paused.
// Call this before performing background operations.
func (bg *BackgroundWork) WaitIfPaused() {
	bg.mu.Lock()
	for bg.paused {
		bg.pauseCond.Wait()
	}
	bg.mu.Unlock()
}

// MaybeScheduleCompaction signals that compaction may be needed.
func (bg *BackgroundWork) MaybeScheduleCompaction() {
	select {
	case bg.compactionCh <- struct{}{}:
	default:
		// Already signaled
	}
}

// MaybeScheduleFlush signals that flush may be needed.
func (bg *BackgroundWork) MaybeScheduleFlush() {
	select {
	case bg.flushCh <- struct{}{}:
	default:
		// Already signaled
	}
}

// backgroundLoop is the main background worker loop.
func (bg *BackgroundWork) backgroundLoop() {
	defer bg.backgroundDone.Done()

	for {
		select {
		case <-bg.shutdownCh:
			return

		case <-bg.flushCh:
			bg.doFlushWork()

		case <-bg.compactionCh:
			bg.doCompactionWork()
		}
	}
}

// doFlushWork performs background flush if needed.
func (bg *BackgroundWork) doFlushWork() {
	// Whitebox [synctest]: barrier at background flush start
	_ = testutil.SP(testutil.SPBGFlushStart)

	bg.mu.Lock()
	if bg.flushRunning {
		bg.mu.Unlock()
		return
	}
	bg.flushRunning = true
	bg.mu.Unlock()

	defer func() {
		bg.mu.Lock()
		bg.flushRunning = false
		bg.mu.Unlock()
	}()

	// Check if flush is needed
	bg.db.mu.Lock()
	needsFlush := bg.db.imm != nil
	bg.db.mu.Unlock()

	if !needsFlush {
		return
	}

	// Whitebox [synctest]: barrier before flush execution
	_ = testutil.SP(testutil.SPBGFlushExecute)

	// Perform flush
	err := bg.db.Flush(nil)
	if err != nil {
		// Record background error for I/O failures
		bg.db.SetBackgroundError(err)
		bg.IncrementBackgroundErrors()
	}

	// Whitebox [synctest]: barrier at background flush complete
	_ = testutil.SP(testutil.SPBGFlushComplete)

	// After flush, check if compaction is needed
	bg.MaybeScheduleCompaction()
}

// doCompactionWork performs background compaction if needed.
func (bg *BackgroundWork) doCompactionWork() {
	// Whitebox [synctest]: barrier at background compaction start
	_ = testutil.SP(testutil.SPBGCompactionStart)

	bg.mu.Lock()
	if bg.compactionRunning {
		bg.mu.Unlock()
		return
	}
	bg.compactionRunning = true
	bg.mu.Unlock()

	defer func() {
		bg.mu.Lock()
		bg.compactionRunning = false
		bg.mu.Unlock()
	}()

	// Get current version
	bg.db.mu.RLock()
	v := bg.db.versions.Current()
	if v != nil {
		v.Ref()
	}
	bg.db.mu.RUnlock()

	if v == nil {
		return
	}
	defer v.Unref()

	// Check if compaction is needed
	if !bg.picker.NeedsCompaction(v) {
		return
	}

	// Pick compaction
	bg.db.mu.Lock()
	c := bg.picker.PickCompaction(v)
	if c == nil {
		bg.db.mu.Unlock()
		return
	}
	// Mark files as being compacted (under lock to prevent concurrent pick of same files)
	c.MarkFilesBeingCompacted(true)
	bg.db.mu.Unlock()

	// Whitebox [synctest]: barrier after compaction picked
	_ = testutil.SP(testutil.SPBGCompactionPickComplete)

	// Execute compaction (defer unmark even on error)
	defer func() {
		bg.db.mu.Lock()
		c.MarkFilesBeingCompacted(false)
		bg.db.mu.Unlock()
	}()

	// Whitebox [synctest]: barrier before compaction execution
	_ = testutil.SP(testutil.SPBGCompactionExecute)

	// Whitebox [crashtest]: crash before compaction starts
	testutil.MaybeKill(testutil.KPCompactionStart0)

	err := bg.executeCompaction(c)
	if err != nil {
		// Record background error for I/O failures
		bg.db.SetBackgroundError(err)
		bg.IncrementBackgroundErrors()
		return
	}

	// Whitebox [synctest]: barrier at compaction complete
	_ = testutil.SP(testutil.SPBGCompactionComplete)

	// Check if more compaction is needed
	bg.MaybeScheduleCompaction()
}

// executeCompaction runs a compaction job.
func (bg *BackgroundWork) executeCompaction(c *compaction.Compaction) error {
	// Handle FIFO deletion compaction (no merge, just delete files)
	if c.IsDeletionCompaction {
		return bg.executeDeletionCompaction(c)
	}

	bg.db.mu.Lock()
	dbPath := bg.db.name
	fs := bg.db.fs
	tableCache := bg.db.tableCache
	versions := bg.db.versions

	// Verify all input files still exist before proceeding
	for _, input := range c.Inputs {
		for _, f := range input.Files {
			path := fmt.Sprintf("%s/%06d.sst", dbPath, f.FD.GetNumber())
			if !fs.Exists(path) {
				bg.db.mu.Unlock()
				return fmt.Errorf("input file %d no longer exists", f.FD.GetNumber())
			}
		}
	}
	bg.db.mu.Unlock()

	// File number generator
	nextFileNum := func() uint64 {
		return versions.NextFileNumber()
	}

	// Create and run the compaction job
	// Use parallel compaction if MaxSubcompactions > 1 and job is large enough
	var outputFiles []*manifest.FileMetaData
	var err error

	// Create rate limiter adapter if configured
	var rl compaction.RateLimiter
	if bg.rateLimiter != nil {
		rl = &rateLimiterAdapter{limiter: bg.rateLimiter}
	}

	// Get compaction filter from database options
	var compFilter compaction.Filter
	if bg.db.options.CompactionFilterFactory != nil {
		// Factory creates a new filter for each compaction
		// A compaction is considered "full" if it involves multiple input levels
		isFull := len(c.Inputs) > 1 && c.OutputLevel > 1
		ctx := CompactionFilterContext{
			IsFull:         isFull,
			IsManual:       false,
			ColumnFamilyID: 0,
		}
		filter := bg.db.options.CompactionFilterFactory.CreateCompactionFilter(ctx)
		compFilter = &compactionFilterAdapter{filter: filter}
	} else if bg.db.options.CompactionFilter != nil {
		compFilter = &compactionFilterAdapter{filter: bg.db.options.CompactionFilter}
	}

	// Get merge operator from database options
	var mergeOp compaction.MergeOperator
	if bg.db.options.MergeOperator != nil {
		mergeOp = &mergeOperatorAdapter{op: bg.db.options.MergeOperator}
	}

	if bg.maxSubcompactions > 1 && c.NumInputFiles() >= 4 {
		// Use parallel compaction for larger jobs
		parallelJob := compaction.NewParallelCompactionJob(
			c, dbPath, fs, tableCache, nextFileNum, bg.maxSubcompactions,
		)
		// TODO: Add filter and merge operator support to parallel compaction job
		if mergeOp != nil {
			parallelJob.SetMergeOperator(mergeOp)
		}
		outputFiles, err = parallelJob.Run()
	} else {
		// Use single-threaded compaction with rate limiter
		job := compaction.NewCompactionJobWithRateLimiter(
			c, dbPath, fs, tableCache, nextFileNum, 0, rl,
		)
		if compFilter != nil {
			job.SetFilter(compFilter)
		}
		if mergeOp != nil {
			job.SetMergeOperator(mergeOp)
		}
		outputFiles, err = job.Run()
	}
	if err != nil {
		return err
	}

	// Whitebox [crashtest]: crash after SST write — output exists, manifest not updated
	testutil.MaybeKill(testutil.KPCompactionWriteSST0)

	// Whitebox [crashtest]: crash before input deletion — both inputs and outputs exist
	testutil.MaybeKill(testutil.KPCompactionDeleteInput0)

	// Mark input files for deletion
	c.AddInputDeletions()

	// Apply the version edit
	bg.db.mu.Lock()
	defer bg.db.mu.Unlock()

	err = versions.LogAndApply(c.Edit)
	if err != nil {
		return err
	}

	// Recalculate write stall condition after compaction
	bg.db.recalculateWriteStall()

	// Evict input files from table cache
	for _, input := range c.Inputs {
		for _, f := range input.Files {
			tableCache.Evict(f.FD.GetNumber())
		}
	}

	// Compaction complete
	// TODO: Add proper statistics/metrics tracking instead of logging
	_ = len(outputFiles)

	return nil
}

// executeDeletionCompaction handles FIFO-style deletion compaction.
// It simply marks files for deletion without merging data.
func (bg *BackgroundWork) executeDeletionCompaction(c *compaction.Compaction) error {
	bg.db.mu.Lock()
	defer bg.db.mu.Unlock()

	tableCache := bg.db.tableCache
	versions := bg.db.versions

	// Mark input files for deletion
	c.AddInputDeletions()

	// Apply the version edit
	err := versions.LogAndApply(c.Edit)
	if err != nil {
		return err
	}

	// Evict and schedule deletion
	for _, input := range c.Inputs {
		for _, f := range input.Files {
			tableCache.Evict(f.FD.GetNumber())
		}
	}

	// TODO: Add proper statistics/metrics tracking for FIFO compaction
	return nil
}

// IsCompactionPending returns true if compaction has been scheduled but not yet started.
func (bg *BackgroundWork) IsCompactionPending() bool {
	bg.mu.Lock()
	defer bg.mu.Unlock()

	// Check if there's pending work
	select {
	case <-bg.compactionCh:
		// Put it back
		select {
		case bg.compactionCh <- struct{}{}:
		default:
		}
		return true
	default:
		return false
	}
}

// NumRunningFlushes returns the number of currently running flush operations.
func (bg *BackgroundWork) NumRunningFlushes() int {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	if bg.flushRunning {
		return 1
	}
	return 0
}

// NumRunningCompactions returns the number of currently running compaction operations.
func (bg *BackgroundWork) NumRunningCompactions() int {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	if bg.compactionRunning {
		return 1
	}
	return 0
}

// NumBackgroundErrors returns the number of background errors that have occurred.
func (bg *BackgroundWork) NumBackgroundErrors() int {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	return bg.backgroundErrors
}

// IncrementBackgroundErrors increments the background error count.
func (bg *BackgroundWork) IncrementBackgroundErrors() {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	bg.backgroundErrors++
}

// rateLimiterAdapter adapts the db.RateLimiter interface to compaction.RateLimiter.
type rateLimiterAdapter struct {
	limiter RateLimiter
}

// Request implements compaction.RateLimiter.
func (a *rateLimiterAdapter) Request(bytes int64, priority int) {
	if a.limiter != nil {
		a.limiter.Request(bytes, IOPriority(priority))
	}
}
