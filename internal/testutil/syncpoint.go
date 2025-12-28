//go:build synctest

// Package testutil provides test utilities for stress testing and verification.
//
// SyncPoint provides synchronization points for testing concurrent code.
//
// Reference: RocksDB v10.7.5
//   - test_util/sync_point.h
//   - test_util/sync_point.cc
//
// SyncPoints allow tests to:
// - Inject deterministic behavior into concurrent code
// - Force specific orderings of operations
// - Inject errors or delays at specific points
// - Verify that code paths are executed
//
// Usage:
//
//	// In production code (only active when enabled):
//	testutil.SyncPointProcess("point_name")
//
//	// In test code:
//	sp := testutil.NewSyncPointManager()
//	sp.SetCallback("point_name", func() {
//	    // Do something when this point is reached
//	})
//	sp.EnableProcessing()
//	defer sp.DisableProcessing()
package testutil

import (
	"sync"
	"sync/atomic"
	"time"
)

// SyncPointManager manages sync points for a test.
type SyncPointManager struct {
	mu sync.RWMutex

	// enabled controls whether sync points are processed
	enabled atomic.Bool

	// callbacks maps sync point names to callback functions
	callbacks map[string][]SyncPointCallback

	// hitCounts tracks how many times each sync point was hit
	hitCounts map[string]int64

	// blockedPoints are points where execution will wait
	blockedPoints map[string]chan struct{}

	// clearedPoints are points that have been signaled to continue
	clearedPoints map[string]bool

	// errorInjections maps sync point names to errors to return
	errorInjections map[string]error

	// delays maps sync point names to delays to introduce
	delays map[string]time.Duration

	// dependencies maps "after" -> "before" for ordering constraints
	// e.g., dependencies["B"] = ["A"] means B cannot proceed until A is hit
	dependencies map[string][]string

	// dependencySignals tracks which points have been signaled
	dependencySignals map[string]chan struct{}
}

// SyncPointCallback is called when a sync point is reached.
// It receives the sync point name and can return an error to propagate.
type SyncPointCallback func(name string) error

// globalSyncPointManager is the global manager used by SyncPointProcess.
var globalSyncPointManager atomic.Pointer[SyncPointManager]

// NewSyncPointManager creates a new SyncPointManager.
func NewSyncPointManager() *SyncPointManager {
	return &SyncPointManager{
		callbacks:         make(map[string][]SyncPointCallback),
		hitCounts:         make(map[string]int64),
		blockedPoints:     make(map[string]chan struct{}),
		clearedPoints:     make(map[string]bool),
		errorInjections:   make(map[string]error),
		delays:            make(map[string]time.Duration),
		dependencies:      make(map[string][]string),
		dependencySignals: make(map[string]chan struct{}),
	}
}

// EnableProcessing enables sync point processing.
func (sp *SyncPointManager) EnableProcessing() {
	sp.enabled.Store(true)
}

// DisableProcessing disables sync point processing.
func (sp *SyncPointManager) DisableProcessing() {
	sp.enabled.Store(false)
}

// IsEnabled returns whether sync point processing is enabled.
func (sp *SyncPointManager) IsEnabled() bool {
	return sp.enabled.Load()
}

// SetGlobal sets this manager as the global sync point manager.
func (sp *SyncPointManager) SetGlobal() {
	globalSyncPointManager.Store(sp)
}

// ClearGlobal clears the global sync point manager.
func ClearGlobal() {
	globalSyncPointManager.Store(nil)
}

// SetCallback registers a callback for a sync point.
func (sp *SyncPointManager) SetCallback(name string, callback SyncPointCallback) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.callbacks[name] = append(sp.callbacks[name], callback)
}

// ClearCallback removes all callbacks for a sync point.
func (sp *SyncPointManager) ClearCallback(name string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	delete(sp.callbacks, name)
}

// ClearAllCallbacks removes all callbacks.
func (sp *SyncPointManager) ClearAllCallbacks() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.callbacks = make(map[string][]SyncPointCallback)
}

// SetDelayBeforeProcessing adds a delay when a sync point is reached.
func (sp *SyncPointManager) SetDelayBeforeProcessing(name string, delay time.Duration) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.delays[name] = delay
}

// ClearDelay removes the delay for a sync point.
func (sp *SyncPointManager) ClearDelay(name string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	delete(sp.delays, name)
}

// SetErrorInjection sets an error to be returned when a sync point is reached.
func (sp *SyncPointManager) SetErrorInjection(name string, err error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.errorInjections[name] = err
}

// ClearErrorInjection removes error injection for a sync point.
func (sp *SyncPointManager) ClearErrorInjection(name string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	delete(sp.errorInjections, name)
}

// BlockSyncPoint causes execution to block at the named sync point until
// ClearSyncPoint is called.
func (sp *SyncPointManager) BlockSyncPoint(name string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if _, exists := sp.blockedPoints[name]; !exists {
		sp.blockedPoints[name] = make(chan struct{})
	}
	sp.clearedPoints[name] = false
}

// ClearSyncPoint signals blocked executions at the named sync point to continue.
func (sp *SyncPointManager) ClearSyncPoint(name string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.clearedPoints[name] = true
	if ch, exists := sp.blockedPoints[name]; exists {
		close(ch)
		// Create a new channel for future blocks
		sp.blockedPoints[name] = make(chan struct{})
	}
}

// ClearAllSyncPoints signals all blocked sync points to continue.
func (sp *SyncPointManager) ClearAllSyncPoints() {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for name, ch := range sp.blockedPoints {
		sp.clearedPoints[name] = true
		close(ch)
	}
	sp.blockedPoints = make(map[string]chan struct{})
}

// LoadDependency sets up an ordering dependency: "after" point will wait
// until "before" point has been hit.
func (sp *SyncPointManager) LoadDependency(dependencies []SyncPointDependency) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for _, dep := range dependencies {
		sp.dependencies[dep.After] = append(sp.dependencies[dep.After], dep.Before)
		if _, exists := sp.dependencySignals[dep.Before]; !exists {
			sp.dependencySignals[dep.Before] = make(chan struct{})
		}
	}
}

// SyncPointDependency defines an ordering: After point waits for Before point.
type SyncPointDependency struct {
	Before string
	After  string
}

// ClearDependency removes all dependencies for a sync point.
func (sp *SyncPointManager) ClearDependency(name string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	delete(sp.dependencies, name)
}

// ClearAllDependencies removes all dependencies.
func (sp *SyncPointManager) ClearAllDependencies() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.dependencies = make(map[string][]string)
	sp.dependencySignals = make(map[string]chan struct{})
}

// GetHitCount returns the number of times a sync point was hit.
func (sp *SyncPointManager) GetHitCount(name string) int64 {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.hitCounts[name]
}

// Reset clears all callbacks, dependencies, and hit counts.
func (sp *SyncPointManager) Reset() {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.callbacks = make(map[string][]SyncPointCallback)
	sp.hitCounts = make(map[string]int64)
	sp.blockedPoints = make(map[string]chan struct{})
	sp.clearedPoints = make(map[string]bool)
	sp.errorInjections = make(map[string]error)
	sp.delays = make(map[string]time.Duration)
	sp.dependencies = make(map[string][]string)
	sp.dependencySignals = make(map[string]chan struct{})
	sp.enabled.Store(false)
}

// Process is called when a sync point is reached.
// Returns an error if error injection is configured for this point.
func (sp *SyncPointManager) Process(name string) error {
	if !sp.enabled.Load() {
		return nil
	}

	// Wait for dependencies first
	sp.waitForDependencies(name)

	// Apply delay if configured
	sp.mu.RLock()
	delay := sp.delays[name]
	sp.mu.RUnlock()
	if delay > 0 {
		time.Sleep(delay)
	}

	// Wait if blocked
	sp.waitIfBlocked(name)

	// Update hit count
	sp.mu.Lock()
	sp.hitCounts[name]++
	sp.mu.Unlock()

	// Signal that this point was hit (for dependencies)
	sp.signalDependency(name)

	// Execute callbacks
	sp.mu.RLock()
	callbacks := sp.callbacks[name]
	sp.mu.RUnlock()

	for _, cb := range callbacks {
		if err := cb(name); err != nil {
			return err
		}
	}

	// Check error injection
	sp.mu.RLock()
	injectedErr := sp.errorInjections[name]
	sp.mu.RUnlock()

	return injectedErr
}

// waitForDependencies waits for all dependencies to be satisfied.
func (sp *SyncPointManager) waitForDependencies(name string) {
	sp.mu.RLock()
	deps := sp.dependencies[name]
	signals := make([]chan struct{}, 0, len(deps))
	for _, dep := range deps {
		if sig, exists := sp.dependencySignals[dep]; exists {
			signals = append(signals, sig)
		}
	}
	sp.mu.RUnlock()

	// Wait for all dependencies
	for _, sig := range signals {
		<-sig
	}
}

// signalDependency signals that this sync point was hit.
func (sp *SyncPointManager) signalDependency(name string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if sig, exists := sp.dependencySignals[name]; exists {
		select {
		case <-sig:
			// Already closed
		default:
			close(sig)
		}
	}
}

// waitIfBlocked waits if the sync point is blocked.
func (sp *SyncPointManager) waitIfBlocked(name string) {
	sp.mu.RLock()
	ch, isBlocked := sp.blockedPoints[name]
	cleared := sp.clearedPoints[name]
	sp.mu.RUnlock()

	if isBlocked && !cleared {
		<-ch
	}
}

// SyncPointProcess is called from production code to process a sync point.
// This uses the global sync point manager.
// In production builds, this should be optimized away (inlined to nothing).
func SyncPointProcess(name string) error {
	mgr := globalSyncPointManager.Load()
	if mgr == nil {
		return nil
	}
	return mgr.Process(name)
}

// SyncPointProcessWithData is like SyncPointProcess but allows passing data.
// The data can be accessed in callbacks via a thread-local mechanism.
// TODO: Implement thread-local data storage for sync point callbacks.
func SyncPointProcessWithData(name string, _ any) error {
	mgr := globalSyncPointManager.Load()
	if mgr == nil {
		return nil
	}
	return mgr.Process(name)
}

// MarkerFunc creates a marker function that records when called.
// Useful for verifying code paths in tests.
func (sp *SyncPointManager) MarkerFunc(name string) func() {
	return func() {
		_ = sp.Process(name) // Marker functions ignore errors
	}
}

// WaitUntilHit blocks until the named sync point has been hit at least once.
// timeout specifies the maximum time to wait.
// Returns true if the point was hit, false if timeout elapsed.
func (sp *SyncPointManager) WaitUntilHit(name string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sp.GetHitCount(name) > 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// WaitUntilHitCount blocks until the named sync point has been hit at least n times.
func (sp *SyncPointManager) WaitUntilHitCount(name string, n int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sp.GetHitCount(name) >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}
