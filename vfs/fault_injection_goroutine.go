// Package vfs provides filesystem abstractions including fault injection for testing.
//
// This file provides goroutine-local fault injection capabilities,
// allowing different goroutines to have different error injection settings.
// This is useful for testing concurrent code with targeted fault injection.
//
// Reference: RocksDB v10.7.5
//   - utilities/fault_injection_fs.h (thread-local error injection)
//   - utilities/fault_injection_fs.cc
package vfs

import (
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
)

// ErrorType represents the type of error to inject.
type ErrorType int

const (
	// ErrorTypeStatus returns an error status
	ErrorTypeStatus ErrorType = iota
	// ErrorTypeCorruption corrupts data (for reads)
	ErrorTypeCorruption
	// ErrorTypeTruncated returns truncated data (for reads)
	ErrorTypeTruncated
)

// GoroutineFaultContext holds fault injection settings for a goroutine.
type GoroutineFaultContext struct {
	// Error injection settings
	ReadErrorOneIn     int // Inject read error 1 in N times (0 = disabled)
	WriteErrorOneIn    int // Inject write error 1 in N times (0 = disabled)
	MetadataErrorOneIn int // Inject metadata error 1 in N times (0 = disabled)
	SyncErrorOneIn     int // Inject sync error 1 in N times (0 = disabled)

	// Error characteristics
	ErrorType ErrorType // Type of error to inject
	Retryable bool      // Whether injected errors are retryable

	// Statistics
	ReadErrorsInjected  atomic.Uint64
	WriteErrorsInjected atomic.Uint64
	SyncErrorsInjected  atomic.Uint64

	// Random seed for this context
	rng *rand.Rand
	mu  sync.Mutex
}

// NewGoroutineFaultContext creates a new fault context with the given seed.
func NewGoroutineFaultContext(seed int64) *GoroutineFaultContext {
	return &GoroutineFaultContext{
		rng: rand.New(rand.NewSource(seed)),
	}
}

// ShouldInjectReadError returns true if a read error should be injected.
func (ctx *GoroutineFaultContext) ShouldInjectReadError() bool {
	if ctx.ReadErrorOneIn <= 0 {
		return false
	}
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.rng.Intn(ctx.ReadErrorOneIn) == 0 {
		ctx.ReadErrorsInjected.Add(1)
		return true
	}
	return false
}

// ShouldInjectWriteError returns true if a write error should be injected.
func (ctx *GoroutineFaultContext) ShouldInjectWriteError() bool {
	if ctx.WriteErrorOneIn <= 0 {
		return false
	}
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.rng.Intn(ctx.WriteErrorOneIn) == 0 {
		ctx.WriteErrorsInjected.Add(1)
		return true
	}
	return false
}

// ShouldInjectSyncError returns true if a sync error should be injected.
func (ctx *GoroutineFaultContext) ShouldInjectSyncError() bool {
	if ctx.SyncErrorOneIn <= 0 {
		return false
	}
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.rng.Intn(ctx.SyncErrorOneIn) == 0 {
		ctx.SyncErrorsInjected.Add(1)
		return true
	}
	return false
}

// GoroutineFaultManager manages per-goroutine fault injection contexts.
type GoroutineFaultManager struct {
	mu       sync.RWMutex
	contexts map[int64]*GoroutineFaultContext

	// Global settings that apply to all goroutines without specific contexts
	globalEnabled         atomic.Bool
	globalReadErrorOneIn  atomic.Int32
	globalWriteErrorOneIn atomic.Int32
	globalSyncErrorOneIn  atomic.Int32

	// Persistent stats that survive context cleanup.
	// These are incremented when contexts are cleared.
	totalReadErrors  atomic.Uint64
	totalWriteErrors atomic.Uint64
	totalSyncErrors  atomic.Uint64
}

// NewGoroutineFaultManager creates a new goroutine fault manager.
func NewGoroutineFaultManager() *GoroutineFaultManager {
	return &GoroutineFaultManager{
		contexts: make(map[int64]*GoroutineFaultContext),
	}
}

// getGoroutineID returns the current goroutine ID.
// This uses a runtime trick and should only be used for testing.
func getGoroutineID() int64 {
	buf := make([]byte, 64)
	n := runtime.Stack(buf, false)
	// Parse goroutine ID from stack trace
	// Format: "goroutine N [...]"
	var id int64
	for i := len("goroutine "); i < n; i++ {
		if buf[i] == ' ' {
			break
		}
		id = id*10 + int64(buf[i]-'0')
	}
	return id
}

// SetContext sets the fault context for the current goroutine.
func (m *GoroutineFaultManager) SetContext(ctx *GoroutineFaultContext) {
	gid := getGoroutineID()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.contexts[gid] = ctx
}

// ClearContext clears the fault context for the current goroutine.
// Stats from the context are accumulated before clearing.
func (m *GoroutineFaultManager) ClearContext() {
	gid := getGoroutineID()
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx, ok := m.contexts[gid]; ok {
		// Accumulate stats before clearing
		m.totalReadErrors.Add(ctx.ReadErrorsInjected.Load())
		m.totalWriteErrors.Add(ctx.WriteErrorsInjected.Load())
		m.totalSyncErrors.Add(ctx.SyncErrorsInjected.Load())
		delete(m.contexts, gid)
	}
}

// GetContext gets the fault context for the current goroutine.
func (m *GoroutineFaultManager) GetContext() *GoroutineFaultContext {
	gid := getGoroutineID()
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.contexts[gid]
}

// SetGlobalReadErrorRate sets the global read error injection rate.
func (m *GoroutineFaultManager) SetGlobalReadErrorRate(oneIn int) {
	m.globalReadErrorOneIn.Store(int32(oneIn))
	m.globalEnabled.Store(true)
}

// SetGlobalWriteErrorRate sets the global write error injection rate.
func (m *GoroutineFaultManager) SetGlobalWriteErrorRate(oneIn int) {
	m.globalWriteErrorOneIn.Store(int32(oneIn))
	m.globalEnabled.Store(true)
}

// SetGlobalSyncErrorRate sets the global sync error injection rate.
func (m *GoroutineFaultManager) SetGlobalSyncErrorRate(oneIn int) {
	m.globalSyncErrorOneIn.Store(int32(oneIn))
	m.globalEnabled.Store(true)
}

// DisableGlobal disables global error injection.
func (m *GoroutineFaultManager) DisableGlobal() {
	m.globalEnabled.Store(false)
	m.globalReadErrorOneIn.Store(0)
	m.globalWriteErrorOneIn.Store(0)
	m.globalSyncErrorOneIn.Store(0)
}

// ShouldInjectReadError checks if a read error should be injected
// for the current goroutine.
func (m *GoroutineFaultManager) ShouldInjectReadError() bool {
	// Check goroutine-specific context first
	ctx := m.GetContext()
	if ctx != nil {
		return ctx.ShouldInjectReadError()
	}

	// Fall back to global settings
	if !m.globalEnabled.Load() {
		return false
	}
	oneIn := int(m.globalReadErrorOneIn.Load())
	if oneIn <= 0 {
		return false
	}
	if rand.Intn(oneIn) == 0 {
		m.totalReadErrors.Add(1)
		return true
	}
	return false
}

// ShouldInjectWriteError checks if a write error should be injected
// for the current goroutine.
func (m *GoroutineFaultManager) ShouldInjectWriteError() bool {
	ctx := m.GetContext()
	if ctx != nil {
		return ctx.ShouldInjectWriteError()
	}

	if !m.globalEnabled.Load() {
		return false
	}
	oneIn := int(m.globalWriteErrorOneIn.Load())
	if oneIn <= 0 {
		return false
	}
	if rand.Intn(oneIn) == 0 {
		m.totalWriteErrors.Add(1)
		return true
	}
	return false
}

// ShouldInjectSyncError checks if a sync error should be injected
// for the current goroutine.
func (m *GoroutineFaultManager) ShouldInjectSyncError() bool {
	ctx := m.GetContext()
	if ctx != nil {
		return ctx.ShouldInjectSyncError()
	}

	if !m.globalEnabled.Load() {
		return false
	}
	oneIn := int(m.globalSyncErrorOneIn.Load())
	if oneIn <= 0 {
		return false
	}
	if rand.Intn(oneIn) == 0 {
		m.totalSyncErrors.Add(1)
		return true
	}
	return false
}

// Stats returns aggregate statistics across all contexts.
func (m *GoroutineFaultManager) Stats() (reads, writes, syncs uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Include stats from accumulated (cleared) contexts
	reads = m.totalReadErrors.Load()
	writes = m.totalWriteErrors.Load()
	syncs = m.totalSyncErrors.Load()

	// Add stats from still-active contexts
	for _, ctx := range m.contexts {
		reads += ctx.ReadErrorsInjected.Load()
		writes += ctx.WriteErrorsInjected.Load()
		syncs += ctx.SyncErrorsInjected.Load()
	}
	return
}

// GoroutineLocalFaultInjectionFS extends FaultInjectionFS with goroutine-local capabilities.
type GoroutineLocalFaultInjectionFS struct {
	*FaultInjectionFS
	faultManager *GoroutineFaultManager
}

// NewGoroutineLocalFaultInjectionFS creates a new goroutine-local fault injection FS.
func NewGoroutineLocalFaultInjectionFS(base FS) *GoroutineLocalFaultInjectionFS {
	return &GoroutineLocalFaultInjectionFS{
		FaultInjectionFS: NewFaultInjectionFS(base),
		faultManager:     NewGoroutineFaultManager(),
	}
}

// FaultManager returns the goroutine fault manager.
func (fs *GoroutineLocalFaultInjectionFS) FaultManager() *GoroutineFaultManager {
	return fs.faultManager
}

// Create creates a new file with goroutine-local fault injection.
func (fs *GoroutineLocalFaultInjectionFS) Create(name string) (WritableFile, error) {
	if fs.faultManager.ShouldInjectWriteError() {
		return nil, ErrInjectedWriteError
	}
	return fs.FaultInjectionFS.Create(name)
}

// Open opens a file with goroutine-local fault injection.
func (fs *GoroutineLocalFaultInjectionFS) Open(name string) (SequentialFile, error) {
	if fs.faultManager.ShouldInjectReadError() {
		return nil, ErrInjectedReadError
	}
	return fs.FaultInjectionFS.Open(name)
}

// OpenRandomAccess opens a file with goroutine-local fault injection.
func (fs *GoroutineLocalFaultInjectionFS) OpenRandomAccess(name string) (RandomAccessFile, error) {
	if fs.faultManager.ShouldInjectReadError() {
		return nil, ErrInjectedReadError
	}
	return fs.FaultInjectionFS.OpenRandomAccess(name)
}
