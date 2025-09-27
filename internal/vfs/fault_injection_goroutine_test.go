package vfs

import (
	"errors"
	"sync"
	"testing"
)

func TestGoroutineFaultContext_Basic(t *testing.T) {
	ctx := NewGoroutineFaultContext(42)

	// Initially no errors should be injected
	if ctx.ShouldInjectReadError() {
		t.Error("Unexpected read error without setting rate")
	}

	// Set error rate to 1 in 1 (always inject)
	ctx.ReadErrorOneIn = 1
	if !ctx.ShouldInjectReadError() {
		t.Error("Expected read error with 1 in 1 rate")
	}

	// Only the second call (with rate set) should have counted
	if ctx.ReadErrorsInjected.Load() != 1 {
		t.Errorf("Expected 1 injected read error, got %d", ctx.ReadErrorsInjected.Load())
	}
}

func TestGoroutineFaultContext_WriteErrors(t *testing.T) {
	ctx := NewGoroutineFaultContext(42)
	ctx.WriteErrorOneIn = 1

	count := 0
	for range 10 {
		if ctx.ShouldInjectWriteError() {
			count++
		}
	}

	if count != 10 {
		t.Errorf("Expected 10 write errors with 1 in 1 rate, got %d", count)
	}
}

func TestGoroutineFaultContext_Probabilistic(t *testing.T) {
	ctx := NewGoroutineFaultContext(12345)
	ctx.ReadErrorOneIn = 10

	// Run many iterations to test probabilistic injection
	errorCount := 0
	iterations := 10000
	for range iterations {
		if ctx.ShouldInjectReadError() {
			errorCount++
		}
	}

	// With 1 in 10, we expect roughly 10% errors
	// Allow for some variance (5% to 15%)
	expectedMin := iterations / 20     // 5%
	expectedMax := iterations * 3 / 20 // 15%

	if errorCount < expectedMin || errorCount > expectedMax {
		t.Errorf("Expected error count between %d and %d, got %d", expectedMin, expectedMax, errorCount)
	}
}

func TestGoroutineFaultManager_PerGoroutineContext(t *testing.T) {
	manager := NewGoroutineFaultManager()

	// Set context for this goroutine
	ctx := NewGoroutineFaultContext(42)
	ctx.ReadErrorOneIn = 1
	manager.SetContext(ctx)

	// Should get the context we set
	retrieved := manager.GetContext()
	if retrieved != ctx {
		t.Error("Expected to retrieve the same context")
	}

	// Clear and verify
	manager.ClearContext()
	if manager.GetContext() != nil {
		t.Error("Expected nil context after clear")
	}
}

func TestGoroutineFaultManager_DifferentGoroutines(t *testing.T) {
	manager := NewGoroutineFaultManager()

	var wg sync.WaitGroup
	errors := make([]bool, 10)

	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Even goroutines get error injection, odd ones don't
			if idx%2 == 0 {
				ctx := NewGoroutineFaultContext(int64(idx))
				ctx.ReadErrorOneIn = 1
				manager.SetContext(ctx)
			}

			errors[idx] = manager.ShouldInjectReadError()
			manager.ClearContext()
		}(i)
	}

	wg.Wait()

	for i, gotError := range errors {
		expected := i%2 == 0
		if gotError != expected {
			t.Errorf("Goroutine %d: expected error=%v, got %v", i, expected, gotError)
		}
	}
}

func TestGoroutineFaultManager_GlobalFallback(t *testing.T) {
	manager := NewGoroutineFaultManager()

	// Set global rate
	manager.SetGlobalReadErrorRate(1) // Always inject

	// Without specific context, should use global
	if !manager.ShouldInjectReadError() {
		t.Error("Expected global read error")
	}

	// Disable global
	manager.DisableGlobal()
	if manager.ShouldInjectReadError() {
		t.Error("Unexpected read error after disabling global")
	}
}

func TestGoroutineLocalFaultInjectionFS(t *testing.T) {
	base := Default()
	fs := NewGoroutineLocalFaultInjectionFS(base)

	// Set up goroutine-local fault injection
	ctx := NewGoroutineFaultContext(42)
	ctx.WriteErrorOneIn = 1
	fs.FaultManager().SetContext(ctx)

	// Should get error when creating file
	_, err := fs.Create("/tmp/test-fault-injection-goroutine")
	if !errors.Is(err, ErrInjectedWriteError) {
		t.Errorf("Expected ErrInjectedWriteError, got %v", err)
	}

	// Clear and try again
	fs.FaultManager().ClearContext()
	tmpDir := t.TempDir()
	f, err := fs.Create(tmpDir + "/test.txt")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	f.Close()
}

func TestGoroutineFaultManager_Stats(t *testing.T) {
	manager := NewGoroutineFaultManager()

	ctx1 := NewGoroutineFaultContext(1)
	ctx1.ReadErrorOneIn = 1
	ctx1.WriteErrorOneIn = 1

	ctx2 := NewGoroutineFaultContext(2)
	ctx2.SyncErrorOneIn = 1

	// Inject errors using contexts directly
	for range 5 {
		ctx1.ShouldInjectReadError()
		ctx1.ShouldInjectWriteError()
	}
	for range 3 {
		ctx2.ShouldInjectSyncError()
	}

	// Add contexts to manager
	manager.mu.Lock()
	manager.contexts[1] = ctx1
	manager.contexts[2] = ctx2
	manager.mu.Unlock()

	reads, writes, syncs := manager.Stats()
	if reads != 5 {
		t.Errorf("Expected 5 read errors, got %d", reads)
	}
	if writes != 5 {
		t.Errorf("Expected 5 write errors, got %d", writes)
	}
	if syncs != 3 {
		t.Errorf("Expected 3 sync errors, got %d", syncs)
	}
}
