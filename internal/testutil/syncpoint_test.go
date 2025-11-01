package testutil

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSyncPointManagerBasic(t *testing.T) {
	sp := NewSyncPointManager()
	if sp == nil {
		t.Fatal("expected non-nil SyncPointManager")
	}

	if sp.IsEnabled() {
		t.Error("new manager should be disabled")
	}

	sp.EnableProcessing()
	if !sp.IsEnabled() {
		t.Error("manager should be enabled after EnableProcessing")
	}

	sp.DisableProcessing()
	if sp.IsEnabled() {
		t.Error("manager should be disabled after DisableProcessing")
	}
}

func TestSyncPointCallback(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	var called bool
	sp.SetCallback("test_point", func(name string) error {
		called = true
		if name != "test_point" {
			t.Errorf("callback name = %q, want %q", name, "test_point")
		}
		return nil
	})

	err := sp.Process("test_point")
	if err != nil {
		t.Errorf("Process returned error: %v", err)
	}

	if !called {
		t.Error("callback was not called")
	}
}

func TestSyncPointMultipleCallbacks(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	var count atomic.Int32
	sp.SetCallback("point", func(name string) error {
		count.Add(1)
		return nil
	})
	sp.SetCallback("point", func(name string) error {
		count.Add(10)
		return nil
	})

	sp.Process("point")

	if count.Load() != 11 {
		t.Errorf("count = %d, want 11", count.Load())
	}
}

func TestSyncPointCallbackError(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	expectedErr := errors.New("callback error")
	sp.SetCallback("error_point", func(name string) error {
		return expectedErr
	})

	err := sp.Process("error_point")
	if !errors.Is(err, expectedErr) {
		t.Errorf("Process error = %v, want %v", err, expectedErr)
	}
}

func TestSyncPointClearCallback(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	var called bool
	sp.SetCallback("point", func(name string) error {
		called = true
		return nil
	})

	sp.ClearCallback("point")
	sp.Process("point")

	if called {
		t.Error("callback should not be called after ClearCallback")
	}
}

func TestSyncPointHitCount(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	if sp.GetHitCount("point") != 0 {
		t.Errorf("initial hit count = %d, want 0", sp.GetHitCount("point"))
	}

	sp.Process("point")
	sp.Process("point")
	sp.Process("point")

	if sp.GetHitCount("point") != 3 {
		t.Errorf("hit count = %d, want 3", sp.GetHitCount("point"))
	}
}

func TestSyncPointDisabled(t *testing.T) {
	sp := NewSyncPointManager()
	// Not enabled

	var called bool
	sp.SetCallback("point", func(name string) error {
		called = true
		return nil
	})

	sp.Process("point")

	if called {
		t.Error("callback should not be called when disabled")
	}

	if sp.GetHitCount("point") != 0 {
		t.Error("hit count should be 0 when disabled")
	}
}

func TestSyncPointErrorInjection(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	expectedErr := errors.New("injected error")
	sp.SetErrorInjection("error_point", expectedErr)

	err := sp.Process("error_point")
	if !errors.Is(err, expectedErr) {
		t.Errorf("Process error = %v, want %v", err, expectedErr)
	}

	sp.ClearErrorInjection("error_point")
	err = sp.Process("error_point")
	if err != nil {
		t.Errorf("Process error after clear = %v, want nil", err)
	}
}

func TestSyncPointDelay(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	delay := 50 * time.Millisecond
	sp.SetDelayBeforeProcessing("delay_point", delay)

	start := time.Now()
	sp.Process("delay_point")
	elapsed := time.Since(start)

	if elapsed < delay {
		t.Errorf("elapsed = %v, want at least %v", elapsed, delay)
	}

	sp.ClearDelay("delay_point")
	start = time.Now()
	sp.Process("delay_point")
	elapsed = time.Since(start)

	if elapsed > delay/2 {
		t.Errorf("after clear, elapsed = %v, expected minimal delay", elapsed)
	}
}

func TestSyncPointBlockAndClear(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	sp.BlockSyncPoint("blocked_point")

	var reached atomic.Bool
	go func() {
		sp.Process("blocked_point")
		reached.Store(true)
	}()

	// Give some time for the goroutine to reach the block
	time.Sleep(50 * time.Millisecond)

	if reached.Load() {
		t.Error("goroutine should be blocked")
	}

	sp.ClearSyncPoint("blocked_point")

	// Wait for goroutine to complete
	time.Sleep(50 * time.Millisecond)

	if !reached.Load() {
		t.Error("goroutine should have completed after clear")
	}
}

func TestSyncPointDependency(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	// Set up: B depends on A (B cannot run until A is hit)
	sp.LoadDependency([]SyncPointDependency{
		{Before: "point_A", After: "point_B"},
	})

	var order []string
	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(2)

	// Start B first (it should wait)
	go func() {
		defer wg.Done()
		sp.Process("point_B")
		mu.Lock()
		order = append(order, "B")
		mu.Unlock()
	}()

	// Give B time to reach dependency wait
	time.Sleep(50 * time.Millisecond)

	// Now hit A
	go func() {
		defer wg.Done()
		mu.Lock()
		order = append(order, "A")
		mu.Unlock()
		sp.Process("point_A")
	}()

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(order) != 2 {
		t.Fatalf("order has %d items, want 2", len(order))
	}
	if order[0] != "A" || order[1] != "B" {
		t.Errorf("order = %v, want [A, B]", order)
	}
}

func TestSyncPointReset(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	sp.SetCallback("point", func(name string) error { return nil })
	sp.Process("point")
	sp.SetErrorInjection("point", errors.New("err"))

	sp.Reset()

	if sp.IsEnabled() {
		t.Error("should be disabled after reset")
	}
	if sp.GetHitCount("point") != 0 {
		t.Error("hit count should be 0 after reset")
	}
}

func TestSyncPointGlobal(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()
	sp.SetGlobal()
	defer ClearGlobal()

	var called bool
	sp.SetCallback("global_point", func(name string) error {
		called = true
		return nil
	})

	err := SyncPointProcess("global_point")
	if err != nil {
		t.Errorf("SyncPointProcess error = %v", err)
	}

	if !called {
		t.Error("global callback not called")
	}
}

func TestSyncPointProcessNoGlobal(t *testing.T) {
	ClearGlobal()

	// Should not panic
	err := SyncPointProcess("any_point")
	if err != nil {
		t.Errorf("SyncPointProcess with no global = %v, want nil", err)
	}
}

func TestSyncPointWaitUntilHit(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	go func() {
		time.Sleep(50 * time.Millisecond)
		sp.Process("wait_point")
	}()

	result := sp.WaitUntilHit("wait_point", 200*time.Millisecond)
	if !result {
		t.Error("WaitUntilHit should return true")
	}
}

func TestSyncPointWaitUntilHitTimeout(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	result := sp.WaitUntilHit("never_hit", 50*time.Millisecond)
	if result {
		t.Error("WaitUntilHit should return false on timeout")
	}
}

func TestSyncPointWaitUntilHitCount(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	go func() {
		for range 5 {
			time.Sleep(10 * time.Millisecond)
			sp.Process("count_point")
		}
	}()

	result := sp.WaitUntilHitCount("count_point", 3, 200*time.Millisecond)
	if !result {
		t.Error("WaitUntilHitCount should return true")
	}
}

func TestSyncPointMarkerFunc(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	marker := sp.MarkerFunc("marker_point")
	marker()
	marker()

	if sp.GetHitCount("marker_point") != 2 {
		t.Errorf("hit count = %d, want 2", sp.GetHitCount("marker_point"))
	}
}

func TestSyncPointClearAllCallbacks(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	var count atomic.Int32
	sp.SetCallback("point1", func(name string) error {
		count.Add(1)
		return nil
	})
	sp.SetCallback("point2", func(name string) error {
		count.Add(10)
		return nil
	})

	sp.ClearAllCallbacks()

	sp.Process("point1")
	sp.Process("point2")

	if count.Load() != 0 {
		t.Errorf("count = %d, want 0", count.Load())
	}
}

func TestSyncPointClearAllSyncPoints(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	sp.BlockSyncPoint("point1")
	sp.BlockSyncPoint("point2")

	var count atomic.Int32
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		sp.Process("point1")
		count.Add(1)
	}()
	go func() {
		defer wg.Done()
		sp.Process("point2")
		count.Add(1)
	}()

	// Give time to reach blocks
	time.Sleep(50 * time.Millisecond)

	if count.Load() != 0 {
		t.Error("goroutines should be blocked")
	}

	sp.ClearAllSyncPoints()
	wg.Wait()

	if count.Load() != 2 {
		t.Errorf("count = %d, want 2", count.Load())
	}
}

func TestSyncPointClearAllDependencies(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	sp.LoadDependency([]SyncPointDependency{
		{Before: "A", After: "B"},
		{Before: "C", After: "D"},
	})

	sp.ClearAllDependencies()

	// B should not wait for A anymore
	done := make(chan struct{})
	go func() {
		sp.Process("B") // Should not block
		close(done)
	}()

	select {
	case <-done:
		// Success - B didn't wait
	case <-time.After(100 * time.Millisecond):
		t.Error("B should not wait after dependencies cleared")
	}
}

func TestSyncPointClearDependency(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	sp.LoadDependency([]SyncPointDependency{
		{Before: "A", After: "B"},
	})

	sp.ClearDependency("B")

	// B should not wait for A anymore
	done := make(chan struct{})
	go func() {
		sp.Process("B")
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Error("B should not wait after its dependency cleared")
	}
}

func TestSyncPointConcurrentProcess(t *testing.T) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	var count atomic.Int64
	sp.SetCallback("concurrent", func(name string) error {
		count.Add(1)
		return nil
	})

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			for range 100 {
				sp.Process("concurrent")
			}
		})
	}

	wg.Wait()

	expected := int64(100 * 100)
	if count.Load() != expected {
		t.Errorf("count = %d, want %d", count.Load(), expected)
	}
	if sp.GetHitCount("concurrent") != expected {
		t.Errorf("hit count = %d, want %d", sp.GetHitCount("concurrent"), expected)
	}
}

func BenchmarkSyncPointDisabled(b *testing.B) {
	sp := NewSyncPointManager()
	// Not enabled - should be fast

	for b.Loop() {
		sp.Process("benchmark_point")
	}
}

func BenchmarkSyncPointEnabled(b *testing.B) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()

	for b.Loop() {
		sp.Process("benchmark_point")
	}
}

func BenchmarkSyncPointWithCallback(b *testing.B) {
	sp := NewSyncPointManager()
	sp.EnableProcessing()
	sp.SetCallback("benchmark_point", func(name string) error { return nil })

	for b.Loop() {
		sp.Process("benchmark_point")
	}
}
