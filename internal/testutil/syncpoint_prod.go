//go:build synctest

// Package testutil provides test utilities for stress testing and verification.
//
// This file provides sync point hooks for testing. Only available with -tags synctest.
//
// Sync points are named locations in the code where tests can:
// - Inject delays
// - Inject errors
// - Force specific orderings of concurrent operations
// - Verify that code paths are executed
//
// Reference: RocksDB v10.7.5
//   - test_util/sync_point.h
//   - test_util/sync_point_impl.cc
package testutil

// SyncPointEnabled controls whether sync points are processed.
// In production, this should be false for zero overhead.
// Tests set this to true and configure the global manager.
var SyncPointEnabled = false

// ProcessSyncPoint is the main entry point for sync point processing.
// It's designed to have minimal overhead when disabled.
//
// Usage in production code:
//
//	if testutil.SyncPointEnabled {
//	    testutil.ProcessSyncPoint("DBImpl::Write:Start")
//	}
//
// Or use the convenience function:
//
//	testutil.SP("DBImpl::Write:Start")
func ProcessSyncPoint(name string) error {
	if !SyncPointEnabled {
		return nil
	}
	return SyncPointProcess(name)
}

// SP is a convenience alias for ProcessSyncPoint.
// It's short to minimize code noise in production code.
func SP(name string) error {
	if !SyncPointEnabled {
		return nil
	}
	return SyncPointProcess(name)
}

// SPCallback processes a sync point with optional callback data.
func SPCallback(name string, data any) error {
	if !SyncPointEnabled {
		return nil
	}
	return SyncPointProcessWithData(name, data)
}

// EnableSyncPoints enables sync point processing globally.
// Call this at the start of tests that need sync points.
func EnableSyncPoints() *SyncPointManager {
	mgr := NewSyncPointManager()
	mgr.EnableProcessing()
	mgr.SetGlobal()
	SyncPointEnabled = true
	return mgr
}

// DisableSyncPoints disables sync point processing.
// Call this to restore normal operation after tests.
func DisableSyncPoints() {
	SyncPointEnabled = false
	ClearGlobal()
}
