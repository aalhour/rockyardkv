//go:build crashtest

// Package testutil provides test utilities for stress testing and verification.
//
// Kill points provide a mechanism to deterministically exit a process at specific
// code locations for whitebox crash testing. Unlike sync points (which pause
// execution), kill points terminate the process to simulate crashes.
//
// Reference: RocksDB v10.7.5
//   - test_util/sync_point.h (TEST_KILL_RANDOM macros)
//   - tools/db_crashtest.py (whitebox mode)
//
// Usage:
//
//	// In production code (compiled out without build tag):
//	testutil.MaybeKill("WAL.Sync:1")
//
//	// In test harness (set via env var or API):
//	testutil.SetKillPoint("WAL.Sync:1")
//
// Build with kill points enabled:
//
//	go build -tags crashtest ./...
package testutil

import (
	"os"
	"sync"
	"sync/atomic"
)

// killPointState holds the global kill point configuration.
type killPointState struct {
	// target is the name of the kill point that should trigger exit.
	// Empty string means no kill point is set.
	target atomic.Value // stores string

	// armed controls whether kill points are active.
	// This allows temporarily disabling kill points without clearing the target.
	armed atomic.Bool

	// hitCount tracks how many times each kill point was reached.
	// Useful for debugging and verification.
	mu        sync.RWMutex
	hitCounts map[string]int64
}

// globalKillPoint is the singleton kill point state.
var globalKillPoint = &killPointState{
	hitCounts: make(map[string]int64),
}

// KillPointEnvVar is the environment variable used to set the kill point target.
const KillPointEnvVar = "ROCKYARDKV_KILL_POINT"

func init() {
	// Check environment variable on startup
	if target := os.Getenv(KillPointEnvVar); target != "" {
		globalKillPoint.target.Store(target)
		globalKillPoint.armed.Store(true)
	}
}

// SetKillPoint sets the target kill point name.
// When MaybeKill is called with this name, the process will exit.
func SetKillPoint(name string) {
	globalKillPoint.target.Store(name)
	globalKillPoint.armed.Store(true)
}

// ClearKillPoint clears the kill point target.
func ClearKillPoint() {
	globalKillPoint.target.Store("")
	globalKillPoint.armed.Store(false)
}

// ArmKillPoint enables kill point processing.
func ArmKillPoint() {
	globalKillPoint.armed.Store(true)
}

// DisarmKillPoint disables kill point processing without clearing the target.
func DisarmKillPoint() {
	globalKillPoint.armed.Store(false)
}

// IsKillPointArmed returns whether kill points are currently armed.
func IsKillPointArmed() bool {
	return globalKillPoint.armed.Load()
}

// GetKillPointTarget returns the current kill point target.
func GetKillPointTarget() string {
	if v := globalKillPoint.target.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// GetKillPointHitCount returns how many times a kill point was reached.
func GetKillPointHitCount(name string) int64 {
	globalKillPoint.mu.RLock()
	defer globalKillPoint.mu.RUnlock()
	return globalKillPoint.hitCounts[name]
}

// ResetKillPointCounts resets all hit counts.
func ResetKillPointCounts() {
	globalKillPoint.mu.Lock()
	defer globalKillPoint.mu.Unlock()
	globalKillPoint.hitCounts = make(map[string]int64)
}

// MaybeKill checks if the named kill point matches the target and exits if so.
// This is the primary entry point for kill points in production code.
//
// If the kill point is armed and the name matches the target, the process
// exits with code 0 (clean exit, not a crash signal).
func MaybeKill(name string) {
	if !globalKillPoint.armed.Load() {
		return
	}

	// Track hit count
	globalKillPoint.mu.Lock()
	globalKillPoint.hitCounts[name]++
	globalKillPoint.mu.Unlock()

	// Check if this is the target
	target, ok := globalKillPoint.target.Load().(string)
	if !ok || target == "" {
		return
	}

	if target == name {
		// Exit cleanly to simulate a crash
		// Exit code 0 indicates intentional kill, not an error
		os.Exit(0)
	}
}

// KillPointNames defines the standard kill point names.
// These follow RocksDB's naming convention: "Component.Operation:N"
// where N is 0 for "before" and 1 for "after".
const (
	// WAL kill points
	KPWALAppend0 = "WAL.Append:0" // During WAL append (before write completes)
	KPWALSync0   = "WAL.Sync:0"   // Before WAL sync
	KPWALSync1   = "WAL.Sync:1"   // After WAL sync

	// MANIFEST kill points
	KPManifestWrite0 = "Manifest.Write:0" // During MANIFEST record write
	KPManifestSync0  = "Manifest.Sync:0"  // Before MANIFEST sync
	KPManifestSync1  = "Manifest.Sync:1"  // After MANIFEST sync

	// CURRENT file kill points
	KPCurrentWrite0 = "Current.Write:0" // Before CURRENT update
	KPCurrentWrite1 = "Current.Write:1" // After CURRENT update

	// Flush kill points
	KPFlushStart0          = "Flush.Start:0"          // At flush start
	KPFlushWriteSST0       = "Flush.WriteSST:0"       // During SST write
	KPFlushUpdateManifest0 = "Flush.UpdateManifest:0" // Before MANIFEST update for flush
	KPFlushUpdateManifest1 = "Flush.UpdateManifest:1" // After MANIFEST update for flush

	// Compaction kill points
	KPCompactionStart0       = "Compaction.Start:0"       // At compaction start
	KPCompactionWriteSST0    = "Compaction.WriteSST:0"    // During SST write
	KPCompactionDeleteInput0 = "Compaction.DeleteInput:0" // Before deleting old SST

	// SST file kill points
	KPSSTClose0 = "SST.Close:0" // Before SST close
	KPSSTClose1 = "SST.Close:1" // After SST close

	// Generic file kill points
	KPFileSync0 = "File.Sync:0" // Before file sync
	KPFileSync1 = "File.Sync:1" // After file sync
)
