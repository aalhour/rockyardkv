//go:build !crashtest

// Package testutil provides test utilities for stress testing and verification.
//
// This file provides no-op implementations of kill point functions for
// production builds. When built without the "crashtest" tag, all kill point
// calls are effectively eliminated by the compiler.
//
// Reference: RocksDB v10.7.5
//   - test_util/sync_point.h (TEST_KILL_RANDOM macros)
//   - tools/db_crashtest.py (whitebox mode)
package testutil

// KillPointEnvVar is the environment variable used to set the kill point target.
// In production builds, this is defined but ignored.
const KillPointEnvVar = "ROCKYARDKV_KILL_POINT"

// SetKillPoint is a no-op in production builds.
func SetKillPoint(_ string) {}

// ClearKillPoint is a no-op in production builds.
func ClearKillPoint() {}

// ArmKillPoint is a no-op in production builds.
func ArmKillPoint() {}

// DisarmKillPoint is a no-op in production builds.
func DisarmKillPoint() {}

// IsKillPointArmed always returns false in production builds.
func IsKillPointArmed() bool { return false }

// GetKillPointTarget always returns empty string in production builds.
func GetKillPointTarget() string { return "" }

// GetKillPointHitCount always returns 0 in production builds.
func GetKillPointHitCount(_ string) int64 { return 0 }

// ResetKillPointCounts is a no-op in production builds.
func ResetKillPointCounts() {}

// MaybeKill is a no-op in production builds.
// The compiler should inline and eliminate this entirely.
func MaybeKill(_ string) {}

// Kill point name constants - defined for API compatibility even in prod builds.
const (
	// WAL kill points
	KPWALAppend0 = "WAL.Append:0"
	KPWALSync0   = "WAL.Sync:0"
	KPWALSync1   = "WAL.Sync:1"

	// MANIFEST kill points
	KPManifestWrite0 = "Manifest.Write:0"
	KPManifestSync0  = "Manifest.Sync:0"
	KPManifestSync1  = "Manifest.Sync:1"

	// CURRENT file kill points
	KPCurrentWrite0 = "Current.Write:0"
	KPCurrentWrite1 = "Current.Write:1"

	// Flush kill points
	KPFlushStart0          = "Flush.Start:0"
	KPFlushWriteSST0       = "Flush.WriteSST:0"
	KPFlushUpdateManifest0 = "Flush.UpdateManifest:0"
	KPFlushUpdateManifest1 = "Flush.UpdateManifest:1"

	// Compaction kill points
	KPCompactionStart0       = "Compaction.Start:0"
	KPCompactionWriteSST0    = "Compaction.WriteSST:0"
	KPCompactionDeleteInput0 = "Compaction.DeleteInput:0"

	// SST file kill points
	KPSSTClose0 = "SST.Close:0"
	KPSSTClose1 = "SST.Close:1"

	// Generic file kill points
	KPFileSync0 = "File.Sync:0"
	KPFileSync1 = "File.Sync:1"

	// Directory sync kill points
	KPDirSync0 = "Dir.Sync:0"
	KPDirSync1 = "Dir.Sync:1"
)
