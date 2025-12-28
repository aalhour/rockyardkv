//go:build !synctest

// Package testutil provides test utilities for stress testing and verification.
//
// This file provides no-op stubs for sync point functions in production builds.
// These compile to nothing, ensuring zero runtime overhead.
//
// To enable sync points for testing, build with: go build -tags synctest
package testutil

// SP is a no-op in production builds.
// In test builds (-tags synctest), this processes sync points for test coordination.
func SP(_ string) error { return nil }

// SPCallback is a no-op in production builds.
func SPCallback(_ string, _ any) error { return nil }

// ProcessSyncPoint is a no-op in production builds.
func ProcessSyncPoint(_ string) error { return nil }

// EnableSyncPoints is a no-op in production builds.
// Returns nil since SyncPointManager is not available.
func EnableSyncPoints() *SyncPointManager { return nil }

// DisableSyncPoints is a no-op in production builds.
func DisableSyncPoints() {}

// SyncPointManager is a stub type for production builds.
// The full implementation is only available with -tags synctest.
type SyncPointManager struct{}
