# Fault injection

This document describes the VFS-based fault injection mechanisms for testing durability and error handling.

## Table of contents

- [Overview](#overview)
- [FaultInjectionFS](#faultinjectionfs)
- [GoroutineLocalFaultInjectionFS](#goroutinelocalfaultinjectionfs)
- [Test harness integration](#test-harness-integration)
- [Design decisions](#design-decisions)
- [References](#references)

## Overview

RockyardKV provides a layered fault injection system through the Virtual File System (VFS) abstraction.
Tests use these injectors to simulate filesystem failures, lies, and anomalies.

| Component | Location | Purpose |
|-----------|----------|---------|
| `FaultInjectionFS` | `vfs/fault_injection.go` | Deterministic filesystem fault simulation |
| `GoroutineLocalFaultInjectionFS` | `vfs/fault_injection_goroutine.go` | Per-goroutine probabilistic fault injection |

## FaultInjectionFS

`FaultInjectionFS` wraps an underlying VFS and provides deterministic control over filesystem behavior.
Use it to simulate crashes, lies, and anomalies at specific filesystem operations.

### Basic error injection

Inject errors at specific filesystem operations:

```go
fs := vfs.NewFaultInjectionFS(vfs.Default)

// Inject a read error
fs.SetReadError(errors.New("disk read failure"))

// Inject a write error
fs.SetWriteError(errors.New("disk full"))

// Inject a sync error
fs.SetSyncError(errors.New("sync timeout"))
```

### Crash simulation

Track unsynced data and simulate crash recovery:

```go
fs := vfs.NewFaultInjectionFS(vfs.Default)

// Write data (tracked as unsynced)
f, _ := fs.Create("data.sst")
f.Write([]byte("contents"))

// Simulate crash: unsynced data is lost
fs.ResetUnsyncedData()

// Data written before Sync() is gone
```

### Rename durability model

Track rename operations that aren't durable until directory sync.
This models POSIX semantics where `rename()` is only durable after `fsync()` on the parent directory.

```go
fs := vfs.NewFaultInjectionFS(vfs.Default)

// Rename a file
fs.Rename("old.sst", "new.sst")

// Check pending (undurable) renames
if fs.HasPendingRenames() {
    fmt.Printf("Pending renames: %d\n", fs.PendingRenameCount())
}

// Simulate crash: renames are reverted
fs.RevertUnsyncedRenames()

// After SyncDir(), renames become durable
fs.SyncDir("/path/to/dir")
if !fs.HasPendingRenames() {
    fmt.Println("Renames are durable")
}
```

### SyncDir lie mode

Simulate a filesystem that claims directory sync succeeded but doesn't make renames durable:

```go
fs := vfs.NewFaultInjectionFS(vfs.Default)

// Enable lie mode
fs.SetSyncDirLieMode(true)

// Rename a file
fs.Rename("CURRENT.tmp", "CURRENT")

// SyncDir returns success but renames stay pending
fs.SyncDir("/db")

// Crash simulation: renames are reverted
fs.RevertUnsyncedRenames()
// CURRENT.tmp exists, CURRENT doesn't
```

Use this to verify the database handles lying filesystems correctly.

### File sync lie mode

Simulate a filesystem that claims file sync succeeded but doesn't persist data:

```go
fs := vfs.NewFaultInjectionFS(vfs.Default)

// Lie about all files
fs.SetFileSyncLieMode(true, "")

// Lie about specific file patterns
fs.SetFileSyncLieMode(true, ".log")  // WAL files only
fs.SetFileSyncLieMode(true, ".sst")  // SST files only
fs.SetFileSyncLieMode(true, "MANIFEST")  // MANIFEST files

// Write and "sync"
f, _ := fs.Create("000001.log")
f.Write([]byte("WAL record"))
f.Sync()  // Returns success but data is unsynced

// Crash simulation: unsynced data is lost
fs.ResetUnsyncedData()
```

### Rename anomaly modes

Simulate filesystem rename anomalies that can occur during crashes:

```go
fs := vfs.NewFaultInjectionFS(vfs.Default)

// Double-name anomaly: both old and new names exist after crash
fs.SetRenameAnomalyMode("double", "CURRENT")
fs.Rename("CURRENT.tmp", "CURRENT")
// Both CURRENT.tmp and CURRENT exist

// Neither-name anomaly: both names disappear after crash
fs.SetRenameAnomalyMode("neither", "CURRENT")
fs.Rename("CURRENT.tmp", "CURRENT")
// Neither file exists

// Disable anomaly mode
fs.SetRenameAnomalyMode("", "")
```

## GoroutineLocalFaultInjectionFS

`GoroutineLocalFaultInjectionFS` provides probabilistic fault injection that can target specific goroutines.
Use it for stress testing where you want controlled chaos without determinism.

### Per-goroutine contexts

Create fault contexts for specific goroutines:

```go
fs := vfs.NewGoroutineLocalFaultInjectionFS(vfs.Default)
manager := fs.FaultManager()

// Set fault rates for the current goroutine
ctx := manager.GetOrCreateContext()
ctx.ReadErrorRate = 0.1   // 10% read failures
ctx.WriteErrorRate = 0.05 // 5% write failures
ctx.SyncErrorRate = 0.01  // 1% sync failures
ctx.ErrorType = vfs.FaultErrorTypeIO

// Operations on this goroutine may fail
f, err := fs.Create("data.sst")
if err != nil {
    // Handle injected error
}

// Clear context when done
manager.ClearContext()
```

### Global fault rates

Apply fault injection to all goroutines (including internal database goroutines):

```go
fs := vfs.NewGoroutineLocalFaultInjectionFS(vfs.Default)
manager := fs.FaultManager()

// Set global rates (affects all operations)
manager.SetGlobalReadErrorRate(0.01)   // 1% read failures
manager.SetGlobalWriteErrorRate(0.02)  // 2% write failures
manager.SetGlobalSyncErrorRate(0.005)  // 0.5% sync failures

// Check if global injection is active
if manager.IsGlobalInjectionActive() {
    fmt.Println("Global fault injection enabled")
}

// Disable global injection
manager.SetGlobalReadErrorRate(0)
manager.SetGlobalWriteErrorRate(0)
manager.SetGlobalSyncErrorRate(0)
```

### Statistics

Collect fault injection statistics:

```go
stats := manager.Stats()
fmt.Printf("Errors injected: read=%d write=%d sync=%d\n",
    stats.ReadErrorsInjected,
    stats.WriteErrorsInjected,
    stats.SyncErrorsInjected)
```

### Error types

Configure the type of error to inject:

| Error type | Description |
|------------|-------------|
| `FaultErrorTypeIO` | Returns `os.ErrClosed` or similar I/O errors |
| `FaultErrorTypeCorruption` | Returns corruption-related errors |
| `FaultErrorTypeTimeout` | Returns deadline/timeout errors |

### Deferred activation

Defer fault injection until after initial setup (e.g., database creation):

```go
fs := vfs.NewGoroutineLocalFaultInjectionFS(vfs.Default)
manager := fs.FaultManager()

// Create database with no faults
database, _ := rockyardkv.Open(path, opts)

// Activate faults after successful open
manager.SetGlobalWriteErrorRate(0.1)
manager.SetGlobalSyncErrorRate(0.05)

// Run stress workload with faults
runStressWorkload(database)
```

## Test harness integration

### cmd/stresstest

The stress test binary supports goroutine-local fault injection:

```bash
bin/stresstest \
    -goroutine-faults \
    -fault-writer-write=10 \
    -fault-writer-sync=5 \
    -fault-flusher-sync=2 \
    -fault-reopener-read=3 \
    -fault-error-type=io \
    -duration=5m
```

| Flag | Description |
|------|-------------|
| `-goroutine-faults` | Enable goroutine-local fault injection |
| `-fault-writer-read=N` | Inject read error 1 in N for writer goroutines |
| `-fault-writer-write=N` | Inject write error 1 in N for writer goroutines |
| `-fault-writer-sync=N` | Inject sync error 1 in N for writer goroutines |
| `-fault-flusher-sync=N` | Inject sync error 1 in N for flusher goroutines |
| `-fault-reopener-read=N` | Inject read error 1 in N for reopener goroutines |
| `-fault-error-type=TYPE` | Error type: `io`, `corruption`, or `timeout` |

### cmd/crashtest

Durability scenario tests use `FaultInjectionFS` for deterministic crash simulation:

```bash
# Run all durability scenarios
go test ./cmd/crashtest/... -run TestDurability

# Run specific fault mode tests
go test ./cmd/crashtest/... -run TestDurability_SyncDirLieMode
go test ./cmd/crashtest/... -run TestDurability_FileSyncLieMode
go test ./cmd/crashtest/... -run TestDurability_RenameDoubleNameMode
```

## Design decisions

### Layered abstraction

Fault injection is implemented at the VFS layer, not the database layer.
This provides:

- **Portability** — Tests work with any filesystem
- **Isolation** — Faults don't leak between tests
- **Composability** — Multiple fault modes can combine

### Deterministic vs probabilistic

| Scenario | Use |
|----------|-----|
| Crash recovery verification | `FaultInjectionFS` with deterministic control |
| Stress testing | `GoroutineLocalFaultInjectionFS` with probabilistic rates |
| Edge case reproduction | `FaultInjectionFS` with specific sequences |

### Rename durability model

The rename durability model reflects real POSIX semantics:

1. `rename()` updates directory entries atomically
1. The update isn't durable until `fsync()` on the parent directory
1. A crash before `fsync()` can revert the rename

This catches bugs where the database assumes `rename()` implies durability.

### Lie modes rationale

Lie modes simulate filesystems that violate sync contracts:

- **SyncDir lies** — Models filesystems that buffer directory metadata
- **File sync lies** — Models filesystems with volatile write caches
- **Rename anomalies** — Models crash timing windows during rename

These help verify the database doesn't rely on filesystem behavior beyond POSIX guarantees.

## References

- Refer to [Whitebox crash testing](WHITEBOX.md) for kill points and sync points
- Refer to [Blackbox testing](BLACKBOX.md) for crash, stress, and adversarial test harnesses
- Refer to [Jepsen-style testing](JEPSEN_STYLE.md) for the test classification matrix
