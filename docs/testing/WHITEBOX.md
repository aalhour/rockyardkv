# Whitebox Crash Testing

Whitebox tests inject behavior at specific code points to verify recovery boundaries.
Unlike blackbox tests (random timing), whitebox tests guarantee specific crash points are exercised.

## Overview

RockyardKV provides two whitebox mechanisms:

| Mechanism | Build tag | Purpose |
|---|---|---|
| Kill points | `crashtest` | Deterministic process exit for crash recovery tests |
| Sync points | `synctest` | Barriers for concurrent test coordination |

Without the build tag, these calls compile to no-ops with zero runtime overhead.

## Kill Points

Kill points are named locations where the process can be forced to exit.
They simulate crashes at precise moments in the write path.

### Available Kill Points

#### WAL Operations

| Kill point | Location | Tests |
|---|---|---|
| `WAL.Append:0` | Before WAL record write | Partial write recovery |
| `WAL.Sync:0` | Before WAL sync | Unsynced data loss |
| `WAL.Sync:1` | After WAL sync | Synced data durability |

#### MANIFEST Operations

| Kill point | Location | Tests |
|---|---|---|
| `Manifest.Write:0` | Before MANIFEST write | Partial manifest handling |
| `Manifest.Sync:0` | Before MANIFEST sync | Partial MANIFEST recovery |
| `Manifest.Sync:1` | After MANIFEST sync | MANIFEST durability |

#### CURRENT File Operations

| Kill point | Location | Tests |
|---|---|---|
| `Current.Write:0` | Before CURRENT update | Previous MANIFEST recovery |
| `Current.Write:1` | After CURRENT update | New MANIFEST active |

#### Flush Operations

| Kill point | Location | Tests |
|---|---|---|
| `Flush.Start:0` | Before flush begins | Memtable durability |
| `Flush.WriteSST:0` | Before SST write | Incomplete SST cleanup |
| `Flush.UpdateManifest:0` | Before manifest update | SST orphan handling |
| `Flush.UpdateManifest:1` | After manifest update | Flush complete |

#### SST File Operations

| Kill point | Location | Tests |
|---|---|---|
| `SST.Close:0` | Before SST finalize | Incomplete SST handling |
| `SST.Close:1` | After SST complete | SST valid on disk |

#### Compaction Operations

| Kill point | Location | Tests |
|---|---|---|
| `Compaction.Start:0` | Before compaction | Compaction cancellation |
| `Compaction.WriteSST:0` | After output written | Output exists, manifest not updated |
| `Compaction.DeleteInput:0` | Before input deletion | Both inputs and outputs exist |

#### File Sync Operations

| Kill point | Location | Tests |
|---|---|---|
| `File.Sync:0` | Before file sync | Unsynced SST durability |
| `File.Sync:1` | After file sync | SST fully durable |

#### Directory Sync Operations

| Kill point | Location | Tests |
|---|---|---|
| `Dir.Sync:0` | Before directory sync | CURRENT may not be durable |
| `Dir.Sync:1` | After directory sync | CURRENT fully durable |

### Run Whitebox Tests

Run all whitebox scenario tests:

```bash
go test -tags crashtest ./cmd/crashtest/... -run TestScenarioWhitebox
```

Run the sweep test (exercises all kill points):

```bash
go test -tags crashtest ./cmd/crashtest/... -run TestScenarioWhitebox_Sweep -v
```

Run with strict mode (fail if any kill point is not triggered):

```bash
WHITEBOX_STRICT_SWEEP=1 go test -tags crashtest ./cmd/crashtest/... -run TestScenarioWhitebox_Sweep -v
```

### Artifact Collection

Enable artifact persistence for debugging:

```bash
WHITEBOX_ALWAYS_PERSIST=1 \
WHITEBOX_ARTIFACT_DIR=./artifacts \
  go test -tags crashtest ./cmd/crashtest/... -run TestScenarioWhitebox_WALSync1
```

Artifacts include:

- `db/` — Copy of the database directory
- `run.json` — Test metadata and repro command
- `stdout.log`, `stderr.log` — Captured output

### C++ Oracle Integration

Enable C++ RocksDB tool verification on artifacts:

```bash
ROCKYARDKV_CPP_ORACLE_PATH=/path/to/rocksdb \
WHITEBOX_ALWAYS_PERSIST=1 \
WHITEBOX_ARTIFACT_DIR=./artifacts \
  go test -tags crashtest ./cmd/crashtest/... -run TestScenarioWhitebox_WALSync1
```

The oracle generates:

- `ldb_manifest_dump.txt` — MANIFEST contents
- `ldb_scan.txt` — All key-value pairs
- `sst_dump_*.txt` — SST file verification
- `current.txt`, `manifest_selected.txt`, `manifest_list.txt` — Diagnostic metadata

### Add a New Kill Point

1. Import the `testutil` package
2. Add a comment: `// Whitebox [crashtest]: crash before X — tests Y`
3. Add `testutil.MaybeKill(testutil.KPComponentAction0)` at the target location
4. Define the constant in `internal/testutil/killpoint.go`
5. Write a whitebox scenario test

The kill point name follows the format `Component.Action:N`, where:
- `N=0` means "before" the operation
- `N=1` means "after" the operation

## Sync Points

Sync points are named barriers where tests can inject behavior during concurrent operations.

### Capabilities

Sync points allow tests to:

- Inject delays between operations
- Inject errors at specific points
- Force specific orderings of concurrent operations
- Verify that code paths are executed

### Run Sync Point Tests

```bash
go test -tags synctest ./internal/testutil/... -run TestSync
```

### Combined Testing

For tests that need both mechanisms:

```bash
go test -tags "crashtest synctest" ./...
```

## Production Overhead

Both kill points and sync points compile to no-ops without their respective build tags:

```bash
# Production build (zero overhead)
go build ./...

# Test builds
go build -tags crashtest ./...
go build -tags synctest ./...
go build -tags "crashtest synctest" ./...
```

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `WHITEBOX_KILL_POINT` | Kill point to trigger (child process) |
| `WHITEBOX_DB_PATH` | Database path (child process) |
| `WHITEBOX_STRICT_SWEEP` | Fail sweep if any kill point is not hit |
| `WHITEBOX_ALWAYS_PERSIST` | Save artifacts even on success |
| `WHITEBOX_ARTIFACT_DIR` | Directory for artifacts |
| `ROCKYARDKV_CPP_ORACLE_PATH` | Path to C++ RocksDB tools |

## References

- Refer to [Fault injection](FAULT_INJECTION.md) for VFS-based fault simulation
- Refer to [Blackbox testing](BLACKBOX.md) for crash, stress, and adversarial harnesses
- RocksDB `db_crashtest.py` — whitebox testing mode
- RocksDB `test_util/sync_point.h` — `TEST_KILL_RANDOM` macros

