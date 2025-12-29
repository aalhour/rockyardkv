# Jepsen-style tests and capability matrix

This document describes the Jepsen-style test classes that exist in RockyardKV.
You use it to understand what the test harness validates today.
You use it to identify missing instances within a test class.

## What Jepsen-style means in this repo

You treat failures as invariant breaches.
You collect artifacts for each breach.
You use RocksDB v10.7.5 as the oracle when the on-disk format or recovery classification matters.

## Supported instances map

This section lists the supported instances for each test class.
Use it as the source of truth for what is implemented.

### Crash testing

#### Blackbox crash loops

Supported instances:

- `make test-e2e-crash`.
- `cmd/crashtest` orchestrator mode:
  - Runs `cmd/stresstest` as a child process.
  - SIGKILL or equivalent termination during the run.
  - Parent reopens and verifies against the persisted expected state.

#### Whitebox crash testing with kill points

Supported instances:

- Build tag: `crashtest`.
- Targeted killpoint scenarios:
  - `cmd/crashtest/scenario_whitebox_test.go` (`TestScenarioWhitebox_*`).
- Killpoint coverage sweep:
  - `TestScenarioWhitebox_Sweep`.
  - Strict coverage mode via `WHITEBOX_STRICT_SWEEP=1`.
- Artifact persistence:
  - `WHITEBOX_ARTIFACT_DIR` selects the artifact directory.
  - `WHITEBOX_ALWAYS_PERSIST=1` persists artifacts on success.
- Oracle outputs on persisted artifacts:
  - Set `ROCKYARDKV_CPP_ORACLE_PATH=/path/to/rocksdb`.

#### Whitebox crash testing with sync points

Supported instances:

- Build tag: `synctest`.
- Syncpoint crash scenarios:
  - `cmd/crashtest/scenario_syncpoint_test.go` (`TestScenarioSyncpoint_*`).
- Artifact persistence:
  - `WHITEBOX_ARTIFACT_DIR` selects the artifact directory.
  - `WHITEBOX_ALWAYS_PERSIST=1` persists artifacts on success.

### Filesystem fault injection

Supported instances:

- Fault injection filesystem:
  - `internal/vfs/FaultInjectionFS`.
- Unsynced data loss:
  - `FaultInjectionFS.DropUnsyncedData()`.
- Delete unsynced files:
  - `FaultInjectionFS.DeleteUnsyncedFiles()`.
- N05 rename durability model:
  - `FaultInjectionFS.Rename()` tracks pending renames.
  - `FaultInjectionFS.RevertUnsyncedRenames()` simulates crash behavior for directory entries.
- Directory fsync lie mode:
  - `FaultInjectionFS.SetSyncDirLieMode(true)`.
  - Scenario: `cmd/crashtest/scenario_durability_test.go` `TestDurability_SyncDirLieMode_DBRecoversConsistently`.
- File fsync lie mode:
  - `FaultInjectionFS.SetFileSyncLieMode(true, <pattern>)`.
  - Supported patterns:
    - `.log` for WAL files.
    - `.sst` for SST files.
    - `MANIFEST` for MANIFEST files (pattern support exists).
  - Scenarios:
    - WAL: `TestDurability_FileSyncLieMode_WAL_LosesUnsyncedWrites`.
    - SST: `TestDurability_FileSyncLieMode_SST_FailsOnRead`.

### On-disk corruption attacks

Supported instances:

- `cmd/adversarialtest` corruption category:
  - `TruncateWAL`.
  - `CorruptWALCRC`.
  - `CorruptSSTBlock`.
  - `ZeroFillWAL`.
  - `DeleteCurrent`.
- `cmd/crashtest` deterministic pointer corruption:
  - `TestDurability_TornCURRENT_FailsLoud`.

### Oracle and differential testing

Supported instances:

- Golden tests (oracle-driven format compatibility):
  - `cmd/goldentest` (`go test -v ./cmd/goldentest/...`).
  - SST format and compression matrices.
  - WAL and MANIFEST contracts.
  - C++ fixture reads and Go-written artifact reads by C++ tools.
- External corpus suite:
  - `make test-e2e-golden-corpus` (requires `REDTEAM_CPP_CORPUS_ROOT`).
- Oracle outputs for crash artifacts:
  - `cmd/crashtest/oracle_helpers.go` runs `ldb` and `sst_dump` when enabled.

### Trace capture and replay

Supported instances:

- Trace capture:
  - `cmd/stresstest -trace-out <TRACE_FILE>`.
- Trace analysis and replay:
  - `cmd/traceanalyzer stats <TRACE_FILE>`.
  - `cmd/traceanalyzer dump <TRACE_FILE>`.
  - `cmd/traceanalyzer -db <DB_PATH> replay <TRACE_FILE>`.

## Capability matrix

The tables in this section describe what is supported and what is not supported.
Use the “Missing instances” column to find gaps that fit the same class.

### Crash testing

| Class | Tooling | Evidence | Oracle | Support | Missing instances |
| --- | --- | --- | --- | --- | --- |
| Blackbox crash loops | `cmd/crashtest` + `cmd/stresstest` | Persistent expected state + run directory on failure | Optional via `ROCKYARDKV_CPP_ORACLE_PATH` for scenario artifacts | Supported | Add faultfs-driven “lying FS” crash loops that preserve the “success” return codes. |
| Whitebox killpoint crashes | `cmd/crashtest` (build tag `crashtest`) | Artifact bundles via `WHITEBOX_ARTIFACT_DIR` | Supported on persisted artifacts | Supported | Persist oracle-anchored artifacts for the sweep runner. |
| Whitebox syncpoint crashes | `cmd/crashtest` (build tag `synctest`) | Artifact bundles via `WHITEBOX_ARTIFACT_DIR` | Partial | Add syncpoint scenarios for MANIFEST rotation and CURRENT update boundaries. |

### Filesystem fault injection

| Fault class | Mechanism | Where it lives | Evidence | Support | Missing instances |
| --- | --- | --- | --- | --- | --- |
| Unsynced data loss | `FaultInjectionFS.DropUnsyncedData()` | `internal/vfs` + `cmd/stresstest` + `cmd/crashtest` | Scenario artifacts + repro | Supported | Add file-type specific drop policies for metadata-only tests. |
| Delete unsynced files | `FaultInjectionFS.DeleteUnsyncedFiles()` | `internal/vfs` + `cmd/stresstest` | Stress/crash harness artifacts | Supported | Add deterministic scenarios that prove orphan handling matches oracle. |
| Rename not durable without `SyncDir` (N05) | `FaultInjectionFS.pendingRenames` + `RevertUnsyncedRenames()` | `internal/vfs` + `cmd/crashtest` | Scenario artifacts + oracle outputs | Supported | Add rename anomaly variants (double-name, neither-name, revert-after-success). |
| Directory fsync lies (N05) | `FaultInjectionFS.SetSyncDirLieMode(true)` | `internal/vfs` + `cmd/crashtest` | Scenario artifacts + oracle outputs | Supported | Expand to cover “success lies” across other metadata operations. |
| File fsync lies | `FaultInjectionFS.SetFileSyncLieMode(true, <pattern>)` | `internal/vfs` + `cmd/crashtest` | Scenario artifacts | Partial | Add a MANIFEST sync lie scenario. |
| Goroutine-local fault injection | `GoroutineFaultManager` | `internal/vfs/fault_injection_goroutine.go` | None | Not supported (no cmd scenario) | Add a cmd-level harness that enables per-goroutine metadata and sync faults. |

### On-disk corruption attacks

| Attack | Tooling | Evidence | Support | Missing instances |
| --- | --- | --- | --- | --- |
| WAL truncation and WAL checksum corruption | `cmd/adversarialtest` | Artifact bundle via `-run-dir` | Supported | Add “stop-after-corruption” equivalence checks against oracle classification. |
| SST block corruption | `cmd/adversarialtest` + `cmd/goldentest` | Artifact bundle + oracle tools | Supported | Add coverage for metaindex and property block corruption classes. |
| CURRENT missing and CURRENT torn contents | `cmd/adversarialtest` + `cmd/crashtest` | Scenario artifacts + oracle outputs | Supported | Add partial-write simulation at the VFS layer (not only direct edits). |
| MANIFEST corruption | `cmd/goldentest` (oracle-driven) | Golden fixtures | Partial | Add adversarial edits for MANIFEST truncation and tag corruption. |

### Oracle and differential testing

| Class | Tooling | Direction | Support | Missing instances |
| --- | --- | --- | --- | --- |
| Golden format compatibility | `cmd/goldentest` | Go writes, C++ reads and C++ writes, Go reads | Supported | Add oracle `checkconsistency` runs where the classification adds signal. |
| External corpus | `make test-e2e-golden-corpus` | C++ fixtures, Go reads | Supported (opt-in) | Add a dedicated “C04 lies suite” target that fails fast when oracle is requested but not configured. |
| Trace replay | `cmd/stresstest -trace-out` + `cmd/traceanalyzer replay` | Go trace, Go replay | Supported | Add a standard acceptance signal that compares replay DB state to an expected-state snapshot. |

## Missing instances checklist

Use this table to track “category completeness” gaps.
Each row describes a class where you support some instances but not all instances.

| Category | Implemented instances | Missing instances |
| --- | --- | --- |
| Sync lies | Directory fsync lie, WAL file fsync lie, SST file fsync lie | MANIFEST file fsync lie, CURRENT temp file fsync lie. |
| Rename lies | Rename not durable without dir sync, dir sync lie mode | Rename success but both names exist, rename success but neither name exists, rename replace anomalies. |
| Whitebox sweeps | Strict killpoint coverage | Oracle-anchored artifact persistence for sweep runs. |
| Goroutine-local faults | Goroutine-local fault manager exists | Cmd-level scenario that enables per-goroutine metadata and sync faults in concurrent workloads. |


