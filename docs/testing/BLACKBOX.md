# Blackbox Testing

Blackbox tests find bugs through randomization and chaos.
Unlike whitebox tests (specific crash points), blackbox tests use random timing to discover unexpected failure modes.

## Overview

| Test type | Location | Purpose |
|-----------|----------|---------|
| Crash tests | `cmd/crashtest/` | Recovery after SIGKILL under load |
| Stress tests | `cmd/stresstest/` | Concurrency bugs with expected-state oracle |
| Adversarial tests | `cmd/adversarialtest/` | Hostile inputs and fault injection |
| Smoke tests | `cmd/smoketest/` | Fast end-to-end sanity checks |

## Stress Tests

Stress tests run concurrent workloads and verify results using an expected-state oracle.
They target concurrency bugs, deadlocks, and hidden correctness drift.

### Algorithm

1. Generate random operations across a keyspace
2. Apply operations concurrently against the database
3. Oracle validates reads and scans remain consistent with expected state

The oracle uses:

- Per-key locks to avoid races between DB operation and oracle state updates
- Pending state tracking for operations that need commit semantics
- Pre and post read snapshots to tolerate concurrent mutation

### Run Stress Tests

```bash
make test-e2e-stress
make test-e2e-stress-long
```

Or run directly:

```bash
./bin/stresstest -duration 5m -ops-per-worker 10000
```

### Expected-State Oracle

The stress tool persists expected state to disk. Crash tests depend on this.

| Flag | Purpose |
|------|---------|
| `-expected-state` | Path to expected state file |
| `-save-expected` | Enable persistence |
| `-save-expected-interval` | Persist periodically during run |

SIGKILL can prevent the oracle state from reaching disk.
Use periodic persistence when running crash tests.

### Durability Flags

| Flag | Purpose |
|------|---------|
| `-sync` | Enable synced writes |
| `-disable-wal` | Disable the WAL |

Use `-sync` when recovery should preserve acknowledged writes.
Use `-disable-wal` to isolate memtable flush and MANIFEST behavior.

### Cleanup Flags

| Flag | Purpose |
|------|---------|
| `-cleanup` | Remove old test directories before run |
| `-keep` | Keep DB directory after run for debugging |

## Crash Tests

Crash tests simulate process death under load.
They use SIGKILL to approximate process-level power loss.
They reuse the stress test and expected-state oracle.

### Algorithm

1. Run stress test for random interval
2. SIGKILL the stress process
3. Run verification pass on same DB directory
4. Repeat for N cycles

The crash harness keeps a persistent oracle file (`expected_state.bin`).
The verify pass loads it and checks database state.

This model is based on RocksDB's `db_crashtest.py`.

### Scenario Tests

Scenario tests verify specific durability contracts without randomness.
They use a subprocess model: spawn child, perform operation, exit, reopen, verify.

```bash
go test ./cmd/crashtest/... -run TestScenario
```

| Test | Contract |
|------|----------|
| `TestScenario_SyncedWriteSurvivesCrash` | Synced writes survive crash |
| `TestScenario_FlushedDataSurvivesCrash` | Flushed data survives crash |
| `TestScenario_SyncedDeleteSurvivesCrash` | Synced deletes survive crash |
| `TestScenario_WriteBatchAtomicity` | Batch writes are all-or-nothing |
| `TestScenario_DoubleCrashRecovery` | Recovery is stable after multiple crashes |

### Run Crash Tests

```bash
make test-e2e-crash
make test-e2e-crash-long
```

Or run directly:

```bash
./bin/crashtest -cycles=10 -sync
```

### Crash Flags

| Flag | Purpose |
|------|---------|
| `-seed` | Set RNG seed for reproducibility |
| `-cycles` | Run fixed number of cycles |
| `-db` | Write into specific DB directory |
| `-keep` | Keep DB directory after run |
| `-sync` | Run with synced writes |
| `-disable-wal` | Run without WAL |
| `-run-dir` | Collect artifacts on failure |

## Fault Injection

Fault injection tests verify durability under filesystem anomalies.
They use `FaultInjectionFS`, a virtual filesystem wrapper.

### Simulated Anomalies

| Anomaly | Description |
|---------|-------------|
| Fsync lies | Data appears synced but isn't. On crash, unsynced data is lost. |
| Directory sync anomalies | File renames not durable until parent directory synced. |

### Fault Injection Flags

| Flag | Effect |
|------|--------|
| `-faultfs` | Enable FaultInjectionFS |
| `-faultfs-drop-unsynced` | Simulate fsync lies |
| `-faultfs-delete-unsynced` | Simulate directory sync anomalies |

### Durability Scenario Tests

```bash
go test ./cmd/crashtest/... -run 'TestScenario_Fsync|TestScenario_DirSync|TestScenario_Multiple'
```

| Test | Contract |
|------|----------|
| `TestScenario_FsyncLies_SyncedWritesSurvive` | Synced writes survive fsync lies |
| `TestScenario_FsyncLies_FlushMakesDurable` | Flushed data survives fsync lies |
| `TestScenario_DirSync_CURRENTFileDurable` | CURRENT update durable after sync |
| `TestScenario_DirSync_RecoveryAfterUnsyncedDataLoss` | Recovery consistent after unsynced loss |
| `TestScenario_MultipleFlushCycles_DurabilityCheckpoints` | Each flush creates durable checkpoint |

### Run with Fault Injection

```bash
./bin/crashtest -faultfs -faultfs-drop-unsynced -cycles 5 -duration 2m
```

## Adversarial Tests

Adversarial tests try to violate invariants with hostile inputs.
They probe error paths and corruption handling.

```bash
make test-e2e-adversarial
```

Or run directly:

```bash
./bin/adversarialtest -run-dir ./artifacts
```

## Smoke Tests

Smoke tests provide fast end-to-end checks.
They are not a substitute for crash or stress testing.

```bash
make test-e2e-smoke
```

## Fuzz Tests

Fuzz tests target parsers and codecs.
They should assert semantic properties, not just "no crash."

```bash
make test-fuzz
```

## Evidence and Reproducibility

Test failures require evidence.
Use artifact collection and trace emission to capture context.

### Artifact Collection

Use `-run-dir` to collect artifacts on failure:

```bash
./bin/crashtest -run-dir ./artifacts -cycles 10
./bin/stresstest -run-dir ./artifacts -duration 5m
./bin/adversarialtest -run-dir ./artifacts
```

Artifacts include:

- `run.json` — Configuration, flags, seeds, git commit, timestamps
- `db/` — Copy of database directory
- `expected_state.bin` — Expected-state oracle file
- `stdout.log`, `stderr.log` — Captured output

### Trace Emission

Record operations during stress test:

```bash
./bin/stresstest -trace-out ./trace.log -duration 1m
```

The trace includes operation types, keys, values, and timestamps.

### Trace Replay

Replay against a fresh database:

```bash
./bin/traceanalyzer replay -db /tmp/replay_db ./trace.log
```

Use `-dry-run` to count operations without applying:

```bash
./bin/traceanalyzer replay -dry-run ./trace.log
```

Use `-preserve-timing` to replay at original pace:

```bash
./bin/traceanalyzer replay -preserve-timing -db /tmp/replay_db ./trace.log
```

## References

- RocksDB `db_crashtest.py`
- RocksDB `db_stress`
- [Jepsen Testing](https://jepsen.io/)

