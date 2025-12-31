# Durability report

This document describes crash durability behaviors under investigation.
It provides reproduction commands and expected evidence for each behavior.

## Summary

RockyardKV is in `v0.2.x`.
Durability guarantees are under active verification.

| Behavior | Configuration | Status |
|----------|---------------|--------|
| Internal key collision (C02-01) | WAL disabled, fault injection | Resolved (2025-12-29) |
| Durable state divergence (HARNESS-02) | WAL disabled, fault injection | Resolved (2025-12-29) |
| DisableWAL verification contract (C02-03) | WAL disabled, fault injection | Resolved (2025-12-29) |
| Value regression after crash (C01-01) | WAL enabled, sync writes | Resolved (2025-12-29) |

### Recent fixes (2025-12-29)

Internal key collision across SSTs:
- Root cause: Orphaned SST files after crash and missing orphan cleanup during recovery.
- Fix: Delete orphaned SST files during recovery and keep `LastSequence` monotonic.
- Commits: `947e3a0`, `ac461fe`.
- Evidence: `docs/redteam/REPORTS/2025-12-29_C02_run11.md`.

Verifier mismatch under DisableWAL+faultfs:
- Root cause: The harness treated `Flush()` as a durability boundary, but a crash before `MANIFEST` sync loses flushed data.
- Fix: Add `-allow-data-loss` for DisableWAL+faultfs mode.
- Evidence: Refer to the HARNESS-02 entry in `docs/status/durability_report.md` and linked redteam reports.

DisableWAL verification contract (seqno-prefix model):
- Root cause: The original DisableWAL verifier expected a strict "no missing writes" contract.
- Fix: Verify a seqno-prefix (no holes) contract aligned with the recovery sequence number.
- Evidence: `docs/redteam/REPORTS/2025-12-29_C02_run16.md`.

WAL enabled with sync writes:
- Evidence: `docs/redteam/REPORTS/2025-12-29_C01_run10.md`.

## What crash durability means

A crash is an abrupt process stop.
The process does not run cleanup code.
The operating system can drop buffered file data that is not forced to disk.

Crash durability means:
- The database returns a valid state after restart and recovery.
- Acknowledged writes survive when the configuration promises they survive.
- The database detects and reports corruption instead of returning silent wrong results.

## Why this matters

If the database acknowledges a write, your application assumes it is durable by contract.
If the database returns an older value or a missing key after a crash, the database breaks that contract.

## How the reproductions work

The reproductions use the `crashtest` tool.
It runs a workload, terminates the process at random times, restarts, and verifies expected results.
Verification compares the database contents after recovery to the expected state recorded during the run.

## How this report works

This report is evidence-first.
Reproduce each behavior with a fixed command line and a fixed seed.
Treat a behavior as resolved only when you capture the exit code, the log output, and the artifacts.

Each command writes output to a run directory.
Set `<RUN_DIR>` to any location you want.

### Prerequisites

Build the test binaries:

```bash
make build
```

### Run using the scripts and Makefile targets

Use the tracked scripts:

```bash
bash scripts/status/run_durability_repros.sh wal-sync "<RUN_DIR>"
bash scripts/status/run_durability_repros.sh disablewal-faultfs "<RUN_DIR>"
```

Use the campaign runner groups (recommended):

```bash
./bin/campaignrunner -tier=quick  -group=status.durability.wal_sync        -run-root "<RUN_DIR>"
./bin/campaignrunner -tier=quick  -group=status.durability.disablewal_faultfs -run-root "<RUN_DIR>"
./bin/campaignrunner -tier=nightly -group=status.durability.wal_sync_sweep -run-root "<RUN_DIR>"
```

### What you get from a failing run

Each failing run writes:

- A log file you can grep for `Verify:` lines.
- A run directory that contains the database files for the failing run.
- An artifact bundle under the `-run-dir` path.

## Behavior: WAL enabled + sync writes read back older values after crash recovery

### What it means

With WAL enabled and sync writes, the database acknowledges a write only after it forces the write to stable storage.
After a crash and restart, recovery replays durable state and returns the latest acknowledged values.
This scenario detects older values or missing keys after recovery.
This scenario is resolved in the latest verification run.

### How it happens

The workload issues writes with `WriteOptions.Sync=true`.  
The crash interrupts the process at arbitrary times.  
After restart, recovery returns a state that is missing some acknowledged updates.

Minimal repro:

```bash
cd "<REPO_ROOT>"
RUN_DIR="<RUN_DIR>"
rm -rf "$RUN_DIR" && mkdir -p "$RUN_DIR"

./bin/crashtest -seed=9101 -cycles=5 -duration=6m -interval=10s -min-interval=2s \
  -kill-mode=sigkill -sync -db "$RUN_DIR/db_sync" -run-dir "$RUN_DIR/artifacts" -keep -v \
  2>&1 | tee "$RUN_DIR/crashtest.log"
```

Expected evidence (resolved):

- The command exits zero.
- The log does not include failing `Verify:` lines.
- The `artifacts` directory contains a bundle suitable for future reproduction.

## Behavior: WAL disabled + flush barriers diverge under fault injection after crash recovery

### Status: Resolved (2025-12-29)

This section tracks DisableWAL+faultfs crash behavior.
It includes verifier behavior and internal-key collision checks.

Fixed issues:
1. Internal key collision due to orphaned SST files.
1. A verifier contract mismatch when the run crashes before `MANIFEST` sync.

Use `-seqno-prefix-verify` when you need oracle-aligned DisableWAL verification.

### What it means

When you disable the WAL, you accept that unflushed writes are not durable.
Under fault injection, the process can crash after a flush and before `MANIFEST` sync.
In that case, the recovered database can lose flushed data.
Treat that data loss as expected in DisableWAL+faultfs mode.

### Verification command

```bash
./bin/campaignrunner -tier=quick -group=status.composite.internal_key_collision -run-root "<RUN_DIR>"
```

Expected evidence:
- The command exits zero.
- `collision-check` reports `OK: no internal-key collisions detected`.
- The log can include allowed data loss messages for DisableWAL+faultfs mode.

## Verify file format compatibility

Run the golden test suite:

```bash
make test-e2e-golden
```
