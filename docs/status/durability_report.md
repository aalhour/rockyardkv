# Durability report

This document describes crash durability behaviors under investigation.
It provides reproduction commands and expected evidence for each behavior.

## Summary

RockyardKV is in **v0.1.x**.
Durability guarantees are under active verification.

Two crash durability behaviors reproduce as of 2025-12-28:

| Behavior | Configuration | Status |
|----------|---------------|--------|
| Value regression after crash | WAL enabled, sync writes | Under investigation |
| Durable state divergence | WAL disabled, fault injection | Under investigation (test harness issue) |

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
You reproduce each behavior with a fixed command line and a fixed seed.  
You treat the result as verified only when you have the exit code, the log output, and the artifacts.

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

Use the Makefile targets:

```bash
make status-golden
make status-durability
make status-check
```

### What you get from a failing run

Each failing run writes:

- A log file you can grep for `Verify:` lines.
- A run directory that contains the database files for the failing run.
- An artifact bundle under the `-run-dir` path.

## Behavior: WAL enabled + sync writes read back older values after crash recovery

### What it means

With WAL enabled and sync writes, the database acknowledges a write only after it forces the write to stable storage.  
After a crash and restart, recovery should replay durable state and return the latest acknowledged values.  
This behavior returns an older value for some acknowledged updates.  
This behavior is under investigation.

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

Expected evidence:

- The command exits non-zero.
- The log includes `Verify:` lines that report `value base mismatch` or `expected to exist`.
- The `artifacts` directory contains a bundle suitable for reproduction.

## Behavior: WAL disabled + flush barriers diverge under fault injection after crash recovery

### What it means

When you disable the WAL, you accept that unflushed writes are not durable.  
Flush becomes the durability boundary for data that is present in on-disk tables.  
This behavior reports missing or older values even relative to the durable state captured at flush boundaries.  
This behavior is under investigation.

### How it happens

The workload runs with `DisableWAL=true`.  
The test forces flushes and records a durable-state snapshot at those boundaries.  
The test also enables fault injection that simulates storage dropping unsynced data.  
After restart, the database reads back a state that is older than the durable-state snapshot.

Minimal repro:

```bash
cd "<REPO_ROOT>"
RUN_DIR="<RUN_DIR>"
rm -rf "$RUN_DIR" && mkdir -p "$RUN_DIR"

./bin/crashtest -seed=8201 -cycles=25 -duration=8m -interval=6s -min-interval=0.5s -kill-mode=sigterm \
  -disable-wal -faultfs -faultfs-drop-unsynced -faultfs-delete-unsynced \
  -db "$RUN_DIR/db_faultfs_disable_wal" -run-dir "$RUN_DIR/artifacts" -keep -v \
  2>&1 | tee "$RUN_DIR/crashtest.log"
```

Expected evidence:

- The command exits non-zero.
- The log reports a large number of verification failures late in the run.
- The `artifacts` directory contains a bundle suitable for reproduction.

## Verify file format compatibility

Run the golden test suite:

```bash
make test-e2e-golden
```
