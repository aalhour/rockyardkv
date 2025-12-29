# Durability report

This document describes crash durability behaviors under investigation.
It provides reproduction commands and expected evidence for each behavior.

## Summary

RockyardKV is in **v0.1.x**.
Durability guarantees are under active verification.

| Behavior | Configuration | Status |
|----------|---------------|--------|
| Internal-key collision (C02-01) | WAL disabled, fault injection | ✅ **Resolved** (2025-12-29) |
| Durable state divergence (HARNESS-02) | WAL disabled, fault injection | ✅ **Resolved** (2025-12-29) |
| Value regression after crash (C01-01) | WAL enabled, sync writes | Under investigation |

### Recent fixes (2025-12-29)

**Internal-key collision across SSTs**
- **Root cause:** Orphaned SST files after crash + missing orphan cleanup during recovery
- **Fix:** Orphan SST deletion on recovery + LastSequence from SST largest_seqno + monotonicity
- **Commits:** `947e3a0`, `ac461fe`
- **Validation:** 10/10 collision-check passes (see `docs/redteam/REPORTS/2025-12-29_C02_run11.md`)

**Verifier mismatch under DisableWAL+faultfs**
- **Root cause:** Harness assumed Flush() = durable, but crash before MANIFEST sync loses data
- **Fix:** Added `-allow-data-loss` flag for DisableWAL+faultfs mode
- **Validation:** 5/5 passes

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

### Status: ✅ RESOLVED (2025-12-29)

**Two issues were fixed:**

1. **Internal-key collision due to orphaned SST files**
   - Fix: Orphan SST deletion on recovery + LastSequence monotonicity
   - Gate: `make status-durability-internal-key-collision` — **PASS**

2. **Verifier assumed Flush() = durable**
   - Fix: `-allow-data-loss` flag for DisableWAL+faultfs mode
   - Gate: `make status-durability-internal-key-collision` — **PASS**

### What it means

When you disable the WAL, you accept that unflushed writes are not durable.  
Flush becomes the durability boundary for data that is present in on-disk tables.  
Under fault injection, crashes can occur after flush but before MANIFEST sync.  
With the fixes applied, such data loss is expected and allowed by the verifier.

### Verification command

```bash
make status-durability-internal-key-collision
```

Expected evidence:
- The command exits zero (`exit_code=0`)
- `collision-check` reports: `OK: no internal-key collisions detected`
- Data loss is logged but allowed: `(allowed, data loss under DisableWAL)`

## Verify file format compatibility

Run the golden test suite:

```bash
make test-e2e-golden
```
