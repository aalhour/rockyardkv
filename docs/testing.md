# Testing

This document describes how RockyardKV tests correctness and compatibility.
It covers the testing philosophy, test structure, and test harnesses.

## Philosophy: test contracts, not bugs

RockyardKV tests _contracts_, not _bugs_.
A contract is what a component promises to do.
If a bug violates a contract, a contract test catches it.

This approach:

- Survives refactoring because tests describe behavior, not implementation.
- Prevents regressions because each contract has a test.
- Scales because new features add new contracts, not new "regression tests."

Don't name tests after bugs.
Name tests after the behavior they verify.

| Do | Don't |
| --- | --- |
| `TestFooter_DecodeAll_SupportedVersions` | `TestRegression_Issue3` |
| `TestWAL_Recovery_SyncedWritesSurvive` | `TestFixForCrashBug` |
| `TestSST_ReadZlibCompressed_FormatV6` | `TestZlibFix` |

## Test structure

RockyardKV organizes tests into three layers.

### Layer 1: contract tests

Contract tests verify each component's promises.
They live in `internal/*/_test.go` and `db/*_test.go`.

Use table-driven tests to cover version and compression matrices:

```go
func TestSST_FormatVersionMatrix(t *testing.T) {
    versions := []uint32{0, 3, 4, 5, 6}
    compressions := []compression.Type{
        compression.NoCompression,
        compression.Snappy,
        compression.Zlib,
    }
    for _, v := range versions {
        for _, c := range compressions {
            t.Run(fmt.Sprintf("v%d/%s", v, c), func(t *testing.T) {
                // Test SST round-trip with this version and compression.
            })
        }
    }
}
```

### Layer 2: oracle tests

Oracle tests verify C++ and Go compatibility.
They live in `cmd/goldentest/`.

Fixtures are the tests.
Each file in `testdata/cpp_generated/` is automatically tested.
When you fix a format bug, generate a C++ fixture that exercises the bug and add it to the fixtures directory.

### Layer 3: chaos tests

Chaos tests find bugs through randomization.
They live in `cmd/crashtest/`, `cmd/stresstest/`, and `cmd/adversarialtest/`.

These tests don't target specific bugs.
They test invariants:

- Crash test: "Synced writes survive SIGKILL."
- Stress test: "Concurrent operations don't corrupt data."
- Adversarial test: "Malformed inputs don't cause panics."

## Prevent regressions

Use this table to determine where to add coverage when you fix a bug.

| Bug type | Add coverage in |
| --- | --- |
| Format (zlib, footer, metaindex) | `format_matrix_test.go` + C++ fixture |
| Recovery (WAL replay, crash) | `db/db_recovery_test.go` |
| Concurrency (race, deadlock) | `db/db_concurrent_test.go` + stress test |
| Parsing (malformed input) | Fuzz test + add to corpus |
| API behavior | `db/db_basic_test.go` or `db/*_test.go` |

## Use the testing pyramid

Use these layers in order.
Start with deterministic unit tests.
Move up to crash and differential tests before trusting scale.

| Layer | Goal | Where |
| --- | --- | --- |
| Golden tests | Prove byte-level compatibility with RocksDB v10.7.5 artifacts. | `cmd/goldentest/` |
| Crash tests | Prove recovery after SIGKILL under load. | `cmd/crashtest/` + `cmd/stresstest/` |
| Stress tests | Find concurrency bugs with an expected-state oracle. | `cmd/stresstest/` |
| Smoke tests | Catch broken feature paths end to end. | `cmd/smoketest/` |
| Adversarial tests | Break invariants with hostile inputs and fault patterns. | `cmd/adversarialtest/` |
| Integration tests | Validate component interactions. | `db/*_test.go` |
| Unit tests | Validate codecs and invariants in isolation. | `internal/*_test.go` |

## Use Jepsen-style invariants

Treat these as contracts.
Keep them strict.

- Acknowledge and durability must match.
  If a write returns success under a durability mode, recovery must preserve it.
- Recovery must not invent state.
  Recovered state must be a prefix of acknowledged operations under the selected durability contract.
- Snapshots must be consistent.
  Snapshot reads must not observe non-monotonic histories.
- Iterators must be ordered.
  Iterator order must match the comparator.

## Run tests

Use `make` targets for standard runs.
These targets build and run the correct binaries.

```bash
make test-short
make test
make test-fuzz
make test-e2e-smoke
make test-e2e-stress
make test-e2e-adversarial
make test-e2e-crash
make test-e2e-golden
```

## Golden tests

Golden tests validate on-disk compatibility with RocksDB v10.7.5.
They are standard Go tests that:

- Read C++-generated fixtures from `testdata/cpp_generated/`.
- Write Go artifacts and verify with C++ tools (`ldb`, `sst_dump`).
- Test format version × compression matrices.

### Test files

| File | Tests |
| --- | --- |
| `constants_test.go` | Magic numbers, property names, footer sizes |
| `db_test.go` | Database round-trip, C++ corpus reading |
| `manifest_test.go` | Read/write, unknown tags, corruption |
| `sst_test.go` | C++ fixtures, sst_dump verification |
| `sst_format_test.go` | Format version × compression matrix |
| `sst_contract_test.go` | Behavioral edge cases (binary keys, deletions) |
| `wal_test.go` | WAL round-trip and C++ compatibility |

### Run golden tests

Run using make:

```bash
make test-e2e-golden
```

Or run directly:

```bash
go test -v ./cmd/goldentest/...
```

### Prerequisites

Build the RocksDB tools before running tests that invoke C++ verification:

```bash
cd /path/to/rocksdb
make ldb sst_dump
```

Set `DYLD_LIBRARY_PATH` (macOS) or `LD_LIBRARY_PATH` (Linux) if needed.

## Smoke tests

Smoke tests provide fast end-to-end checks.
They are not a substitute for crash or differential testing.

Run the smoke tests:

```bash
make test-e2e-smoke
```

## Stress tests

Stress tests run concurrent workloads and verify results using an expected-state oracle.
They target concurrency bugs, deadlocks, and hidden correctness drift.

### Stress algorithm

The stress test uses a keyspace and an operation generator.
Workers apply random operations against the DB.
The oracle validates that reads and scans remain consistent with expected state.

The oracle design matters.
It uses per-key locks to avoid self-races between the DB operation and oracle state updates.
It uses pending state tracking for operations that need commit semantics.
It uses pre and post read snapshots to tolerate concurrent mutation.

### Use the expected-state oracle

The stress tool can persist expected state to disk.
Crash tests depend on this.

- Set `-expected-state` to a file path.
- Set `-save-expected` to enable persistence.
- Use `-save-expected-interval` to persist periodically during the run.

SIGKILL can prevent the oracle state from reaching disk.
Use periodic persistence when you run crash tests.

### Use durability flags

These flags define the durability contract that the stress and crash tests assert.

- `-sync`: enable synced writes.
- `-disable-wal`: disable the WAL.

Use `-sync` when you want recovery to preserve acknowledged writes.
Use `-disable-wal` when you want to isolate memtable flush and MANIFEST behavior.

### Use cleanup flags

Use `-cleanup` to remove old test directories before a run.
Use `-keep` if you need the DB directory for debugging.

### Run stress tests

```bash
make test-e2e-stress
make test-e2e-stress-long
```

## Crash tests

Crash tests simulate process death under load.
They use SIGKILL to approximate process-level power loss boundaries.
They reuse the stress test and expected-state oracle.

### Crash algorithm

The crash harness runs in cycles.
Each cycle runs the stress test for a random interval.
It kills the stress process.
It then runs a verification pass on the same DB directory.

The crash harness keeps a persistent oracle file.
The stress tool persists `expected_state.bin`.
The verify pass loads it and checks the database state.

This model is based on RocksDB’s `db_crashtest.py` workflow.

### Run crash tests

```bash
make test-e2e-crash
make test-e2e-crash-long
```

Or run directly with custom flags:

```bash
go run ./cmd/crashtest -cycles=10 -sync
```

### Important crash flags

Use these flags to make crashes reproducible and debuggable.

- `-seed`: set the RNG seed.
- `-cycles`: run a fixed number of cycles.
- `-db`: write into a specific DB directory.
- `-keep`: keep the DB directory after the run.
- `-sync`: run stress and verification with synced writes.
- `-disable-wal`: run stress and verification without WAL.

### Keep crash artifacts in a workspace directory

Crash tests can produce many small files.
Keep them under `rockyardkv/tmp/`.
Write crash artifacts into `rockyardkv/tmp/` if you want them to survive restarts.

## Adversarial tests

Adversarial tests try to violate invariants with hostile inputs.
They also probe error paths and corruption handling.

Run adversarial tests:

```bash
make test-e2e-adversarial
```

## Fuzz tests

Fuzz tests target parsers and codecs.
They should assert semantic properties.
They should assert more than “no crash”.

Run fuzz tests:

```bash
make test-fuzz
```

## Run Go unit tests and race detection

Run short unit tests:

```bash
make test-short
```

Run the full test suite with the race detector:

```bash
make test
```

## Contribute new tests

Prefer tests that fail on real bugs.
Avoid tests that only assert “no error”.
Write tests that explain which invariant they protect.
