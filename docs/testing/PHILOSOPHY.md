# Testing philosophy

## Table of contents

- [Test contracts, not bugs](#test-contracts-not-bugs)
- [Naming conventions](#naming-conventions)
- [Contract test categories](#contract-test-categories)
- [The testing pyramid](#the-testing-pyramid)
- [Contract documentation pattern](#contract-documentation-pattern)
- [Invariants as contracts](#invariants-as-contracts)
- [Campaign mindset (Jepsen-style, adapted)](#campaign-mindset-jepsen-style-adapted)
- [Table-driven tests](#table-driven-tests)
- [Prevent regressions](#prevent-regressions)
- [Prefer tests that fail on real bugs](#prefer-tests-that-fail-on-real-bugs)
- [References](#references)

## Test contracts, not bugs

RockyardKV tests _contracts_, not _bugs_.
A contract is what a component promises to do.
If a bug violates a contract, a contract test catches it.

This approach:

- **Survives refactoring** because tests describe behavior, not implementation.
- **Prevents regressions** because each contract has a test.
- **Scales** because new features add new contracts, not "regression tests."

## Naming conventions

Don't name tests after bugs.
Name tests after the behavior they verify.

| Do | Don't |
|---|---|
| `TestFooter_DecodeAll_SupportedVersions` | `TestRegression_Issue3` |
| `TestWAL_Recovery_SyncedWritesSurvive` | `TestFixForCrashBug` |
| `TestSST_ReadZlibCompressed_FormatV6` | `TestZlibFix` |

## Contract test categories

Contract tests verify guarantees at different levels.
The table below shows the five categories used in this codebase.

| Category | What it tests | Execution model | I/O |
|---|---|---|---|
| Semantic unit | API method produces correct internal state | In-process | None |
| Handler compliance | Interface implementations preserve semantics | In-process | None |
| Format compatibility | Binary format matches C++ RocksDB | Go writes, C++ reads | File + subprocess |
| Durability/crash | Guarantees survive process death | Fork, crash, verify | File + fork |
| Behavioral integration | Multi-component behavior | In-process | Real file I/O |

### Semantic unit tests

These tests verify that an API method produces the correct internal state.
They run in-process with no I/O.

```go
// Contract: WriteBatch.SingleDelete() encodes a TypeSingleDeletion record.
// Note: this example uses internal/batch parsing to validate the encoded record type.
func TestWriteBatch_RecordTypePreservation(t *testing.T) {
    wb := NewWriteBatch()
    wb.SingleDelete(key)

    parsed, err := batch.NewFromData(wb.Data())
    if err != nil {
        t.Fatalf("parse write batch: %v", err)
    }
    if !parsed.HasSingleDelete() {
        t.Error("SingleDelete must produce TypeSingleDeletion")
    }
}
```

Use semantic unit tests when you need to verify:

- API methods produce correct record types
- State transitions follow expected rules
- Invariants hold after operations

### Handler compliance tests

These tests verify that all implementations of an interface follow its contract.
They run in-process with no I/O.

```go
// Contract: All batch.Handler implementations must preserve record types
func TestBatchCopier_PreservesAllRecordTypes(t *testing.T) {
    src := batch.New()
    src.SingleDelete(key)

    dst := batch.New()
    copier := &batchCopier{target: dst}
    src.Iterate(copier)

    if !dst.HasSingleDelete() {
        t.Error("batchCopier must preserve SingleDelete records")
    }
}
```

Use handler compliance tests when you need to verify:

- All implementations of an interface follow the same contract
- Record types are preserved during iteration or copying
- Interface contracts aren't violated by new implementations

### Format compatibility tests

These tests verify that Go-written files are readable by C++ RocksDB tools.
They write a file in Go, then invoke a C++ tool to read it.

```go
// Contract: Go SST files are readable by C++ sst_dump
func TestSST_Contract_ZlibCompression(t *testing.T) {
    // Go writes SST
    builder.Add(key, value)
    builder.Finish()

    // C++ reads it
    output := exec.Command("sst_dump", "--file="+sstPath).Output()
    if strings.Contains(output, "Corruption") {
        t.Error("C++ rejected Go-written SST")
    }
}
```

Use format compatibility tests when you need to verify:

- Binary formats match C++ RocksDB exactly
- Compression algorithms produce C++-compatible output
- Checksums are computed correctly

### Durability and crash tests

These tests verify that guarantees survive process death.
They fork a child process, crash it, and verify data in the parent.

```go
// Contract: Synced writes survive crash
func TestDurability_WALEnabled_WritesAreDurable(t *testing.T) {
    if os.Getenv("CHILD") == "1" {
        database.Put(key, value)
        os.Exit(0)  // Simulate crash
    }

    // Parent spawns child, then reopens and verifies
    exec.Command(os.Args[0], "-test.run=ThisTest").Run()
    db = Open(path)
    if _, err := database.Get(key); err != nil {
        t.Error("Synced write must survive crash")
    }
}
```

Use durability tests when you need to verify:

- Synced writes survive ungraceful termination
- Recovery produces a consistent prefix of operations
- WAL and MANIFEST atomicity guarantees hold

### Behavioral integration tests

These tests verify multi-component behavior with real I/O.
They run in-process but use the filesystem.

```go
// Contract: Orphan SST files are deleted during recovery
func TestOrphanCleanup_MultipleOrphans(t *testing.T) {
    db := Open(path)
    database.Put(key, value)
    database.Flush()
    database.Close()

    // Create orphan files
    os.WriteFile(path+"/orphan.sst", data, 0644)

    // Reopen - orphans are deleted
    db = Open(path)
    if _, err := os.Stat(path + "/orphan.sst"); !os.IsNotExist(err) {
        t.Error("Orphan SST must be deleted during recovery")
    }
}
```

Use behavioral integration tests when you need to verify:

- Recovery cleans up orphaned files
- Sequence numbers increase monotonically across restarts
- Multiple components interact correctly

## The testing pyramid

Use layers in order.
Start with deterministic unit tests.
Move up to crash and differential tests before trusting scale.

| Layer | Goal | Location |
|---|---|---|
| Unit tests | Validate codecs and invariants in isolation | `internal/*_test.go` |
| Integration tests | Validate component interactions | `db/*_test.go` |
| Golden tests | Prove byte-level compatibility with RocksDB v10.7.5 | `cmd/goldentest/` |
| Whitebox crash tests | Prove recovery at specific crash boundaries | `cmd/crashtest/` |
| Blackbox crash tests | Prove recovery under random SIGKILL | `cmd/crashtest/` |
| Stress tests | Find concurrency bugs with expected-state oracle | `cmd/stresstest/` |
| Adversarial tests | Break invariants with hostile inputs | `cmd/adversarialtest/` |
| Smoke tests | Catch broken feature paths end to end | `cmd/smoketest/` |

## Contract documentation pattern

All contract tests include a `// Contract:` comment that states the guarantee.
This makes tests self-documenting and helps reviewers understand intent.

```go
// TestSST_Contract_BinaryKeys tests that binary keys are preserved.
//
// Contract: Keys containing arbitrary bytes (including \x00) are handled correctly.
func TestSST_Contract_BinaryKeys(t *testing.T) {
    // ...
}
```

When you write a contract test:

1. Add a `// Contract:` comment stating the guarantee
1. Make the test fail when the contract is violated
1. Make the test pass when the contract is satisfied
1. Ensure the test survives implementation changes

## Invariants as contracts

Treat these as contracts.
Keep them strict.

## Campaign mindset (Jepsen-style, adapted)

This repo treats failures as **invariant breaches** backed by evidence.
You avoid “rerun until it feels OK” workflows.
You ratchet coverage by adding or refining contracts and the campaigns that exercise them.

Key practices:

- **Name the scenario**: a failure is only actionable when you can describe the workload, the fault model, and the checker.
- **Make it reproducible**: record seeds and knobs so a run can be reproduced without guesswork.
- **Treat the oracle as truth**: when on-disk format or recovery classification matters, validate against the RocksDB tools.
- **Prefer evidence over reruns**: failures should persist enough artifacts to debug without repeating the campaign.
- **Deduplicate**: repeated failures should not bury novel ones.

### External inspiration (workflows worth copying)

These are patterns we reuse because they scale testing beyond ad-hoc runs:

- **Registry-in-code runners** (e.g., CockroachDB `roachtest`): a single runner owns the list of named tests and supports selecting subsets.
- **Seeded stress engines** (e.g., RocksDB `db_stress`): one powerful engine explored by fixed seed sets and parameter matrices.
- **Repeatability-first fault injection** (e.g., FoundationDB simulation, Antithesis): faults are most valuable when they lead to stable replay and faster minimization.
- **Report re-evaluation** (e.g., etcd robustness): validators improve over time; old artifacts should remain useful.

These inspirations inform the testing workflow, but the operational details live in the testing tool docs (e.g., blackbox/whitebox and campaign runner docs).

### How you use it in development and review

Use the runner to make coverage reviewable.
Add or update an instance when you add a new fault model, checker, or durability promise.
Review changes by asking which instance covers the new behavior and what evidence it records.

### How you use it in nightly and pre release

Use a fixed nightly matrix to explore failure space systematically.
Use a fixed pre release matrix to gate durability and compatibility claims.
Treat oracle required instances as setup requirements, not optional checks.

### Acknowledge and durability must match

If a write returns success under a durability mode, recovery must preserve it.

- Synced writes must survive crash.
- Unsynced writes may be lost, but acknowledged writes must not be lost if sync was requested.

### Recovery must not invent state

Recovered state must be a prefix of acknowledged operations under the selected durability contract.

- Recovery can't produce keys that were never written.
- Recovery can't produce values that were never committed.

### Snapshots must be consistent

Snapshot reads must not observe non-monotonic histories.

- A snapshot must see all writes before its sequence number.
- A snapshot must not see writes after its sequence number.

### Iterators must be ordered

Iterator order must match the comparator.

- Forward iteration visits keys in ascending order.
- Reverse iteration visits keys in descending order.

## Table-driven tests

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

## Prevent regressions

Use this table to determine where to add coverage when you fix a bug.

| Bug type | Add coverage in |
|---|---|
| Format (zlib, footer, metaindex) | `format_matrix_test.go` + C++ fixture |
| Recovery (WAL replay, crash) | `db/db_recovery_test.go` |
| Concurrency (race, deadlock) | `db/db_concurrent_test.go` + stress test |
| Parsing (malformed input) | Fuzz test + add to corpus |
| API behavior | `db/db_basic_test.go` or `db/*_test.go` |

## Prefer tests that fail on real bugs

Avoid tests that only assert "no error."
Write tests that explain which invariant they protect.

A good test:

1. States what contract it verifies
1. Fails when the contract is violated
1. Passes when the contract is satisfied
1. Survives implementation changes that preserve the contract

## References

- [Contracts, Not Tests](https://blog.ploeh.dk/2013/09/11/death-by-unit-test/)
- [Test Pyramid](https://martinfowler.com/articles/practical-test-pyramid.html)
- [Jepsen Methodology](https://jepsen.io/analyses)
