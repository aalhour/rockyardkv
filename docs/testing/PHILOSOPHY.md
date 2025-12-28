# Testing Philosophy

## Test contracts, not bugs

RockyardKV tests _contracts_, not _bugs_.
A contract is what a component promises to do.
If a bug violates a contract, a contract test catches it.

This approach:

- **Survives refactoring** because tests describe behavior, not implementation.
- **Prevents regressions** because each contract has a test.
- **Scales** because new features add new contracts, not "regression tests."

## Naming conventions

Don't name tests after bugs. Name tests after the behavior they verify.

| Do | Don't |
|---|---|
| `TestFooter_DecodeAll_SupportedVersions` | `TestRegression_Issue3` |
| `TestWAL_Recovery_SyncedWritesSurvive` | `TestFixForCrashBug` |
| `TestSST_ReadZlibCompressed_FormatV6` | `TestZlibFix` |

## The testing pyramid

Use layers in order. Start with deterministic unit tests.
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

## Jepsen-style invariants

Treat these as contracts. Keep them strict.

### Acknowledge and durability must match

If a write returns success under a durability mode, recovery must preserve it.

- Synced writes must survive crash.
- Unsynced writes may be lost, but acknowledged writes must not be lost if sync was requested.

### Recovery must not invent state

Recovered state must be a prefix of acknowledged operations under the selected durability contract.

- Recovery cannot produce keys that were never written.
- Recovery cannot produce values that were never committed.

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
2. Fails when the contract is violated
3. Passes when the contract is satisfied
4. Survives implementation changes that preserve the contract

## References

- [Contracts, Not Tests](https://blog.ploeh.dk/2013/09/11/death-by-unit-test/)
- [Test Pyramid](https://martinfowler.com/articles/practical-test-pyramid.html)
- [Jepsen Methodology](https://jepsen.io/analyses)

