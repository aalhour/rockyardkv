# Testing Guide

RockyardKV uses a layered testing strategy to guarantee correctness, durability, and compatibility.
This guide describes the testing infrastructure and how to use it.

## Quick Start

```bash
make check              # Lint + short tests
make test               # Full test suite with race detection
make test-e2e-golden    # C++ compatibility verification (requires RocksDB ldb + sst_dump)
make test-e2e-crash     # Crash recovery tests
make test-e2e-stress    # Concurrency tests
```

## Documentation Index

| Document | Description |
|----------|-------------|
| [PHILOSOPHY.md](PHILOSOPHY.md) | Testing principles and when to use each approach |
| [JEPSEN_STYLE.md](JEPSEN_STYLE.md) | Jepsen-style test classes and capability matrix |
| [WHITEBOX.md](WHITEBOX.md) | Deterministic crash testing with kill points |
| [BLACKBOX.md](BLACKBOX.md) | Chaos testing: crash, stress, adversarial |
| [GOLDEN.md](GOLDEN.md) | C++ oracle compatibility testing |
| [TOOLS.md](TOOLS.md) | Database inspection and analysis tools |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to add new tests |

## Testing Layers

RockyardKV organizes tests into layers. Use them in order, from bottom to top.

| Layer | Purpose | Location |
|-------|---------|----------|
| **Unit Tests** | Codecs and invariants in isolation | `internal/*_test.go` |
| **Integration Tests** | Component interactions | `db/*_test.go` |
| **Adversarial Tests** | Hostile inputs and fault injection | `cmd/adversarialtest/` |
| **Stress Tests** | Concurrent operations + expected-state oracle | `cmd/stresstest/` |
| **Blackbox Crash Tests** | Random timing SIGKILL + expected-state oracle | `cmd/crashtest/` |
| **Whitebox Crash Tests** | Deterministic crashes at 21 code points | `cmd/crashtest/` |
| **Golden Tests** | Byte-level RocksDB v10.7.5 compatibility | `cmd/goldentest/` |

## Quality Guarantees

These tests collectively validate:

| Property | Verified by |
|----------|-------------|
| **Durability** | Whitebox crash tests, blackbox crash tests, fault injection |
| **Correctness** | Expected-state oracle, unit tests, integration tests |
| **Compatibility** | Golden tests with C++ RocksDB artifacts |
| **Concurrency** | Stress tests with race detector |
| **Robustness** | Adversarial tests, fuzz tests |

## Build Tags

Some tests require build tags:

| Tag | Purpose | Example |
|-----|---------|---------|
| `crashtest` | Enable kill points | `go test -tags crashtest ./cmd/crashtest/...` |
| `synctest` | Enable sync points | `go test -tags synctest ./internal/testutil/...` |

Without tags, whitebox hooks compile to no-ops with zero runtime overhead.

## Test Locations

| Test type | Location |
|-----------|----------|
| Unit tests | `internal/*_test.go` |
| Integration tests | `db/*_test.go` |
| Golden tests | `cmd/goldentest/` |
| Crash tests | `cmd/crashtest/` |
| Stress tests | `cmd/stresstest/` |
| Adversarial tests | `cmd/adversarialtest/` |
| Smoke tests | `cmd/smoketest/` |

## References

- [RocksDB Testing Philosophy](https://github.com/facebook/rocksdb/wiki/Stress-test)
- [Jepsen Testing Methodology](https://jepsen.io/analyses)

