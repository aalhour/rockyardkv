# Command-line tools & test harnesses

This directory contains:
1. **Utilities**: Standard executables for database inspection (`ldb`, `sstdump`).
2. **Harnesses**: Test suites and runners compiled as binaries or test packages (`goldentest`, `crashtest`).

Refer to [`docs/testing/README.md`](../docs/testing/README.md) for details about the testing model and algorithms.

## Naming convention

All packages follow Go idioms: lowercase, no underscores, compound words for clarity.
Test harnesses use the `*test` suffix for easy identification.

## Inspection tools (user-facing)

- `ldb`: database inspection and manipulation.
- `sstdump`: SST file inspection.
- `traceanalyzer`: trace file analysis.

Note: C++ oracle tools (`ldb`, `sst_dump`) are external RocksDB tools. See [`docs/testing/GOLDEN.md`](../docs/testing/GOLDEN.md).

## Test harnesses (Internal/CI)

These are used by `make test-*` targets and the campaign runner.

- `smoketest`: fast end-to-end feature checks.
- `stresstest`: concurrency stress with an expected-state oracle.
- `crashtest`: SIGKILL crash cycles built on the stress tool.
- `adversarialtest`: hostile inputs and fault patterns.
- `goldentest`: RocksDB v10.7.5 compatibility checks (test suite package).
- `campaignrunner`: oracle-gated runner that executes a fixed matrix of named instances.

## Build

```bash
make build
```

## Run

- Use `make test-e2e-*` targets.
- Refer to [`docs/testing/README.md`](docs/testing/README.md) for flags and workflows.
- For campaign orchestration, see [`docs/testing/CAMPAIGN_RUNNER.md`](docs/testing/CAMPAIGN_RUNNER.md).
