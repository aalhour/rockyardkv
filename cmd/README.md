# Command-line tools

This directory contains command-line utilities for inspection and testing.
Keep this file brief.
Refer to [`docs/testing/README.md`](docs/testing/README.md) for details about the testing model and algorithms.

## Naming convention

All packages follow Go idioms: lowercase, no underscores, compound words for clarity.
Test harnesses use the `*test` suffix for easy identification.

## Inspection tools

- `ldb`: database inspection and manipulation.
- `sstdump`: SST file inspection.
- `traceanalyzer`: trace file analysis.

Note: C++ oracle tools (`ldb`, `sst_dump`) are external RocksDB tools. See [`docs/testing/GOLDEN.md`](docs/testing/GOLDEN.md).

## Test harnesses

- `smoketest`: fast end-to-end feature checks.
- `stresstest`: concurrency stress with an expected-state oracle.
- `crashtest`: SIGKILL crash cycles built on the stress tool.
- `adversarialtest`: hostile inputs and fault patterns.
- `goldentest`: RocksDB v10.7.5 compatibility checks.
- `campaignrunner`: oracle-gated runner that executes a fixed matrix of named instances.

## Build

```bash
make build
```

## Run

- Use `make test-e2e-*` targets.
- Refer to [`docs/testing/README.md`](docs/testing/README.md) for flags and workflows.
- For campaign orchestration, see [`docs/testing/CAMPAIGN_RUNNER.md`](docs/testing/CAMPAIGN_RUNNER.md).
