# Campaign runner (oracle-gated)

This document describes `bin/campaignrunner`: a single runner that executes named testing campaigns as a fixed matrix of instances.

It does not replace the underlying test binaries.
It orchestrates them with consistent artifacts and consistent accounting.

## Table of contents

- [What the runner is for](#what-the-runner-is-for)
- [Concepts](#concepts)
- [Tools the runner orchestrates](#tools-the-runner-orchestrates)
- [Quick start](#quick-start)
- [Oracle gating](#oracle-gating)
- [Artifacts](#artifacts)
- [Trace capture and replay](#trace-capture-and-replay)
- [Minimization (reduce a failure to a smaller repro)](#minimization-reduce-a-failure-to-a-smaller-repro)

## What the runner is for

Use `bin/campaignrunner` when you need:

- a stable list of named instances (no ad-hoc “run01” loops),
- stable seeds and stop conditions,
- strict oracle gating for oracle-required checks,
- per-run artifacts you can triage without reruns,
- and failure dedupe so repeated failures don’t bury new failures.

## Concepts

- **Instance**: one named scenario with a tool, arguments, seed list, and stop conditions.
- **Tier**: an intensity level (e.g., `quick` vs `nightly`) that selects a matrix size and timeouts.
- **Group**: a prefix filter over instances (run a subset of the matrix).
- **Oracle-required**: instances that must run C++ RocksDB tools to classify/validate artifacts.
- **Stop conditions**: what “pass” means for the instance (termination, final verification, oracle OK).

## Tools the runner orchestrates

The runner shells out to existing binaries and suites:

- `bin/stresstest`
- `bin/crashtest`
- `bin/adversarialtest`
- `go test ./cmd/goldentest/...`

For background on these tools:

- Blackbox harnesses: [BLACKBOX.md](BLACKBOX.md)
- Whitebox harnesses: [WHITEBOX.md](WHITEBOX.md)
- Fault models: [VFS_FAULT_INJECTION.md](VFS_FAULT_INJECTION.md)
- Oracle testing: [GOLDEN.md](GOLDEN.md)
- Inspectors: [TOOLS.md](TOOLS.md)

## Quick start

List groups:

```bash
./bin/campaignrunner -list-groups
```

Run the quick tier:

```bash
./bin/campaignrunner -tier=quick -run-root <RUN_DIR>
```

Run one group:

```bash
./bin/campaignrunner -tier=quick -group=stress.read.status.1in7 -run-root <RUN_DIR>
```

Stop on first failure:

```bash
./bin/campaignrunner -tier=quick -fail-fast -run-root <RUN_DIR>
```

## Oracle gating

Some instances require a C++ oracle to validate a produced DB snapshot.
For those instances, the runner treats “oracle missing” as a setup failure.

Configure the oracle via:

- `ROCKSDB_PATH`: path to a RocksDB checkout/build that provides `ldb` and `sst_dump`.

If an instance is oracle-required and the oracle is not configured, the runner must fail that instance (and should fail fast before running anything).

## Artifacts

Each seed run writes a run directory under `-run-root`.

Minimum expected artifacts per run:

- `run.json`: run metadata (instance, seed, exit code, pass/fail, fingerprint)
- `output.log`: combined stdout/stderr for the tool

When oracle checks run, the runner writes an `oracle/` subdirectory containing captured tool outputs.

## Trace capture and replay

The runner can enable trace capture for `stresstest` runs.

Enable capture:

```bash
./bin/campaignrunner -tier=quick -capture-trace -run-root <RUN_DIR>
```

Expected artifacts when enabled:

- `trace/ops.bin`: the trace file
- `replay.sh`: a convenience script that replays the trace via `bin/traceanalyzer`
- `run.json` fields that record trace path and replay command

## Minimization (reduce a failure to a smaller repro)

When enabled, minimization runs additional attempts for eligible *new* failures to reduce:

- duration → threads → keys

Enable minimization (also enables trace capture):

```bash
./bin/campaignrunner -tier=quick -minimize -run-root <RUN_DIR>
```

The runner should preserve evidence for each minimization attempt and record reduction steps in `run.json`.
