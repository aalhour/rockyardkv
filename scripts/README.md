# Scripts

This directory contains helper scripts for building oracle tools and validating test fixtures.

## Table of contents

- [Oracle tooling](#oracle-tooling)
- [Fixture validation](#fixture-validation)
- [Fixture generation](#fixture-generation)
- [Status runners](#status-runners)

## Oracle tooling

### Build RocksDB oracle tools

Build the RocksDB C++ oracle tools (`ldb`, `sst_dump`) from a local RocksDB checkout:

```bash
export ROCKSDB_PATH=/path/to/rocksdb
scripts/build-oracle.sh
```

## Fixture validation

### Validate `testdata/` fixtures

Validate that fixture files exist and that the Go tests which consume them still pass:

```bash
scripts/check-testdata.sh
```

Optional oracle-enabled verification:

```bash
export ROCKSDB_PATH=/path/to/rocksdb
scripts/check-testdata.sh --with-oracle
```

### Validate fixtures (wrapper)

Convenience wrapper around `scripts/check-testdata.sh`:

```bash
scripts/fixtures/validate.sh
```

Oracle-enabled:

```bash
export ROCKSDB_PATH="$HOME/Workspace/rocksdb"  # or /path/to/rocksdb
scripts/fixtures/validate.sh --with-oracle
```

## Fixture generation

### Oracle environment doctor

Check whether the RocksDB C++ oracle tools are configured:

```bash
export ROCKSDB_PATH="$HOME/Workspace/rocksdb"  # or /path/to/rocksdb
scripts/fixtures/doctor.sh
```

### Generate a RocksDB database using db_stress

This generates databases for corpus-style validation and ad-hoc oracle triage.
It does not write into repository `testdata/` by default.

```bash
export ROCKSDB_PATH="$HOME/Workspace/rocksdb"  # or /path/to/rocksdb
scripts/fixtures/generate_db_stress.sh --out-dir tmp/redteam/fixtures/db_stress_run1 --seconds 20 --seed 12345
```

## Status runners

### Durability status repros

Legacy durability repro runner (kept for workflow compatibility):

```bash
scripts/status/run_durability_repros.sh
```


