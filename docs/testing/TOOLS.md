# Inspection tools

RockyardKV provides command-line tools for inspecting databases and files.
Use these to debug test failures and understand database state.

Note: There are two families of tools with overlapping names:

- Go tools built from this repo (e.g., `./bin/ldb`, `./bin/sstdump`)
- C++ RocksDB oracle tools (e.g., `$ROCKSDB_PATH/ldb`, `$ROCKSDB_PATH/sst_dump`)

## Table of contents

- [Overview](#overview)
- [Environment variables](#environment-variables)
- [Build tools](#build-tools)
- [Fixture validation script](#fixture-validation-script)
- [`ldb` — database tool](#ldb--database-tool)
- [`sstdump` — SST file tool](#sstdump--sst-file-tool)
- [`traceanalyzer` — trace tool](#traceanalyzer--trace-tool)
- [C++ RocksDB oracle tools (`ldb`, `sst_dump`)](#c-rocksdb-oracle-tools-ldb-sst_dump)
- [Debugging workflow](#debugging-workflow)

## Overview

| Tool | Purpose | Location |
|------|---------|----------|
| `ldb` | Database operations and inspection | `cmd/ldb/` |
| `sstdump` | SST file inspection and verification | `cmd/sstdump/` |
| `traceanalyzer` | Trace file analysis and replay | `cmd/traceanalyzer/` |

## Environment variables

The following environment variables must be present in order to use the tools properly.

| Variable | Purpose |
|----------|---------|
| `ROCKSDB_PATH` | Path to a RocksDB checkout/build that provides `ldb` and `sst_dump` |
| `ROCKYARDKV_CPP_ORACLE_PATH` | Path to RocksDB tools for some oracle-enabled tests (if present in a harness, treat it as equivalent to `ROCKSDB_PATH`) |
| `DYLD_LIBRARY_PATH` | macOS dynamic library path |
| `LD_LIBRARY_PATH` | Linux dynamic library path |

### What these variables are

- **`ROCKSDB_PATH`**: a directory that contains the C++ oracle executables:
  - `$ROCKSDB_PATH/ldb`
  - `$ROCKSDB_PATH/sst_dump`
- **`ROCKYARDKV_CPP_ORACLE_PATH`**: some test harnesses look for this name instead of `ROCKSDB_PATH`.
  Treat it as an alias of `ROCKSDB_PATH` when you see it.
- **`DYLD_LIBRARY_PATH` / `LD_LIBRARY_PATH`**: dynamic linker search paths.
  They must include the directory where `librocksdb*.{dylib,so}` can be found (and sometimes compression libs depending on how you built RocksDB).

### Example setup (assume RocksDB checkout at `/path/to/rocksdb`)

Assuming you have RocksDB source checked out at `/path/to/rocksdb`, and you have the required build tooling and compression dependencies installed, export the following variables so the C++ oracle tools in this document work.

Install dependencies (examples):

```bash
# Ubuntu/Debian (examples)
sudo apt-get update
sudo apt-get install -y build-essential cmake git zlib1g-dev libsnappy-dev liblz4-dev libzstd-dev
```

```bash
# macOS Homebrew (examples)
brew install snappy lz4 zstd
```

Build the C++ oracle tools:

```bash
export ROCKSDB_PATH=/path/to/rocksdb
( cd "$ROCKSDB_PATH" && make shared_lib ldb sst_dump )
```

Export env vars for tool execution:

```bash
export ROCKSDB_PATH=/path/to/rocksdb
export ROCKYARDKV_CPP_ORACLE_PATH="$ROCKSDB_PATH"

# macOS
export DYLD_LIBRARY_PATH="$ROCKSDB_PATH${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"

# Linux
export LD_LIBRARY_PATH="$ROCKSDB_PATH${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

If you have a RocksDB checkout in the standard sibling workspace location, you can use:

```bash
export ROCKSDB_PATH="$HOME/Workspace/rocksdb"
export ROCKYARDKV_CPP_ORACLE_PATH="$ROCKSDB_PATH"
```

## Build tools

```bash
make build
```

Or build individually:

```bash
go build -o bin/ldb ./cmd/ldb
go build -o bin/sstdump ./cmd/sstdump
go build -o bin/traceanalyzer ./cmd/traceanalyzer
```

## Fixture validation script

Use `scripts/check-testdata.sh` to validate that repository fixtures are present and consistent with the Go tests that consume them.

Quick validation (no C++ oracle required):

```bash
scripts/check-testdata.sh
```

Oracle-enabled validation (requires RocksDB `ldb` + `sst_dump`):

```bash
export ROCKSDB_PATH="$HOME/Workspace/rocksdb"  # or /path/to/rocksdb
scripts/check-testdata.sh --with-oracle
```

Related helpers (optional):

```bash
scripts/fixtures/doctor.sh
scripts/fixtures/validate.sh --with-oracle
```

## ldb — database tool

### Scan all keys

```bash
./bin/ldb --db=/path/to/db scan
```


### Get specific key

```bash
./bin/ldb --db=/path/to/db get mykey
```

Example:

```bash
./bin/ldb --db=<RUN_DIR>/db get mykey
```

### Dump MANIFEST

Show version edit history:

```bash
./bin/ldb --db=/path/to/db manifest_dump
```

Example:

```bash
./bin/ldb --db=<RUN_DIR>/db manifest_dump
```

Verbose output with file details:

```bash
./bin/ldb --db=/path/to/db manifest_dump -v
```

### Print database info

```bash
./bin/ldb --db=/path/to/db info
```

### List SST files

```bash
./bin/ldb --db=/path/to/db sstfiles
```

## sstdump — SST file tool

### Check SST integrity

Verify block checksums:

```bash
./bin/sstdump --file=/path/to/file.sst --command=check --verify_checksums
```

Verbose block-level progress:

```bash
./bin/sstdump --file=/path/to/file.sst --command=check --verify_checksums -v
```

### Scan SST contents

```bash
./bin/sstdump --file=/path/to/file.sst --command=scan
```

### Show SST properties

```bash
./bin/sstdump --file=/path/to/file.sst --command=properties
```

## traceanalyzer — trace tool

### View trace statistics

```bash
./bin/traceanalyzer stats /path/to/trace.bin
```

Output includes:

- Total operations by type
- Duration and throughput

### Dump trace records

Show first 100 operations:

```bash
./bin/traceanalyzer -limit 100 dump /path/to/trace.bin
```

### Replay trace

Apply operations to a fresh database:

```bash
./bin/traceanalyzer -db /path/to/replay_db -create=true -dry-run=false replay /path/to/trace.bin
```

Count operations without applying:

```bash
./bin/traceanalyzer -db /path/to/replay_db -create=true -dry-run=true replay /path/to/trace.bin
```

Replay at original pace:

```bash
./bin/traceanalyzer -db /path/to/replay_db -create=true -dry-run=false -preserve-timing replay /path/to/trace.bin
```

## C++ RocksDB oracle tools (`ldb`, `sst_dump`)

For format compatibility verification, use C++ RocksDB tools as oracles.

### Build C++ tools

```bash
export ROCKSDB_PATH=<ROCKSDB_PATH>
( cd "$ROCKSDB_PATH" && make shared_lib ldb sst_dump )
```

### Set library path

```bash
# macOS
export DYLD_LIBRARY_PATH="$ROCKSDB_PATH${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"

# Linux
export LD_LIBRARY_PATH="$ROCKSDB_PATH${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

### C++ ldb

```bash
# Manifest dump
"$ROCKSDB_PATH/ldb" --db=/path/to/db manifest_dump

# Scan
"$ROCKSDB_PATH/ldb" --db=/path/to/db scan
```

### C++ sst_dump

```bash
# Check integrity
"$ROCKSDB_PATH/sst_dump" --file=/path/to/file.sst --command=check

# Verify checksums
"$ROCKSDB_PATH/sst_dump" --file=/path/to/file.sst --command=verify
```

Example:

```bash
"$ROCKSDB_PATH/sst_dump" --file=<RUN_DIR>/db/<SST_FILE> --command=verify
```

## Debugging workflow

### After a crash test failure

1. **Inspect artifacts:**
   ```bash
   ls artifacts/
   cat artifacts/run.json
   ```

2. **Check MANIFEST:**
   ```bash
   ./bin/ldb --db=artifacts/db manifest_dump
   ```

3. **Scan database:**
   ```bash
   ./bin/ldb --db=artifacts/db scan
   ```

4. **Verify SST files:**
   ```bash
   for sst in artifacts/db/*.sst; do
     ./bin/sstdump --file=$sst --command=check --verify_checksums
   done
   ```

### After a trace replay failure

1. **Get trace statistics:**
   ```bash
   ./bin/traceanalyzer stats ./trace.bin
   ```

2. **Find problematic operation:**
   ```bash
   ./bin/traceanalyzer -limit 1000 dump ./trace.bin
   ```

3. **Replay to specific point:**
   ```bash
   # Use dump to inspect a prefix, then re-run replay with -v to print handler errors.
   ./bin/traceanalyzer -limit 500 dump ./trace.bin
   ./bin/traceanalyzer -v -db /path/to/debug_db -create=true -dry-run=false replay ./trace.bin
   ```

### Comparing Go and C++ output

1. **Write with Go, verify with C++:**
   ```bash
   ./bin/ldb --db=<DB_PATH> scan > go_output.txt
   "$ROCKSDB_PATH/ldb" --db=<DB_PATH> scan > cpp_output.txt
   diff go_output.txt cpp_output.txt
   ```

2. **Byte-level comparison:**
   ```bash
   xxd <GO_SST_PATH> > go_sst.hex
   xxd <CPP_SST_PATH> > cpp_sst.hex
   diff go_sst.hex cpp_sst.hex
   ```
