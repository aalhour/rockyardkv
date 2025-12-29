# Inspection Tools

RockyardKV provides command-line tools for inspecting databases and files.
Use these to debug test failures and understand database state.

## Overview

| Tool | Purpose | Location |
|------|---------|----------|
| `ldb` | Database operations and inspection | `cmd/ldb/` |
| `sstdump` | SST file inspection and verification | `cmd/sstdump/` |
| `traceanalyzer` | Trace file analysis and replay | `cmd/traceanalyzer/` |

## Build Tools

```bash
make build
```

Or build individually:

```bash
go build -o bin/ldb ./cmd/ldb
go build -o bin/sstdump ./cmd/sstdump
go build -o bin/traceanalyzer ./cmd/traceanalyzer
```

## ldb — Database Tool

### Scan All Keys

```bash
./bin/ldb --db=/path/to/db scan
```

### Get Specific Key

```bash
./bin/ldb --db=/path/to/db get mykey
```

### Dump MANIFEST

Show version edit history:

```bash
./bin/ldb --db=/path/to/db manifest_dump
```

Verbose output with file details:

```bash
./bin/ldb --db=/path/to/db manifest_dump -v
```

### Dump WAL Files

```bash
./bin/ldb --db=/path/to/db dump_wal
```

## sstdump — SST File Tool

### Check SST Integrity

Verify block checksums:

```bash
./bin/sstdump --file=/path/to/file.sst --command=check --verify_checksums
```

Verbose block-level progress:

```bash
./bin/sstdump --file=/path/to/file.sst --command=check --verify_checksums -v
```

### Scan SST Contents

```bash
./bin/sstdump --file=/path/to/file.sst --command=scan
```

### Show SST Properties

```bash
./bin/sstdump --file=/path/to/file.sst --command=properties
```

## traceanalyzer — Trace Tool

### View Trace Statistics

```bash
./bin/traceanalyzer stats ./trace.log
```

Output includes:

- Total operations by type
- Duration and throughput

### Dump Trace Records

Show first 100 operations:

```bash
./bin/traceanalyzer dump -limit 100 ./trace.log
```

### Replay Trace

Apply operations to a fresh database:

```bash
./bin/traceanalyzer -db /tmp/replay_db -create=true -dry-run=false replay ./trace.bin
```

Count operations without applying:

```bash
./bin/traceanalyzer -db /tmp/replay_db -create=true -dry-run=true replay ./trace.bin
```

Replay at original pace:

```bash
./bin/traceanalyzer -db /tmp/replay_db -create=true -dry-run=false -preserve-timing replay ./trace.bin
```

## C++ RocksDB Tools

For format compatibility verification, use C++ RocksDB tools as oracles.

### Build C++ Tools

```bash
cd /path/to/rocksdb
make ldb sst_dump
```

### Set Library Path

```bash
# macOS
export DYLD_LIBRARY_PATH=/path/to/rocksdb

# Linux
export LD_LIBRARY_PATH=/path/to/rocksdb
```

### C++ ldb

```bash
# Manifest dump
/path/to/rocksdb/ldb --db=/path/to/db manifest_dump

# Scan
/path/to/rocksdb/ldb --db=/path/to/db scan
```

### C++ sst_dump

```bash
# Check integrity
/path/to/rocksdb/sst_dump --file=/path/to/file.sst --command=check

# Verify checksums
/path/to/rocksdb/sst_dump --file=/path/to/file.sst --verify_checksums
```

## Debugging Workflow

### After a Crash Test Failure

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

### After a Trace Replay Failure

1. **Get trace statistics:**
   ```bash
   ./bin/traceanalyzer stats ./trace.bin
   ```

2. **Find problematic operation:**
   ```bash
   ./bin/traceanalyzer dump -limit 1000 ./trace.bin
   ```

3. **Replay to specific point:**
   ```bash
   # Use dump to inspect a prefix, then re-run replay with -v to print handler errors.
   ./bin/traceanalyzer dump -limit 500 ./trace.bin
   ./bin/traceanalyzer -v -db /tmp/debug_db -create=true -dry-run=false replay ./trace.bin
   ```

### Comparing Go and C++ Output

1. **Write with Go, verify with C++:**
   ```bash
   ./bin/ldb --db=/tmp/testdb scan > go_output.txt
   /path/to/rocksdb/ldb --db=/tmp/testdb scan > cpp_output.txt
   diff go_output.txt cpp_output.txt
   ```

2. **Byte-level comparison:**
   ```bash
   xxd /tmp/testdb/000001.sst > go_sst.hex
   xxd /tmp/cpp_testdb/000001.sst > cpp_sst.hex
   diff go_sst.hex cpp_sst.hex
   ```

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `ROCKYARDKV_CPP_ORACLE_PATH` | Path to C++ RocksDB build directory |
| `DYLD_LIBRARY_PATH` | macOS dynamic library path |
| `LD_LIBRARY_PATH` | Linux dynamic library path |

