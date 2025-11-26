# Golden Test Framework

This tool verifies bit-level compatibility between RockyardKV (Go) and RocksDB (C++).

## Prerequisites

1. Build the RocksDB tools (ldb and sst_dump):
   ```bash
   cd /path/to/rocksdb
   make ldb sst_dump
   ```

2. Generate C++ fixtures:
   ```bash
   ./generate_fixtures.sh
   ```

## Running Tests

```bash
# Run all tests
go run ./cmd/goldentest \
  -fixtures ./cmd/goldentest/testdata/cpp_generated \
  -output ./cmd/goldentest/testdata/go_generated \
  -ldb /path/to/rocksdb/ldb \
  -sst-dump /path/to/rocksdb/sst_dump \
  -v

# Run specific test categories
# (modify main.go to enable/disable test suites)
```

## Test Categories

### 1. WAL Format Tests
- Verifies Go can read C++ WAL files
- Verifies C++ can read Go-generated WAL files

### 2. MANIFEST Format Tests  
- Verifies Go can read C++ MANIFEST files
- Verifies C++ can read Go-generated MANIFEST files

### 3. Block Format Tests
- Verifies Go can read C++ data blocks

### 4. SST Format Tests
- Verifies Go can read C++ SST files
- Verifies C++ sst_dump can read Go-generated SST files

### 5. Full Database Tests
- Verifies Go can open C++ databases
- Verifies C++ ldb can open Go-generated databases

## Fixture Generation

C++ fixtures are generated using a custom tool that creates:
- WAL files with various record types
- MANIFEST files with different VersionEdit configurations
- Data blocks with prefix compression
- Complete SST files from a test database

Go fixtures are generated during test runs.

## Reference

- RocksDB v10.7.5 (commit 812b12b)
- tools/ldb.cc
- tools/sst_dump.cc
- tools/generate_golden_fixtures.cc

