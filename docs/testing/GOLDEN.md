# Golden Tests (C++ Oracle)

Golden tests validate on-disk compatibility with RocksDB v10.7.5.
They prove that RockyardKV produces and consumes files identical to C++ RocksDB.

## Purpose

Golden tests answer: "Can C++ RocksDB read what Go writes, and vice versa?"

This is the strongest compatibility guarantee.
If a golden test passes, the format is correct.

## Test Structure

Golden tests are standard Go tests that:

1. Read C++-generated fixtures from `testdata/cpp_generated/`
2. Write Go artifacts and verify with C++ tools (`ldb`, `sst_dump`)
3. Test format version × compression matrices

## Test Files

| File | Tests |
|------|-------|
| `constants_test.go` | Magic numbers, property names, footer sizes |
| `db_test.go` | Database round-trip, C++ corpus reading |
| `manifest_test.go` | Read/write, unknown tags, corruption |
| `sst_test.go` | C++ fixtures, sst_dump verification |
| `sst_format_test.go` | Format version × compression matrix |
| `sst_contract_test.go` | Behavioral edge cases (binary keys, deletions) |
| `wal_test.go` | WAL round-trip and C++ compatibility |

## Run Golden Tests

Using make:

```bash
make test-e2e-golden
```

Or directly:

```bash
go test -v ./cmd/goldentest/...
```

## Prerequisites

Build RocksDB tools before running tests that invoke C++ verification:

```bash
cd /path/to/rocksdb
make ldb sst_dump
```

Set library path if needed:

```bash
# macOS
export DYLD_LIBRARY_PATH=/path/to/rocksdb

# Linux
export LD_LIBRARY_PATH=/path/to/rocksdb
```

## Fixtures

Fixtures are the tests. Each file in `testdata/cpp_generated/` is automatically tested.

### Adding Fixtures

When you fix a format bug:

1. Generate a C++ fixture that exercises the bug
2. Add it to `testdata/cpp_generated/`
3. The golden test suite automatically picks it up

### Fixture Categories

| Category | Purpose |
|----------|---------|
| `sst/` | SST files across format versions |
| `db/` | Complete database directories |
| `wal/` | WAL files with various record types |
| `manifest/` | MANIFEST files with version edits |

## C++ Tool Integration

Golden tests use C++ RocksDB tools as oracles:

| Tool | Purpose |
|------|---------|
| `ldb` | Database operations, manifest dump, scan |
| `sst_dump` | SST file inspection, checksum verification |

### ldb Commands

```bash
# Dump manifest
./ldb --db=/path/to/db manifest_dump

# Scan all keys
./ldb --db=/path/to/db scan

# Get specific key
./ldb --db=/path/to/db get mykey
```

### sst_dump Commands

```bash
# Check SST integrity
./sst_dump --file=/path/to/file.sst --command=check --verify_checksums

# Dump SST contents
./sst_dump --file=/path/to/file.sst --command=scan
```

## Format Version Matrix

Golden tests cover all supported format versions:

| Version | Features |
|---------|----------|
| 0 | Legacy format (LevelDB compatible) |
| 3 | Block-based table |
| 4 | Data block hash index |
| 5 | Full filter, compression dictionary |
| 6 | Indexed filter, cache-friendly format |

## Compression Matrix

Golden tests cover all supported compression types:

| Compression | Notes |
|-------------|-------|
| None | Baseline |
| Snappy | Fast, default |
| Zlib | Better ratio |
| LZ4 | Very fast |
| Zstd | Best ratio |

## Test Patterns

### Go Reads C++ Fixture

```go
func TestSST_ReadCppFixture(t *testing.T) {
    // Load C++ generated SST
    reader, err := table.OpenReader(fs, "testdata/cpp_generated/sst/v6.sst")
    require.NoError(t, err)
    defer reader.Close()
    
    // Verify contents match expected
    iter := reader.NewIterator(nil)
    // ... verify keys and values
}
```

### Go Writes, C++ Verifies

```go
func TestSST_CppVerifiesGoOutput(t *testing.T) {
    // Write SST with Go
    path := t.TempDir() + "/test.sst"
    writeSST(t, path, testData)
    
    // Verify with C++ sst_dump
    runSstDump(t, path, "--command=check", "--verify_checksums")
}
```

### Round-Trip Test

```go
func TestDB_RoundTrip(t *testing.T) {
    dir := t.TempDir()
    
    // Write with Go
    db := openDB(t, dir)
    db.Put(opts, key, value)
    db.Close()
    
    // Read with C++ ldb
    output := runLdb(t, "--db="+dir, "get", string(key))
    require.Equal(t, string(value), output)
}
```

## Debugging Format Issues

When a golden test fails:

1. Check the C++ tool output for specific error
2. Use `xxd` or hex dump to compare byte-level differences
3. Check format version and compression settings
4. Verify magic numbers and checksums

### Common Issues

| Issue | Cause |
|-------|-------|
| "Bad magic number" | Footer encoding wrong |
| "Checksum mismatch" | Block encoding or checksum algorithm wrong |
| "Unknown compression" | Compression type byte wrong |
| "Corrupted keys" | Key encoding differs from C++ |

## References

- RocksDB format specifications
- `include/rocksdb/table.h` — format version history
- `table/format.cc` — footer and block encoding

