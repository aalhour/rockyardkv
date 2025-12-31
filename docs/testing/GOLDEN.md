# Golden tests (C++ oracle)

Golden tests validate on-disk compatibility with RocksDB v10.7.5.
They check that C++ RocksDB tools can read Go-written files and that RockyardKV can read C++-generated fixtures.

## Table of contents

- [Purpose](#purpose)
- [Test structure](#test-structure)
- [Test files](#test-files)
- [Run golden tests](#run-golden-tests)
- [Prerequisites](#prerequisites)
- [Fixtures](#fixtures)
- [C++ tool integration](#c-tool-integration)
- [Format version matrix](#format-version-matrix)
- [Compression matrix](#compression-matrix)
- [Test patterns](#test-patterns)
- [Debugging format issues](#debugging-format-issues)
- [External corpus tests](#external-corpus-tests)
- [References](#references)

## Purpose

Golden tests answer: "Can C++ RocksDB read what Go writes, and vice versa?"

This is the strongest compatibility signal in this repository.
If a golden test passes, the tested file shape is compatible with the oracle for that scenario.

## Test structure

Golden tests are standard Go tests that:

1. Read C++-generated fixtures from `testdata/rocksdb/v10.7.5/`
2. Write Go artifacts and verify with C++ tools (`ldb`, `sst_dump`)
3. Test format version × compression matrices

## Test files

| File | Tests |
|------|-------|
| `constants_test.go` | Magic numbers, property names, footer sizes |
| `db_test.go` | Database round-trip, C++ corpus reading |
| `manifest_test.go` | Read/write, unknown tags, corruption |
| `sst_test.go` | C++ fixtures, sst_dump verification |
| `sst_format_test.go` | Format version × compression matrix |
| `sst_contract_test.go` | Behavioral edge cases (binary keys, deletions) |
| `wal_test.go` | WAL round-trip and C++ compatibility |

## Run golden tests

Using make:

```bash
make test-e2e-golden
```

Or directly:

```bash
go test -v ./cmd/goldentest/...
```

## Prerequisites

Golden tests include **C++ oracle verification**. To run the full suite (including
Go-writes → C++-verifies tests), you must build the RocksDB v10.7.5 CLI tools:
`ldb` and `sst_dump`.

### 1) Build C++ oracle tools (RocksDB v10.7.5)

```bash
export ROCKSDB_PATH="/path/to/rocksdb"   # repo root containing ./ldb and ./sst_dump
( cd "$ROCKSDB_PATH" && make shared_lib ldb sst_dump )
```

Notes:
- `shared_lib` is required on macOS so `ldb` / `sst_dump` can locate `librocksdb*.dylib`.
- Some environments may need additional shared libraries (snappy/lz4/zstd) available
  to the dynamic linker (see below).

### 2) Dynamic linker setup (macOS/Linux)

The golden tests execute `ldb` / `sst_dump` and set `DYLD_LIBRARY_PATH` /
`LD_LIBRARY_PATH` to the directory containing those binaries. If your RocksDB build
or compression deps are not in default linker locations, export:

```bash
# macOS
export DYLD_LIBRARY_PATH="$ROCKSDB_PATH${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"

# Linux
export LD_LIBRARY_PATH="$ROCKSDB_PATH${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

If RocksDB depends on external shared libraries (snappy/lz4/zstd) that are not in
standard linker paths, you may need to extend the variables above to include that
directory too (e.g., a Homebrew/Conda lib directory).

### 3) What happens if tools are missing?

- Some tests may **skip** if `ldb` / `sst_dump` are not found.
- For compatibility verification (CI and local “oracle-locked” runs), treat missing
  tools as a setup error and install/build them as described above.

## Fixtures

Fixtures are the tests. Each file in `testdata/rocksdb/v10.7.5/` is automatically tested.

### Adding Fixtures

When you fix a format bug:

1. Generate a C++ fixture that exercises the bug
2. Add it to `testdata/rocksdb/v10.7.5/` (see `testdata/rocksdb/README.md` for layout)
3. The golden test suite automatically picks it up

### Fixture Categories

| Category | Purpose |
|----------|---------|
| `sst/` | SST files across format versions |
| `db/` | Complete database directories |
| `wal/` | WAL files with various record types |
| `manifest/` | MANIFEST files with version edits |

## C++ tool integration

Golden tests use C++ RocksDB tools as oracles:

| Tool | Purpose |
|------|---------|
| `ldb` | Database operations, manifest dump, scan |
| `sst_dump` | SST file inspection, checksum verification |

### ldb commands

```bash
# Dump manifest
./ldb --db=/path/to/db manifest_dump

# Scan all keys
./ldb --db=/path/to/db scan

# Get specific key
./ldb --db=/path/to/db get mykey
```

### sst_dump commands

```bash
# Check SST integrity
./sst_dump --file=/path/to/file.sst --command=check

# Verify checksums
./sst_dump --file=/path/to/file.sst --command=verify

# Scan SST contents
./sst_dump --file=/path/to/file.sst --command=scan
```

## Format version matrix

Golden tests cover all supported format versions:

| Version | Features |
|---------|----------|
| 0 | Legacy format (LevelDB compatible) |
| 3 | Block-based table |
| 4 | Data block hash index |
| 5 | Full filter, compression dictionary |
| 6 | Indexed filter, cache-friendly format |

## Compression matrix

Golden tests cover all supported compression types:

| Compression | Notes |
|-------------|-------|
| None | Baseline |
| Snappy | Fast, default |
| Zlib | Better ratio |
| LZ4 | Very fast |
| Zstd | Best ratio |

## Test patterns

### Go reads C++ fixture

```go
func TestSST_ReadCppFixture(t *testing.T) {
    // Load C++ generated SST
    reader, err := table.OpenReader(fs, "testdata/rocksdb/v10.7.5/sst_samples/v6.sst")
    require.NoError(t, err)
    defer reader.Close()
    
    // Verify contents match expected
    iter := reader.NewIterator(nil)
    // ... verify keys and values
}
```

### Go writes, C++ verifies

```go
func TestSST_CppVerifiesGoOutput(t *testing.T) {
    // Write SST with Go
    path := t.TempDir() + "/test.sst"
    writeSST(t, path, testData)
    
    // Verify with C++ sst_dump
    runSstDump(t, path, "--command=check", "--verify_checksums")
}
```

### Round-trip test

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

## Debugging format issues

When a golden test fails:

1. Check the C++ tool output for specific error
2. Use `xxd` or hex dump to compare byte-level differences
3. Check format version and compression settings
4. Verify magic numbers and checksums

### Common issues

| Issue | Cause |
|-------|-------|
| "Bad magic number" | Footer encoding wrong |
| "Checksum mismatch" | Block encoding or checksum algorithm wrong |
| "Unknown compression" | Compression type byte wrong |
| "Corrupted keys" | Key encoding differs from C++ |

## External corpus tests

External corpus tests verify that Go reads C++-generated databases correctly.
You run these tests against a directory of fixtures created by C++ RocksDB.

### Set up the corpus

Export the `REDTEAM_CPP_CORPUS_ROOT` environment variable:

```bash
export REDTEAM_CPP_CORPUS_ROOT=/path/to/corpus
```

### Corpus layout

The corpus directory contains subdirectories with complete RocksDB databases.

| Subdirectory | Fixture type |
|--------------|--------------|
| `multi_cf_db/db/` | Multiple column families |
| `rangedel_db/db/` | Range deletions |
| `zlib_small_blocks_db/db/` | Zlib-compressed SST files |

Each `db/` subdirectory contains `CURRENT`, `MANIFEST-*`, and `*.sst` files.

### Run corpus tests

Use make to fail fast when the environment variable isn't set:

```bash
make test-e2e-golden-corpus
```

Run tests directly to skip when the environment variable isn't set:

```bash
go test -v -run TestCppCorpus ./cmd/goldentest/...
```

### Corpus tests

| Test | Contract |
|------|----------|
| `TestCppCorpus_MultiCF` | Go reads C++ multi-column-family databases |
| `TestCppCorpus_RangeDel` | Go reads C++ range deletion databases |
| `TestCppCorpus_ZlibSST` | Go reads C++ zlib-compressed SST files |

### Create fixtures

Generate fixtures with C++ RocksDB.
Copy the resulting database directory to your corpus location.

```cpp
Options options;
options.compression = kZlibCompression;
DB* db;
DB::Open(options, "/path/to/zlib_small_blocks_db/db", &db);
db->Put(WriteOptions(), "key", "value");
db->Close();
```

### Troubleshoot skipped tests

Corpus tests skip when fixtures are missing.
Verify these conditions:

1. `REDTEAM_CPP_CORPUS_ROOT` is set and points to the corpus directory.
1. Each expected subdirectory exists (refer to the layout table).
1. Each `db/` subdirectory contains valid RocksDB files.

## References

- RocksDB format specifications
- `include/rocksdb/table.h` — format version history
- `table/format.cc` — footer and block encoding
