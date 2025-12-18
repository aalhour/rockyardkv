# Golden Test Fixtures

This directory contains golden test fixtures generated from RocksDB v10.7.5 (commit 812b12b).

These fixtures are used to verify that our Go implementation produces bit-compatible output with the C++ RocksDB implementation.

## Directory Structure

```
golden/
├── v10.7.5/
│   ├── wal/
│   │   ├── simple.log      # Single WriteBatch (key3=value3)
│   │   ├── multi.log       # Single WriteBatch (key5=value5)
│   │   └── fragmented.log  # Large record spanning multiple blocks (~50KB)
│   ├── manifest/
│   │   ├── simple.manifest # Basic VersionEdits
│   │   └── newfile.manifest # VersionEdits with NewFile4 entries
│   ├── sst/
│   │   └── simple.sst      # SST file with key1,key2,key3
│   ├── CURRENT             # CURRENT file format
│   └── options.txt         # OPTIONS file format
└── README.md
```

## Regenerating Fixtures

To regenerate fixtures from a fresh RocksDB build:

```bash
cd ~/Workspace/rocksdb
git checkout v10.7.5
make clean && make static_lib ldb -j$(nproc)

# Create test database
rm -rf /tmp/rocksdb_test
./ldb --db=/tmp/rocksdb_test --create_if_missing put key1 value1
./ldb --db=/tmp/rocksdb_test put key2 value2
./ldb --db=/tmp/rocksdb_test put key3 value3

# Copy generated files
cp /tmp/rocksdb_test/*.log ~/Workspace/rockyardkv/testdata/golden/v10.7.5/wal/
cp /tmp/rocksdb_test/MANIFEST-* ~/Workspace/rockyardkv/testdata/golden/v10.7.5/manifest/
cp /tmp/rocksdb_test/*.sst ~/Workspace/rockyardkv/testdata/golden/v10.7.5/sst/
```

## Fixture Details

### WAL Files
- `simple.log`: Single WriteBatch with seq=3, key3=value3 (32 bytes)
- `multi.log`: Single WriteBatch with seq=5, key5=value5 (32 bytes)
- `fragmented.log`: Large record ~50KB that spans multiple 32KB blocks

### MANIFEST Files
- `simple.manifest`: Multiple VersionEdits including:
  - Comparator: leveldb.BytewiseComparator
  - LogNumber updates
  - NextFileNumber updates
  - LastSequence updates
  - NewFile4 entries for key1 and key2

- `newfile.manifest`: More complex VersionEdits with multiple NewFile4 entries:
  - Files for key1, key2, key3, key4
  - Compaction moving key1 to level 6

### SST Files
- `simple.sst`: Block-based table with key1=value1

## Usage in Tests

Golden tests read these files and verify:
1. Our WAL reader can parse C++-generated WAL files
2. Our VersionEdit decoder can parse C++-generated MANIFEST files
3. Round-trip: encode -> decode produces identical results

```go
func TestGoldenWALSimple(t *testing.T) {
    data, _ := os.ReadFile("testdata/golden/v10.7.5/wal/simple.log")
    reader := wal.NewReader(bytes.NewReader(data), nil, true, 0)
    record, err := reader.ReadRecord()
    // verify record contains expected data
}
```
