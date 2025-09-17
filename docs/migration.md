# Migrating from C++ RocksDB to RockyardKV

This guide helps you migrate from C++ RocksDB (or CGo wrappers like `gorocksdb`) to the pure-Go RockyardKV implementation.

## Why Migrate?

| Aspect | CGo RocksDB | RockyardKV |
|--------|-------------|---------------|
| Build | Requires C++ toolchain | `go build` only |
| Cross-compile | Complex | Trivial |
| Debugging | Hard (mixed stacks) | Native Go tools |
| GC | Manual memory mgmt | Automatic |
| Container size | Large (200MB+) | Small (~10MB) |

## API Mapping

### Opening a Database

**C++ RocksDB / CGo:**
```cpp
rocksdb::Options options;
options.create_if_missing = true;
rocksdb::DB* db;
rocksdb::Status status = rocksdb::DB::Open(options, "/path/to/db", &db);
```

**RockyardKV:**
```go
opts := db.DefaultOptions()
opts.CreateIfMissing = true
database, err := db.Open("/path/to/db", opts)
```

### Put/Get/Delete

**C++ RocksDB:**
```cpp
db->Put(rocksdb::WriteOptions(), "key", "value");
std::string value;
db->Get(rocksdb::ReadOptions(), "key", &value);
db->Delete(rocksdb::WriteOptions(), "key");
```

**RockyardKV:**
```go
database.Put(nil, []byte("key"), []byte("value"))
value, err := database.Get(nil, []byte("key"))
database.Delete(nil, []byte("key"))
```

### WriteBatch

**C++ RocksDB:**
```cpp
rocksdb::WriteBatch batch;
batch.Put("key1", "value1");
batch.Put("key2", "value2");
batch.Delete("key3");
db->Write(rocksdb::WriteOptions(), &batch);
```

**RockyardKV:**
```go
wb := batch.New()
wb.Put([]byte("key1"), []byte("value1"))
wb.Put([]byte("key2"), []byte("value2"))
wb.Delete([]byte("key3"))
database.Write(nil, wb)
```

### Iterator

**C++ RocksDB:**
```cpp
rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString() << std::endl;
}
delete it;
```

**RockyardKV:**
```go
iter := database.NewIterator(nil)
defer iter.Close()
for iter.SeekToFirst(); iter.Valid(); iter.Next() {
    fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
}
```

### Snapshot

**C++ RocksDB:**
```cpp
const rocksdb::Snapshot* snapshot = db->GetSnapshot();
rocksdb::ReadOptions options;
options.snapshot = snapshot;
db->Get(options, "key", &value);
db->ReleaseSnapshot(snapshot);
```

**RockyardKV:**
```go
snapshot := database.GetSnapshot()
defer database.ReleaseSnapshot(snapshot)
opts := db.DefaultReadOptions()
opts.Snapshot = snapshot
value, _ := database.Get(opts, []byte("key"))
```

### Column Family

**C++ RocksDB:**
```cpp
rocksdb::ColumnFamilyHandle* cf;
db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "mycf", &cf);
db->Put(rocksdb::WriteOptions(), cf, "key", "value");
db->Get(rocksdb::ReadOptions(), cf, "key", &value);
```

**RockyardKV:**
```go
cf, _ := database.CreateColumnFamily(db.ColumnFamilyOptions{}, "mycf")
database.PutCF(nil, cf, []byte("key"), []byte("value"))
value, _ := database.GetCF(nil, cf, []byte("key"))
```

## Options Mapping

| C++ Option | Go Equivalent | Notes |
|------------|--------------|-------|
| `create_if_missing` | `CreateIfMissing` | |
| `error_if_exists` | `ErrorIfExists` | |
| `paranoid_checks` | `ParanoidChecks` | |
| `write_buffer_size` | `WriteBufferSize` | |
| `max_write_buffer_number` | `MaxWriteBufferNumber` | |
| `max_open_files` | `MaxOpenFiles` | |
| `merge_operator` | `MergeOperator` | |
| `comparator` | `Comparator` | |

### WriteOptions

| C++ Option | Go Equivalent |
|------------|--------------|
| `sync` | `Sync` |
| `disableWAL` | `DisableWAL` |

### ReadOptions

| C++ Option | Go Equivalent |
|------------|--------------|
| `verify_checksums` | `VerifyChecksums` |
| `fill_cache` | `FillCache` |
| `snapshot` | `Snapshot` |

## Error Handling

**C++ RocksDB:**
```cpp
rocksdb::Status status = db->Get(...);
if (status.IsNotFound()) {
    // Handle not found
} else if (!status.ok()) {
    // Handle error
}
```

**RockyardKV:**
```go
value, err := database.Get(...)
if err == db.ErrNotFound {
    // Handle not found
} else if err != nil {
    // Handle error
}
```

## Common Errors

| C++ Status | Go Error |
|------------|----------|
| `Status::NotFound()` | `db.ErrNotFound` |
| `Status::IOError()` | `os` package errors |
| `Status::Corruption()` | `db.ErrCorruption` (if applicable) |

## Data Migration

RockyardKV uses **the same on-disk format** as RocksDB v10.7.5:

- SST files are compatible
- WAL format is compatible
- MANIFEST format is compatible

### Option 1: Direct Usage

If your existing database was created with RocksDB v10.7.5 (or compatible), you can simply open it with RockyardKV:

```go
database, err := db.Open("/path/to/existing/rocksdb", opts)
```

### Option 2: Export/Import

For older or incompatible versions:

```bash
# Export from C++ RocksDB
ldb --db=/path/to/db dump > data.txt

# Import to RockyardKV
# (Write a simple Go program to read the dump and Put each key)
```

### Option 3: SST File Ingestion

```go
// Create SST files with sst_file_writer in C++
// Then open the database with RockyardKV
// SST files will be read directly
```

## Feature Differences

### Fully Supported

- Basic operations (Put/Get/Delete)
- WriteBatch
- Iterators (forward/reverse)
- Snapshots
- Column Families
- Transactions (optimistic)
- Merge Operators
- Compression (Snappy, Zlib)
- Bloom Filters
- DeleteRange
- MultiGet
- SingleDelete

### Not Yet Implemented

- Block cache (in-progress)
- Table cache tuning
- Prefix seek optimization
- Direct I/O
- Rate limiter
- Backup engine
- Checkpoint
- Compaction filters
- TTL

### Behavioral Differences

1. **Memory Management**: Go's GC handles all memory automatically
2. **Concurrency**: Uses Go's sync primitives instead of std::mutex
3. **Background Work**: Uses goroutines instead of background threads
4. **Error Handling**: Uses Go error values instead of Status objects

## Performance Considerations

RockyardKV typically achieves:

- **Writes**: ~290k ops/sec (sequential), ~200k ops/sec (random)
- **Reads**: ~2M ops/sec (memtable), ~500k ops/sec (SST)
- **Batches**: ~1.3M ops/sec (batch of 100)

This is competitive with C++ RocksDB for most workloads.

### Tuning for Performance

```go
opts := db.DefaultOptions()
opts.WriteBufferSize = 128 * 1024 * 1024  // Larger memtable
opts.MaxWriteBufferNumber = 4             // More buffers
opts.MaxOpenFiles = 5000                  // More file handles
```

## Migration Checklist

- [ ] Replace CGo wrapper imports with RockyardKV imports
- [ ] Update database open code
- [ ] Convert byte slices (Slice to []byte)
- [ ] Update error handling (Status to error)
- [ ] Replace iterators (ensure Close() is called)
- [ ] Update WriteBatch usage
- [ ] Migrate custom comparators (if any)
- [ ] Migrate merge operators (if any)
- [ ] Remove CGo build dependencies
- [ ] Test with existing data files
- [ ] Run benchmarks to verify performance
