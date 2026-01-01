# Configuration Reference

RockyardKV configuration options and their C++ RocksDB compatibility status.

**Target version:** RocksDB v10.7.5  
**Reference:** `include/rocksdb/options.h`, `include/rocksdb/advanced_options.h`

---

## Table of Contents

- [Database Options](#database-options)
- [Read Options](#read-options)
- [Write Options](#write-options)
- [Flush Options](#flush-options)
- [Compaction](#compaction)
- [Compression](#compression)
- [Checksums](#checksums)
- [BlobDB](#blobdb)
- [Transactions](#transactions)
- [SST File Ingestion](#sst-file-ingestion)
- [Rate Limiting](#rate-limiting)
- [Column Families](#column-families)
- [Compatibility Notes](#compatibility-notes)

---

## Database Options

Main options passed to `db.Open()`.

| Option | Type | Default | C++ Compatible | Description |
|--------|------|---------|----------------|-------------|
| `CreateIfMissing` | `bool` | `false` | ✅ | Create database if it doesn't exist |
| `ErrorIfExists` | `bool` | `false` | ✅ | Error if database already exists |
| `ParanoidChecks` | `bool` | `false` | ✅ | Enable additional integrity checks |
| `FS` | `vfs.FS` | OS FS | N/A | Custom filesystem (Go-specific) |
| `Comparator` | `Comparator` | Bytewise | ✅ | Key ordering comparator |
| `WriteBufferSize` | `int` | 64 MB | ✅ | Memtable size before flush |
| `MaxWriteBufferNumber` | `int` | 2 | ✅ | Max memtables in memory |
| `MaxOpenFiles` | `int` | 1000 | ✅ | Max SST file handles |
| `BlockSize` | `int` | 4 KB | ✅ | SST data block size |
| `BlockRestartInterval` | `int` | 16 | ✅ | Keys between restart points |
| `ChecksumType` | `checksum.Type` | CRC32C | ✅ | Block checksum algorithm |
| `FormatVersion` | `uint32` | 3 | ✅ | SST format version (0-6) |
| `MergeOperator` | `MergeOperator` | `nil` | ✅ | Custom merge operator |
| `PrefixExtractor` | `PrefixExtractor` | `nil` | ✅ | Prefix for bloom filters |
| `Level0FileNumCompactionTrigger` | `int` | 4 | ✅ | L0 files to trigger compaction |
| `MaxBytesForLevelBase` | `int64` | 256 MB | ✅ | Max size for L1 |
| `BloomFilterBitsPerKey` | `int` | 10 | ✅ | Bloom filter bits (0 = disabled) |
| `Level0SlowdownWritesTrigger` | `int` | 20 | ✅ | L0 files to slow writes |
| `Level0StopWritesTrigger` | `int` | 36 | ✅ | L0 files to stop writes |
| `DisableAutoCompactions` | `bool` | `false` | ✅ | Disable background compaction |
| `CompactionFilter` | `CompactionFilter` | `nil` | ✅ | Per-key compaction filter |
| `CompactionFilterFactory` | `CompactionFilterFactory` | `nil` | ✅ | Filter factory |
| `CompactionStyle` | `CompactionStyle` | Level | ✅ | Compaction strategy |
| `Compression` | `CompressionType` | None | ✅ | SST block compression |
| `MaxSubcompactions` | `int` | 1 | ✅ | Parallel subcompactions |
| `UseDirectReads` | `bool` | `false` | ✅ | O_DIRECT for reads |
| `UseDirectIOForFlushAndCompaction` | `bool` | `false` | ✅ | O_DIRECT for background I/O |
| `Logger` | `Logger` | stderr | N/A | Log interface (Go-specific) |
| `RateLimiter` | `RateLimiter` | `nil` | ✅ | I/O rate limiter |

### Usage

```go
opts := db.DefaultOptions()
opts.CreateIfMissing = true
opts.WriteBufferSize = 128 * 1024 * 1024 // 128 MB
opts.Compression = db.LZ4Compression

database, err := db.Open("/path/to/db", opts)
```

---

## Read Options

Options for `Get()`, `NewIterator()`, and related operations.

| Option | Type | Default | C++ Compatible | Description |
|--------|------|---------|----------------|-------------|
| `VerifyChecksums` | `bool` | `true` | ✅ | Verify block checksums |
| `FillCache` | `bool` | `true` | ✅ | Populate block cache |
| `Snapshot` | `*Snapshot` | `nil` | ✅ | Read from snapshot |
| `Timestamp` | `[]byte` | `nil` | ✅ | Upper bound timestamp |
| `IterStartTimestamp` | `[]byte` | `nil` | ✅ | Lower bound timestamp |
| `TotalOrderSeek` | `bool` | `false` | ✅ | Bypass prefix bloom |
| `PrefixSameAsStart` | `bool` | `false` | ✅ | Optimize same-prefix iteration |
| `IterateUpperBound` | `[]byte` | `nil` | ✅ | Stop iteration at key |
| `IterateLowerBound` | `[]byte` | `nil` | ✅ | Start iteration at key |

### Usage

```go
ro := db.DefaultReadOptions()
ro.VerifyChecksums = true
ro.FillCache = false  // For large scans

value, err := database.Get(ro, key)
```

---

## Write Options

Options for `Put()`, `Delete()`, and `Write()`.

| Option | Type | Default | C++ Compatible | Description |
|--------|------|---------|----------------|-------------|
| `Sync` | `bool` | `false` | ✅ | Fsync WAL before returning |
| `DisableWAL` | `bool` | `false` | ✅ | Skip WAL (data loss on crash) |

### Durability Semantics

| Configuration | Durability | Performance |
|--------------|------------|-------------|
| `Sync=false, DisableWAL=false` | Data survives crash (WAL recovery) | Default |
| `Sync=true, DisableWAL=false` | Data survives crash + power loss | Slowest |
| `Sync=false, DisableWAL=true` | **Data lost on crash** until `Flush()` | Fastest |

> ⚠️ **Warning:** With `DisableWAL=true`, call `Flush()` before shutdown to persist data.

### Usage

```go
wo := db.DefaultWriteOptions()
wo.Sync = true  // Maximum durability

err := database.Put(wo, key, value)
```

---

## Flush Options

Options for `Flush()`.

| Option | Type | Default | C++ Compatible | Description |
|--------|------|---------|----------------|-------------|
| `Wait` | `bool` | `true` | ✅ | Block until flush completes |
| `AllowWriteStall` | `bool` | `false` | ✅ | Allow stall during flush |

---

## Compaction

### Compaction Styles

| Style | Constant | Best For | Write Amp | Read Amp | Space Amp |
|-------|----------|----------|-----------|----------|-----------|
| Leveled | `CompactionStyleLevel` | Read-heavy | Higher | Lower | Lower |
| Universal | `CompactionStyleUniversal` | Write-heavy | Lower | Higher | Higher |
| FIFO | `CompactionStyleFIFO` | Time-series | Lowest | N/A | Lowest |

### Universal Compaction Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `SizeRatio` | `int` | 1 | Percentage trigger for size ratio |
| `MinMergeWidth` | `int` | 2 | Minimum files to merge |
| `MaxMergeWidth` | `int` | INT_MAX | Maximum files to merge |
| `MaxSizeAmplificationPercent` | `int` | 200 | Trigger full compaction |
| `AllowTrivialMove` | `bool` | `false` | Allow trivial moves |

### FIFO Compaction Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `MaxTableFilesSize` | `uint64` | 1 GB | Max total size before deletion |
| `TTL` | `time.Duration` | 0 | Time-to-live (0 = disabled) |
| `AllowCompaction` | `bool` | `false` | Allow intra-L0 compaction |

### Usage

```go
opts := db.DefaultOptions()
opts.CompactionStyle = db.CompactionStyleUniversal
opts.UniversalCompactionOptions = &db.UniversalCompactionOptions{
    SizeRatio: 10,
    MinMergeWidth: 2,
}
```

---

## Compression

### Supported Types

| Type | Constant | C++ Compatible | Notes |
|------|----------|----------------|-------|
| None | `NoCompression` | ✅ | No compression |
| Snappy | `SnappyCompression` | ✅ | Fast, moderate ratio |
| Zlib | `ZlibCompression` | ✅ | Slower, good ratio |
| LZ4 | `LZ4Compression` | ✅ | Very fast, good ratio |
| LZ4HC | `LZ4HCCompression` | ✅ | LZ4 high compression |
| ZSTD | `ZstdCompression` | ✅ | Best ratio, fast decompression |

### Not Implemented

| Type | Constant | Reason |
|------|----------|--------|
| BZip2 | `BZip2Compression` | Rarely used |
| Xpress | `XpressCompression` | Windows-specific |

### Compression Format Notes

- **Snappy:** Embeds uncompressed size internally (no external prefix)
- **LZ4/ZSTD:** Raw block format with varint32 size prefix (format_version ≥ 2)
- All compression is verified against C++ `sst_dump` for compatibility

### Usage

```go
opts := db.DefaultOptions()
opts.Compression = db.LZ4Compression
```

---

## Checksums

### Supported Types

| Type | Constant | Default | Notes |
|------|----------|---------|-------|
| None | `TypeNoChecksum` | | No verification |
| CRC32C | `TypeCRC32C` | ✅ | Default, hardware-accelerated |
| XXHash | `TypeXXHash` | | XXHash32 |
| XXHash64 | `TypeXXHash64` | | XXHash64 |
| XXH3 | `TypeXXH3` | | format_version 5+ |

### Usage

```go
opts := db.DefaultOptions()
opts.ChecksumType = checksum.TypeCRC32C
```

---

## BlobDB

Integrated blob storage for large values (reduces write amplification).

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Enable` | `bool` | `false` | Enable BlobDB |
| `MinBlobSize` | `int` | 4 KB | Threshold for blob storage |
| `BlobFileSize` | `int64` | 256 MB | Target blob file size |
| `BlobCompressionType` | `compression.Type` | None | Blob compression |
| `EnableBlobGC` | `bool` | `true` | Enable blob garbage collection |
| `BlobGCAgeCutoff` | `float64` | 0.25 | Age cutoff for GC (0.0–1.0) |

---

## Transactions

### TransactionDB Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `MaxNumLocks` | `uint64` | 0 | Max locks (0 = unlimited) |
| `NumStripes` | `int` | 16 | Lock striping |
| `TransactionLockTimeout` | `int64` | 5000 ms | Default lock timeout |

### Transaction Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `SetSnapshot` | `bool` | `false` | Set snapshot at creation |
| `Deadlock Detection` | `bool` | `true` | Enable deadlock detection |
| `LockTimeout` | `int64` | 5000 ms | Per-operation timeout |
| `Expiration` | `int64` | 0 | Transaction expiration |

### Usage

```go
txnDB, err := db.OpenTransactionDB(path, opts, db.DefaultTransactionDBOptions())

txn := txnDB.BeginTransaction(db.DefaultWriteOptions(), db.DefaultTransactionOptions())
txn.Put([]byte("key"), []byte("value"))
err = txn.Commit()
```

---

## SST File Ingestion

External SST files created by `SstFileWriter` can be ingested into the database.

### Ingestion Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `MoveFiles` | `bool` | `false` | Move vs. copy files |
| `SnapshotConsistency` | `bool` | `true` | Hide from existing snapshots |
| `AllowGlobalSeqNo` | `bool` | `true` | Assign global sequence numbers |
| `AllowBlockingFlush` | `bool` | `true` | Flush memtable if overlap |
| `IngestBehind` | `bool` | `false` | Skip duplicates, ingest at bottom |
| `FailIfNotBottommostLevel` | `bool` | `false` | Require bottommost placement |
| `VerifyChecksumsBeforeIngest` | `bool` | `false` | Verify checksums before ingesting |

### Usage

```go
writer, err := db.NewSstFileWriter(db.DefaultSstFileWriterOptions())
writer.Open("/tmp/data.sst")
writer.Put(key1, value1)
writer.Put(key2, value2)
writer.Finish()

err = database.IngestExternalFile(
    []string{"/tmp/data.sst"},
    db.DefaultIngestExternalFileOptions(),
)
```

---

## Rate Limiting

Control I/O rate for background operations.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `BytesPerSecond` | `int64` | - | Max bytes per second |
| `RefillPeriodMicros` | `int64` | 100000 | Token refill period |
| `Fairness` | `int64` | 10 | Fairness factor |
| `Mode` | `RateLimiterMode` | WritesOnly | Rate limit scope |
| `SingleBurstBytes` | `int64` | - | Single burst limit |

---

## Column Families

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Comparator` | `Comparator` | DB default | Per-CF key ordering |
| `MergeOperator` | `MergeOperator` | `nil` | Per-CF merge operator |
| `CompactionFilter` | `CompactionFilter` | `nil` | Per-CF compaction filter |
| `Compression` | `CompressionType` | DB default | Per-CF compression |

---

## Compatibility Notes

### Format Versions

| Version | Features | Notes |
|---------|----------|-------|
| 0 | Legacy format | Fully compatible |
| 3 | Default for Go | Recommended |
| 4 | Index value delta encoding | ⚠️ T04.1 pending |
| 5 | XXH3 checksums | Fully compatible |
| 6 | Footer checksum context | ⚠️ T04.2 pending |

### Not Yet Implemented

| Feature | Status | Reference |
|---------|--------|-----------|
| WAL rotation on memtable switch | Design decision | Single WAL per session |
| Column family recovery | TODO | T01.3 |
| Index block value delta encoding | Pending | T04.1 |
| Footer checksum context (v6+) | Pending | T04.2 |
| BlobCache | Blocked | T05.4 |

### Differences from C++ RocksDB

| Behavior | C++ RocksDB | RockyardKV | Notes |
|----------|-------------|------------|-------|
| WAL rotation | Per memtable | Per DB session | Architectural |
| Logger interface | InfoLog* | `Logger` interface | Go-specific |
| Filesystem | Env* | `vfs.FS` | Go-specific |
| Memory allocation | Custom allocators | Go GC | Go runtime |

### Durability Guarantees

RockyardKV matches C++ RocksDB durability semantics:

- ✅ Acknowledged writes (WAL enabled) survive crashes
- ✅ Orphaned SST files are cleaned on recovery
- ✅ MANIFEST LastSequence is monotonic
- ⚠️ `DisableWAL=true` loses data until `Flush()` — this is expected behavior

See `docs/status/durability_report.md` for detailed durability verification status.

---

## Quick Reference

### Minimal Configuration

```go
opts := db.DefaultOptions()
opts.CreateIfMissing = true
db, err := db.Open("/path/to/db", opts)
```

### High-Throughput Writes

```go
opts := db.DefaultOptions()
opts.CreateIfMissing = true
opts.WriteBufferSize = 256 * 1024 * 1024     // 256 MB
opts.MaxWriteBufferNumber = 4
opts.DisableAutoCompactions = false
opts.CompactionStyle = db.CompactionStyleUniversal
opts.Compression = db.LZ4Compression
```

### Read-Heavy Workload

```go
opts := db.DefaultOptions()
opts.CreateIfMissing = true
opts.BloomFilterBitsPerKey = 10
opts.CompactionStyle = db.CompactionStyleLevel
opts.MaxBytesForLevelBase = 512 * 1024 * 1024
```

### Maximum Durability

```go
opts := db.DefaultOptions()
opts.CreateIfMissing = true
opts.ParanoidChecks = true

wo := db.DefaultWriteOptions()
wo.Sync = true
```

