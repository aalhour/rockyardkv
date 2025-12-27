# Compatibility

RockyardKV is bit-compatible with C++ RocksDB v10.7.5.

## Version pinning

RockyardKV is pinned to RocksDB v10.7.5 (commit 812b12b).

All on-disk formats are bit-compatible with this version.
Files created by RockyardKV can be opened by C++ RocksDB, and vice versa.

## File format compatibility

| Format | Status | Notes |
| ------ | ------ | ----- |
| SST (BlockBasedTable) | Compatible | format_version 0, 3, 4, 5, 6 |
| WAL | Compatible | RecordIO format with CRC32c checksums |
| MANIFEST | Compatible | VersionEdit encoding with unknown tag preservation |
| CURRENT | Compatible | Plain text pointer to MANIFEST |
| OPTIONS | Read-only | Parsed but not written |

## API parity

RockyardKV implements the core RocksDB API:

### Database operations

- `Open`, `Close`
- `Put`, `Get`, `Delete`, `SingleDelete`
- `DeleteRange`
- `Merge`
- `Write` (batch operations)
- `MultiGet`
- `KeyMayExist`

### Iterators

- `NewIterator`
- `Seek`, `SeekForPrev`, `SeekToFirst`, `SeekToLast`
- `Next`, `Prev`
- `Valid`, `Key`, `Value`

### Snapshots

- `GetSnapshot`
- `ReleaseSnapshot`

### Column families

- `CreateColumnFamily`
- `DropColumnFamily`
- `GetColumnFamilyHandle`
- Column-family-aware variants of all operations

### Transactions

- `BeginTransaction`
- `Commit`, `Rollback`
- `Prepare` (two-phase commit)
- Pessimistic locking

### Maintenance

- `Flush`
- `CompactRange`
- `CompactFiles`
- `WaitForCompact`
- `GetLiveFiles`, `GetLiveFilesMetaData`
- `DisableFileDeletions`, `EnableFileDeletions`
- `PauseBackgroundWork`, `ContinueBackgroundWork`

### Backup and restore

- `BackupEngine`
- `CreateNewBackup`
- `RestoreDBFromBackup`
- `PurgeOldBackups`

### Utilities

- `IngestExternalFile`
- `GetProperty`, `GetIntProperty`, `GetMapProperty`
- `GetApproximateSizes`, `GetApproximateMemTableStats`

## Advanced features

RockyardKV supports:

- BlobDB (separated values for large blobs)
- Secondary instances (read replicas via `OpenAsSecondary`)
- TTL (time-to-live with automatic expiration)
- Compaction filters (custom filtering during compaction)
- Rate limiter (I/O throttling)
- Block cache with configurable size
- Direct I/O
- User timestamps

Not yet implemented:

- Remote compaction (offloading to remote workers)
- Statistics and metrics collection
- Persistent cache (block cache on SSD)
- Encryption at rest

## Compression codecs

| Codec | Status |
| ----- | ------ |
| None | Supported |
| Snappy | Supported |
| LZ4 | Supported |
| LZ4HC | Supported |
| Zstd | Supported |
| Zlib | Supported |
| BZip2 | Not supported |

## Verify compatibility

Run the golden test suite to verify compatibility:

```bash
make test-e2e-golden
```

The tests:

1. Read SST, WAL, and MANIFEST files created by C++ RocksDB.
1. Write files and verify C++ RocksDB can read them.
1. Compare checksums and parsed values.

## Report issues

If you encounter a compatibility issue, open a GitHub issue with:

1. RocksDB version used.
1. File that failed to open or parse.
1. Error message.
1. Steps to reproduce.

