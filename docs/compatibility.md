# Compatibility

This document describes RockyardKV's compatibility with C++ RocksDB.

## Version pinning

RockyardKV is pinned to RocksDB v10.7.5 (commit 812b12b).

All on-disk formats are bit-compatible with this version.
Files created by RockyardKV can be opened by C++ RocksDB, and vice versa.

## File format compatibility

| Format | Status | Notes |
| ------ | ------ | ----- |
| SST (BlockBasedTable) | Compatible | Full support for format_version 5 |
| WAL | Compatible | RecordIO format with CRC32c |
| MANIFEST | Compatible | VersionEdit encoding |
| CURRENT | Compatible | Plain text pointer to MANIFEST |
| OPTIONS | Read-only | Options files are parsed but not written |

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

The following features are implemented:

- BlobDB (separated values for large blobs)
- Secondary instances (read replicas via `OpenAsSecondary`)
- TTL (time-to-live with automatic expiration)
- Compaction filters (custom filtering during compaction)
- Rate limiter (I/O throttling)
- Block cache with configurable size
- Direct I/O support
- User timestamps

The following advanced features are not yet implemented:

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
| Zlib | Not supported |
| BZip2 | Not supported |

## Verifying compatibility

The golden test suite verifies compatibility by:

1. Reading SST, WAL, and MANIFEST files created by C++ RocksDB
1. Writing files and verifying C++ RocksDB can read them
1. Comparing checksums and parsed values

Run compatibility tests:

```bash
make test-e2e-golden
```

## Reporting issues

If you encounter a compatibility issue, open a GitHub issue with:

1. RocksDB version used
1. File that failed to open or parse
1. Error message
1. Steps to reproduce

