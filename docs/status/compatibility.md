# Compatibility

RockyardKV targets bit-compatibility with C++ RocksDB v10.7.5.
This document describes compatibility claims and how to verify them.

## Summary

| Item | Value |
|------|-------|
| Target version | RocksDB v10.7.5 (commit 812b12b) |
| Project version | v0.1.x |
| Status | File format compatibility verified; durability under investigation |

RockyardKV reads files created by C++ RocksDB and C++ RocksDB reads files created by RockyardKV.

## File format compatibility

| Format | Status | Notes |
| ------ | ------ | ----- |
| SST (BlockBasedTable) | Compatible | `format_version` 0, 3, 4, 5, 6 |
| WAL | Compatible | RecordIO format with CRC32c checksums |
| MANIFEST | Compatible | VersionEdit encoding with unknown tag preservation |
| CURRENT | Compatible | Plain text pointer to MANIFEST |
| OPTIONS | Read-only | Parsed but not written |

## Durability guarantees

RockyardKV durability semantics are under active verification.  
Refer to `docs/status/durability_report.md` for the current status and known limitations.

### With WAL enabled (default)

Acknowledged writes should survive crashes.  
The WAL records writes before acknowledgment.  
Recovery replays the WAL to restore committed data.

### With WAL disabled

**Data loss is expected after a crash.**

When you set `WriteOptions.DisableWAL = true`, writes go directly to the memtable.  
If the process crashes before a flush, unflushed writes are lost.

### Sync writes

Set `WriteOptions.Sync = true` to force an `fsync` after each write.  
This provides the strongest durability but reduces throughput.

## Verify compatibility

Run the golden test suite to verify compatibility:

```bash
make test-e2e-golden
```

## Compatibility matrix

This section summarizes compatibility status and how to verify it.

### What “compatible” means

Compatibility means:

- RockyardKV reads files created by C++ RocksDB v10.7.5.
- C++ RocksDB v10.7.5 reads files created by RockyardKV.
- Golden tests lock this behavior with fixtures and oracle tooling.

### Verification matrix

| Area | Claim | Verification | Location |
|------|-------|--------------|----------|
| SST | Read and write compatible with RocksDB v10.7.5 | Golden tests + C++ `sst_dump` | `cmd/goldentest/`, `docs/testing/GOLDEN.md` |
| WAL | Read and write compatible with RocksDB v10.7.5 | Golden tests + C++ `ldb dump_wal` | `cmd/goldentest/`, `docs/testing/GOLDEN.md` |
| MANIFEST | Read and write compatible with RocksDB v10.7.5 | Golden tests + C++ `ldb manifest_dump` | `cmd/goldentest/`, `docs/testing/GOLDEN.md` |
| CURRENT | Read and write compatible with RocksDB v10.7.5 | Golden tests | `cmd/goldentest/`, `docs/testing/GOLDEN.md` |
| Durability semantics | Verified crash durability contracts | Crash tests + whitebox tests | `docs/status/durability_report.md`, `docs/testing/WHITEBOX.md` |
