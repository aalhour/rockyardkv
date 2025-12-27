# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2024-12-27

### Fixed
- MANIFEST corruption validation now rejects corrupted checksums (Issue 5+6)
- Comparator name validation on DB open prevents silent corruption (Issue 2)
- VersionEdit preserves unknown safe-to-ignore tags (Issue 1)
- Column family isolation enforced for Get and iterators (Issue 7)
- Zlib decompression supports raw deflate format (Issue 8)
- XXH3 checksum implementation replaced with zeebo/xxh3 (Issue 9)

### Added
- Strict WAL reader for MANIFEST recovery
- Adversarial tests for all fixed issues
- Golden test reorganization by category

### Changed
- Added github.com/zeebo/xxh3 dependency

## [0.1.0] - 2024-12-27

Initial release with core RocksDB functionality and v10.7.5 format compatibility.

### Added

**Core database operations**

- `Open`, `Close`, `Put`, `Get`, `Delete`, `Write`
- `MultiGet` for batch key retrieval
- `SingleDelete` for single-version deletes
- `DeleteRange` for range deletions
- `Merge` with pluggable merge operators
- Column family support (`CreateColumnFamily`, `DropColumnFamily`, `*CF` variants)
- Optimistic transactions with snapshot isolation
- Iterators with `Seek`, `SeekToFirst`, `SeekToLast`, `Next`, `Prev`
- Snapshots for consistent point-in-time reads
- Prefix seek with `PrefixExtractor`

**Storage engine**

- WAL (Write-Ahead Log) with RocksDB v10.7.5 format
- SST (Sorted String Table) with block-based tables
- MANIFEST and VersionEdit encoding
- MemTable with skip list implementation
- Leveled compaction
- Write stalling (L0 slowdown and stop triggers)
- Bloom filter support
- CRC32C, XXHash32, and XXH3 checksums
- Snappy, LZ4, and Zstd compression
- VFS (Virtual File System) abstraction
- Block cache with LRU eviction and sharding
- BlobDB for large value separation
- Direct I/O support

**Advanced features**

- TTL (time-to-live) with automatic expiration via `OpenWithTTL`
- Compaction filters for custom filtering during compaction
- Rate limiter for I/O throttling
- Secondary instances (read replicas) via `OpenAsSecondary`
- User timestamps for MVCC
- Subcompactions for parallel compaction

**Durability and backup**

- Checkpoint for point-in-time snapshots
- Backup engine with incremental support
- WAL sync options

**Testing infrastructure**

- Unit tests with table-driven patterns
- Integration tests
- Golden tests for C++ RocksDB format compatibility
- Fuzz tests for WAL, checksum, and manifest
- Stress test with expected state oracle
- Crash test with persistent state verification
- Smoke test for basic functionality
- Adversarial test for edge cases and error paths

**Documentation**

- README with quick start guide
- Integration and migration guides
- Architecture documentation
- Performance tuning guide
- Contributing guidelines

### Known limitations

The following advanced features are not yet implemented:

- Remote compaction (offloading compaction to remote workers)
- Statistics and metrics collection
- Persistent cache (block cache on SSD)
- Encryption at rest

These features may be addressed in future releases.
