# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2025-12-29

### Added

Durability verification:
- Seqno-prefix verification mode for DisableWAL crash recovery (C02-03)
- Trace format V2 with sequence numbers for oracle-aligned verification
- Collision-check-only gate for crashtest (HARNESS-03)

Testing infrastructure:
- 83 contract tests for public APIs (Iterator, Transaction, MergeOperator, Comparator, CompactionFilter, Handler2PC, ReplayHandler)
- Fuzz corpus entries for table reader edge cases
- Memory safety limits in fuzz tests to prevent OOM

Public API:
- `db.WriteBatch` public API for batch writes
- Working examples in `examples/` using public API

Tools:
- `manifestdump` utility for MANIFEST file inspection
- `ldb checkcollision` subcommand for internal key collision detection

Documentation:
- API compatibility matrix in status docs
- Configuration reference with C++ RocksDB equivalents
- Contract test classification in testing philosophy

### Fixed
- LZ4 compression uses raw block format matching RocksDB
- Snappy prefix handling for C++ compatibility
- Internal key collision from orphaned SSTs (C02-01)
- Iterator error propagation
- Memtable log number tracking for WAL advancement
- Fuzz test infrastructure (-fuzz=Fuzz matching multiple tests)
- Hardcoded local paths removed from goldentest
- DisableWAL data loss under faultfs now allowed (HARNESS-02)

### Changed
- Trace replay applies operations in real mode
- Crashtest uses sstdump for collision checking
- Tools migrated to public WriteBatch API
- Lint exclusions added for examples directory

### Removed
- Standalone `check_collision` tool (replaced by ldb subcommand)
- Redundant `durability_contract_test.go`

## [0.1.3] - 2024-12-27

### Fixed
- Durability barrier tracking for DisableWAL mode in stresstest
- Merge operator applied during compaction

## [0.1.2] - 2024-12-27

### Fixed
- Zlib compression now uses raw deflate format matching RocksDB (Go→C++ compatibility)
- Compressed blocks include varint32 size prefix for format_version≥2 (compress_format_version=2)
- Metaindex entries sorted for Format V6+ C++ compatibility (Issue 3)
- Legacy footer encoding buffer offset math corrected (Issue 4)
- Background errors tracked instead of silent failures (Issue 10)
- Live-file APIs delegated properly in read-only and secondary modes (Issue 11)
- Flush waits for immutable memtable instead of returning error (Issue 12)
- Crash test race condition in expected state persistence

### Added
- Format V6 context checksum infrastructure
- Golden tests for zlib compression (`TestGoWritesCppReads_ZlibCompression`, `TestGoReadsZlibCompressedSST`)
- Consolidated golden tests into `cmd/goldentest/` package

### Changed
- `compression.Compress()` uses `flate.NewWriter` instead of `zlib.NewWriter` for raw deflate output
- `ExpectedStateV2.SaveToFile()` assumes pending operations will complete for crash consistency
- CI workflow and test fixtures updated

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

Core database operations:

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

Storage engine:

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

Advanced features:

- TTL (time-to-live) with automatic expiration via `OpenWithTTL`
- Compaction filters for custom filtering during compaction
- Rate limiter for I/O throttling
- Secondary instances (read replicas) via `OpenAsSecondary`
- User timestamps for MVCC
- Subcompactions for parallel compaction

Durability and backup:

- Checkpoint for point-in-time snapshots
- Backup engine with incremental support
- WAL sync options

Testing infrastructure:

- Unit tests with table-driven patterns
- Integration tests
- Golden tests for C++ RocksDB format compatibility
- Fuzz tests for WAL, checksum, and manifest
- Stress test with expected state oracle
- Crash test with persistent state verification
- Smoke test for basic functionality
- Adversarial test for edge cases and error paths

Documentation:

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
