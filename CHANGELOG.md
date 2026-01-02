# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.4] - 2026-01-02

### Changed

Package restructure (Wave 1-4 complete):
- Promote `internal/vfs/` to public `vfs/` package
- Move `LockManager`, `recovery_2pc.go` to `internal/txn/`
- Move `FlushJob` to `internal/flush/`
- Move `parsedOptions` to `internal/options/`
- Move `bufferPool` to `internal/mempool/`
- Add public type aliases for `ChecksumType`, `CompressionType`, `LockManagerOptions`
- Add capability interfaces: `ReadOnlyDB`, `SecondaryDB`, `ReplicationDB`, `WriteStallController`

Documentation:
- Update all docs to use `rockyardkv.*` instead of `db.*`
- Update all docs to use public `vfs/` instead of `internal/vfs/`
- Add test coverage report (`docs-internal/TEST_COVERAGE_REPORT.md`)

### Added

Tooling:
- `cmd/apileakcheck` — AST-based API leak detection tool
- `make check-api-leaks` target integrated into `make check`

### Fixed
- Linter issues in `cmd/apileakcheck/main.go` (wastedassign)
- Formatting issues in `cmd/stresstest/main_test.go`

## [0.3.3] - 2026-01-01

### Fixed
- Package docs now render correctly on pkg.go.dev/pkgsite (package comment lives in `doc.go`, and file header comments no longer steal the overview).

### Added
- Runnable pkg.go.dev example (`ExampleOpen`).

## [0.3.2] - 2026-01-01

### Changed

Package structure:
- Move `db/*` to module root for cleaner import paths (`github.com/aalhour/rockyardkv`)
- Reorganize internal docs to `docs-internal/`

### Fixed
- Clean up `.gitignore` for campaign artifacts

## [0.3.1] - 2026-01-01

### Added

Logging:
- Production logging interface with `Errorf`, `Warnf`, `Infof`, `Debugf`, `Fatalf`
- RocksDB-style `Fatalf` behavior (sets DB to stopped state, rejects writes, allows reads)
- Stateless `DefaultLogger` with configurable level threshold
- `ErrFatal` sentinel error for `errors.Is()` checks

Campaign runner (C05):
- Jepsen-style nightly runner with oracle gating
- Campaign taxonomy (Tier, Tool, FaultKind, FaultScope, FaultErrorType)
- Fixed instance matrix with reproducible seeds
- Artifact bundling with `run.json`, logs, and oracle outputs
- Failure fingerprinting and known-failure quarantine
- Composite instances for multi-step workflows
- Sweep instances for parameter matrix expansion
- Registry introspection with tags and filters
- Schema versioning for `run.json` and `summary.json`
- Artifact recheck mode for policy re-evaluation
- Governance with `-require-quarantine` flag
- Instance-level skip policies
- Trace capture and minimization for stresstest failures

Testing:
- File-level header documentation for `internal/campaign/`
- RocksDB testdata fixtures for golden tests
- Column family adversarial tests refactored with `t.TempDir()`
- Edge case tests for trace writer

### Fixed
- Goroutine-local fault injection hang in stresstest
- Golden tests updated for RocksDB v10.7.5 compatibility

## [0.3.0] - 2025-12-30

### Added

VFS fault injection:
- Goroutine-local fault injection harness for concurrent tests
- Sync/rename lie modes for durability testing
- Directory sync anomaly injection

Testing infrastructure:
- Syncpoint-driven whitebox crash tests (UC-06)
- MANIFEST corruption adversarial tests (UC.T6)
- Durability scenarios for C04 VFS tasks
- Collision-check gate for seqno tests (UC-10)
- Seqno-domain alignment test (C02-02)
- Bloom filter compatibility golden test

Tools:
- Partitioned index unsupported check in SST reader (S05.4)
- Version-aware trace decoder for v2 traces

Documentation:
- Jepsen-style testing compatibility matrix
- Fault injection documentation
- Multi-distro Docker test images

### Fixed
- Syncpoint scenario tests for missing call sites (UC.T5)
- Traceanalyzer uses version-aware decoder
- ASCII art box border alignment in test harnesses
- Internal tracker references removed from code comments
- Local paths replaced with `$ROCKSDB_PATH` placeholders

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
