# Architecture

This document describes the internal architecture of RockyardKV.

## Package structure

```
rockyardkv/
├── db/                 # Public API and database implementation
├── internal/           # Internal packages
│   ├── batch/          # Write batch encoding/decoding
│   ├── block/          # SST block reader/builder
│   ├── bloom/          # Bloom filter implementation
│   ├── cache/          # Block cache (LRU)
│   ├── checksum/       # CRC32c and XXH3 checksums
│   ├── compaction/     # Compaction picker and job execution
│   ├── compression/    # Snappy, LZ4, Zstd codecs
│   ├── dbformat/       # Key encoding and sequence numbers
│   ├── encoding/       # Varint and fixed-width encoding
│   ├── filter/         # Filter policy interface
│   ├── iterator/       # Merging iterator
│   ├── manifest/       # MANIFEST file format
│   ├── memtable/       # Skiplist-based memtable
│   ├── rangedel/       # Range deletion handling
│   ├── table/          # SST file reader/builder
│   ├── testutil/       # Test utilities
│   ├── trace/          # Operation tracing
│   ├── version/        # Version set management
│   ├── vfs/            # Virtual filesystem abstraction
│   └── wal/            # Write-ahead log
└── cmd/                # Command-line tools
```

## Storage engine

RockyardKV implements a Log-Structured Merge-tree (LSM-tree) storage engine.

### Write path

1. Writes are appended to the Write-Ahead Log (WAL)
1. Data is inserted into the active memtable
1. When the memtable reaches capacity, it becomes immutable
1. The immutable memtable is flushed to an SST file on disk

### Read path

1. Check the active memtable
1. Check immutable memtables (newest to oldest)
1. Check SST files using the version set (L0 to Lmax)
1. Use Bloom filters to skip files that don't contain the key

### Compaction

Background compaction merges SST files to:

- Reclaim space from deleted keys
- Reduce read amplification
- Maintain sorted order across levels

Supported strategies:

- Leveled compaction
- Universal compaction

## File formats

### SST files

Sorted String Table files store key-value pairs in sorted order.

Structure:

```
[data block 1]
[data block 2]
...
[data block N]
[filter block]
[index block]
[properties block]
[meta index block]
[footer]
```

### WAL files

Write-Ahead Log files store a sequence of write batches.

Each record contains:

- CRC32c checksum
- Length
- Record type
- Payload (write batch)

### MANIFEST files

MANIFEST files track the database state:

- SST file metadata
- Level assignments
- Sequence numbers
- Column family information

## Concurrency

- Multiple readers can access the database concurrently
- Writers are serialized through a write lock
- Background compaction runs in separate goroutines
- Snapshots provide consistent read views

## Memory management

- Block cache: LRU cache for frequently accessed SST blocks
- Write buffer: Configurable memtable size limit
- Write buffer manager: Controls total memory across column families

