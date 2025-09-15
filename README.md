
# RockyardKV

A pure Go implementation of RocksDB v10.7.5 with bit-compatible on-disk formats and full API parity.

## Overview

RockyardKV lets you read and write RocksDB databases from Go without CGo or C++ dependencies.
The implementation maintains compatibility with RocksDB v10.7.5 at both the file format and API levels.

This project exists in respect and alignment with [RocksDB](https://github.com/facebook/rocksdb), the foundational storage engine that inspired this work.

## Features

- Bit-compatible SST, WAL, and MANIFEST file formats
- LSM-tree storage with leveled and universal compaction
- Column families for data partitioning
- Snapshots and iterators with MVCC isolation
- Merge operators for incremental updates
- Range deletions with efficient tombstone handling
- Bloom filters for read optimization
- Snappy, LZ4, and Zstd compression
- Pessimistic transactions with two-phase commit
- Backup and checkpoint APIs

## Installation

```bash
go get github.com/aalhour/rockyardkv
```

Requires Go 1.21.5 or later.

## Quick start

Open a database and perform basic operations:

```go
package main

import (
    "log"

    "github.com/aalhour/rockyardkv/db"
)

func main() {
    opts := db.DefaultOptions()
    opts.CreateIfMissing = true

    database, err := db.Open("/tmp/mydb", opts)
    if err != nil {
        log.Fatal(err)
    }
    defer database.Close()

    // Write a key-value pair
    err = database.Put(db.DefaultWriteOptions(), []byte("key"), []byte("value"))
    if err != nil {
        log.Fatal(err)
    }

    // Read the value back
    value, err := database.Get(db.DefaultReadOptions(), []byte("key"))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("value: %s", value)
}
```

## Documentation

Refer to the [docs](docs/) directory for detailed guides:

- [Integration guide](docs/integration.md) - Add RockyardKV to your application
- [Migration guide](docs/migration.md) - Migrate from C++ RocksDB or CGo wrappers
- [Architecture](docs/architecture.md) - Internal design and package structure
- [Compatibility](docs/compatibility.md) - RocksDB format and API compatibility
- [Performance tuning](docs/performance.md) - Optimize for your workload
- [Testing](docs/testing.md) - Run and extend the test suite

## Command-line tools

The `cmd/` directory contains utilities for database inspection and testing.
Refer to [cmd/README.md](cmd/README.md) for details.

## Compatibility

RockyardKV targets RocksDB v10.7.5 (commit 812b12b).
Files created by RockyardKV can be read by C++ RocksDB, and vice versa.

The project maintains backward compatibility with this version and tracks upstream changes.

## Benchmarks

Refer to [docs/benchmarks.md](docs/benchmarks.md) for performance measurements.

## Contributing

Refer to [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

Apache 2.0.
Refer to [LICENSE](LICENSE) for the full text.
