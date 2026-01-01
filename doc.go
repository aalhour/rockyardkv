/*
Package rockyardkv provides a pure-Go, RocksDB-compatible embedded durable
key/value store.

RockyardKV targets on-disk format compatibility with RocksDB v10.7.5 for
SST files, WAL, and MANIFEST. It provides an LSM-tree based storage engine
suitable for high-write workloads. It provides APIs and features that are
semantically compatible with RocksDB as well. Full API and feature parity
is underway.

# Usage

For runnable examples, see the repository's examples directory. The examples
are written against the public API and are kept up-to-date as the API evolves.

# Concurrency

A DB instance is safe for concurrent use by multiple goroutines. Individual
Iterator instances are not safe for concurrent use; each goroutine should
use its own iterator.

# Compatibility

SST files created by RockyardKV are intended to be readable by C++ RocksDB
v10.7.5 and vice versa.

Reference: RocksDB v10.7.5 include/rocksdb/db.h
*/
package rockyardkv
