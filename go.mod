module github.com/aalhour/rockyardkv

// This implementation is pinned to RocksDB v10.7.5 (commit 812b12b)
// All on-disk formats must be bit-compatible with this version.

go 1.25

require (
	github.com/golang/snappy v1.0.0
	github.com/klauspost/compress v1.18.2
	github.com/pierrec/lz4/v4 v4.1.23
)
