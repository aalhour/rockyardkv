# Benchmark results

Benchmark results for RockyardKV on Apple Silicon hardware.

**Environment:**

- Date: 2024-12-30
- Go version: go1.25 (darwin/arm64)
- Hardware: Apple M2 Max, 12 cores
- Tool: `go test -bench=. -benchmem`

## Summary

| Operation | Throughput | Latency | Notes |
|-----------|------------|---------|-------|
| Sequential Put | 224k ops/sec | 5.0 us/op | 100-byte values |
| Random Put | 216k ops/sec | 7.8 us/op | 100-byte values |
| Get (memtable) | 1.5M ops/sec | 0.8 us/op | Hot path |
| Get (concurrent) | 5.9M ops/sec | 199 ns/op | 12 readers |
| MultiGet (100 keys) | 18k batches/sec | 65 us/batch | 1.5M keys/sec effective |
| Batch Write (100 ops) | 10k batches/sec | 111 us/batch | 1M ops/sec effective |
| Delete | 128k ops/sec | 9.5 us/op | Point deletes |
| DeleteRange (100 keys) | 2.5k ops/sec | 497 us/op | Range tombstone |
| Merge | 249k ops/sec | 5.8 us/op | Counter increment |
| Iterator Seek | 316k ops/sec | 3.6 us/op | Point seeks |
| Iterator Scan (10k keys) | 862 scans/sec | 1.4 ms/scan | Full iteration |
| Transaction | 176k ops/sec | 8.2 us/op | Read-modify-write |
| Snapshot Create | 1.8M ops/sec | 636 ns/op | Lock-free read |
| DB Open | 56 opens/sec | 21.5 ms/op | With 10k keys, 1 SST (includes fsync) |

## Detailed results

### Single-key operations

```
BenchmarkDBPutSequential-12      223,992 ops    5,035 ns/op    613 B/op    13 allocs/op
BenchmarkDBPutRandom-12          216,463 ops    7,824 ns/op    613 B/op    13 allocs/op
BenchmarkDBGet-12              1,490,748 ops      804 ns/op    175 B/op     3 allocs/op
```

### Batch operations

```
BenchmarkBatchWrite/size_10-12       70,178 batches   15,857 ns/op    7,126 B/op    84 allocs/op
BenchmarkBatchWrite/size_100-12      10,000 batches  111,088 ns/op   81,373 B/op   751 allocs/op
BenchmarkBatchWrite/size_1000-12      1,240 batches 1,079,813 ns/op 856,860 B/op 7,358 allocs/op
```

### MultiGet (batch reads)

```
BenchmarkMultiGet/keys_10-12       208,371 ops    5,686 ns/op    1,840 B/op    22 allocs/op
BenchmarkMultiGet/keys_100-12       18,498 ops   64,792 ns/op   18,880 B/op   202 allocs/op
```

### Delete operations

```
BenchmarkDelete-12            128,266 ops    9,514 ns/op      946 B/op    23 allocs/op
BenchmarkDeleteRange-12         2,497 ops  497,042 ns/op   61,779 B/op 1,346 allocs/op
```

### Merge operations

```
BenchmarkMerge-12             249,471 ops    5,827 ns/op      365 B/op    12 allocs/op
```

### Iterator operations

```
BenchmarkIteratorSeek-12      316,081 ops    3,569 ns/op    5,314 B/op    11 allocs/op
BenchmarkIteratorScan-12          862 ops  1,364,953 ns/op  1,360,758 B/op  20,009 allocs/op
```

### Transaction operations

```
BenchmarkTransaction-12            175,963 ops    8,212 ns/op    1,585 B/op    26 allocs/op
BenchmarkTransactionConflict-12    157,603 ops    7,490 ns/op    1,287 B/op    21 allocs/op
```

### Database lifecycle

```
BenchmarkDBOpen-12                  56 ops 21,500,000 ns/op  5,892,846 B/op  84,273 allocs/op
BenchmarkFlush-12                   69 ops 22,521,290 ns/op  2,108,562 B/op  15,214 allocs/op
```

Note: DBOpen latency increased from 6.5ms to 21.5ms due to durability fixes that add fsync calls during MANIFEST and CURRENT file writes.
This ensures crash safety but increases open time.

### Concurrent operations

```
BenchmarkConcurrentPut-12        163,128 ops    7,822 ns/op    685 B/op    14 allocs/op
BenchmarkConcurrentGet-12      5,949,781 ops      199 ns/op    175 B/op     3 allocs/op
```

### Value size scaling

```
BenchmarkValueSizes/value_100-12       19,933 ops   57,942 ns/op    1.73 MB/s
BenchmarkValueSizes/value_1024-12      15,559 ops   86,115 ns/op   11.89 MB/s
BenchmarkValueSizes/value_10240-12      5,689 ops  370,539 ns/op   27.64 MB/s
BenchmarkValueSizes/value_102400-12     7,830 ops  141,059 ns/op  725.94 MB/s
```

### Snapshots and mixed workloads

```
BenchmarkSnapshot-12        1,812,862 ops        636 ns/op        350 B/op       5 allocs/op
BenchmarkMixedWorkload-12      45,285 ops     28,462 ns/op        355 B/op       8 allocs/op
```

## Performance analysis

### Strengths

**Read performance.**
Get operations complete in approximately 1 us with 3 allocations per operation.

**Concurrent reads.**
Read throughput scales with the number of readers, reaching 5.3M ops/sec with 12 concurrent readers.

**Batch efficiency.**
Amortized cost per key decreases with batch size.
A batch of 100 operations achieves approximately 1M effective ops/sec.

**Large value throughput.**
Writes of 100 KB values achieve over 700 MB/s throughput.

### Improvement opportunities

**Write path allocations.**
The current implementation allocates 13 objects per Put operation.
Object pooling can reduce this.

**Iterator allocations.**
Full scans allocate 20k objects.
An arena allocator can reduce GC pressure.

**Batch allocations.**
Allocations scale linearly with batch size (7.3k allocations for 1000 keys).

## C++ RocksDB comparison

For reference, C++ RocksDB typically achieves:

| Operation | C++ RocksDB | RockyardKV |
|-----------|-------------|---------------|
| Random writes | 200-400k ops/sec | 200k ops/sec |
| Random reads | 300-600k ops/sec | 1M ops/sec (memtable) |
| Batch writes | 1-2M ops/sec | 1M ops/sec |
| MultiGet | 1-3M keys/sec | 1.5M keys/sec |
| Merge | 200-500k ops/sec | 250k ops/sec |
| Iterator Seek | 200-500k ops/sec | 316k ops/sec |
| Transaction | 50-200k ops/sec | 176k ops/sec |

RockyardKV achieves competitive performance for a pure Go implementation.
Read performance is particularly strong due to efficient memtable lookup and optimized concurrent access patterns.

## Run benchmarks

Run all benchmarks:

```bash
make test-bench
```

Run DB benchmarks only:

```bash
make test-bench-db
```

Run with profiling:

```bash
go test ./db/... -bench=BenchmarkDBPut -cpuprofile=cpu.prof -memprofile=mem.prof
```

Generate a flame graph:

```bash
go tool pprof -http=:8080 cpu.prof
```

## Optimization opportunities

1. **Object pooling.** Pool frequently allocated objects like `WriteBatch` and iterators.
1. **Arena allocator.** Use arena allocation for iterator scans to reduce GC pressure.
1. **Lock-free memtable.** Consider a lock-free skip list implementation.
1. **Compression tuning.** Profile Snappy, LZ4, and Zstd for different workloads.
1. **Block cache.** Add block cache for SST file reads.

