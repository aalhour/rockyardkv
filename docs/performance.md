# RockyardKV Performance Tuning Guide

This guide covers performance optimization for RockyardKV, covering configuration options, profiling techniques, and best practices.

## Table of Contents

1. [Key Configuration Options](#key-configuration-options)
2. [Write Performance](#write-performance)
3. [Read Performance](#read-performance)
4. [Memory Management](#memory-management)
5. [Compaction Tuning](#compaction-tuning)
6. [Profiling](#profiling)
7. [Common Patterns](#common-patterns)

---

## Key Configuration Options

### Options Overview

```go
opts := db.DefaultOptions()

// Memory Configuration
opts.WriteBufferSize = 64 * 1024 * 1024     // 64MB memtable
opts.MaxWriteBufferNumber = 4                // Max immutable memtables

// Block/SST Configuration  
opts.BlockSize = 4096                        // 4KB blocks
opts.BlockRestartInterval = 16               // Restart every 16 keys

// Compaction Configuration
opts.Level0FileNumCompactionTrigger = 4      // Trigger at 4 L0 files
opts.MaxBytesForLevelBase = 256 * 1024 * 1024 // 256MB L1 size

// Write Stalling
opts.Level0SlowdownWritesTrigger = 20        // Slow down at 20 L0 files
opts.Level0StopWritesTrigger = 36            // Stop at 36 L0 files

// Bloom Filters
opts.BloomFilterBitsPerKey = 10              // 10 bits = ~1% FP rate
```

---

## Write Performance

### Optimal Write Configuration

For **write-heavy** workloads:

```go
opts := db.DefaultOptions()
opts.WriteBufferSize = 128 * 1024 * 1024    // Larger memtable (128MB)
opts.MaxWriteBufferNumber = 6               // More buffer space
opts.Level0FileNumCompactionTrigger = 8     // Delay compaction triggers
opts.Level0SlowdownWritesTrigger = 40       // Higher slowdown threshold
opts.Level0StopWritesTrigger = 64           // Higher stop threshold
```

### Batch Writes

**Always use batch writes for multiple operations:**

```go
// Good: Single batch
wb := batch.NewWriteBatch()
for i := 0; i < 1000; i++ {
    wb.Put(keys[i], values[i])
}
db.Write(nil, wb)  // Single WAL sync

// Bad: Individual writes
for i := 0; i < 1000; i++ {
    db.Put(nil, keys[i], values[i])  // 1000 WAL syncs!
}
```

### WAL Configuration

For maximum write throughput (with durability trade-offs):

```go
writeOpts := db.DefaultWriteOptions()
writeOpts.DisableWAL = true  // WARNING: Data loss on crash
writeOpts.Sync = false       // Default, async writes
```

For maximum durability:

```go
writeOpts.Sync = true  // Force sync on every write
```

---

## Read Performance

### Bloom Filters

Bloom filters dramatically improve point lookup performance:

```go
opts.BloomFilterBitsPerKey = 10  // 10 bits = ~1% false positive rate

// More bits = lower FP rate but more memory
// 10 bits per key: ~1% FP
// 14 bits per key: ~0.1% FP
// 20 bits per key: ~0.01% FP
```

### Block Cache (via TableCache)

The table cache reduces I/O for frequently accessed SST files:

```go
// TableCache is configured internally
// Increase max open files for better caching:
opts.MaxOpenFiles = 10000  // More open SST files
```

### Iterator Bounds

Use iterator bounds to reduce scan range:

```go
readOpts := db.DefaultReadOptions()
readOpts.IterateUpperBound = []byte("zzz")  // Stop at key "zzz"
readOpts.IterateLowerBound = []byte("aaa")  // Start at key "aaa"
```

### Prefix Seek

For prefix-based access patterns:

```go
opts.PrefixExtractor = db.NewFixedPrefixExtractor(4)  // 4-byte prefix

readOpts := db.DefaultReadOptions()
readOpts.PrefixSameAsStart = true  // Optimize for prefix iteration
```

---

## Memory Management

### Reducing Allocations

Key areas for allocation reduction:

1. **Batch Writes**: Pre-allocate batch buffers
2. **Iterators**: Reuse iterators when possible
3. **Keys/Values**: Use pooled buffers for large values

```go
// Reuse write batch
var wbPool = sync.Pool{
    New: func() interface{} {
        return batch.NewWriteBatch()
    },
}

func writeBatch(db *db.DBImpl, ops []Op) error {
    wb := wbPool.Get().(*batch.WriteBatch)
    defer func() {
        wb.Clear()
        wbPool.Put(wb)
    }()
    
    for _, op := range ops {
        wb.Put(op.Key, op.Value)
    }
    return db.Write(nil, wb)
}
```

### Memory Monitoring

Monitor memory usage:

```bash
# Run with memory profiling
./scripts/profile.sh mem

# Analyze allocations
go tool pprof -alloc_space profiles/mem.pprof
```

---

## Compaction Tuning

### Level Multiplier

Each level is approximately 10x the size of the previous:

```
Level 0: 4 files * 64MB memtable = ~256MB
Level 1: 256MB (MaxBytesForLevelBase)
Level 2: 2.56GB
Level 3: 25.6GB
...
```

### Write Stalling

Write stalling prevents compaction from falling behind:

```go
// Aggressive stalling (lower latency, more stalls)
opts.Level0SlowdownWritesTrigger = 8
opts.Level0StopWritesTrigger = 12

// Relaxed stalling (higher throughput, more L0 files)
opts.Level0SlowdownWritesTrigger = 40
opts.Level0StopWritesTrigger = 64
```

### Monitoring Compaction

Watch for these warning signs:
- L0 files > SlowdownWritesTrigger
- Frequent "Stopping writes" messages
- High write latency spikes

---

## Profiling

### Using the Profiling Script

```bash
# CPU profiling
./scripts/profile.sh cpu

# Memory profiling
./scripts/profile.sh mem

# Contention profiling
./scripts/profile.sh block

# All benchmarks
./scripts/profile.sh bench

# View profile in browser
./scripts/profile.sh analyze profiles/cpu.pprof
```

### Running Benchmarks

```bash
# All benchmarks
go test ./db/... -bench=. -benchmem -run="^$"

# Specific benchmark
go test ./db/... -bench=BenchmarkDBPut -benchmem -run="^$" -benchtime=10s

# With CPU profile
go test ./db/... -bench=BenchmarkDBPut -cpuprofile=cpu.pprof -benchtime=5s
```

### Interpreting Benchmark Results

```
BenchmarkDBPutSequential-12    321586    4342 ns/op    637 B/op    14 allocs/op
                                 ^          ^            ^           ^
                                 |          |            |           |
                            iterations   time/op    bytes/op   allocs/op
```

Target metrics:
- **Put**: < 10µs/op
- **Get**: < 1µs/op (cached)
- **Get**: < 100µs/op (from disk)

---

## Common Patterns

### Time-Series Data

For time-series workloads (sequential writes, range scans):

```go
opts := db.DefaultOptions()
opts.WriteBufferSize = 128 * 1024 * 1024  // Larger memtable
opts.BloomFilterBitsPerKey = 0             // No bloom (always scan)

// Use timestamp as key prefix for natural ordering
key := fmt.Sprintf("%020d-%s", timestamp, id)
```

### Key-Value Cache

For cache workloads (random reads, TTL-based):

```go
opts := db.DefaultOptions()
opts.BloomFilterBitsPerKey = 14            // Lower false positives
opts.MaxOpenFiles = 20000                   // More cached files

// Fast TTL check with prefix
readOpts.PrefixSameAsStart = true
```

### Queue/Log

For queue workloads (append-only, sequential reads):

```go
opts := db.DefaultOptions()
opts.WriteBufferSize = 256 * 1024 * 1024   // Very large memtable
writeOpts.Sync = false                      // Async for throughput
```

---

## Quick Reference

| Parameter | Default | Write-Heavy | Read-Heavy |
|-----------|---------|-------------|------------|
| WriteBufferSize | 64MB | 128MB+ | 64MB |
| MaxWriteBufferNumber | 2 | 4-6 | 2-3 |
| Level0FileNumCompactionTrigger | 4 | 8 | 4 |
| BloomFilterBitsPerKey | 10 | 0-10 | 14-20 |
| MaxOpenFiles | 1000 | 1000 | 10000+ |

---

## Further Reading

- [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)
- [Write Stalling](https://github.com/facebook/rocksdb/wiki/Write-Stalls)
- [Bloom Filter](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter)

