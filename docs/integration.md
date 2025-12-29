# RockyardKV Integration Guide

This guide explains how to integrate RockyardKV into your Go application.

## Installation

```bash
go get github.com/aalhour/rockyardkv
```

## Basic Usage

### Opening a Database

```go
package main

import (
    "log"
    
    "github.com/aalhour/rockyardkv/db"
)

func main() {
    // Configure options
    opts := db.DefaultOptions()
    opts.CreateIfMissing = true  // Create if it doesn't exist
    
    // Open the database
    database, err := db.Open("/path/to/mydb", opts)
    if err != nil {
        log.Fatal(err)
    }
    defer database.Close()
    
    // Use the database...
}
```

### CRUD Operations

```go
// Write a key-value pair
err := database.Put(nil, []byte("user:1"), []byte(`{"name": "Alice"}`))
if err != nil {
    log.Fatal(err)
}

// Read a value
value, err := database.Get(nil, []byte("user:1"))
if err == db.ErrNotFound {
    log.Println("Key not found")
} else if err != nil {
    log.Fatal(err)
} else {
    log.Printf("Value: %s", value)
}

// Delete a key
err = database.Delete(nil, []byte("user:1"))
if err != nil {
    log.Fatal(err)
}
```

### Batch Writes

For atomic multi-key operations:

```go
wb := db.NewWriteBatch()
wb.Put([]byte("key1"), []byte("value1"))
wb.Put([]byte("key2"), []byte("value2"))
wb.Delete([]byte("key3"))

err := database.Write(nil, wb)
if err != nil {
    log.Fatal(err)
}

// Reuse the batch
wb.Clear()
```

### Iteration

```go
iter := database.NewIterator(nil)
defer iter.Close()

// Full scan
for iter.SeekToFirst(); iter.Valid(); iter.Next() {
    log.Printf("Key: %s, Value: %s", iter.Key(), iter.Value())
}

// Prefix scan
prefix := []byte("user:")
for iter.Seek(prefix); iter.Valid(); iter.Next() {
    key := iter.Key()
    if !bytes.HasPrefix(key, prefix) {
        break
    }
    log.Printf("Key: %s", key)
}

// Reverse iteration
for iter.SeekToLast(); iter.Valid(); iter.Prev() {
    log.Printf("Key: %s", iter.Key())
}
```

### Snapshots

For consistent reads:

```go
snapshot := database.GetSnapshot()
defer database.ReleaseSnapshot(snapshot)

opts := db.DefaultReadOptions()
opts.Snapshot = snapshot

// All reads will see data as of snapshot time
value1, _ := database.Get(opts, []byte("key1"))
value2, _ := database.Get(opts, []byte("key2"))
```

### Column Families

For data partitioning:

```go
// Create a column family
cf, err := database.CreateColumnFamily(db.ColumnFamilyOptions{}, "logs")
if err != nil {
    log.Fatal(err)
}

// Write to column family
err = database.PutCF(nil, cf, []byte("log:1"), []byte("message"))

// Read from column family
value, err := database.GetCF(nil, cf, []byte("log:1"))

// Iterate column family
iter := database.NewIteratorCF(nil, cf)
defer iter.Close()

// Drop column family (when no longer needed)
err = database.DropColumnFamily(cf)
```

### Transactions

For read-modify-write operations:

```go
txn := database.BeginTransaction(db.TransactionOptions{}, nil)
defer txn.Rollback() // No-op if committed

// Read current value
value, err := txn.Get([]byte("counter"))
if err != nil && err != db.ErrNotFound {
    log.Fatal(err)
}

// Modify
counter := parseCounter(value) + 1
txn.Put([]byte("counter"), []byte(strconv.Itoa(counter)))

// Commit (fails if key was modified by another writer)
if err := txn.Commit(); err != nil {
    log.Printf("Transaction conflict: %v", err)
}
```

### Merge Operations

For incremental updates:

```go
// Configure merge operator
opts := db.DefaultOptions()
opts.CreateIfMissing = true
opts.MergeOperator = &db.UInt64AddOperator{} // Built-in counter

database, _ := db.Open("/path/to/db", opts)
defer database.Close()

// Initialize counter
database.Put(nil, []byte("views"), db.EncodeUint64(0))

// Increment counter (no read required)
database.Merge(nil, []byte("views"), db.EncodeUint64(1))
database.Merge(nil, []byte("views"), db.EncodeUint64(1))
database.Merge(nil, []byte("views"), db.EncodeUint64(1))

// Read final value: 3
```

## Configuration Options

### Database Options

```go
opts := db.DefaultOptions()

// Required for new databases
opts.CreateIfMissing = true

// Memory settings
opts.WriteBufferSize = 64 * 1024 * 1024  // 64MB memtable
opts.MaxWriteBufferNumber = 2             // Keep 2 memtables

// SST settings
opts.BlockSize = 4096                     // 4KB blocks
opts.MaxOpenFiles = 1000                  // Max open SST files

// Custom merge operator
opts.MergeOperator = &db.StringAppendOperator{Delimiter: ","}
```

### Write Options

```go
writeOpts := db.DefaultWriteOptions()

// Sync each write to disk (slower but durable)
writeOpts.Sync = true

// Skip WAL (faster but may lose data on crash)
writeOpts.DisableWAL = true
```

### Read Options

```go
readOpts := db.DefaultReadOptions()

// Enable checksum verification
readOpts.VerifyChecksums = true

// Use a snapshot for consistent reads
readOpts.Snapshot = database.GetSnapshot()
```

### Flush Options

```go
flushOpts := db.DefaultFlushOptions()

// Wait for flush to complete
flushOpts.Wait = true

// Force flush
database.Flush(flushOpts)
```

## Error Handling

```go
value, err := database.Get(nil, []byte("key"))
switch {
case err == nil:
    // Success
case err == db.ErrNotFound:
    // Key doesn't exist
case err == db.ErrDBClosed:
    // Database was closed
default:
    // Other error
    log.Printf("Error: %v", err)
}
```

## Best Practices

### 1. Use Batches for Multiple Writes

```go
// Bad: Individual writes
for _, kv := range items {
    database.Put(nil, kv.Key, kv.Value)
}

// Good: Batch write
wb := batch.New()
for _, kv := range items {
    wb.Put(kv.Key, kv.Value)
}
database.Write(nil, wb)
```

### 2. Reuse Read Options

```go
// Create once
readOpts := db.DefaultReadOptions()

// Reuse for multiple reads
for _, key := range keys {
    database.Get(readOpts, key)
}
```

### 3. Close Iterators Promptly

```go
iter := database.NewIterator(nil)
// Use iterator...
iter.Close() // Don't forget!
```

### 4. Use Snapshots for Consistent Multi-Key Reads

```go
snap := database.GetSnapshot()
defer database.ReleaseSnapshot(snap)

opts := db.DefaultReadOptions()
opts.Snapshot = snap

// These reads are guaranteed consistent
v1, _ := database.Get(opts, []byte("key1"))
v2, _ := database.Get(opts, []byte("key2"))
```

### 5. Handle Write Conflicts in Transactions

```go
for retry := 0; retry < 3; retry++ {
    txn := database.BeginTransaction(db.TransactionOptions{}, nil)
    // ... do work ...
    if err := txn.Commit(); err == nil {
        break // Success
    }
    txn.Rollback()
}
```

## Testing

For testing, use the in-memory filesystem:

```go
import "github.com/aalhour/rockyardkv/internal/vfs"

opts := db.DefaultOptions()
opts.CreateIfMissing = true
opts.FS = vfs.NewMemFS() // In-memory for tests

database, _ := db.Open("/test/db", opts)
defer database.Close()
```

