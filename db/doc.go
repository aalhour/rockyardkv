// Package db provides a pure-Go implementation of RocksDB, a high-performance
// embedded key-value store.
//
// RockyardKV is a 100% Go reimplementation of RocksDB v10.7.5, with identical
// on-disk formats for SST files, WAL, and MANIFEST. It provides an LSM-tree based
// storage engine suitable for high-write workloads.
//
// # Quick Start
//
// Opening and using a database:
//
//	import "github.com/aalhour/rockyardkv/db"
//
//	// Open or create a database
//	opts := db.DefaultOptions()
//	opts.CreateIfMissing = true
//	database, err := db.Open("/path/to/db", opts)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer database.Close()
//
//	// Write data
//	err = database.Put(db.DefaultWriteOptions(), []byte("key"), []byte("value"))
//
//	// Read data
//	value, err := database.Get(nil, []byte("key"))
//
//	// Delete data
//	err = database.Delete(db.DefaultWriteOptions(), []byte("key"))
//
// # Batch Writes
//
// For atomic multi-key operations, use WriteBatch:
//
//	wb := db.NewWriteBatch()
//	wb.Put([]byte("key1"), []byte("value1"))
//	wb.Put([]byte("key2"), []byte("value2"))
//	wb.Delete([]byte("key3"))
//	err := database.Write(db.DefaultWriteOptions(), wb)
//
// # Iteration
//
// Iterate over keys in sorted order:
//
//	iter := database.NewIterator(db.DefaultReadOptions())
//	defer iter.Close()
//
//	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
//	    fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
//	}
//
//	// Seek to a specific key
//	iter.Seek([]byte("prefix"))
//
// # Snapshots
//
// Read a consistent view of the database:
//
//	snap := database.GetSnapshot()
//	defer database.ReleaseSnapshot(snap)
//
//	opts := db.DefaultReadOptions()
//	opts.Snapshot = snap
//	value, err := database.Get(opts, []byte("key"))
//
// # Column Families
//
// Use column families to partition data:
//
//	cf, err := database.CreateColumnFamily(db.ColumnFamilyOptions{}, "mycf")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	err = database.PutCF(db.DefaultWriteOptions(), cf, []byte("key"), []byte("value"))
//	value, err := database.GetCF(nil, cf, []byte("key"))
//
// # Transactions
//
// Use optimistic transactions for read-modify-write operations:
//
//	txn := database.BeginTransaction(db.TransactionOptions{}, nil)
//	defer txn.Rollback() // No-op if already committed
//
//	value, err := txn.Get([]byte("counter"))
//	newValue := incrementCounter(value)
//	txn.Put([]byte("counter"), newValue)
//
//	err = txn.Commit() // Will fail if "counter" was modified by another writer
//
// # Features
//
// Core features:
//   - LSM-tree architecture with memtable and SST files
//   - Write-ahead log (WAL) for durability
//   - Background compaction (leveled)
//   - Bloom filters for read optimization
//   - Snappy and Zlib compression
//   - Column families
//   - Optimistic transactions
//   - Snapshots and iterators
//
// # Thread Safety
//
// A DB instance is safe for concurrent access by multiple goroutines.
// Individual Iterator instances are NOT safe for concurrent access -
// each goroutine should create its own iterator.
//
// # Performance
//
// For best performance:
//   - Use batch writes for multiple keys
//   - Configure appropriate write buffer size
//   - Enable bloom filters for read-heavy workloads
//   - Use compression for large values
//
// # Compatibility
//
// RockyardKV uses the same on-disk format as RocksDB v10.7.5:
//   - SST files (format version 3+)
//   - WAL log records
//   - MANIFEST/VersionEdit format
//
// SST files created by RockyardKV can be read by C++ RocksDB and vice versa.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h
//   - db/db_impl/db_impl.h
package db
