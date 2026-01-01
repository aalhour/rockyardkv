// benchmark_test.go implements tests for benchmark.
package rockyardkv

import (
	"fmt"
	"math/rand"
	"testing"
)

// =============================================================================
// DB Benchmarks
// =============================================================================

func BenchmarkDBPutSequential(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	value := make([]byte, 100)
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := range b.N {
		key := fmt.Appendf(nil, "key%016d", i)
		if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N), "ops")
}

func BenchmarkDBPutRandom(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	value := make([]byte, 100)
	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for b.Loop() {
		key := fmt.Appendf(nil, "key%016d", rng.Int63())
		if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkDBGet(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Pre-populate
	value := make([]byte, 100)
	for i := range 10000 {
		key := fmt.Appendf(nil, "key%016d", i)
		if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}

	rng := rand.New(rand.NewSource(42))

	for b.Loop() {
		key := fmt.Appendf(nil, "key%016d", rng.Intn(10000))
		_, _ = db.Get(nil, key)
	}
	b.StopTimer()
}

func BenchmarkBatchWrite(b *testing.B) {
	sizes := []int{10, 100, 1000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			dir := b.TempDir()
			opts := DefaultOptions()
			opts.CreateIfMissing = true

			db, err := Open(dir, opts)
			if err != nil {
				b.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			value := make([]byte, 100)

			b.ResetTimer()
			for i := range b.N {
				wb := NewWriteBatch()
				for j := range size {
					key := fmt.Appendf(nil, "key%016d", i*size+j)
					wb.Put(key, value)
				}
				if err := db.Write(DefaultWriteOptions(), wb); err != nil {
					b.Fatalf("Write error: %v", err)
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(b.N*size), "ops")
		})
	}
}

func BenchmarkIteratorScan(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Pre-populate with 10,000 keys
	value := make([]byte, 100)
	for i := range 10000 {
		key := fmt.Appendf(nil, "key%016d", i)
		if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}

	b.ResetTimer()
	for b.Loop() {
		iter := db.NewIterator(DefaultReadOptions())
		count := 0
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			_ = iter.Key()
			_ = iter.Value()
			count++
		}
		iter.Close()
	}
	b.StopTimer()
}

func BenchmarkFlush(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	value := make([]byte, 100)

	b.ResetTimer()
	for i := range b.N {
		// Write some data
		for j := range 1000 {
			key := fmt.Appendf(nil, "key%d_%d", i, j)
			if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
				b.Fatalf("Put error: %v", err)
			}
		}
		// Flush
		if err := db.Flush(nil); err != nil {
			b.Fatalf("Flush error: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkMixedWorkload(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Pre-populate
	value := make([]byte, 100)
	for i := range 1000 {
		key := fmt.Appendf(nil, "key%016d", i)
		if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for b.Loop() {
		op := rng.Intn(100)
		keyNum := rng.Intn(1000)
		key := fmt.Appendf(nil, "key%016d", keyNum)

		if op < 50 {
			// 50% reads
			_, _ = db.Get(nil, key)
		} else if op < 90 {
			// 40% writes
			_ = db.Put(DefaultWriteOptions(), key, value)
		} else {
			// 10% deletes
			_ = db.Delete(DefaultWriteOptions(), key)
		}
	}
	b.StopTimer()
}

func BenchmarkValueSizes(b *testing.B) {
	sizes := []int{100, 1024, 10240, 102400}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("value_%d", size), func(b *testing.B) {
			dir := b.TempDir()
			opts := DefaultOptions()
			opts.CreateIfMissing = true

			db, err := Open(dir, opts)
			if err != nil {
				b.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			value := make([]byte, size)
			for i := range value {
				value[i] = byte(i % 256)
			}

			b.ResetTimer()
			for i := range b.N {
				key := fmt.Appendf(nil, "key%016d", i)
				if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
					b.Fatalf("Put error: %v", err)
				}
			}
			b.StopTimer()

			b.SetBytes(int64(size))
		})
	}
}

func BenchmarkConcurrentPut(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	value := make([]byte, 100)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Appendf(nil, "key%016d_%d", i, rand.Int63())
			if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
				b.Errorf("Put error: %v", err)
			}
			i++
		}
	})
	b.StopTimer()
}

func BenchmarkConcurrentGet(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Pre-populate
	value := make([]byte, 100)
	for i := range 10000 {
		key := fmt.Appendf(nil, "key%016d", i)
		if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			key := fmt.Appendf(nil, "key%016d", rng.Intn(10000))
			_, _ = db.Get(nil, key)
		}
	})
	b.StopTimer()
}

func BenchmarkSnapshot(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Pre-populate
	value := make([]byte, 100)
	for i := range 1000 {
		key := fmt.Appendf(nil, "key%016d", i)
		if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}

	b.ResetTimer()
	for i := range b.N {
		snap := db.GetSnapshot()
		// Do a read with snapshot
		key := fmt.Appendf(nil, "key%016d", i%1000)
		readOpts := DefaultReadOptions()
		readOpts.Snapshot = snap
		_, _ = db.Get(readOpts, key)
		db.ReleaseSnapshot(snap)
	}
	b.StopTimer()
}

// =============================================================================
// MultiGet Benchmark - Batch reads are a common optimization in RocksDB
// =============================================================================

func BenchmarkMultiGet(b *testing.B) {
	sizes := []int{10, 100}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("keys_%d", size), func(b *testing.B) {
			dir := b.TempDir()
			opts := DefaultOptions()
			opts.CreateIfMissing = true

			db, err := Open(dir, opts)
			if err != nil {
				b.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			// Pre-populate
			value := make([]byte, 100)
			for i := range 10000 {
				key := fmt.Appendf(nil, "key%016d", i)
				if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
					b.Fatalf("Put error: %v", err)
				}
			}

			// Prepare keys for MultiGet
			rng := rand.New(rand.NewSource(42))
			keys := make([][]byte, size)
			for i := range keys {
				keys[i] = fmt.Appendf(nil, "key%016d", rng.Intn(10000))
			}

			b.ResetTimer()
			for b.Loop() {
				_, _ = db.MultiGet(nil, keys)
			}
			b.StopTimer()

			b.ReportMetric(float64(size), "keys/op")
		})
	}
}

// =============================================================================
// Delete Benchmarks - Point delete vs range delete
// =============================================================================

func BenchmarkDelete(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	value := make([]byte, 100)

	b.ResetTimer()
	for i := range b.N {
		key := fmt.Appendf(nil, "key%016d", i)
		// Write then delete
		if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
		if err := db.Delete(DefaultWriteOptions(), key); err != nil {
			b.Fatalf("Delete error: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkDeleteRange(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	value := make([]byte, 100)

	b.ResetTimer()
	for i := range b.N {
		// Write 100 keys in a range
		prefix := fmt.Sprintf("range%08d_", i)
		for j := range 100 {
			key := fmt.Appendf(nil, "%skey%04d", prefix, j)
			if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
				b.Fatalf("Put error: %v", err)
			}
		}
		// Delete the entire range with one operation
		start := []byte(prefix + "key0000")
		end := []byte(prefix + "key9999")
		if err := db.DeleteRange(DefaultWriteOptions(), start, end); err != nil {
			b.Fatalf("DeleteRange error: %v", err)
		}
	}
	b.StopTimer()

	b.ReportMetric(100, "keys/op")
}

// =============================================================================
// Merge Benchmark - Unique RocksDB feature for incremental updates
// =============================================================================

func BenchmarkMerge(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &UInt64AddOperator{}

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Initialize counters
	for i := range 100 {
		key := fmt.Appendf(nil, "counter%04d", i)
		if err := db.Put(DefaultWriteOptions(), key, encodeUint64(0)); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}

	b.ResetTimer()
	for i := range b.N {
		key := fmt.Appendf(nil, "counter%04d", i%100)
		if err := db.Merge(DefaultWriteOptions(), key, encodeUint64(1)); err != nil {
			b.Fatalf("Merge error: %v", err)
		}
	}
	b.StopTimer()
}

// =============================================================================
// Iterator Seek Benchmark - Point seeks vs full scans
// =============================================================================

func BenchmarkIteratorSeek(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Pre-populate with 100,000 keys for realistic seek behavior
	value := make([]byte, 100)
	for i := range 100000 {
		key := fmt.Appendf(nil, "key%016d", i)
		if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}

	// Flush to SST to test seek across levels
	if err := db.Flush(nil); err != nil {
		b.Fatalf("Flush error: %v", err)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	iter := db.NewIterator(DefaultReadOptions())
	for b.Loop() {
		target := fmt.Appendf(nil, "key%016d", rng.Intn(100000))
		iter.Seek(target)
		if iter.Valid() {
			_ = iter.Key()
			_ = iter.Value()
		}
	}
	iter.Close()
	b.StopTimer()
}

// =============================================================================
// DB Open Benchmark - Recovery/startup time
// =============================================================================

func BenchmarkDBOpen(b *testing.B) {
	// Create a database with data first
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}

	// Write data and flush to create SST files
	value := make([]byte, 100)
	for i := range 10000 {
		key := fmt.Appendf(nil, "key%016d", i)
		if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}
	if err := db.Flush(nil); err != nil {
		b.Fatalf("Flush error: %v", err)
	}
	db.Close()

	// Benchmark reopening
	b.ResetTimer()
	for b.Loop() {
		db, err := Open(dir, opts)
		if err != nil {
			b.Fatalf("Open() error = %v", err)
		}
		db.Close()
	}
	b.StopTimer()
}

// =============================================================================
// Transaction Benchmark - Optimistic concurrency control overhead
// =============================================================================

func BenchmarkTransaction(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Pre-populate
	value := make([]byte, 100)
	for i := range 1000 {
		key := fmt.Appendf(nil, "key%016d", i)
		if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}

	rng := rand.New(rand.NewSource(42))
	txnOpts := TransactionOptions{}

	b.ResetTimer()
	for b.Loop() {
		txn := db.BeginTransaction(txnOpts, nil)

		// Read-modify-write pattern
		key := fmt.Appendf(nil, "key%016d", rng.Intn(1000))
		_, _ = txn.Get(key)
		_ = txn.Put(key, value)

		if err := txn.Commit(); err != nil {
			// Conflicts are expected in benchmarks, just rollback
			txn.Rollback()
		}
	}
	b.StopTimer()
}

func BenchmarkTransactionConflict(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Single key to force conflicts
	key := []byte("hotkey")
	value := make([]byte, 100)
	if err := db.Put(DefaultWriteOptions(), key, value); err != nil {
		b.Fatalf("Put error: %v", err)
	}

	txnOpts := TransactionOptions{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			txn := db.BeginTransaction(txnOpts, nil)
			_, _ = txn.Get(key)
			_ = txn.Put(key, value)
			if err := txn.Commit(); err != nil {
				txn.Rollback()
			}
		}
	})
	b.StopTimer()
}
