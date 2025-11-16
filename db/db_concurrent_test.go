// db_concurrent_test.go - Concurrency and thread safety tests
//
// These tests verify that the database correctly handles concurrent access
// from multiple goroutines without data races or corruption.

package db

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aalhour/rockyardkv/internal/batch"
)

// =============================================================================
// Concurrent Read/Write Tests
// =============================================================================

func TestConcurrentPuts(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	var wg sync.WaitGroup
	const numGoroutines = 10
	const numOpsPerGoroutine = 100

	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := range numOpsPerGoroutine {
				key := fmt.Appendf(nil, "key_%d_%d", goroutineID, i)
				value := fmt.Appendf(nil, "value_%d_%d", goroutineID, i)
				db.Put(nil, key, value)
			}
		}(g)
	}
	wg.Wait()

	// Verify all writes
	for g := range numGoroutines {
		for i := range numOpsPerGoroutine {
			key := fmt.Appendf(nil, "key_%d_%d", g, i)
			_, err := db.Get(nil, key)
			if err != nil {
				t.Errorf("Missing key_%d_%d", g, i)
			}
		}
	}
}

func TestConcurrentReadsWrites(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Pre-populate
	for i := range 100 {
		db.Put(nil, fmt.Appendf(nil, "key%d", i), []byte("initial"))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var readCount, writeCount atomic.Int64

	// Readers
	for range 5 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					key := fmt.Appendf(nil, "key%d", readCount.Load()%100)
					db.Get(nil, key)
					readCount.Add(1)
				}
			}
		})
	}

	// Writers
	for w := range 3 {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := fmt.Appendf(nil, "key%d", writeCount.Load()%100)
					db.Put(nil, key, fmt.Appendf(nil, "writer%d", writerID))
					writeCount.Add(1)
				}
			}
		}(w)
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	t.Logf("Reads: %d, Writes: %d", readCount.Load(), writeCount.Load())
}

func TestConcurrentPutSameKey(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	key := []byte("shared_key")
	var wg sync.WaitGroup
	const numGoroutines = 10
	const numOps = 100

	for g := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range numOps {
				db.Put(nil, key, fmt.Appendf(nil, "value_%d_%d", id, i))
			}
		}(g)
	}
	wg.Wait()

	// Key should exist with some value
	_, err := db.Get(nil, key)
	if err != nil {
		t.Errorf("Key not found after concurrent writes")
	}
}

// =============================================================================
// Concurrent Iterator Tests
// =============================================================================

func TestConcurrentIterators(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Populate
	for i := range 100 {
		db.Put(nil, fmt.Appendf(nil, "iter_key%03d", i), []byte("value"))
	}

	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			iter := db.NewIterator(nil)
			defer iter.Close()

			count := 0
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				_ = iter.Key()
				count++
			}
			if count != 100 {
				t.Errorf("Iterator count = %d, want 100", count)
			}
		})
	}
	wg.Wait()
}

func TestConcurrentIteratorWithWrites(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Initial data
	for i := range 50 {
		db.Put(nil, fmt.Appendf(nil, "key%03d", i), []byte("v1"))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Iterator reader
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				iter := db.NewIterator(nil)
				for iter.SeekToFirst(); iter.Valid(); iter.Next() {
					_ = iter.Key()
				}
				iter.Close()
			}
		}
	})

	// Writer
	wg.Go(func() {
		i := 50
		for {
			select {
			case <-stop:
				return
			default:
				db.Put(nil, fmt.Appendf(nil, "key%03d", i), []byte("v2"))
				i++
				if i > 100 {
					i = 50
				}
			}
		}
	})

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// =============================================================================
// Concurrent Batch Tests
// =============================================================================

func TestConcurrentBatches(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	var wg sync.WaitGroup
	const numWriters = 5
	const numBatches = 20

	for w := range numWriters {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for b := range numBatches {
				wb := batch.New()
				for j := range 10 {
					key := fmt.Appendf(nil, "batch_w%d_b%d_j%d", writer, b, j)
					wb.Put(key, []byte("value"))
				}
				db.Write(nil, wb)
			}
		}(w)
	}
	wg.Wait()

	// Verify some keys
	for w := range numWriters {
		key := fmt.Appendf(nil, "batch_w%d_b0_j0", w)
		_, err := db.Get(nil, key)
		if err != nil {
			t.Errorf("Writer %d key not found", w)
		}
	}
}

// =============================================================================
// Concurrent Flush Tests
// =============================================================================

func TestConcurrentFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	var wg sync.WaitGroup

	// Writers
	for w := range 3 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 100 {
				key := fmt.Appendf(nil, "flush_w%d_%d", id, i)
				db.Put(nil, key, []byte("value"))
			}
		}(w)
	}

	// Flusher
	wg.Go(func() {
		for range 5 {
			time.Sleep(20 * time.Millisecond)
			db.Flush(nil)
		}
	})

	wg.Wait()
}

func TestConcurrentFlushWithReads(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Pre-populate
	for i := range 100 {
		db.Put(nil, fmt.Appendf(nil, "key%d", i), []byte("value"))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				for i := range 100 {
					db.Get(nil, fmt.Appendf(nil, "key%d", i))
				}
			}
		}
	})

	// Flusher
	wg.Go(func() {
		for range 10 {
			time.Sleep(10 * time.Millisecond)
			db.Flush(nil)
		}
		close(stop)
	})

	wg.Wait()
}

// =============================================================================
// Race Condition Tests
// These tests are designed to catch specific race conditions.
// =============================================================================

func TestIteratorRaceWithFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	for i := range 100 {
		db.Put(nil, fmt.Appendf(nil, "race_key%d", i), []byte("value"))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Iterator
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				iter := db.NewIterator(nil)
				for iter.SeekToFirst(); iter.Valid(); iter.Next() {
					_ = iter.Key()
				}
				iter.Close()
			}
		}
	})

	// Flush repeatedly
	wg.Go(func() {
		for range 20 {
			db.Flush(nil)
		}
		close(stop)
	})

	wg.Wait()
}

func TestGetRaceWithFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	db.Put(nil, []byte("race_key"), []byte("value"))

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_, err := db.Get(nil, []byte("race_key"))
				if err != nil && !errors.Is(err, ErrNotFound) {
					t.Errorf("Get error: %v", err)
				}
			}
		}
	})

	// Writer + Flusher
	wg.Go(func() {
		for i := range 50 {
			db.Put(nil, []byte("race_key"), fmt.Appendf(nil, "v%d", i))
			if i%5 == 0 {
				db.Flush(nil)
			}
		}
		close(stop)
	})

	wg.Wait()
}
