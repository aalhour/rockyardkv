package db

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestIngestExternalFile_StressIngestion stress tests SST ingestion
// with many concurrent operations.
func TestIngestExternalFile_StressIngestion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "db")

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	const (
		numGoroutines = 4
		numIngestions = 20
		keysPerFile   = 50
		testDuration  = 5 * time.Second
	)

	var (
		totalIngested int64
		totalKeys     int64
		errCount      int64
		wg            sync.WaitGroup
		done          = make(chan struct{})
	)

	// Start the stress test
	startTime := time.Now()

	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(goroutineID)))

			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}

				// Create SST file with unique keys
				sstPath := filepath.Join(tmpDir, "external", fmt.Sprintf("g%d_i%d.sst", goroutineID, i))
				os.MkdirAll(filepath.Dir(sstPath), 0755)

				data := make(map[string]string)
				for j := range keysPerFile {
					// Create unique key based on goroutine ID and iteration
					key := fmt.Sprintf("g%02d_i%04d_k%04d", goroutineID, i, j)
					value := fmt.Sprintf("value_%d_%d_%d", goroutineID, i, j)
					data[key] = value
				}

				createExternalSSTStress(t, sstPath, data)

				// Ingest the file
				ingestOpts := DefaultIngestExternalFileOptions()
				ingestOpts.MoveFiles = rng.Intn(2) == 0 // Randomly use move or copy
				if err := db.IngestExternalFile([]string{sstPath}, ingestOpts); err != nil {
					t.Logf("Ingestion error (may be OK under contention): %v", err)
					atomic.AddInt64(&errCount, 1)
					continue
				}

				atomic.AddInt64(&totalIngested, 1)
				atomic.AddInt64(&totalKeys, int64(keysPerFile))
			}
		}(g)
	}

	// Let it run for the test duration
	time.Sleep(testDuration)
	close(done)
	wg.Wait()

	elapsed := time.Since(startTime)
	ingested := atomic.LoadInt64(&totalIngested)
	keys := atomic.LoadInt64(&totalKeys)
	errs := atomic.LoadInt64(&errCount)

	t.Logf("Stress test completed in %v", elapsed)
	t.Logf("Total ingestions: %d", ingested)
	t.Logf("Total keys: %d", keys)
	t.Logf("Errors: %d", errs)
	t.Logf("Ingestion rate: %.1f files/sec", float64(ingested)/elapsed.Seconds())

	// Verify some random keys
	readOpts := DefaultReadOptions()
	for i := range min(10, int(ingested)) {
		// Try to read a key from a random ingestion
		key := fmt.Sprintf("g%02d_i%04d_k%04d", i%numGoroutines, 0, 0)
		_, err := db.Get(readOpts, []byte(key))
		if err != nil && !errors.Is(err, ErrNotFound) {
			t.Errorf("Failed to read key %s: %v", key, err)
		}
	}
}

// TestIngestExternalFile_StressMixedWorkload tests ingestion with concurrent reads and writes.
func TestIngestExternalFile_StressMixedWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "db")

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	const testDuration = 3 * time.Second

	var (
		writes     int64
		reads      int64
		ingestions int64
		wg         sync.WaitGroup
		done       = make(chan struct{})
	)

	// Writer goroutine
	wg.Go(func() {
		wo := DefaultWriteOptions()
		i := 0
		for {
			select {
			case <-done:
				return
			default:
			}
			key := fmt.Sprintf("write_key_%08d", i)
			if err := db.Put(wo, []byte(key), []byte("write_value")); err == nil {
				atomic.AddInt64(&writes, 1)
			}
			i++
		}
	})

	// Reader goroutine
	wg.Go(func() {
		ro := DefaultReadOptions()
		i := 0
		for {
			select {
			case <-done:
				return
			default:
			}
			key := fmt.Sprintf("write_key_%08d", i%1000)
			db.Get(ro, []byte(key))
			atomic.AddInt64(&reads, 1)
			i++
		}
	})

	// Ingest goroutine
	wg.Go(func() {
		fileNum := 0
		for {
			select {
			case <-done:
				return
			default:
			}

			sstPath := filepath.Join(tmpDir, "external", fmt.Sprintf("ingest_%d.sst", fileNum))
			os.MkdirAll(filepath.Dir(sstPath), 0755)

			data := make(map[string]string)
			for j := range 10 {
				key := fmt.Sprintf("ingest_key_%d_%d", fileNum, j)
				data[key] = "ingest_value"
			}
			createExternalSSTStress(t, sstPath, data)

			if err := db.IngestExternalFile([]string{sstPath}, DefaultIngestExternalFileOptions()); err == nil {
				atomic.AddInt64(&ingestions, 1)
			}
			fileNum++
		}
	})

	// Snapshot goroutine
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			snap := db.GetSnapshot()
			ro := DefaultReadOptions()
			ro.Snapshot = snap
			db.Get(ro, []byte("some_key"))
			db.ReleaseSnapshot(snap)
			time.Sleep(time.Millisecond)
		}
	})

	// Run for test duration
	time.Sleep(testDuration)
	close(done)
	wg.Wait()

	t.Logf("Writes: %d", atomic.LoadInt64(&writes))
	t.Logf("Reads: %d", atomic.LoadInt64(&reads))
	t.Logf("Ingestions: %d", atomic.LoadInt64(&ingestions))
}

// TestIngestExternalFile_StressRecovery tests ingestion followed by recovery.
func TestIngestExternalFile_StressRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "db")

	// First, open DB and ingest files
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	// Ingest several files
	const numFiles = 5
	const keysPerFile = 100
	allKeys := make(map[string]string)

	for f := range numFiles {
		sstPath := filepath.Join(tmpDir, "external", fmt.Sprintf("file_%d.sst", f))
		os.MkdirAll(filepath.Dir(sstPath), 0755)

		data := make(map[string]string)
		for k := range keysPerFile {
			key := fmt.Sprintf("recovery_key_%03d_%04d", f, k)
			value := fmt.Sprintf("value_%d_%d", f, k)
			data[key] = value
			allKeys[key] = value
		}
		createExternalSSTStress(t, sstPath, data)

		if err := db.IngestExternalFile([]string{sstPath}, DefaultIngestExternalFileOptions()); err != nil {
			t.Fatalf("IngestExternalFile failed: %v", err)
		}
	}

	// Close DB
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Reopen DB
	db, err = Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer db.Close()

	// Verify all keys are still accessible
	ro := DefaultReadOptions()
	missing := 0
	for key, expectedValue := range allKeys {
		val, err := db.Get(ro, []byte(key))
		if err != nil {
			missing++
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("Value mismatch for %s: expected %q, got %q", key, expectedValue, val)
		}
	}

	if missing > 0 {
		t.Errorf("%d keys missing after recovery (out of %d)", missing, len(allKeys))
	} else {
		t.Logf("All %d keys verified after recovery", len(allKeys))
	}
}

func createExternalSSTStress(t testing.TB, path string, data map[string]string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(path); err != nil {
		t.Fatalf("Failed to open SST writer: %v", err)
	}

	// Sort keys
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sortStringsStress(keys)

	for _, k := range keys {
		if err := writer.Put([]byte(k), []byte(data[k])); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	if _, err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish SST: %v", err)
	}
}

func sortStringsStress(s []string) {
	for i := range len(s) - 1 {
		for j := i + 1; j < len(s); j++ {
			if s[i] > s[j] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}
