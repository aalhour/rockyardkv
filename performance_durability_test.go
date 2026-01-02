package rockyardkv

// performance_durability_test.go implements Performance benchmarks for durability fixes to quantify overhead.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/vfs"
)

// BenchmarkRecovery_WithOrphanCleanup measures recovery time with varying
// numbers of orphaned SST files.
//
// Purpose: Verify orphan cleanup is O(n) in live SSTs, not orphans, and
// has acceptable overhead (<1ms per orphan).
func BenchmarkRecovery_WithOrphanCleanup(b *testing.B) {
	orphanCounts := []int{0, 10, 100, 1000}

	for _, orphanCount := range orphanCounts {
		b.Run(fmt.Sprintf("orphans=%d", orphanCount), func(b *testing.B) {
			dir := b.TempDir()

			opts := DefaultOptions()
			opts.CreateIfMissing = true
			fs := vfs.Default()
			opts.FS = fs

			writeOpts := DefaultWriteOptions()
			writeOpts.DisableWAL = true

			// Create DB with baseline data
			database, err := Open(dir, opts)
			if err != nil {
				b.Fatalf("Failed to open DB: %v", err)
			}

			for i := range 50 {
				key := fmt.Appendf(nil, "key_%04d", i)
				value := fmt.Appendf(nil, "value_%04d", i)
				if err := database.Put(writeOpts, key, value); err != nil {
					b.Fatalf("Put failed: %v", err)
				}
			}

			if err := database.Flush(nil); err != nil {
				b.Fatalf("Flush failed: %v", err)
			}

			database.Close()

			// Create orphaned SSTs (SST files are in dir directly, not dir/db)

			// Get a template SST to copy
			entries, err := fs.ListDir(dir)
			if err != nil {
				b.Fatalf("Failed to list directory: %v", err)
			}

			var templateSST string
			for _, entry := range entries {
				if filepath.Ext(entry) == ".sst" {
					templateSST = filepath.Join(dir, entry)
					break
				}
			}

			if templateSST == "" {
				b.Fatal("No SST file found")
			}

			data, err := os.ReadFile(templateSST)
			if err != nil {
				b.Fatalf("Failed to read template: %v", err)
			}

			// Create orphans
			for i := range orphanCount {
				orphanPath := filepath.Join(dir, fmt.Sprintf("%06d.sst", 900000+i))
				if err := os.WriteFile(orphanPath, data, 0644); err != nil {
					b.Fatalf("Failed to create orphan: %v", err)
				}
			}

			// Benchmark recovery (with orphan cleanup)
			b.ResetTimer()
			for b.Loop() {
				database, err := Open(dir, opts)
				if err != nil {
					b.Fatalf("Failed to open DB: %v", err)
				}
				database.Close()
			}
			b.StopTimer()

			b.ReportMetric(float64(orphanCount), "orphans")
		})
	}
}

// BenchmarkFlush_LastSequenceOverhead measures flush latency with the new
// LastSequence logic (max calculation + monotonicity check).
//
// Purpose: Quantify overhead of durability fix. Acceptable: <5% regression.
func BenchmarkFlush_LastSequenceOverhead(b *testing.B) {
	dir := b.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	writeOpts := DefaultWriteOptions()
	writeOpts.DisableWAL = true

	database, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer database.Close()

	// Pre-populate with some data to make flush more realistic
	for i := range 100 {
		key := fmt.Appendf(nil, "key_%04d", i)
		value := fmt.Appendf(nil, "value_%04d", i)
		if err := database.Put(writeOpts, key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	b.ResetTimer()

	for i := range b.N {
		// Write data to memtable
		for j := range 50 {
			key := fmt.Appendf(nil, "bench_%d_%04d", i, j)
			value := fmt.Appendf(nil, "value_%d_%04d", i, j)
			if err := database.Put(writeOpts, key, value); err != nil {
				b.Fatalf("Put failed: %v", err)
			}
		}

		// Benchmark flush (includes LastSequence logic)
		if err := database.Flush(nil); err != nil {
			b.Fatalf("Flush failed: %v", err)
		}
	}
}

// BenchmarkFlush_LastSequenceMonotonicityCheck measures just the monotonicity
// check overhead in isolation.
//
// Purpose: Isolate the cost of the max(newSeq, prevSeq) check.
func BenchmarkFlush_LastSequenceMonotonicityCheck(b *testing.B) {
	dir := b.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	writeOpts := DefaultWriteOptions()
	writeOpts.DisableWAL = true

	database, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer database.Close()

	b.ResetTimer()

	for i := range b.N {
		// Small write to minimize memtable overhead
		key := fmt.Appendf(nil, "k%d", i)
		value := fmt.Appendf(nil, "v%d", i)
		if err := database.Put(writeOpts, key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}

		// Flush (the monotonicity check happens here)
		if err := database.Flush(nil); err != nil {
			b.Fatalf("Flush failed: %v", err)
		}
	}

	b.ReportMetric(float64(b.N), "flushes")
}

// BenchmarkRecovery_SequenceRestoration measures the cost of restoring
// sequence numbers from MANIFEST during recovery.
//
// Purpose: Verify sequence restoration is fast (<1ms overhead).
func BenchmarkRecovery_SequenceRestoration(b *testing.B) {
	dir := b.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	writeOpts := DefaultWriteOptions()
	writeOpts.DisableWAL = true

	// Create DB with multiple flushes (creates multiple sequence updates in MANIFEST)
	database, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}

	for flushNum := range 10 {
		for i := range 100 {
			key := fmt.Appendf(nil, "key_%d_%04d", flushNum, i)
			value := fmt.Appendf(nil, "value_%d_%04d", flushNum, i)
			if err := database.Put(writeOpts, key, value); err != nil {
				b.Fatalf("Put failed: %v", err)
			}
		}
		if err := database.Flush(nil); err != nil {
			b.Fatalf("Flush failed: %v", err)
		}
	}

	database.Close()

	b.ResetTimer()

	// Benchmark just the recovery (includes sequence restoration)
	for b.Loop() {
		database, err := Open(dir, opts)
		if err != nil {
			b.Fatalf("Failed to open DB: %v", err)
		}
		database.Close()
	}
}

// BenchmarkOrphanCleanup_ScanSpeed measures the speed of scanning for orphaned
// SST files in the directory.
//
// Purpose: Verify directory scan is fast even with many files.
func BenchmarkOrphanCleanup_ScanSpeed(b *testing.B) {
	fileCounts := []int{10, 100, 1000}

	for _, fileCount := range fileCounts {
		b.Run(fmt.Sprintf("files=%d", fileCount), func(b *testing.B) {
			dir := b.TempDir()

			opts := DefaultOptions()
			opts.CreateIfMissing = true
			fs := vfs.Default()
			opts.FS = fs

			writeOpts := DefaultWriteOptions()
			writeOpts.DisableWAL = true

			// Create DB
			database, err := Open(dir, opts)
			if err != nil {
				b.Fatalf("Failed to open DB: %v", err)
			}

			// Create many flushes to generate many SST files
			for flushNum := range fileCount / 10 {
				for i := range 100 {
					key := fmt.Appendf(nil, "key_%d_%04d", flushNum, i)
					value := fmt.Appendf(nil, "value_%04d", i)
					if err := database.Put(writeOpts, key, value); err != nil {
						b.Fatalf("Put failed: %v", err)
					}
				}
				if err := database.Flush(nil); err != nil {
					b.Fatalf("Flush failed: %v", err)
				}
			}

			database.Close()

			// Create additional non-SST files to increase directory size
			for i := range fileCount - (fileCount / 10) {
				dummyPath := filepath.Join(dir, fmt.Sprintf("dummy_%06d.log", i))
				if err := os.WriteFile(dummyPath, []byte("dummy"), 0644); err != nil {
					b.Fatalf("Failed to create dummy file: %v", err)
				}
			}

			b.ResetTimer()

			// Benchmark recovery (includes orphan scan)
			for b.Loop() {
				database, err := Open(dir, opts)
				if err != nil {
					b.Fatalf("Failed to open DB: %v", err)
				}
				database.Close()
			}

			b.ReportMetric(float64(fileCount), "total_files")
		})
	}
}
