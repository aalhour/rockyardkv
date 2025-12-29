// Adversarial test runner for RockyardKV.
//
// Use `adversarialtest` to exercise hostile inputs and error paths.
// Use this tool to validate that the DB fails loudly on corruption and rejects invalid input.
//
// Run a short suite:
//
// ```bash
// ./bin/adversarialtest
// ```
//
// Run a category:
//
// ```bash
// ./bin/adversarialtest -category=corruption
// ```
//
// Collect artifacts on failure:
//
// ```bash
// ./bin/adversarialtest -run-dir <RUN_DIR>
// ```
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/testutil"
)

// Configuration
var (
	longMode     = flag.Bool("long", false, "Run in long mode (10 minutes, 256 threads)")
	category     = flag.String("category", "all", "Category to run: all, edge, corruption, go, protocol, concurrency")
	threads      = flag.Int("threads", 0, "Number of concurrent threads (0 = auto)")
	duration     = flag.Duration("duration", 0, "Test duration (0 = auto based on mode)")
	keepTempDirs = flag.Bool("keep", false, "Keep temporary directories for debugging")
	cleanup      = flag.Bool("cleanup", false, "Clean up old test directories before running")
	runDir       = flag.String("run-dir", "", "Directory for artifact collection on failure (default: none)")
	seed         = flag.Int64("seed", 0, "Random seed for reproducibility (0 = time-based)")
)

const adversarialTestDirPrefix = "rockyard-adversarial-"

// Test result
type testResult struct {
	name     string
	passed   bool
	duration time.Duration
	message  string
}

// Stats
type stats struct {
	passed atomic.Int64
	failed atomic.Int64
}

var globalStats stats

func main() {
	flag.Parse()

	// Initialize seed
	actualSeed := *seed
	if actualSeed == 0 {
		actualSeed = time.Now().UnixNano()
	}

	// Clean up old test directories if requested
	if *cleanup {
		cleanupOldTestDirs()
	}

	// Determine configuration
	numThreads := *threads
	testDuration := *duration

	if *longMode {
		if numThreads == 0 {
			numThreads = 256
		}
		if testDuration == 0 {
			testDuration = 10 * time.Minute
		}
	} else {
		if numThreads == 0 {
			numThreads = runtime.NumCPU() * 2
		}
		if testDuration == 0 {
			testDuration = 2 * time.Minute
		}
	}

	printBanner(numThreads, testDuration)

	startTime := time.Now()

	// Setup artifact bundle for failure collection (if run-dir specified)
	var artifactBundle *testutil.ArtifactBundle
	if *runDir != "" {
		var err error
		artifactBundle, err = testutil.NewArtifactBundle(*runDir, "adversarialtest", actualSeed)
		if err != nil {
			fmt.Printf("⚠️  Failed to create artifact bundle: %v\n", err)
		} else {
			artifactBundle.SetFlags(map[string]any{
				"long":     *longMode,
				"category": *category,
				"threads":  numThreads,
				"duration": testDuration.String(),
				"seed":     actualSeed,
			})
		}
	}

	// Run test categories
	categories := getCategories(*category)
	results := make([]testResult, 0)

	for _, cat := range categories {
		catResults := runCategory(cat, numThreads, testDuration)
		results = append(results, catResults...)
	}

	// Print summary
	elapsed := time.Since(startTime)
	printSummary(results, elapsed)

	// Exit code
	if globalStats.failed.Load() > 0 {
		if artifactBundle != nil {
			failErr := fmt.Errorf("failed tests: %d", globalStats.failed.Load())
			if bundleErr := artifactBundle.RecordFailure(failErr, elapsed); bundleErr != nil {
				fmt.Printf("⚠️  Artifact collection error: %v\n", bundleErr)
			} else {
				fmt.Printf("📦 Artifacts collected at: %s\n", artifactBundle.RunDir)
			}
		}
		os.Exit(1)
	}

	if artifactBundle != nil {
		artifactBundle.RecordSuccess(elapsed)
	}
}

func printBanner(threads int, duration time.Duration) {
	const boxWidth = 72 // inner width between ║ and ║

	line := func(content string) {
		padded := fmt.Sprintf(" %-*s", boxWidth-2, content)
		if len(padded) > boxWidth-1 {
			padded = padded[:boxWidth-1]
		}
		fmt.Printf("║%s║\n", padded)
	}

	repeatChar := func(ch rune, n int) string {
		result := make([]rune, n)
		for i := range result {
			result[i] = ch
		}
		return string(result)
	}

	center := func(s string, width int) string {
		if len(s) >= width {
			return s
		}
		pad := (width - len(s)) / 2
		return fmt.Sprintf("%*s%s%*s", pad, "", s, width-len(s)-pad, "")
	}

	mode := "SHORT"
	if *longMode {
		mode = "LONG"
	}

	border := "╔" + repeatChar('═', boxWidth) + "╗"
	middle := "╠" + repeatChar('═', boxWidth) + "╣"
	bottom := "╚" + repeatChar('═', boxWidth) + "╝"

	fmt.Println(border)
	line(center("RockyardKV Adversarial Test Suite", boxWidth-2))
	fmt.Println(middle)
	line(fmt.Sprintf("Mode: %-6s  Threads: %-4d  Duration: %s", mode, threads, duration))
	line(fmt.Sprintf("Category: %s", *category))
	fmt.Println(bottom)
	fmt.Println()
}

func getCategories(cat string) []string {
	switch cat {
	case "all":
		return []string{"edge", "corruption", "go", "protocol", "concurrency"}
	case "edge", "corruption", "go", "protocol", "concurrency":
		return []string{cat}
	default:
		fmt.Fprintf(os.Stderr, "Unknown category: %s\n", cat)
		os.Exit(1)
		return nil
	}
}

func runCategory(cat string, threads int, duration time.Duration) []testResult {
	fmt.Printf("━━━ Category: %s ━━━\n", strings.ToUpper(cat))

	var results []testResult

	switch cat {
	case "edge":
		results = runEdgeTests()
	case "corruption":
		results = runCorruptionTests()
	case "go":
		results = runGoSpecificTests()
	case "protocol":
		results = runProtocolTests()
	case "concurrency":
		results = runConcurrencyTests(threads, duration)
	}

	fmt.Println()
	return results
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func runEdgeTests() []testResult {
	tests := []struct {
		name string
		fn   func(string) error
	}{
		{"EmptyKey", testEmptyKey},
		{"EmptyValue", testEmptyValue},
		{"HugeKey", testHugeKey},
		{"HugeValue", testHugeValue},
		{"BinaryKeys", testBinaryKeys},
		{"DuplicateKeysInBatch", testDuplicateKeysInBatch},
		{"RangeDeleteEntireDB", testRangeDeleteEntireDB},
		{"NilKeyValue", testNilKeyValue},
	}

	return runTestSuite(tests)
}

func testEmptyKey(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	emptyKey := []byte{}
	value := []byte("value_for_empty_key")

	if err := database.Put(nil, emptyKey, value); err != nil {
		return fmt.Errorf("put empty key: %w", err)
	}

	got, err := database.Get(nil, emptyKey)
	if err != nil {
		return fmt.Errorf("get empty key: %w", err)
	}
	if !bytes.Equal(got, value) {
		return fmt.Errorf("wrong value: got %q, want %q", got, value)
	}

	return nil
}

func testEmptyValue(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	if err := database.Put(nil, []byte("key"), []byte{}); err != nil {
		return fmt.Errorf("put empty value: %w", err)
	}

	got, err := database.Get(nil, []byte("key"))
	if err != nil {
		return fmt.Errorf("get empty value: %w", err)
	}
	if len(got) != 0 {
		return fmt.Errorf("expected empty value, got %q", got)
	}

	return nil
}

func testHugeKey(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	hugeKey := bytes.Repeat([]byte("K"), 8*1024) // 8KB
	value := []byte("value")

	if err := database.Put(nil, hugeKey, value); err != nil {
		// Error is acceptable for huge keys
		return nil
	}

	got, err := database.Get(nil, hugeKey)
	if err != nil {
		return fmt.Errorf("get huge key: %w", err)
	}
	if !bytes.Equal(got, value) {
		return fmt.Errorf("wrong value")
	}

	return nil
}

func testHugeValue(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	key := []byte("huge_value_key")
	hugeValue := bytes.Repeat([]byte("V"), 2*1024*1024) // 2MB

	if err := database.Put(nil, key, hugeValue); err != nil {
		return fmt.Errorf("put huge value: %w", err)
	}

	got, err := database.Get(nil, key)
	if err != nil {
		return fmt.Errorf("get huge value: %w", err)
	}
	if !bytes.Equal(got, hugeValue) {
		return fmt.Errorf("wrong huge value (len: %d vs %d)", len(got), len(hugeValue))
	}

	return nil
}

func testBinaryKeys(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	testCases := [][]byte{
		[]byte("key\x00with\x00nulls"),
		{0xFF, 0xFE, 0xFD},
		{0x00, 0x00, 0x00},
		{0xFF, 0xFF, 0xFF},
		{0x00, 'a', 0xFF, 'z', 0x00},
	}

	for i, key := range testCases {
		value := fmt.Appendf(nil, "value%d", i)
		if err := database.Put(nil, key, value); err != nil {
			return fmt.Errorf("put binary key %d: %w", i, err)
		}

		got, err := database.Get(nil, key)
		if err != nil {
			return fmt.Errorf("get binary key %d: %w", i, err)
		}
		if !bytes.Equal(got, value) {
			return fmt.Errorf("binary key %d: wrong value", i)
		}
	}

	return nil
}

func testDuplicateKeysInBatch(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	key := []byte("duplicate_key")
	b := db.NewWriteBatch()
	b.Put(key, []byte("first"))
	b.Put(key, []byte("second"))
	b.Put(key, []byte("third"))

	if err := database.Write(nil, b); err != nil {
		return fmt.Errorf("write batch: %w", err)
	}

	got, err := database.Get(nil, key)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if !bytes.Equal(got, []byte("third")) {
		return fmt.Errorf("wrong value: got %q, want %q", got, "third")
	}

	return nil
}

func testRangeDeleteEntireDB(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	for i := range 100 {
		if err := database.Put(nil, []byte{byte(i)}, []byte("value")); err != nil {
			return fmt.Errorf("put: %w", err)
		}
	}

	if err := database.DeleteRange(nil, []byte{}, []byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		return fmt.Errorf("delete range: %w", err)
	}

	iter := database.NewIterator(nil)
	defer iter.Close()
	iter.SeekToFirst()
	if iter.Valid() {
		return fmt.Errorf("keys still exist after range delete")
	}

	return nil
}

func testNilKeyValue(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	// Should not panic
	_ = database.Put(nil, nil, []byte("value"))
	_, _ = database.Get(nil, nil)
	_ = database.Delete(nil, nil)

	// Nil value should work
	if err := database.Put(nil, []byte("key"), nil); err != nil {
		return fmt.Errorf("put nil value: %w", err)
	}

	return nil
}

// =============================================================================
// Corruption Tests
// =============================================================================

func runCorruptionTests() []testResult {
	tests := []struct {
		name string
		fn   func(string) error
	}{
		{"TruncateWAL", testTruncateWAL},
		{"CorruptWALCRC", testCorruptWALCRC},
		{"CorruptSSTBlock", testCorruptSSTBlock},
		{"ZeroFillWAL", testZeroFillWAL},
		{"DeleteCurrent", testDeleteCurrent},
	}

	return runTestSuite(tests)
}

func testTruncateWAL(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}

	for i := range 100 {
		if err := database.Put(nil, []byte{byte(i)}, bytes.Repeat([]byte{byte(i)}, 100)); err != nil {
			return fmt.Errorf("put: %w", err)
		}
	}
	_ = database.SyncWAL()
	database.Close()

	// Find and truncate WAL
	files, _ := os.ReadDir(dir)
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".log" {
			walPath := filepath.Join(dir, f.Name())
			info, _ := os.Stat(walPath)
			if info.Size() > 100 {
				_ = os.Truncate(walPath, info.Size()/2)
			}
			break
		}
	}

	// Should not panic on reopen
	database2, err := openDB(dir)
	if err != nil {
		return nil // Error is acceptable
	}
	database2.Close()
	return nil
}

func testCorruptWALCRC(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}

	for i := range 50 {
		if err := database.Put(nil, []byte{byte(i)}, bytes.Repeat([]byte{byte(i)}, 100)); err != nil {
			return fmt.Errorf("put: %w", err)
		}
	}
	_ = database.SyncWAL()
	database.Close()

	// Find and corrupt WAL
	files, _ := os.ReadDir(dir)
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".log" {
			walPath := filepath.Join(dir, f.Name())
			data, _ := os.ReadFile(walPath)
			if len(data) > 50 {
				offset := len(data) / 2
				data[offset] ^= 0xFF
				data[offset+1] ^= 0xFF
				_ = os.WriteFile(walPath, data, 0644)
			}
			break
		}
	}

	// Should not panic
	database2, err := openDB(dir)
	if err != nil {
		return nil // Error is acceptable
	}
	database2.Close()
	return nil
}

func testCorruptSSTBlock(dir string) error {
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024
	database, err := db.Open(dir, opts)
	if err != nil {
		return err
	}

	for i := range 100 {
		if err := database.Put(nil, []byte{byte(i)}, bytes.Repeat([]byte{byte(i)}, 50)); err != nil {
			return fmt.Errorf("put: %w", err)
		}
	}
	_ = database.Flush(nil)
	database.Close()

	// Find and corrupt SST
	files, _ := os.ReadDir(dir)
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			sstPath := filepath.Join(dir, f.Name())
			data, _ := os.ReadFile(sstPath)
			if len(data) > 100 {
				offset := len(data) / 2
				data[offset] ^= 0xFF
				_ = os.WriteFile(sstPath, data, 0644)
			}
			break
		}
	}

	// Should not panic
	database2, err := db.Open(dir, opts)
	if err != nil {
		return nil // Error is acceptable
	}
	database2.Close()
	return nil
}

func testZeroFillWAL(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}

	for i := range 50 {
		if err := database.Put(nil, []byte{byte(i)}, bytes.Repeat([]byte{byte(i)}, 50)); err != nil {
			return fmt.Errorf("put: %w", err)
		}
	}
	_ = database.SyncWAL()
	database.Close()

	// Zero-fill WAL region
	files, _ := os.ReadDir(dir)
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".log" {
			walPath := filepath.Join(dir, f.Name())
			data, _ := os.ReadFile(walPath)
			if len(data) > 200 {
				start := len(data) / 3
				for i := start; i < start+100 && i < len(data); i++ {
					data[i] = 0
				}
				_ = os.WriteFile(walPath, data, 0644)
			}
			break
		}
	}

	// Should not panic
	database2, err := openDB(dir)
	if err != nil {
		return nil
	}
	database2.Close()
	return nil
}

func testDeleteCurrent(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	for i := range 10 {
		_ = database.Put(nil, []byte{byte(i)}, []byte{byte(i)})
	}
	database.Close()

	// Delete CURRENT
	_ = os.Remove(filepath.Join(dir, "CURRENT"))

	// Without CreateIfMissing, should fail
	opts := db.DefaultOptions()
	opts.CreateIfMissing = false
	_, err = db.Open(dir, opts)
	if err == nil {
		return fmt.Errorf("should fail without CURRENT and CreateIfMissing=false")
	}

	return nil
}

// =============================================================================
// Go-Specific Tests
// =============================================================================

func runGoSpecificTests() []testResult {
	tests := []struct {
		name string
		fn   func(string) error
	}{
		{"SliceAliasing", testSliceAliasing},
		{"GoroutineLeakOpenClose", testGoroutineLeakOpenClose},
		{"GoroutineLeakIterator", testGoroutineLeakIterator},
	}

	return runTestSuite(tests)
}

func testSliceAliasing(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	key := []byte("test_key")
	original := []byte("original_value_12345")

	if err := database.Put(nil, key, original); err != nil {
		return fmt.Errorf("put: %w", err)
	}

	// Get and modify
	val1, err := database.Get(nil, key)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}

	// ATTACK: modify returned slice
	for i := range val1 {
		val1[i] = 'X'
	}

	// Get again - should NOT be affected
	val2, err := database.Get(nil, key)
	if err != nil {
		return fmt.Errorf("get2: %w", err)
	}

	if bytes.Equal(val2, val1) {
		return fmt.Errorf("VULNERABILITY: slice aliasing - modification affected subsequent Get")
	}

	if !bytes.Equal(val2, original) {
		return fmt.Errorf("wrong value: got %q, want %q", val2, original)
	}

	return nil
}

func testGoroutineLeakOpenClose(dir string) error {
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	for i := range 10 {
		d := filepath.Join(dir, fmt.Sprintf("db%d", i))
		database, err := openDB(d)
		if err != nil {
			return err
		}
		for j := range 10 {
			_ = database.Put(nil, []byte{byte(j)}, []byte{byte(j)})
		}
		database.Close()
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	final := runtime.NumGoroutine()

	leaked := final - baseline
	if leaked > 5 {
		return fmt.Errorf("goroutine leak: %d leaked", leaked)
	}

	return nil
}

func testGoroutineLeakIterator(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	for i := range 100 {
		_ = database.Put(nil, []byte{byte(i)}, []byte{byte(i)})
	}

	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	for range 100 {
		iter := database.NewIterator(nil)
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			_ = iter.Key()
		}
		iter.Close()
	}

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	final := runtime.NumGoroutine()

	leaked := final - baseline
	if leaked > 2 {
		return fmt.Errorf("goroutine leak from iterators: %d", leaked)
	}

	return nil
}

// =============================================================================
// Protocol Tests
// =============================================================================

func runProtocolTests() []testResult {
	tests := []struct {
		name string
		fn   func(string) error
	}{
		{"UseAfterClose", testUseAfterClose},
		{"DoubleClose", testDoubleClose},
		{"DoubleCloseIterator", testDoubleCloseIterator},
		{"EmptyBatch", testEmptyBatch},
		{"ConflictingOptions", testConflictingOptions},
	}

	return runTestSuite(tests)
}

func testUseAfterClose(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	database.Close()

	// Should return error, not panic
	err = database.Put(nil, []byte("key"), []byte("value"))
	if err == nil {
		return fmt.Errorf("put on closed DB should return error")
	}

	_, err = database.Get(nil, []byte("key"))
	if err == nil {
		return fmt.Errorf("get on closed DB should return error")
	}

	return nil
}

func testDoubleClose(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}

	database.Close()
	database.Close() // Should not panic

	return nil
}

func testDoubleCloseIterator(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	for i := range 10 {
		_ = database.Put(nil, []byte{byte(i)}, []byte{byte(i)})
	}

	iter := database.NewIterator(nil)
	iter.SeekToFirst()
	iter.Close()
	iter.Close() // Should not panic

	return nil
}

func testEmptyBatch(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	b := db.NewWriteBatch()
	_ = database.Write(nil, b) // Should not panic

	return nil
}

func testConflictingOptions(dir string) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	database.Close()

	opts := db.DefaultOptions()
	opts.ErrorIfExists = true
	_, err = db.Open(dir, opts)
	if err == nil {
		return fmt.Errorf("open with ErrorIfExists on existing DB should fail")
	}

	return nil
}

// =============================================================================
// Concurrency Tests
// =============================================================================

func runConcurrencyTests(threads int, duration time.Duration) []testResult {
	fmt.Printf("  Running concurrency stress for %s with %d threads...\n", duration, threads)

	dir, err := os.MkdirTemp("", "adversarial-concurrency-*")
	if err != nil {
		return []testResult{{name: "ConcurrencyStress", passed: false, message: err.Error()}}
	}
	if !*keepTempDirs {
		defer os.RemoveAll(dir)
	}

	start := time.Now()
	result := testConcurrencyStress(dir, threads, duration)
	elapsed := time.Since(start)

	if result == nil {
		globalStats.passed.Add(1)
		fmt.Printf("  ✅ ConcurrencyStress (%s)\n", elapsed.Truncate(time.Millisecond))
		return []testResult{{name: "ConcurrencyStress", passed: true, duration: elapsed}}
	}

	globalStats.failed.Add(1)
	fmt.Printf("  ❌ ConcurrencyStress: %v\n", result)
	return []testResult{{name: "ConcurrencyStress", passed: false, duration: elapsed, message: result.Error()}}
}

func testConcurrencyStress(dir string, threads int, duration time.Duration) error {
	database, err := openDB(dir)
	if err != nil {
		return err
	}
	defer database.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, threads)

	// Writers
	for i := range threads / 2 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := fmt.Appendf(nil, "key-%d-%d", id, time.Now().UnixNano())
					value := fmt.Appendf(nil, "value-%d", id)
					if err := database.Put(nil, key, value); err != nil {
						if !errors.Is(err, db.ErrDBClosed) {
							select {
							case errCh <- err:
							default:
							}
						}
						return
					}
				}
			}
		}(i)
	}

	// Readers
	for i := range threads / 2 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := fmt.Appendf(nil, "key-%d-%d", id, time.Now().UnixNano())
					_, _ = database.Get(nil, key) // Errors are expected for non-existent keys
				}
			}
		}(i)
	}

	// Wait for duration
	time.Sleep(duration)
	close(stop)
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// =============================================================================
// Helpers
// =============================================================================

func openDB(dir string) (db.DB, error) {
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	return db.Open(dir, opts)
}

func runTestSuite(tests []struct {
	name string
	fn   func(string) error
}) []testResult {
	results := make([]testResult, 0, len(tests))

	for _, t := range tests {
		dir, err := os.MkdirTemp("", "adversarial-*")
		if err != nil {
			results = append(results, testResult{name: t.name, passed: false, message: err.Error()})
			globalStats.failed.Add(1)
			continue
		}

		start := time.Now()
		err = t.fn(dir)
		elapsed := time.Since(start)

		if !*keepTempDirs {
			os.RemoveAll(dir)
		}

		if err != nil {
			results = append(results, testResult{name: t.name, passed: false, duration: elapsed, message: err.Error()})
			globalStats.failed.Add(1)
			fmt.Printf("  ❌ %s: %v\n", t.name, err)
		} else {
			results = append(results, testResult{name: t.name, passed: true, duration: elapsed})
			globalStats.passed.Add(1)
			fmt.Printf("  ✅ %s (%s)\n", t.name, elapsed.Truncate(time.Microsecond))
		}
	}

	return results
}

func printSummary(results []testResult, elapsed time.Duration) {
	passed := globalStats.passed.Load()
	failed := globalStats.failed.Load()
	total := passed + failed

	fmt.Println("═══════════════════════════════════════════════════════════════════════")
	fmt.Printf("Results: %d passed, %d failed, %d total\n", passed, failed, total)
	fmt.Printf("Duration: %s\n", elapsed.Truncate(time.Millisecond))
	fmt.Println("═══════════════════════════════════════════════════════════════════════")

	if failed > 0 {
		fmt.Println("\n❌ ADVERSARIAL TEST FAILED")
		fmt.Println("\nFailed tests:")
		for _, r := range results {
			if !r.passed {
				fmt.Printf("  - %s: %s\n", r.name, r.message)
			}
		}
	} else {
		fmt.Println("\n✅ ADVERSARIAL TEST PASSED")
	}
}

// cleanupOldTestDirs removes old test directories from previous runs.
// This handles cleanup from crashed test processes where defer didn't run.
func cleanupOldTestDirs() {
	tempDir := os.TempDir()
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		fmt.Printf("Warning: could not read temp dir for cleanup: %v\n", err)
		return
	}

	var cleaned int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) >= len(adversarialTestDirPrefix) && name[:len(adversarialTestDirPrefix)] == adversarialTestDirPrefix {
			fullPath := filepath.Join(tempDir, name)
			if err := os.RemoveAll(fullPath); err != nil {
				fmt.Printf("Warning: could not remove %s: %v\n", fullPath, err)
			} else {
				cleaned++
			}
		}
	}

	if cleaned > 0 {
		fmt.Printf("🧹 Cleaned up %d old test directories\n", cleaned)
	}
}
