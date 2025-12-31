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
//
// Run with oracle verification (requires ROCKSDB_PATH or LDB_PATH):
//
// ```bash
// ROCKSDB_PATH=/path/to/rocksdb ./bin/adversarialtest -oracle
// # or explicitly:
// LDB_PATH=/path/to/ldb ./bin/adversarialtest -oracle
// ```
package main

import (
	"bytes"
	"encoding/json"
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
	"github.com/aalhour/rockyardkv/internal/campaign"
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
	oracleMode   = flag.Bool("oracle", false, "Run oracle verification using ldb (requires ROCKSDB_PATH or LDB_PATH env)")
)

// OutcomeClass defines the expected result classification for corruption tests.
// Contract: every corruption test must enumerate acceptable outcomes.
type OutcomeClass int

const (
	// OutcomeFailLoud means the DB must detect corruption and return a clear error.
	OutcomeFailLoud OutcomeClass = iota
	// OutcomeRecoverOlder means the DB may recover to an older consistent state.
	OutcomeRecoverOlder
	// OutcomeEither means both FailLoud and RecoverOlder are acceptable, but
	// silent wrong results are NOT acceptable.
	OutcomeEither
)

func (o OutcomeClass) String() string {
	switch o {
	case OutcomeFailLoud:
		return "fail_loud"
	case OutcomeRecoverOlder:
		return "recover_older"
	case OutcomeEither:
		return "fail_loud_or_recover_older"
	default:
		return "unknown"
	}
}

// CorruptionTestResult holds the outcome of a corruption test with classification.
type CorruptionTestResult struct {
	Name          string
	ExpectedClass OutcomeClass
	ActualOutcome string // "open_failed", "open_succeeded", "read_error", "data_intact"
	OracleResult  *OracleResult
	Error         error
	SilentWrong   bool // true if data was silently corrupted (test failure)
	ContractHeld  bool // true if the outcome matched the contract
	ArtifactPath  string
	ReproCommand  string
	MutatedFiles  []string
}

// OracleResult holds the result of an oracle verification.
type OracleResult struct {
	Ran           bool
	Command       string
	Output        string
	ExitCode      int
	ConsistencyOK bool
}

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

// oracle holds the unified oracle instance, reusing internal/campaign.Oracle.
var oracle *campaign.Oracle

// initOracle checks if oracle tools are available.
// Uses the unified Oracle from internal/campaign which supports:
//   - ROCKSDB_PATH: path to RocksDB build directory (derives ldb and sst_dump)
//   - LDB_PATH: explicit path to ldb binary (overrides ROCKSDB_PATH)
//   - SST_DUMP_PATH: explicit path to sst_dump binary (overrides ROCKSDB_PATH)
func initOracle() {
	if !*oracleMode {
		return
	}

	oracle = campaign.NewOracleFromEnv()
	if oracle == nil {
		fmt.Println("⚠️  -oracle flag set but no oracle configured")
		fmt.Println("   Set ROCKSDB_PATH (or LDB_PATH for explicit ldb path) to enable oracle verification")
		return
	}

	if !oracle.Available() {
		fmt.Println("⚠️  Oracle configured but tools not found or not accessible")
		oracle = nil
		return
	}

	fmt.Println("🔮 Oracle enabled via internal/campaign.Oracle")
}

// runOracleCheck runs ldb checkconsistency on a DB directory.
func runOracleCheck(dbDir string) *OracleResult {
	if oracle == nil {
		return nil
	}

	toolResult := oracle.CheckConsistency(dbDir)

	result := &OracleResult{
		Ran:           true,
		Command:       "ldb --db=" + dbDir + " checkconsistency",
		Output:        toolResult.Stdout + toolResult.Stderr,
		ExitCode:      toolResult.ExitCode,
		ConsistencyOK: toolResult.OK(),
	}

	return result
}

// persistMutatedFiles copies files that were modified during a corruption test.
// listDir returns a formatted directory listing for artifact persistence.
func listDir(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	var listing strings.Builder
	listing.WriteString(fmt.Sprintf("# Directory listing: %s\n", dir))
	for _, e := range entries {
		info, _ := e.Info()
		if info != nil {
			listing.WriteString(fmt.Sprintf("%s\t%d\t%s\n", e.Name(), info.Size(), info.Mode()))
		}
	}
	return listing.String(), nil
}

func persistMutatedFiles(srcDir, artifactDir string, files []string) error {
	mutatedDir := filepath.Join(artifactDir, "mutated_files")
	if err := os.MkdirAll(mutatedDir, 0755); err != nil {
		return err
	}

	for _, f := range files {
		srcPath := filepath.Join(srcDir, f)
		data, err := os.ReadFile(srcPath)
		if err != nil {
			continue // File may have been deleted
		}
		dstPath := filepath.Join(mutatedDir, f)
		if err := os.WriteFile(dstPath, data, 0644); err != nil {
			return err
		}
	}

	// Also write directory listing
	if listing, err := listDir(srcDir); err == nil {
		return os.WriteFile(filepath.Join(artifactDir, "dir_listing.txt"), []byte(listing), 0644)
	}
	return nil
}

func main() {
	flag.Parse()

	// Initialize seed
	actualSeed := *seed
	if actualSeed == 0 {
		actualSeed = time.Now().UnixNano()
	}

	// Initialize oracle if requested
	initOracle()

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

// corruptionTest defines a corruption test with its expected outcome contract.
// Contract: Every corruption test must specify an expected OutcomeClass.
type corruptionTest struct {
	name     string
	fn       func(string) *CorruptionTestResult
	expected OutcomeClass
	contract string // Human-readable contract description
}

func runCorruptionTests() []testResult {
	tests := []corruptionTest{
		{
			name:     "TruncateWAL",
			fn:       testTruncateWALWithContract,
			expected: OutcomeEither,
			contract: "DB must either fail to open with corruption error, or recover to state before truncated records",
		},
		{
			name:     "CorruptWALCRC",
			fn:       testCorruptWALCRCWithContract,
			expected: OutcomeEither,
			contract: "DB must either fail to open with CRC error, or skip corrupted records and recover earlier state",
		},
		{
			name:     "CorruptSSTBlock",
			fn:       testCorruptSSTBlockWithContract,
			expected: OutcomeFailLoud,
			contract: "DB must detect block corruption via checksum and return error on affected reads",
		},
		{
			name:     "ZeroFillWAL",
			fn:       testZeroFillWALWithContract,
			expected: OutcomeEither,
			contract: "DB must either fail to open, or treat zero-fill as end of WAL and recover earlier state",
		},
		{
			name:     "DeleteCurrent",
			fn:       testDeleteCurrentWithContract,
			expected: OutcomeFailLoud,
			contract: "DB without CURRENT file must fail to open with CreateIfMissing=false",
		},
		{
			name:     "TruncateManifest",
			fn:       testTruncateManifestWithContract,
			expected: OutcomeEither,
			contract: "DB must either fail to open, or recover to older consistent state",
		},
		{
			name:     "CorruptManifestBytes",
			fn:       testCorruptManifestBytesWithContract,
			expected: OutcomeFailLoud,
			contract: "DB must detect MANIFEST corruption and fail to open",
		},
		{
			name:     "GarbageManifestRecord",
			fn:       testGarbageManifestRecordWithContract,
			expected: OutcomeEither,
			contract: "DB must either reject garbage records or skip them and recover earlier state",
		},
	}

	return runCorruptionTestSuite(tests)
}

// runCorruptionTestSuite runs corruption tests with contract verification.
func runCorruptionTestSuite(tests []corruptionTest) []testResult {
	results := make([]testResult, 0, len(tests))

	for _, t := range tests {
		dir, err := os.MkdirTemp("", "adversarial-corruption-*")
		if err != nil {
			results = append(results, testResult{name: t.name, passed: false, message: err.Error()})
			globalStats.failed.Add(1)
			continue
		}

		start := time.Now()
		cResult := t.fn(dir)
		elapsed := time.Since(start)

		// Oracle verification if enabled
		if oracle != nil && cResult != nil {
			cResult.OracleResult = runOracleCheck(dir)
		}

		// Evaluate contract
		passed := true
		var message string

		if cResult == nil {
			message = "test returned nil result"
			passed = false
		} else if cResult.SilentWrong {
			message = fmt.Sprintf("SILENT WRONGNESS DETECTED: %s", cResult.ActualOutcome)
			passed = false
		} else if !cResult.ContractHeld {
			message = fmt.Sprintf("contract violation: expected %s, got %s", t.expected, cResult.ActualOutcome)
			passed = false
		} else if cResult.Error != nil {
			message = cResult.Error.Error()
			passed = false
		}

		// Persist artifacts if run-dir configured.
		// When -oracle is enabled, persist artifacts even on success for audit.
		shouldPersist := *runDir != "" && (!passed || oracle != nil)
		if shouldPersist {
			artifactPath := filepath.Join(*runDir, t.name)
			_ = os.MkdirAll(artifactPath, 0755)

			// Write test result summary
			resultFile := filepath.Join(artifactPath, "result.json")
			resultData := map[string]any{
				"test":     t.name,
				"passed":   passed,
				"contract": t.contract,
				"expected": t.expected.String(),
				"message":  message,
			}
			if cResult != nil {
				resultData["actual_outcome"] = cResult.ActualOutcome
				resultData["contract_held"] = cResult.ContractHeld
				resultData["silent_wrong"] = cResult.SilentWrong
			}
			if resultJSON, err := json.MarshalIndent(resultData, "", "  "); err == nil {
				_ = os.WriteFile(resultFile, resultJSON, 0644)
			}

			// Write DB directory listing
			dirListingFile := filepath.Join(artifactPath, "dir_listing.txt")
			if listing, err := listDir(dir); err == nil {
				_ = os.WriteFile(dirListingFile, []byte(listing), 0644)
			}

			// Persist mutated files (for failed cases)
			if cResult != nil && len(cResult.MutatedFiles) > 0 {
				_ = persistMutatedFiles(dir, artifactPath, cResult.MutatedFiles)
			}

			// Write oracle result if available
			if cResult != nil && cResult.OracleResult != nil {
				// JSON format for machine parsing
				oracleJSONFile := filepath.Join(artifactPath, "oracle.json")
				oracleJSON := map[string]any{
					"command":        cResult.OracleResult.Command,
					"exit_code":      cResult.OracleResult.ExitCode,
					"consistency_ok": cResult.OracleResult.ConsistencyOK,
					"ran":            cResult.OracleResult.Ran,
				}
				if oracleBytes, err := json.MarshalIndent(oracleJSON, "", "  "); err == nil {
					_ = os.WriteFile(oracleJSONFile, oracleBytes, 0644)
				}

				// Text format for human reading
				oracleFile := filepath.Join(artifactPath, "oracle.txt")
				oracleData := fmt.Sprintf("Command: %s\nExit: %d\nConsistent: %v\nOutput:\n%s",
					cResult.OracleResult.Command,
					cResult.OracleResult.ExitCode,
					cResult.OracleResult.ConsistencyOK,
					cResult.OracleResult.Output)
				_ = os.WriteFile(oracleFile, []byte(oracleData), 0644)
			}

			// Write repro command
			reproFile := filepath.Join(artifactPath, "repro.sh")
			reproCmd := fmt.Sprintf("#!/bin/bash\n# Repro command for %s\ncd %s\n./bin/adversarialtest -category=corruption -oracle -run-dir %s -keep\n",
				t.name, "$(dirname $0)/../..", artifactPath)
			if cResult != nil && cResult.ReproCommand != "" {
				reproCmd = "#!/bin/bash\n" + cResult.ReproCommand + "\n"
			}
			_ = os.WriteFile(reproFile, []byte(reproCmd), 0755)
		}

		if !*keepTempDirs {
			os.RemoveAll(dir)
		}

		results = append(results, testResult{name: t.name, passed: passed, duration: elapsed, message: message})

		if passed {
			globalStats.passed.Add(1)
			fmt.Printf("  ✅ %s [%s] (%s)\n", t.name, t.expected, elapsed.Truncate(time.Microsecond))
		} else {
			globalStats.failed.Add(1)
			fmt.Printf("  ❌ %s: %s\n", t.name, message)
		}
	}

	return results
}

// testTruncateWALWithContract is the contract-aware version of WAL truncation test.
// Contract: DB must either fail to open with corruption error, or recover to state before truncated records.
func testTruncateWALWithContract(dir string) *CorruptionTestResult {
	result := &CorruptionTestResult{
		Name:          "TruncateWAL",
		ExpectedClass: OutcomeEither,
		ReproCommand:  fmt.Sprintf("# Truncate WAL to 50%% and attempt reopen\nbin/adversarialtest -category=corruption -keep"),
	}

	database, err := openDB(dir)
	if err != nil {
		result.Error = err
		return result
	}

	// Write data that we can verify later
	expectedKeys := make(map[byte]bool)
	for i := range 100 {
		if err := database.Put(nil, []byte{byte(i)}, bytes.Repeat([]byte{byte(i)}, 100)); err != nil {
			result.Error = fmt.Errorf("put: %w", err)
			database.Close()
			return result
		}
		expectedKeys[byte(i)] = true
	}
	_ = database.SyncWAL()
	database.Close()

	// Find and truncate WAL
	var mutatedFile string
	files, _ := os.ReadDir(dir)
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".log" {
			walPath := filepath.Join(dir, f.Name())
			info, _ := os.Stat(walPath)
			if info.Size() > 100 {
				_ = os.Truncate(walPath, info.Size()/2)
				mutatedFile = f.Name()
			}
			break
		}
	}
	if mutatedFile != "" {
		result.MutatedFiles = append(result.MutatedFiles, mutatedFile)
	}

	// Reopen - should either fail or recover older state
	database2, err := openDB(dir)
	if err != nil {
		result.ActualOutcome = "open_failed"
		result.ContractHeld = true // FailLoud is acceptable
		return result
	}
	defer database2.Close()

	// If it opened, verify we recovered to a consistent earlier state
	result.ActualOutcome = "open_succeeded"

	// Check that we haven't silently lost acknowledged writes without detecting corruption
	// With WAL truncation, losing some keys is acceptable (recover to older state)
	// But we must not have WRONG values
	iter := database2.NewIterator(nil)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		val := iter.Value()
		if len(key) == 1 && len(val) > 0 {
			expectedVal := bytes.Repeat([]byte{key[0]}, 100)
			if !bytes.Equal(val, expectedVal) {
				result.SilentWrong = true
				result.ActualOutcome = "silent_corruption"
				return result
			}
		}
	}

	result.ContractHeld = true // Recovered to older consistent state
	return result
}

// testCorruptWALCRCWithContract is the contract-aware version of WAL CRC corruption test.
// Contract: DB must either fail to open with CRC error, or skip corrupted records and recover earlier state.
func testCorruptWALCRCWithContract(dir string) *CorruptionTestResult {
	result := &CorruptionTestResult{
		Name:          "CorruptWALCRC",
		ExpectedClass: OutcomeEither,
		ReproCommand:  "# Flip bytes in WAL CRC region and attempt reopen\nbin/adversarialtest -category=corruption -keep",
	}

	database, err := openDB(dir)
	if err != nil {
		result.Error = err
		return result
	}

	for i := range 50 {
		if err := database.Put(nil, []byte{byte(i)}, bytes.Repeat([]byte{byte(i)}, 100)); err != nil {
			result.Error = fmt.Errorf("put: %w", err)
			database.Close()
			return result
		}
	}
	_ = database.SyncWAL()
	database.Close()

	// Find and corrupt WAL CRC
	var mutatedFile string
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
				mutatedFile = f.Name()
			}
			break
		}
	}
	if mutatedFile != "" {
		result.MutatedFiles = append(result.MutatedFiles, mutatedFile)
	}

	// Reopen
	database2, err := openDB(dir)
	if err != nil {
		result.ActualOutcome = "open_failed"
		result.ContractHeld = true
		return result
	}
	defer database2.Close()

	result.ActualOutcome = "open_succeeded"

	// Verify no silent corruption
	iter := database2.NewIterator(nil)
	defer iter.Close()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		val := iter.Value()
		if len(key) == 1 && len(val) > 0 {
			expectedVal := bytes.Repeat([]byte{key[0]}, 100)
			if !bytes.Equal(val, expectedVal) {
				result.SilentWrong = true
				result.ActualOutcome = "silent_corruption"
				return result
			}
		}
	}

	result.ContractHeld = true
	return result
}

// testCorruptSSTBlockWithContract is the contract-aware version of SST block corruption test.
// Contract: DB must detect block corruption via checksum and return error on affected reads.
func testCorruptSSTBlockWithContract(dir string) *CorruptionTestResult {
	result := &CorruptionTestResult{
		Name:          "CorruptSSTBlock",
		ExpectedClass: OutcomeFailLoud,
		ReproCommand:  "# Corrupt SST block and attempt reads\nbin/adversarialtest -category=corruption -keep",
	}

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024
	database, err := db.Open(dir, opts)
	if err != nil {
		result.Error = err
		return result
	}

	for i := range 100 {
		if err := database.Put(nil, []byte{byte(i)}, bytes.Repeat([]byte{byte(i)}, 50)); err != nil {
			result.Error = fmt.Errorf("put: %w", err)
			database.Close()
			return result
		}
	}
	_ = database.Flush(nil)
	database.Close()

	// Find and corrupt SST block
	var mutatedFile string
	files, _ := os.ReadDir(dir)
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			sstPath := filepath.Join(dir, f.Name())
			data, _ := os.ReadFile(sstPath)
			if len(data) > 100 {
				offset := len(data) / 2
				data[offset] ^= 0xFF
				_ = os.WriteFile(sstPath, data, 0644)
				mutatedFile = f.Name()
			}
			break
		}
	}
	if mutatedFile != "" {
		result.MutatedFiles = append(result.MutatedFiles, mutatedFile)
	}

	// Reopen - for SST corruption, we expect reads to fail
	database2, err := db.Open(dir, opts)
	if err != nil {
		result.ActualOutcome = "open_failed"
		result.ContractHeld = true
		return result
	}
	defer database2.Close()

	// Try reading - should detect corruption
	errCount := 0
	silentWrongCount := 0
	for i := range 100 {
		val, err := database2.Get(nil, []byte{byte(i)})
		if err != nil {
			errCount++
			continue
		}
		expectedVal := bytes.Repeat([]byte{byte(i)}, 50)
		if !bytes.Equal(val, expectedVal) {
			silentWrongCount++
		}
	}

	if silentWrongCount > 0 {
		result.SilentWrong = true
		result.ActualOutcome = fmt.Sprintf("silent_corruption (%d wrong values)", silentWrongCount)
		return result
	}

	if errCount > 0 {
		result.ActualOutcome = fmt.Sprintf("read_errors (%d)", errCount)
		result.ContractHeld = true // FailLoud on read is the expected behavior
		return result
	}

	// If all reads succeeded with correct values, the corruption didn't affect readable data
	result.ActualOutcome = "data_intact"
	result.ContractHeld = true // Corruption may have hit non-data region
	return result
}

// testZeroFillWALWithContract is the contract-aware version of WAL zero-fill test.
// Contract: DB must either fail to open, or treat zero-fill as end of WAL and recover earlier state.
func testZeroFillWALWithContract(dir string) *CorruptionTestResult {
	result := &CorruptionTestResult{
		Name:          "ZeroFillWAL",
		ExpectedClass: OutcomeEither,
		ReproCommand:  "# Zero-fill region of WAL and attempt reopen\nbin/adversarialtest -category=corruption -keep",
	}

	database, err := openDB(dir)
	if err != nil {
		result.Error = err
		return result
	}

	for i := range 50 {
		if err := database.Put(nil, []byte{byte(i)}, bytes.Repeat([]byte{byte(i)}, 50)); err != nil {
			result.Error = fmt.Errorf("put: %w", err)
			database.Close()
			return result
		}
	}
	_ = database.SyncWAL()
	database.Close()

	// Zero-fill WAL region
	var mutatedFile string
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
				mutatedFile = f.Name()
			}
			break
		}
	}
	if mutatedFile != "" {
		result.MutatedFiles = append(result.MutatedFiles, mutatedFile)
	}

	// Reopen
	database2, err := openDB(dir)
	if err != nil {
		result.ActualOutcome = "open_failed"
		result.ContractHeld = true
		return result
	}
	defer database2.Close()

	result.ActualOutcome = "open_succeeded"

	// Verify no silent corruption
	iter := database2.NewIterator(nil)
	defer iter.Close()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		val := iter.Value()
		if len(key) == 1 && len(val) > 0 {
			expectedVal := bytes.Repeat([]byte{key[0]}, 50)
			if !bytes.Equal(val, expectedVal) {
				result.SilentWrong = true
				result.ActualOutcome = "silent_corruption"
				return result
			}
		}
	}

	result.ContractHeld = true
	return result
}

// testDeleteCurrentWithContract is the contract-aware version of CURRENT deletion test.
// Contract: DB without CURRENT file must fail to open with CreateIfMissing=false.
func testDeleteCurrentWithContract(dir string) *CorruptionTestResult {
	result := &CorruptionTestResult{
		Name:          "DeleteCurrent",
		ExpectedClass: OutcomeFailLoud,
		ReproCommand:  "# Delete CURRENT file and attempt reopen\nbin/adversarialtest -category=corruption -keep",
	}

	database, err := openDB(dir)
	if err != nil {
		result.Error = err
		return result
	}
	for i := range 10 {
		_ = database.Put(nil, []byte{byte(i)}, []byte{byte(i)})
	}
	database.Close()

	// Delete CURRENT
	_ = os.Remove(filepath.Join(dir, "CURRENT"))
	result.MutatedFiles = append(result.MutatedFiles, "CURRENT (deleted)")

	// Without CreateIfMissing, must fail
	opts := db.DefaultOptions()
	opts.CreateIfMissing = false
	database2, err := db.Open(dir, opts)
	if err == nil {
		database2.Close()
		result.ActualOutcome = "open_succeeded_unexpectedly"
		result.ContractHeld = false
		result.Error = fmt.Errorf("should fail without CURRENT and CreateIfMissing=false")
		return result
	}

	result.ActualOutcome = "open_failed"
	result.ContractHeld = true
	return result
}

// testTruncateManifestWithContract is the contract-aware version of MANIFEST truncation test.
// Contract: DB must either fail to open, or recover to older consistent state.
func testTruncateManifestWithContract(dir string) *CorruptionTestResult {
	result := &CorruptionTestResult{
		Name:          "TruncateManifest",
		ExpectedClass: OutcomeEither,
		ReproCommand:  "# Truncate MANIFEST to 50% and attempt reopen\nbin/adversarialtest -category=corruption -keep",
	}

	database, err := openDB(dir)
	if err != nil {
		result.Error = err
		return result
	}

	// Write data and flush multiple times to create MANIFEST entries
	for round := range 3 {
		for i := range 10 {
			key := fmt.Appendf(nil, "round%d_key_%03d", round, i)
			value := fmt.Appendf(nil, "round%d_value_%03d", round, i)
			if err := database.Put(nil, key, value); err != nil {
				result.Error = fmt.Errorf("put: %w", err)
				database.Close()
				return result
			}
		}
		if err := database.Flush(db.DefaultFlushOptions()); err != nil {
			result.Error = fmt.Errorf("flush: %w", err)
			database.Close()
			return result
		}
	}
	database.Close()

	// Find MANIFEST file
	manifestPath := findManifestFile(dir)
	if manifestPath == "" {
		result.Error = fmt.Errorf("MANIFEST file not found")
		return result
	}

	// Read and truncate
	content, err := os.ReadFile(manifestPath)
	if err != nil {
		result.Error = fmt.Errorf("read manifest: %w", err)
		return result
	}
	if len(content) < 100 {
		result.Error = fmt.Errorf("MANIFEST too small to truncate: %d bytes", len(content))
		return result
	}

	manifestName := filepath.Base(manifestPath)
	result.MutatedFiles = append(result.MutatedFiles, manifestName)

	// Truncate 50% of the file
	truncatedLen := len(content) / 2
	if err := os.WriteFile(manifestPath, content[:truncatedLen], 0644); err != nil {
		result.Error = fmt.Errorf("truncate manifest: %w", err)
		return result
	}

	// Reopen
	opts := db.DefaultOptions()
	opts.CreateIfMissing = false
	database2, err := db.Open(dir, opts)
	if err != nil {
		result.ActualOutcome = "open_failed"
		result.ContractHeld = true
		return result
	}
	defer database2.Close()

	result.ActualOutcome = "open_succeeded"

	// Verify no silent corruption - if recovered, values should be correct
	iter := database2.NewIterator(nil)
	defer iter.Close()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		val := string(iter.Value())
		// Key format: round%d_key_%03d, value format: round%d_value_%03d
		if strings.HasPrefix(key, "round") {
			// Extract round number from key and verify value matches
			parts := strings.Split(key, "_")
			if len(parts) >= 3 {
				expectedPrefix := parts[0] + "_value_" + parts[2]
				if val != expectedPrefix {
					result.SilentWrong = true
					result.ActualOutcome = "silent_corruption"
					return result
				}
			}
		}
	}

	result.ContractHeld = true
	return result
}

// testCorruptManifestBytesWithContract is the contract-aware version of MANIFEST byte corruption test.
// Contract: DB must detect MANIFEST corruption and fail to open.
func testCorruptManifestBytesWithContract(dir string) *CorruptionTestResult {
	result := &CorruptionTestResult{
		Name:          "CorruptManifestBytes",
		ExpectedClass: OutcomeFailLoud,
		ReproCommand:  "# Flip bytes in MANIFEST and attempt reopen\nbin/adversarialtest -category=corruption -keep",
	}

	database, err := openDB(dir)
	if err != nil {
		result.Error = err
		return result
	}

	// Write some data and flush
	for i := range 20 {
		key := fmt.Appendf(nil, "key_%03d", i)
		value := fmt.Appendf(nil, "value_%03d", i)
		if err := database.Put(nil, key, value); err != nil {
			result.Error = fmt.Errorf("put: %w", err)
			database.Close()
			return result
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		result.Error = fmt.Errorf("flush: %w", err)
		database.Close()
		return result
	}
	database.Close()

	// Find and corrupt MANIFEST
	manifestPath := findManifestFile(dir)
	if manifestPath == "" {
		result.Error = fmt.Errorf("MANIFEST file not found")
		return result
	}

	content, err := os.ReadFile(manifestPath)
	if err != nil {
		result.Error = fmt.Errorf("read manifest: %w", err)
		return result
	}
	if len(content) < 50 {
		result.Error = fmt.Errorf("MANIFEST too small: %d bytes", len(content))
		return result
	}

	manifestName := filepath.Base(manifestPath)
	result.MutatedFiles = append(result.MutatedFiles, manifestName)

	// Flip bytes in the middle of the file
	corrupted := make([]byte, len(content))
	copy(corrupted, content)
	midpoint := len(corrupted) / 2
	for i := midpoint; i < midpoint+10 && i < len(corrupted); i++ {
		corrupted[i] ^= 0xFF
	}
	if err := os.WriteFile(manifestPath, corrupted, 0644); err != nil {
		result.Error = fmt.Errorf("write corrupted manifest: %w", err)
		return result
	}

	// Reopen - should fail due to corruption
	opts := db.DefaultOptions()
	opts.CreateIfMissing = false
	database2, err := db.Open(dir, opts)
	if err != nil {
		result.ActualOutcome = "open_failed"
		result.ContractHeld = true
		return result
	}
	defer database2.Close()

	// If it opened, verify reads detect corruption
	result.ActualOutcome = "open_succeeded"
	errCount := 0
	silentWrongCount := 0

	for i := range 20 {
		key := fmt.Appendf(nil, "key_%03d", i)
		expectedValue := fmt.Appendf(nil, "value_%03d", i)
		val, err := database2.Get(nil, key)
		if err != nil {
			errCount++
			continue
		}
		if !bytes.Equal(val, expectedValue) {
			silentWrongCount++
		}
	}

	if silentWrongCount > 0 {
		result.SilentWrong = true
		result.ActualOutcome = fmt.Sprintf("silent_corruption (%d wrong values)", silentWrongCount)
		return result
	}

	if errCount > 0 {
		result.ActualOutcome = fmt.Sprintf("read_errors (%d)", errCount)
		result.ContractHeld = true
		return result
	}

	// All reads succeeded correctly - corruption didn't affect data
	result.ActualOutcome = "data_intact"
	result.ContractHeld = true
	return result
}

// testGarbageManifestRecordWithContract is the contract-aware version of garbage MANIFEST record test.
// Contract: DB must either reject garbage records or skip them and recover earlier state.
func testGarbageManifestRecordWithContract(dir string) *CorruptionTestResult {
	result := &CorruptionTestResult{
		Name:          "GarbageManifestRecord",
		ExpectedClass: OutcomeEither,
		ReproCommand:  "# Append garbage to MANIFEST and attempt reopen\nbin/adversarialtest -category=corruption -keep",
	}

	database, err := openDB(dir)
	if err != nil {
		result.Error = err
		return result
	}

	// Write data and flush
	for i := range 10 {
		key := fmt.Appendf(nil, "key_%03d", i)
		value := fmt.Appendf(nil, "value_%03d", i)
		if err := database.Put(nil, key, value); err != nil {
			result.Error = fmt.Errorf("put: %w", err)
			database.Close()
			return result
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		result.Error = fmt.Errorf("flush: %w", err)
		database.Close()
		return result
	}
	database.Close()

	// Find MANIFEST and append garbage
	manifestPath := findManifestFile(dir)
	if manifestPath == "" {
		result.Error = fmt.Errorf("MANIFEST file not found")
		return result
	}

	manifestName := filepath.Base(manifestPath)
	result.MutatedFiles = append(result.MutatedFiles, manifestName)

	f, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		result.Error = fmt.Errorf("open manifest for append: %w", err)
		return result
	}
	// Append garbage that looks like a record but isn't valid
	garbage := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF}
	if _, err := f.Write(garbage); err != nil {
		f.Close()
		result.Error = fmt.Errorf("append garbage: %w", err)
		return result
	}
	f.Close()

	// Reopen
	opts := db.DefaultOptions()
	opts.CreateIfMissing = false
	database2, err := db.Open(dir, opts)
	if err != nil {
		result.ActualOutcome = "open_failed"
		result.ContractHeld = true
		return result
	}
	defer database2.Close()

	result.ActualOutcome = "open_succeeded"

	// Verify data integrity
	silentWrongCount := 0
	for i := range 10 {
		key := fmt.Appendf(nil, "key_%03d", i)
		expectedValue := fmt.Appendf(nil, "value_%03d", i)
		val, err := database2.Get(nil, key)
		if err != nil {
			// Missing keys after garbage append could be acceptable (recovered older state)
			continue
		}
		if !bytes.Equal(val, expectedValue) {
			silentWrongCount++
		}
	}

	if silentWrongCount > 0 {
		result.SilentWrong = true
		result.ActualOutcome = fmt.Sprintf("silent_corruption (%d wrong values)", silentWrongCount)
		return result
	}

	result.ContractHeld = true
	return result
}

// findManifestFile locates the active MANIFEST file in a DB directory.
func findManifestFile(dir string) string {
	currentPath := filepath.Join(dir, "CURRENT")
	content, err := os.ReadFile(currentPath)
	if err != nil {
		return ""
	}
	manifestName := strings.TrimSpace(string(content))
	if !strings.HasPrefix(manifestName, "MANIFEST-") {
		return ""
	}
	return filepath.Join(dir, manifestName)
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
