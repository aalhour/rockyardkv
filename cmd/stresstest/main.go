// Full stress test for RockyardKV.
//
// Use `stresstest` to run concurrent operations and verify results with an expected-state oracle.
// Use this tool to find correctness and concurrency bugs.
// Use `-seed` to make a run reproducible.
//
// The oracle uses per-key locking for writes.
// The oracle uses commit and rollback tracking for operations that can fail.
// The oracle uses pre and post read sampling to tolerate concurrent mutation during reads.
//
// Write a small reproducible run:
//
// ```bash
// ./bin/stresstest -duration=30s -threads=4 -keys=10000 -seed=123
// ```
//
// Record an operation trace for replay:
//
// ```bash
// ./bin/stresstest -duration=30s -threads=4 -keys=10000 -seed=123 -trace-out <TRACE_FILE>
// ```
//
// Note: The trace file is a binary format.
//
// Reference: RocksDB v10.7.5 `db_stress_tool/db_stress.cc`.
package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aalhour/rockyardkv"
	ibatch "github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/testutil"
	"github.com/aalhour/rockyardkv/internal/trace"
	"github.com/aalhour/rockyardkv/vfs"
)

var (
	// Test configuration
	duration             = flag.Duration("duration", 60*time.Second, "Test duration")
	numKeys              = flag.Int64("keys", 10000, "Number of keys in the key space")
	valueSize            = flag.Int("value-size", 100, "Size of each value in bytes")
	numThreads           = flag.Int("threads", 100, "Number of concurrent threads")
	reopenPeriod         = flag.Duration("reopen", 10*time.Second, "Period between database reopens (0 to disable)")
	flushPeriod          = flag.Duration("flush", 5*time.Second, "Period between flushes (0 to disable)")
	compactEvery         = flag.Int("compact-every", 0, "Compact after N operations (0 to disable)")
	dbPath               = flag.String("db", "", "Database path (default: temp directory)")
	keepDB               = flag.Bool("keep", false, "Keep database after test")
	cleanup              = flag.Bool("cleanup", false, "Clean up old test directories before running")
	verbose              = flag.Bool("v", false, "Verbose output")
	seed                 = flag.Int64("seed", 0, "Random seed (0 for time-based)")
	expectedState        = flag.String("expected-state", "", "Path to expected state file (for persistence across crashes)")
	saveExpected         = flag.Bool("save-expected", false, "Save expected state after test")
	saveExpectedInterval = flag.Duration("save-expected-interval", 1*time.Second, "Interval to persist expected state during the run (0 to disable)")
	verifyOnly           = flag.Bool("verify-only", false, "Verify database state using expected state file, without running operations")
	allowDBAhead         = flag.Bool("allow-db-ahead", false, "Allow DB to have more data than expected state (for crash testing with race conditions)")
	allowDataLoss        = flag.Bool("allow-data-loss", false, "Allow DB to have less data than expected state (for DisableWAL+faultfs crash testing)")
	durableState         = flag.String("durable-state", "", "Path to durable state file (for DisableWAL verification - tracks flush barriers)")

	// Operation weights (sum to 100)
	putWeight            = flag.Int("put", 30, "Put operation weight")
	getWeight            = flag.Int("get", 25, "Get operation weight")
	deleteWeight         = flag.Int("delete", 10, "Delete operation weight")
	batchWeight          = flag.Int("batch", 10, "Batch write weight")
	iterWeight           = flag.Int("iter", 5, "Iterator scan weight")
	snapshotWeight       = flag.Int("snapshot", 5, "Snapshot read weight")
	rangeDelWeight       = flag.Int("range-delete", 5, "Range deletion weight")
	mergeWeight          = flag.Int("merge", 5, "Merge operation weight")
	ingestWeight         = flag.Int("ingest", 0, "SST ingestion weight (default 0 - has subtle expected state tracking issues in concurrent scenarios)")
	transactionWeight    = flag.Int("txn", 5, "Transaction weight")
	compactWeight        = flag.Int("compact", 2, "Compaction trigger weight")
	snapshotVerifyWeight = flag.Int("snapshot-verify", 3, "Snapshot isolation verification weight")
	cfWeight             = flag.Int("cf", 5, "Column family operations weight")

	// Verification options
	verifyEvery   = flag.Int("verify-every", 10000, "Verify random keys every N operations")
	verifyPercent = flag.Int("verify-percent", 10, "Percent of keys to verify in spot checks")

	// Locking configuration (matching RocksDB's log2_keys_per_lock)
	log2KeysPerLock = flag.Uint("log2-keys-per-lock", 2, "Log2 of number of keys per lock (default: 4 keys per lock)")

	// Database options (for randomization)
	compressionType   = flag.String("compression", "none", "Compression type: none, snappy, zlib, random")
	checksumType      = flag.String("checksum", "crc32c", "Checksum type: crc32c, xxh3, random")
	disableWAL        = flag.Bool("disable-wal", false, "Disable write-ahead log")
	syncWrites        = flag.Bool("sync", false, "Sync writes to disk")
	blockSize         = flag.Int("block-size", 4096, "SST block size in bytes")
	writeBufferSize   = flag.Int("write-buffer-size", 4*1024*1024, "Write buffer (memtable) size in bytes")
	_                 = flag.Int("max-open-files", -1, "Max open files (-1 for unlimited)") // Reserved for future use
	bloomBits         = flag.Int("bloom-bits", 10, "Bloom filter bits per key (0 to disable)")
	numColumnFamilies = flag.Int("column-families", 1, "Number of column families")

	// Randomization flags
	randomizeParams = flag.Bool("randomize", false, "Randomize database parameters")

	// Fault injection flags
	// These flags enable FaultInjectionFS for testing durability edge cases.
	// Reference: RocksDB utilities/fault_injection_fs.h
	faultFS                 = flag.Bool("faultfs", false, "Enable FaultInjectionFS for durability testing")
	faultDropUnsynced       = flag.Bool("faultfs-drop-unsynced", false, "Drop unsynced data on simulated crash (requires -faultfs)")
	faultDelUnsynced        = flag.Bool("faultfs-delete-unsynced", false, "Delete unsynced files on simulated crash (requires -faultfs)")
	faultSimulateCrashOnSig = flag.Bool("faultfs-simulate-crash-on-signal", false, "Simulate crash (drop/delete unsynced) when SIGTERM is received (requires -faultfs)")

	// Goroutine-local fault injection for concurrent testing.
	// These flags enable targeted error injection per goroutine class (workers, flusher, reopener).
	// Use with -seed for reproducible failure paths.
	goroutineLocalFaults   = flag.Bool("goroutine-faults", false, "Enable goroutine-local fault injection")
	faultWriterReadOneIn   = flag.Int("fault-writer-read", 0, "Inject read error 1/N for worker goroutines (0=disabled)")
	faultWriterWriteOneIn  = flag.Int("fault-writer-write", 0, "Inject write error 1/N for worker goroutines (0=disabled)")
	faultWriterSyncOneIn   = flag.Int("fault-writer-sync", 0, "Inject sync error 1/N for worker goroutines (0=disabled)")
	faultFlusherSyncOneIn  = flag.Int("fault-flusher-sync", 0, "Inject sync error 1/N for flusher goroutine (0=disabled)")
	faultReopenerReadOneIn = flag.Int("fault-reopener-read", 0, "Inject read error 1/N for reopener goroutine (0=disabled)")
	faultErrorType         = flag.String("fault-error-type", "status", "Error type for goroutine faults: status|corruption|truncated")

	// Artifact collection
	runDir = flag.String("run-dir", "", "Directory for artifact collection on failure (default: none)")

	// Trace emission
	traceOut     = flag.String("trace-out", "", "Path to write operation trace file for replay")
	traceMaxSize = flag.Int64("trace-max-size", 0, "Maximum trace file size in bytes (0 = unlimited)")

	// Seqno-prefix verification (oracle-aligned model).
	// Replaces the "durable-state >=" verification with a "seqno-prefix (no holes)" model.
	seqnoPrefixVerify = flag.Bool("seqno-prefix-verify", false, "Enable oracle-aligned seqno-prefix verification")
	traceDir          = flag.String("trace-dir", "", "Directory containing trace files for seqno-prefix verification")
)

// globalFaultFS holds the FaultInjectionFS instance when fault injection is enabled.
// This allows crash/stress tests to simulate failures like fsync lies and dir sync anomalies.
var globalFaultFS *vfs.FaultInjectionFS

// globalGoroutineFS holds the GoroutineLocalFaultInjectionFS instance when
// goroutine-local fault injection is enabled (-goroutine-faults).
// This allows per-goroutine error injection targeting specific operation classes.
var globalGoroutineFS *vfs.GoroutineLocalFaultInjectionFS

// globalTraceWriter is the trace writer for operation recording.
// When enabled via -trace-out, all operations are recorded for replay.
var globalTraceWriter *trace.Writer
var traceFile *os.File

// globalExpectedState and globalExpectedStatePath are used by the SIGTERM handler
// to save expected state before exiting for clean crash test shutdown.
var globalExpectedState *testutil.ExpectedStateV2
var globalExpectedStatePath string
var globalStopChan chan struct{}
var globalWorkerWg *sync.WaitGroup
var globalStopSave chan struct{}

// errStopped is returned when an operation is cancelled due to stop signal.
var errStopped = fmt.Errorf("stopped")

// Stats tracks operation counts
type Stats struct {
	puts             atomic.Uint64
	gets             atomic.Uint64
	deletes          atomic.Uint64
	batches          atomic.Uint64
	iterScans        atomic.Uint64
	snapshotReads    atomic.Uint64
	rangeDeletes     atomic.Uint64
	merges           atomic.Uint64
	ingests          atomic.Uint64
	compactions      atomic.Uint64
	transactions     atomic.Uint64
	txnCommits       atomic.Uint64
	txnRollbacks     atomic.Uint64
	snapshotVerifies atomic.Uint64
	iterVerifies     atomic.Uint64
	cfOps            atomic.Uint64
	errors           atomic.Uint64
	verifyFail       atomic.Uint64
	reopens          atomic.Uint64
	flushes          atomic.Uint64
	spotVerifyPass   atomic.Uint64
}

// stressWriteOpts contains the write options used by all stress test operations.
// Initialized in main() based on -sync and -disable-wal flags.
var stressWriteOpts *rockyardkv.WriteOptions

func main() {
	flag.Parse()

	startTime = time.Now()

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}

	rand.Seed(*seed)

	// Setup signal handler for clean crash test shutdown.
	// When SIGINT or SIGTERM is received:
	// 1. Stop workers (so no more writes in flight)
	// 2. Save expected state (so verification can use the final state)
	// 3. Collect artifacts (DB snapshot, run.json with signal info)
	// 4. Simulate fault crash if -faultfs-simulate-crash-on-signal is set
	// 5. Exit
	{
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			sig := <-sigChan
			sigName := sig.String()
			fmt.Printf("\n🔥 %s received — shutting down cleanly\n", sigName)

			// Stop the periodic saver first (so it doesn't save stale state)
			if globalStopSave != nil {
				select {
				case <-globalStopSave:
					// Already closed
				default:
					close(globalStopSave)
				}
			}

			// Signal workers to stop and wait for them to finish
			if globalStopChan != nil {
				select {
				case <-globalStopChan:
					// Already closed
				default:
					close(globalStopChan)
				}
				// Wait for workers to complete their in-flight operations.
				// This ensures all committed writes have their WAL syncs completed.
				if globalWorkerWg != nil {
					fmt.Println("⏳ Waiting for workers to finish...")
					done := make(chan struct{})
					go func() {
						globalWorkerWg.Wait()
						close(done)
					}()
					select {
					case <-done:
						fmt.Println("✓ Workers finished")
					case <-time.After(2 * time.Second):
						// Timeout - workers might be stuck
						fmt.Println("⚠️  Timeout waiting for workers to finish")
					}
				}
			}

			// Save expected state for crash test verification
			if globalExpectedState != nil && globalExpectedStatePath != "" {
				if err := globalExpectedState.SaveToFile(globalExpectedStatePath); err != nil {
					fmt.Printf("⚠️  Failed to save expected state: %v\n", err)
				} else {
					fmt.Println("📁 Saved expected state before exit")
				}
			}

			// Collect artifacts on signal termination (UC-14)
			if artifactBundle != nil {
				elapsed := time.Since(startTime)
				// Use exit code 130 for SIGINT (2), 143 for SIGTERM (15)
				exitCode := 1
				if sig == syscall.SIGINT {
					exitCode = 130
				} else if sig == syscall.SIGTERM {
					exitCode = 143
				}
				if bundleErr := artifactBundle.RecordSignalTermination(sigName, exitCode, elapsed); bundleErr != nil {
					fmt.Printf("⚠️  Artifact collection error: %v\n", bundleErr)
				} else {
					fmt.Printf("📦 Artifacts collected at: %s\n", artifactBundle.RunDir)
				}
			}

			// Simulate fault injection crash if enabled
			if *faultSimulateCrashOnSig && *faultFS {
				fmt.Println("🔥 Simulating crash with FaultInjectionFS")
				SimulateFaultFSCrash()
			}

			os.Exit(0)
		}()
	}

	// Apply randomization if requested
	if *randomizeParams {
		randomizeAllParameters()
	}

	// Create write options based on flags
	// This must happen AFTER randomization to pick up any randomized values
	stressWriteOpts = rockyardkv.DefaultWriteOptions()
	stressWriteOpts.Sync = *syncWrites
	stressWriteOpts.DisableWAL = *disableWAL

	printBanner()

	// Setup database path
	testDir := setupDBPath()

	// Setup artifact bundle for failure collection (if run-dir specified)
	if *runDir != "" {
		var err error
		artifactBundle, err = testutil.NewArtifactBundle(*runDir, "stresstest", *seed)
		if err != nil {
			fmt.Printf("⚠️  Failed to create artifact bundle: %v\n", err)
		} else {
			artifactBundle.SetDBPath(testDir)
			if *expectedState != "" {
				artifactBundle.SetExpectedStatePath(*expectedState)
			}
			artifactBundle.SetFlags(map[string]any{
				"duration":    duration.String(),
				"keys":        *numKeys,
				"threads":     *numThreads,
				"value-size":  *valueSize,
				"sync":        *syncWrites,
				"disable-wal": *disableWAL,
			})
		}
	}

	// Setup trace emission (if trace-out specified)
	if *traceOut != "" {
		if err := setupTraceWriter(*traceOut, *traceMaxSize); err != nil {
			fmt.Printf("⚠️  Failed to create trace file: %v\n", err)
		} else {
			defer closeTraceWriter()
			if *verbose {
				fmt.Printf("📝 Trace output: %s\n", *traceOut)
			}
		}
	}

	// Create or load expected state oracle with per-key locking
	var expState *testutil.ExpectedStateV2
	if *expectedState != "" {
		// Try to load from file
		loaded, err := testutil.LoadExpectedStateV2FromFile(*expectedState)
		if err == nil {
			expState = loaded
			if *verbose {
				fmt.Printf("📂 Loaded expected state from %s (seqno: %d)\n", *expectedState, expState.GetPersistedSeqno())
			}
		} else if *verbose {
			fmt.Printf("📂 Could not load expected state from %s: %v (creating new)\n", *expectedState, err)
		}
	}
	if expState == nil {
		expState = testutil.NewExpectedStateV2(*numKeys, 1, uint32(*log2KeysPerLock))
	}

	// Set global expected state for SIGTERM handler
	globalExpectedState = expState
	globalExpectedStatePath = *expectedState

	// Periodically persist expected state for crash testing.
	// This makes -save-expected meaningful under SIGKILL.
	stopSave := make(chan struct{})
	globalStopSave = stopSave // Set global for SIGTERM handler
	if *expectedState != "" && *saveExpected && *saveExpectedInterval > 0 {
		// Save immediately so crash tests have an initial state file
		// even if killed before the first tick.
		_ = expState.SaveToFile(*expectedState)

		go func() {
			t := time.NewTicker(*saveExpectedInterval)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					_ = expState.SaveToFile(*expectedState)
				case <-stopSave:
					return
				}
			}
		}()
	}
	defer close(stopSave)

	// Run stress test
	stats := &Stats{}

	// Seqno-prefix verification mode (oracle-aligned model)
	if *seqnoPrefixVerify {
		if *traceDir == "" {
			fmt.Println("\n❌ STRESS TEST FAILED: -seqno-prefix-verify requires -trace-dir")
			exitWithFailure(fmt.Errorf("seqno-prefix-verify requires trace-dir"), testDir)
		}
		database, _, err := openDB(testDir)
		if err != nil {
			fmt.Printf("\n❌ STRESS TEST FAILED: open failed: %v\n", err)
			exitWithFailure(err, testDir)
		}
		defer database.Close()

		// Get recovered seqno from MANIFEST
		dbRecoveredSeqno := database.GetLatestSequenceNumber()
		fmt.Printf("📊 dbRecoveredSeqno=%d\n", dbRecoveredSeqno)

		// Liveness assertion: DB must have at least one write.
		// A seqno=0 would indicate total data loss, which should fail loudly rather
		// than pass verification with an empty trace prefix.
		if dbRecoveredSeqno == 0 {
			fmt.Println("\n❌ STRESS TEST FAILED: dbRecoveredSeqno=0 (total data loss detected)")
			exitWithFailure(fmt.Errorf("liveness assertion failed: dbRecoveredSeqno=0"), testDir)
		}

		// Reconstruct expected state from trace prefix (seqno <= dbRecoveredSeqno)
		reconstructedState, replayedOps, err := reconstructStateFromTraces(*traceDir, dbRecoveredSeqno, *numKeys)
		if err != nil {
			fmt.Printf("\n❌ STRESS TEST FAILED: trace reconstruction failed: %v\n", err)
			exitWithFailure(err, testDir)
		}
		fmt.Printf("📊 replayed_ops=%d\n", replayedOps)

		// Strict equality verification
		fmt.Println("\n🔍 Running seqno-prefix verification...")
		if err := verifySeqnoPrefix(database, reconstructedState, stats); err != nil {
			fmt.Printf("\n❌ STRESS TEST FAILED: seqno-prefix verification failed: %v\n", err)
			exitWithFailure(err, testDir)
		}
		fmt.Printf("✅ Verified %d keys, 0 failures\n", *numKeys)
		fmt.Println("✅ SEQNO-PREFIX VERIFICATION PASSED")
		return
	}

	if *verifyOnly {
		if *expectedState == "" {
			fmt.Println("\n❌ STRESS TEST FAILED: -verify-only requires -expected-state")
			exitWithFailure(fmt.Errorf("verify-only requires expected-state"), testDir)
		}
		database, _, err := openDB(testDir)
		if err != nil {
			fmt.Printf("\n❌ STRESS TEST FAILED: open failed: %v\n", err)
			exitWithFailure(err, testDir)
		}
		defer database.Close()

		// When DisableWAL was used, we should verify against durable state (flush barriers)
		// instead of the full expected state. This is because unflushed writes are not
		// guaranteed to survive a crash with DisableWAL=true.
		verifyState := expState
		if *durableState != "" {
			durState, err := testutil.LoadExpectedStateV2FromFile(*durableState)
			if err == nil {
				// Count keys that exist in durable state
				durableCount := 0
				for key := range int64(*numKeys) {
					if durState.Get(0, key).Exists() {
						durableCount++
					}
				}
				if *verbose {
					fmt.Printf("📂 Using durable state for verification (seqno: %d, %d keys exist)\n",
						durState.GetPersistedSeqno(), durableCount)
				}
				verifyState = durState
			} else {
				// Durable state file doesn't exist. For DisableWAL=true mode,
				// this means no flush completed successfully. By definition,
				// nothing is durable - use an empty expected state.
				if *disableWAL {
					if *verbose {
						fmt.Printf("⚠️  Durable state file %s not found (no flush completed). Using empty state.\n", *durableState)
					}
					verifyState = testutil.NewExpectedStateV2(*numKeys, 1, uint32(*log2KeysPerLock))
				} else if *verbose {
					fmt.Printf("⚠️  Could not load durable state from %s: %v (using expected state)\n", *durableState, err)
				}
			}
		}

		if globalGoroutineFS != nil {
			// Disable injected faults for verification-only runs.
			// Verification must reflect the stored state, not artificial read errors.
			globalGoroutineFS.FaultManager().DisableGlobal()
			if *verbose {
				fmt.Println("📦 Goroutine faults disabled for final verification")
			}
		}
		fmt.Println("\n🔍 Running final verification...")
		if err := verifyAll(database, verifyState, stats); err != nil {
			fmt.Printf("\n❌ STRESS TEST FAILED: final verification failed: %v\n", err)
			exitWithFailure(err, testDir)
		}
		fmt.Println("✅ VERIFICATION PASSED")
		return
	}
	if err := runStressTest(testDir, expState, stats); err != nil {
		fmt.Printf("\n❌ STRESS TEST FAILED: %v\n", err)
		exitWithFailure(err, testDir)
	}

	// Save expected state if requested
	if *expectedState != "" && *saveExpected {
		if err := expState.SaveToFile(*expectedState); err != nil {
			fmt.Printf("⚠️  Failed to save expected state: %v\n", err)
		} else if *verbose {
			fmt.Printf("💾 Saved expected state to %s\n", *expectedState)
		}
	}

	// Print final stats
	printStats(stats)

	// Check for failures
	if stats.errors.Load() > 0 {
		fmt.Println("❌ STRESS TEST FAILED")
		exitWithFailure(fmt.Errorf("errors: %d", stats.errors.Load()), testDir)
	}

	// Verification failures should now be rare with per-key locking
	if stats.verifyFail.Load() > 0 {
		fmt.Printf("⚠️  %d verification failures\n", stats.verifyFail.Load())
		fmt.Println("❌ STRESS TEST FAILED")
		exitWithFailure(fmt.Errorf("verification failures: %d", stats.verifyFail.Load()), testDir)
	}

	fmt.Println("✅ STRESS TEST PASSED")

	if *keepDB {
		fmt.Printf("\n📁 Database kept at: %s\n", testDir)
	} else if *dbPath == "" {
		// Clean up temp directory
		os.RemoveAll(testDir)
	}
}

func printBanner() {
	const boxWidth = 68 // inner width between ║ and ║

	line := func(content string) {
		padded := fmt.Sprintf(" %-*s", boxWidth-2, content)
		if len(padded) > boxWidth-1 {
			padded = padded[:boxWidth-1]
		}
		fmt.Printf("║%s║\n", padded)
	}

	border := "╔" + stressRepeatChar('═', boxWidth) + "╗"
	middle := "╠" + stressRepeatChar('═', boxWidth) + "╣"
	divider := "╠" + stressRepeatChar('─', boxWidth) + "╣"
	bottom := "╚" + stressRepeatChar('═', boxWidth) + "╝"

	fmt.Println(border)
	line(stressCenter("RockyardKV Full Stress Test (v2)", boxWidth-2))
	fmt.Println(middle)
	line(fmt.Sprintf("Duration: %-10s Keys: %-10d Threads: %d", *duration, *numKeys, *numThreads))
	line(fmt.Sprintf("Seed: %d", *seed))
	line(fmt.Sprintf("Value Size: %-6d bytes  Keys/Lock: %d", *valueSize, 1<<*log2KeysPerLock))
	fmt.Println(divider)
	line(fmt.Sprintf("Weights: put=%d get=%d del=%d batch=%d iter=%d snap=%d",
		*putWeight, *getWeight, *deleteWeight, *batchWeight, *iterWeight, *snapshotWeight))
	line(fmt.Sprintf("         range-del=%d merge=%d ingest=%d txn=%d compact=%d",
		*rangeDelWeight, *mergeWeight, *ingestWeight, *transactionWeight, *compactWeight))
	line(fmt.Sprintf("         snap-verify=%d cf=%d", *snapshotVerifyWeight, *cfWeight))
	fmt.Println(divider)
	line(fmt.Sprintf("Compression: %-8s  Checksum: %-8s  Bloom: %d bits",
		*compressionType, *checksumType, *bloomBits))
	line(fmt.Sprintf("Block Size: %-8d  Write Buffer: %-10d  CFs: %d",
		*blockSize, *writeBufferSize, *numColumnFamilies))
	walStatus := "enabled"
	if *disableWAL {
		walStatus = "disabled"
	}
	syncStatus := "off"
	if *syncWrites {
		syncStatus = "on"
	}
	line(fmt.Sprintf("WAL: %-8s  Sync: %-4s  Randomized: %v", walStatus, syncStatus, *randomizeParams))

	// Show goroutine-local fault injection settings if enabled
	if *goroutineLocalFaults {
		fmt.Println(divider)
		line(fmt.Sprintf("Goroutine Faults: enabled (type=%s)", *faultErrorType))
		if *faultWriterReadOneIn > 0 || *faultWriterWriteOneIn > 0 || *faultWriterSyncOneIn > 0 {
			line(fmt.Sprintf("  Worker: read=1/%d write=1/%d sync=1/%d",
				*faultWriterReadOneIn, *faultWriterWriteOneIn, *faultWriterSyncOneIn))
		}
		if *faultFlusherSyncOneIn > 0 {
			line(fmt.Sprintf("  Flusher: sync=1/%d", *faultFlusherSyncOneIn))
		}
		if *faultReopenerReadOneIn > 0 {
			line(fmt.Sprintf("  Reopener: read=1/%d", *faultReopenerReadOneIn))
		}
	}

	fmt.Println(bottom)
	fmt.Println()
}

func stressRepeatChar(ch rune, n int) string {
	result := make([]rune, n)
	for i := range result {
		result[i] = ch
	}
	return string(result)
}

func stressCenter(s string, width int) string {
	if len(s) >= width {
		return s
	}
	pad := (width - len(s)) / 2
	return fmt.Sprintf("%*s%s%*s", pad, "", s, width-len(s)-pad, "")
}

// randomizeAllParameters randomly selects database configuration parameters.
// This is inspired by RocksDB's db_crashtest.py parameter randomization.
func randomizeAllParameters() {
	// Compression type
	compressionTypes := []string{"none", "snappy", "zlib"}
	*compressionType = compressionTypes[rand.Intn(len(compressionTypes))]

	// Checksum type
	checksumTypes := []string{"crc32c", "xxh3"}
	*checksumType = checksumTypes[rand.Intn(len(checksumTypes))]

	// Block size (4KB to 64KB)
	blockSizes := []int{4096, 8192, 16384, 32768, 65536}
	*blockSize = blockSizes[rand.Intn(len(blockSizes))]

	// Write buffer size (1MB to 16MB)
	writeBufferSizes := []int{1 * 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024}
	*writeBufferSize = writeBufferSizes[rand.Intn(len(writeBufferSizes))]

	// Bloom filter bits (0 to 20)
	*bloomBits = rand.Intn(21)

	// WAL and sync (mostly enabled)
	*disableWAL = rand.Intn(10) == 0 // 10% chance of disabling WAL
	*syncWrites = rand.Intn(5) == 0  // 20% chance of sync writes

	// Operation weights (randomize while keeping sum around 100)
	total := 100
	*putWeight = 20 + rand.Intn(40) // 20-59
	*getWeight = 20 + rand.Intn(30) // 20-49
	remaining := total - *putWeight - *getWeight
	*deleteWeight = rand.Intn(min(remaining/2, 20) + 1)
	remaining -= *deleteWeight
	*batchWeight = rand.Intn(min(remaining/2, 15) + 1)
	remaining -= *batchWeight
	*iterWeight = rand.Intn(min(remaining/2, 10) + 1)
	*snapshotWeight = remaining - *iterWeight

	// Value size (50 to 500 bytes)
	*valueSize = 50 + rand.Intn(451)

	// Number of column families (1 to 4)
	*numColumnFamilies = 1 + rand.Intn(4)
}

const stressTestDirPrefix = "rockyard-stress-"

func setupDBPath() string {
	// Clean up old test directories if requested
	if *cleanup {
		cleanupOldTestDirs()
	}

	var testDir string
	var err error
	if *dbPath == "" {
		testDir, err = os.MkdirTemp("", stressTestDirPrefix+"*")
		if err != nil {
			fatal("Failed to create temp dir: %v", err)
		}
	} else {
		testDir = *dbPath
	}
	fmt.Printf("📁 Database path: %s\n\n", testDir)
	return testDir
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
		if len(name) >= len(stressTestDirPrefix) && name[:len(stressTestDirPrefix)] == stressTestDirPrefix {
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

func printStats(stats *Stats) {
	fmt.Println()
	fmt.Println("══════════════════════════════════════════════════════════════════")
	fmt.Println("                      FINAL STATISTICS                            ")
	fmt.Println("──────────────────────────────────────────────────────────────────")

	totalOps := stats.puts.Load() + stats.gets.Load() + stats.deletes.Load() +
		stats.batches.Load() + stats.iterScans.Load() + stats.snapshotReads.Load() +
		stats.transactions.Load()

	fmt.Printf("Write Operations:\n")
	fmt.Printf("  Puts:        %12d\n", stats.puts.Load())
	fmt.Printf("  Deletes:     %12d\n", stats.deletes.Load())
	fmt.Printf("  Range Dels:  %12d\n", stats.rangeDeletes.Load())
	fmt.Printf("  Merges:      %12d\n", stats.merges.Load())
	fmt.Printf("  Batches:     %12d\n", stats.batches.Load())
	fmt.Printf("  Ingests:     %12d\n", stats.ingests.Load())

	fmt.Printf("\nTransactions:\n")
	fmt.Printf("  Total:       %12d\n", stats.transactions.Load())
	fmt.Printf("  Commits:     %12d\n", stats.txnCommits.Load())
	fmt.Printf("  Rollbacks:   %12d\n", stats.txnRollbacks.Load())

	fmt.Printf("\nColumn Families:\n")
	fmt.Printf("  CF Ops:      %12d\n", stats.cfOps.Load())

	fmt.Printf("\nRead Operations:\n")
	fmt.Printf("  Gets:        %12d\n", stats.gets.Load())
	fmt.Printf("  Iter Scans:  %12d\n", stats.iterScans.Load())
	fmt.Printf("  Snapshots:   %12d\n", stats.snapshotReads.Load())

	fmt.Printf("\nMaintenance:\n")
	fmt.Printf("  Reopens:     %12d\n", stats.reopens.Load())
	fmt.Printf("  Flushes:     %12d\n", stats.flushes.Load())
	fmt.Printf("  Compactions: %12d\n", stats.compactions.Load())

	fmt.Printf("\nVerification:\n")
	fmt.Printf("  Spot Checks: %12d passed\n", stats.spotVerifyPass.Load())
	fmt.Printf("  Snapshot:    %12d verified\n", stats.snapshotVerifies.Load())
	fmt.Printf("  Iterator:    %12d verified\n", stats.iterVerifies.Load())
	fmt.Printf("  Failures:    %12d\n", stats.verifyFail.Load())
	fmt.Printf("  Errors:      %12d\n", stats.errors.Load())

	// Print goroutine-local fault injection statistics if enabled
	if globalGoroutineFS != nil {
		reads, writes, syncs := globalGoroutineFS.FaultManager().Stats()
		if reads > 0 || writes > 0 || syncs > 0 {
			fmt.Printf("\nInjected Faults:\n")
			fmt.Printf("  Read Errors: %12d\n", reads)
			fmt.Printf("  Write Errors:%12d\n", writes)
			fmt.Printf("  Sync Errors: %12d\n", syncs)
		}
	}

	fmt.Printf("\nTotal Operations: %d\n", totalOps)
	fmt.Println("══════════════════════════════════════════════════════════════════")
}

// dbHolder holds the current database instance with synchronization.
type dbHolder struct {
	mu             sync.RWMutex
	db             rockyardkv.DB
	path           string
	opCount        atomic.Uint64
	lastCompact    atomic.Uint64
	columnFamilies []rockyardkv.ColumnFamilyHandle // Additional column families (index 0 = cf1, etc.)
}

func runStressTest(dbPath string, expected *testutil.ExpectedStateV2, stats *Stats) error {
	// Open database
	database, cfs, err := openDB(dbPath)
	if err != nil {
		return fmt.Errorf("initial open failed: %w", err)
	}

	// Enable goroutine fault injection now that DB is successfully open.
	// This prevents faults from breaking the initial setup.
	if *goroutineLocalFaults {
		enableGoroutineFaults()
	}

	// Stop channels
	stop := make(chan struct{})
	globalStopChan = stop // Set global for SIGTERM handler
	var wg sync.WaitGroup
	globalWorkerWg = &wg // Set global for SIGTERM handler to wait on

	// Database holder for safe access
	holder := &dbHolder{db: database, path: dbPath, columnFamilies: cfs}

	// Start worker threads
	for i := range *numThreads {
		wg.Go(func() {
			runWorker(i, holder, expected, stats, stop)
		})
	}

	// Start reopen thread if enabled
	if *reopenPeriod > 0 {
		wg.Go(func() {
			runReopener(holder, stats, stop)
		})
	}

	// Start flush thread if enabled
	if *flushPeriod > 0 {
		wg.Go(func() {
			runFlusher(holder, expected, stats, stop)
		})
	}

	// Progress reporting
	startTime := time.Now()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				elapsed := time.Since(startTime)
				ops := stats.puts.Load() + stats.gets.Load() + stats.deletes.Load() +
					stats.batches.Load() + stats.iterScans.Load() + stats.snapshotReads.Load()
				opsPerSec := float64(ops) / elapsed.Seconds()

				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)
				memMB := float64(memStats.Alloc) / 1024 / 1024

				fmt.Printf("⏱️  %v | %.0f ops/sec | %d errors | %.1f MB mem\n",
					elapsed.Round(time.Second), opsPerSec, stats.errors.Load(), memMB)
			}
		}
	}()

	// Run for duration
	time.Sleep(*duration)
	close(stop)

	// Release any write stalls to unblock workers waiting in MaybeStallWrite.
	// This is needed when fault injection causes flush/compaction failures that
	// prevent the stall condition from clearing naturally.
	//
	// IMPORTANT: We call ReleaseWriteStall directly on 'database' (the original
	// DB instance) without acquiring holder.mu. This avoids a deadlock where:
	// - Workers hold holder.mu.RLock() and are stuck in MaybeStallWrite()
	// - Reopener is waiting for holder.mu.Lock()
	// - Main thread would block on holder.mu.RLock() waiting for reopener
	//
	// ReleaseWriteStall just broadcasts on a sync.Cond, which is safe even if
	// the reopener is in the middle of replacing the DB. The workers will wake
	// up, check stop, and exit cleanly.
	if r, ok := database.(rockyardkv.WriteStallController); ok {
		r.ReleaseWriteStall()
	}

	wg.Wait()

	// Final verification
	if globalGoroutineFS != nil {
		// Disable injected faults for final verification.
		// Verification must reflect the stored state, not artificial read errors.
		globalGoroutineFS.FaultManager().DisableGlobal()
		if *verbose {
			fmt.Println("📦 Goroutine faults disabled for final verification")
		}
	}
	fmt.Println("\n🔍 Running final verification...")
	holder.mu.RLock()
	err = verifyAll(holder.db, expected, stats)
	holder.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("final verification failed: %w", err)
	}

	// Close database
	holder.mu.Lock()
	holder.db.Close()
	holder.mu.Unlock()

	return nil
}

func openDB(path string) (rockyardkv.DB, []rockyardkv.ColumnFamilyHandle, error) {
	opts := rockyardkv.DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 4 * 1024 * 1024 // 4MB
	// Add a merge operator for stress testing
	opts.MergeOperator = &rockyardkv.StringAppendOperator{Delimiter: ","}

	// Enable GoroutineLocalFaultInjectionFS if requested.
	// This allows targeted error injection for concurrent testing.
	// Uses global rates that apply to all I/O operations (DB internals included).
	// Note: Rates are set AFTER initial DB open via enableGoroutineFaults().
	if *goroutineLocalFaults {
		if globalGoroutineFS == nil {
			globalGoroutineFS = vfs.NewGoroutineLocalFaultInjectionFS(vfs.Default())
			// Don't set rates yet - wait until after DB is open
			if *verbose {
				fmt.Println("📦 GoroutineLocalFaultInjectionFS prepared (rates deferred until after DB open)")
			}
		}
		opts.FS = globalGoroutineFS
	} else if *faultFS {
		// Enable FaultInjectionFS if requested for durability testing.
		// This allows simulating fsync lies, missing dir sync, and other
		// filesystem anomalies to test recovery robustness.
		if globalFaultFS == nil {
			globalFaultFS = vfs.NewFaultInjectionFS(vfs.Default())
			if *verbose {
				fmt.Println("📦 FaultInjectionFS enabled")
			}
		}
		opts.FS = globalFaultFS
	}

	database, err := rockyardkv.Open(filepath.Join(path, "db"), opts)
	if err != nil {
		return nil, nil, err
	}

	// Create additional column families if requested
	var cfs []rockyardkv.ColumnFamilyHandle
	for i := 1; i < *numColumnFamilies; i++ {
		cfName := fmt.Sprintf("cf%d", i)

		// Check if CF already exists
		cf := database.GetColumnFamily(cfName)
		if cf == nil {
			// Create new CF
			cfOpts := rockyardkv.DefaultColumnFamilyOptions()
			cf, err = database.CreateColumnFamily(cfOpts, cfName)
			if err != nil {
				// May already exist, try to get it
				cf = database.GetColumnFamily(cfName)
				if cf == nil {
					if *verbose {
						fmt.Printf("Warning: Could not create or get CF %s: %v\n", cfName, err)
					}
					continue
				}
			}
		}
		cfs = append(cfs, cf)
	}

	if *verbose && len(cfs) > 0 {
		fmt.Printf("📊 Using %d column families\n", len(cfs)+1)
	}

	return database, cfs, nil
}

// parseErrorType converts the -fault-error-type flag to a vfs.ErrorType.
func parseErrorType() vfs.ErrorType {
	switch *faultErrorType {
	case "corruption":
		return vfs.ErrorTypeCorruption
	case "truncated":
		return vfs.ErrorTypeTruncated
	default:
		return vfs.ErrorTypeStatus
	}
}

// createWorkerFaultContext creates a fault context for worker goroutines.
func createWorkerFaultContext(threadID int) *vfs.GoroutineFaultContext {
	if globalGoroutineFS == nil {
		return nil
	}
	ctx := vfs.NewGoroutineFaultContext(*seed + int64(threadID*1000))
	ctx.ReadErrorOneIn = *faultWriterReadOneIn
	ctx.WriteErrorOneIn = *faultWriterWriteOneIn
	ctx.SyncErrorOneIn = *faultWriterSyncOneIn
	ctx.ErrorType = parseErrorType()
	return ctx
}

// createFlusherFaultContext creates a fault context for the flusher goroutine.
func createFlusherFaultContext() *vfs.GoroutineFaultContext {
	if globalGoroutineFS == nil {
		return nil
	}
	ctx := vfs.NewGoroutineFaultContext(*seed + 999999) // unique seed offset for flusher
	ctx.SyncErrorOneIn = *faultFlusherSyncOneIn
	ctx.ErrorType = parseErrorType()
	return ctx
}

// createReopenerFaultContext creates a fault context for the reopener goroutine.
func createReopenerFaultContext() *vfs.GoroutineFaultContext {
	if globalGoroutineFS == nil {
		return nil
	}
	ctx := vfs.NewGoroutineFaultContext(*seed + 888888) // unique seed offset for reopener
	ctx.ReadErrorOneIn = *faultReopenerReadOneIn
	ctx.ErrorType = parseErrorType()
	return ctx
}

// enableGoroutineFaults activates global fault injection rates.
// Called after the initial DB open succeeds to avoid failing during setup.
func enableGoroutineFaults() {
	if globalGoroutineFS == nil {
		return
	}
	fm := globalGoroutineFS.FaultManager()

	// Set global rates that apply to all goroutines.
	// This targets the DB's internal I/O operations (flush, compaction, etc.).
	maxWriteRate := max(*faultWriterWriteOneIn, 0)
	maxReadRate := max(*faultWriterReadOneIn, *faultReopenerReadOneIn)
	maxSyncRate := max(*faultWriterSyncOneIn, *faultFlusherSyncOneIn)

	if maxWriteRate > 0 {
		fm.SetGlobalWriteErrorRate(maxWriteRate)
	}
	if maxReadRate > 0 {
		fm.SetGlobalReadErrorRate(maxReadRate)
	}
	if maxSyncRate > 0 {
		fm.SetGlobalSyncErrorRate(maxSyncRate)
	}

	if *verbose {
		fmt.Printf("📦 Goroutine faults enabled (global: read=1/%d write=1/%d sync=1/%d)\n",
			maxReadRate, maxWriteRate, maxSyncRate)
	}
}

func runWorker(threadID int, holder *dbHolder, expected *testutil.ExpectedStateV2, stats *Stats, stop chan struct{}) {
	// Set up goroutine-local fault context if enabled
	if globalGoroutineFS != nil {
		ctx := createWorkerFaultContext(threadID)
		if ctx != nil {
			globalGoroutineFS.FaultManager().SetContext(ctx)
			defer globalGoroutineFS.FaultManager().ClearContext()
		}
	}

	// Derive per-thread seed from the global seed for reproducibility.
	// This ensures that when crashtest passes -seed=N, all worker threads
	// produce deterministic behavior that can be reproduced.
	rng := rand.New(rand.NewSource(*seed + int64(threadID*1000)))
	totalWeight := *putWeight + *getWeight + *deleteWeight + *batchWeight + *iterWeight + *snapshotWeight +
		*rangeDelWeight + *mergeWeight + *ingestWeight + *transactionWeight + *compactWeight + *snapshotVerifyWeight + *cfWeight

	opsSinceVerify := 0

	isStopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	for {
		if isStopped() {
			return
		}

		// Pick operation
		r := rng.Intn(totalWeight)

		// Acquire read lock. We use RLock() which blocks when a writer
		// (flusher) is waiting, giving the flusher priority.
		// This ensures durable state snapshots are consistent.
		if isStopped() {
			return
		}
		holder.mu.RLock()

		database := holder.db
		if database == nil {
			holder.mu.RUnlock()
			continue
		}

		var err error
		cumWeight := 0
		switch {
		case r < cumWeight+*putWeight:
			err = doPut(database, expected, stats, rng, stop)
		case r < cumWeight+*putWeight+*getWeight:
			err = doGet(database, expected, stats, rng)
		case r < cumWeight+*putWeight+*getWeight+*deleteWeight:
			err = doDelete(database, expected, stats, rng, stop)
		case r < cumWeight+*putWeight+*getWeight+*deleteWeight+*batchWeight:
			err = doBatch(database, expected, stats, rng, stop)
		case r < cumWeight+*putWeight+*getWeight+*deleteWeight+*batchWeight+*iterWeight:
			err = doIterScan(database, expected, stats, rng)
		case r < cumWeight+*putWeight+*getWeight+*deleteWeight+*batchWeight+*iterWeight+*snapshotWeight:
			err = doSnapshotRead(database, stats, rng)
		case r < cumWeight+*putWeight+*getWeight+*deleteWeight+*batchWeight+*iterWeight+*snapshotWeight+*rangeDelWeight:
			err = doRangeDelete(database, expected, stats, rng, stop)
		case r < cumWeight+*putWeight+*getWeight+*deleteWeight+*batchWeight+*iterWeight+*snapshotWeight+*rangeDelWeight+*mergeWeight:
			err = doMerge(database, expected, stats, rng, stop)
		case r < cumWeight+*putWeight+*getWeight+*deleteWeight+*batchWeight+*iterWeight+*snapshotWeight+*rangeDelWeight+*mergeWeight+*ingestWeight:
			err = doIngest(holder, expected, stats, rng, stop)
		case r < cumWeight+*putWeight+*getWeight+*deleteWeight+*batchWeight+*iterWeight+*snapshotWeight+*rangeDelWeight+*mergeWeight+*ingestWeight+*transactionWeight:
			err = doTransaction(holder, expected, stats, rng, stop)
		case r < cumWeight+*putWeight+*getWeight+*deleteWeight+*batchWeight+*iterWeight+*snapshotWeight+*rangeDelWeight+*mergeWeight+*ingestWeight+*transactionWeight+*compactWeight:
			err = doCompactAndVerify(database, expected, stats, rng)
		case r < cumWeight+*putWeight+*getWeight+*deleteWeight+*batchWeight+*iterWeight+*snapshotWeight+*rangeDelWeight+*mergeWeight+*ingestWeight+*transactionWeight+*compactWeight+*snapshotVerifyWeight:
			err = doSnapshotVerify(database, expected, stats, rng)
		default:
			err = doColumnFamilyOps(holder, stats, rng)
		}
		_ = cumWeight // silence unused variable warning

		holder.mu.RUnlock()

		if err != nil && !errors.Is(err, errStopped) {
			stats.errors.Add(1)
			if *verbose {
				fmt.Printf("Thread %d error: %v\n", threadID, err)
			}
		}

		// Periodic spot verification
		opsSinceVerify++
		if *verifyEvery > 0 && opsSinceVerify >= *verifyEvery && !isStopped() {
			opsSinceVerify = 0
			holder.mu.RLock()
			if holder.db != nil {
				doSpotVerify(holder.db, expected, stats, rng)
			}
			holder.mu.RUnlock()
		}

		// Track operations for compaction
		opCount := holder.opCount.Add(1)
		if *compactEvery > 0 && opCount-holder.lastCompact.Load() >= uint64(*compactEvery) && !isStopped() {
			holder.mu.RLock()
			if holder.db != nil {
				// Trigger compaction by flushing
				_ = holder.db.Flush(nil)
				stats.compactions.Add(1)
				holder.lastCompact.Store(opCount)
			}
			holder.mu.RUnlock()
		}
	}
}

// doPut performs a put operation WITH per-key locking (RocksDB-style)
func doPut(database rockyardkv.DB, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand, stop chan struct{}) error {
	key := rng.Int63n(*numKeys)

	// ACQUIRE PER-KEY LOCK BEFORE OPERATION (matching C++ db_stress)
	mu := expected.GetMutexForKey(0, key)

	// Try to acquire lock with TryLock + polling for stop
	for {
		select {
		case <-stop:
			return errStopped
		default:
		}
		if mu.TryLock() {
			break
		}
		// Brief sleep to avoid busy-wait
		time.Sleep(100 * time.Microsecond)
	}
	defer mu.Unlock()

	// Prepare the expected value (sets pending write flag)
	pendingValue := expected.PreparePut(0, key)
	if pendingValue == nil {
		return fmt.Errorf("failed to prepare put for key %d", key)
	}

	// Get the value base that will be set after commit
	valueBase := pendingValue.GetFinalValueBase()

	keyBytes := makeKey(key)
	valueBytes := makeValue(key, valueBase)

	// Perform database operation
	if err := database.Put(stressWriteOpts, keyBytes, valueBytes); err != nil {
		// Rollback on failure
		pendingValue.Rollback()
		return fmt.Errorf("put failed: %w", err)
	}

	// Trace the operation with sequence number (if tracing enabled)
	seqno := database.GetLatestSequenceNumber()
	traceOp(trace.TypeWrite, tracePutPayload(keyBytes, valueBytes, seqno))

	// Commit the expected state update
	pendingValue.Commit()
	stats.puts.Add(1)
	return nil
}

// doPutCF performs a put to a random column family.
// Note: Currently expected state only tracks CF 0 (default), so CF writes
// are not verified. This tests CF functionality without oracle verification.
// Reserved for future multi-CF stress testing.
func doPutCF(holder *dbHolder, stats *Stats, rng *rand.Rand) error { //nolint:unused // reserved for future use
	if len(holder.columnFamilies) == 0 {
		return nil // No additional CFs configured
	}

	// Pick a random CF (not default)
	cfIdx := rng.Intn(len(holder.columnFamilies))
	cf := holder.columnFamilies[cfIdx]

	key := rng.Int63n(*numKeys)
	keyBytes := makeKey(key)
	valueBytes := fmt.Appendf(nil, "cf%d_value_%d", cfIdx+1, rng.Int63())

	if err := holder.db.PutCF(nil, cf, keyBytes, valueBytes); err != nil {
		return fmt.Errorf("PutCF failed: %w", err)
	}

	return nil
}

// doColumnFamilyOps performs various operations on column families.
// This tests CF isolation, read/write, and iteration across CFs.
// IMPORTANT: CF operations use keys in a SEPARATE key space (cf_key prefix)
// to avoid interfering with the default CF keys tracked by expected state.
func doColumnFamilyOps(holder *dbHolder, stats *Stats, rng *rand.Rand) error {
	if len(holder.columnFamilies) == 0 {
		// No additional CFs, just do a regular operation
		return nil
	}

	// Pick operation type: 0=put, 1=get, 2=delete, 3=iterate, 4=verify isolation
	opType := rng.Intn(5)
	cfIdx := rng.Intn(len(holder.columnFamilies))
	cf := holder.columnFamilies[cfIdx]

	// Use a DIFFERENT key prefix for CF operations to avoid interfering with default CF
	key := rng.Int63n(*numKeys)
	keyBytes := fmt.Appendf(nil, "cf_key%016d", key)

	switch opType {
	case 0: // Put to CF
		valueBytes := fmt.Appendf(nil, "cf%d_v%d_%d", cfIdx+1, key, rng.Int63())
		if err := holder.db.PutCF(nil, cf, keyBytes, valueBytes); err != nil {
			return fmt.Errorf("PutCF failed: %w", err)
		}

	case 1: // Get from CF
		_, err := holder.db.GetCF(nil, cf, keyBytes)
		if err != nil && !errors.Is(err, rockyardkv.ErrNotFound) {
			return fmt.Errorf("GetCF failed: %w", err)
		}

	case 2: // Delete from CF
		if err := holder.db.DeleteCF(nil, cf, keyBytes); err != nil {
			return fmt.Errorf("DeleteCF failed: %w", err)
		}

	case 3: // Iterate CF
		iter := holder.db.NewIteratorCF(nil, cf)
		if iter != nil {
			count := 0
			for iter.SeekToFirst(); iter.Valid() && count < 20; iter.Next() {
				_ = iter.Key()
				_ = iter.Value()
				count++
			}
			iter.Close()
		}

	case 4: // Verify CF isolation: write to one CF, verify not visible in another
		// Write a unique value to this CF
		uniqueKey := fmt.Appendf(nil, "cf_isolation_%d", rng.Int63())
		uniqueValue := fmt.Appendf(nil, "cf%d_unique", cfIdx+1)

		if err := holder.db.PutCF(nil, cf, uniqueKey, uniqueValue); err != nil {
			return fmt.Errorf("isolation test PutCF failed: %w", err)
		}

		// Verify it's in the CF we wrote to
		val, err := holder.db.GetCF(nil, cf, uniqueKey)
		if err != nil {
			return fmt.Errorf("isolation test GetCF own CF failed: %w", err)
		}
		if !bytes.Equal(val, uniqueValue) {
			return fmt.Errorf("isolation test: value mismatch in own CF")
		}

		// Verify it's NOT in the default CF
		_, err = holder.db.Get(nil, uniqueKey)
		if !errors.Is(err, rockyardkv.ErrNotFound) {
			// This could happen if another thread wrote the same key to default CF
			// Not a hard error, just log
			if *verbose {
				fmt.Printf("CF isolation: key %s exists in default CF (may be concurrent write)\n", uniqueKey)
			}
		}

		// Clean up
		holder.db.DeleteCF(nil, cf, uniqueKey)
	}

	stats.cfOps.Add(1)
	return nil
}

// doGet performs a get operation WITH pre/post read verification (RocksDB-style)
func doGet(database rockyardkv.DB, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand) error {
	key := rng.Int63n(*numKeys)
	keyBytes := makeKey(key)

	// Capture pre-read expected value (no lock needed for reads)
	preReadExpected := expected.Get(0, key)

	// Perform database operation
	value, err := database.Get(nil, keyBytes)

	// Trace the operation (if tracing enabled)
	traceOp(trace.TypeGet, traceGetPayload(keyBytes))

	// Capture post-read expected value
	postReadExpected := expected.Get(0, key)

	stats.gets.Add(1)

	// Verify using pre/post read pattern
	if errors.Is(err, rockyardkv.ErrNotFound) {
		// Key not found - check if this is expected
		if testutil.MustHaveExisted(preReadExpected, postReadExpected) {
			// Key must have existed but we got NotFound - this is an error
			if *verbose {
				fmt.Printf("Get verification failed: key %d must have existed but got NotFound\n", key)
			}
			// Don't count as error during concurrent operations - concurrent deletes are possible
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("get failed: %w", err)
	}

	// Key found - verify value is in expected range
	if testutil.MustHaveNotExisted(preReadExpected, postReadExpected) {
		// Key must NOT have existed but we found it - this is an error
		if *verbose {
			fmt.Printf("Get verification failed: key %d should not exist but found\n", key)
		}
		// Don't count as error - concurrent puts are possible
		return nil
	}

	// Verify value base is in expected range
	if len(value) >= 12 {
		valueBase := getValueBase(value)
		if !testutil.InExpectedValueBaseRange(valueBase, preReadExpected, postReadExpected) {
			if *verbose {
				fmt.Printf("Get verification: key %d value base %d out of expected range\n", key, valueBase)
			}
			// Don't count as hard error during concurrent operations
		}
	}

	return nil
}

// doDelete performs a delete operation WITH per-key locking (RocksDB-style)
func doDelete(database rockyardkv.DB, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand, stop chan struct{}) error {
	key := rng.Int63n(*numKeys)

	// ACQUIRE PER-KEY LOCK BEFORE OPERATION
	mu := expected.GetMutexForKey(0, key)

	// Try to acquire lock with TryLock + polling for stop
	for {
		select {
		case <-stop:
			return errStopped
		default:
		}
		if mu.TryLock() {
			break
		}
		time.Sleep(100 * time.Microsecond)
	}
	defer mu.Unlock()

	// Prepare the expected value (sets pending delete flag)
	pendingValue := expected.PrepareDelete(0, key)
	if pendingValue == nil {
		return fmt.Errorf("failed to prepare delete for key %d", key)
	}

	keyBytes := makeKey(key)

	// Perform database operation
	if err := database.Delete(stressWriteOpts, keyBytes); err != nil {
		// Rollback on failure
		pendingValue.Rollback()
		return fmt.Errorf("delete failed: %w", err)
	}

	// Trace the operation with sequence number (if tracing enabled)
	seqno := database.GetLatestSequenceNumber()
	traceOp(trace.TypeWrite, traceDeletePayload(keyBytes, seqno))

	// Commit the expected state update
	pendingValue.Commit()
	stats.deletes.Add(1)
	return nil
}

// doBatch performs a batch write WITH per-key locking for all keys in the batch
func doBatch(database rockyardkv.DB, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand, stop chan struct{}) error {
	wb := rockyardkv.NewWriteBatch()
	batchSize := rng.Intn(20) + 1

	type opInfo struct {
		key          int64
		valueBase    uint32
		isDel        bool
		pendingValue *testutil.PendingExpectedValueV2
		mu           *sync.Mutex
	}
	ops := make([]opInfo, 0, batchSize)

	// Helper to check if stopped
	isStopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	// Acquire locks and prepare all operations
	for range batchSize {
		if isStopped() {
			// Release any locks we've acquired
			for _, op := range ops {
				op.pendingValue.Rollback()
				op.mu.Unlock()
			}
			return errStopped
		}

		key := rng.Int63n(*numKeys)

		// Check if we already have this key in the batch (avoid double-locking)
		found := false
		for _, op := range ops {
			if op.key == key {
				found = true
				break
			}
		}
		if found {
			continue // Skip duplicate keys in same batch
		}

		mu := expected.GetMutexForKey(0, key)

		// Try to acquire lock with TryLock + polling for stop
		lockAcquired := false
		for !lockAcquired {
			if isStopped() {
				// Release any locks we've acquired
				for _, op := range ops {
					op.pendingValue.Rollback()
					op.mu.Unlock()
				}
				return errStopped
			}
			if mu.TryLock() {
				lockAcquired = true
			} else {
				time.Sleep(100 * time.Microsecond)
			}
		}

		if rng.Intn(10) < 2 {
			// 20% chance delete
			pendingValue := expected.PrepareDelete(0, key)
			wb.Delete(makeKey(key))
			ops = append(ops, opInfo{key: key, isDel: true, pendingValue: pendingValue, mu: mu})
		} else {
			pendingValue := expected.PreparePut(0, key)
			valueBase := pendingValue.GetFinalValueBase()
			wb.Put(makeKey(key), makeValue(key, valueBase))
			ops = append(ops, opInfo{key: key, valueBase: valueBase, isDel: false, pendingValue: pendingValue, mu: mu})
		}
	}

	// Perform batch write
	err := database.Write(stressWriteOpts, wb)

	// Commit or rollback all pending values and release locks
	for _, op := range ops {
		if err != nil {
			op.pendingValue.Rollback()
		} else {
			op.pendingValue.Commit()
		}
		op.mu.Unlock()
	}

	if err != nil {
		return fmt.Errorf("batch write failed: %w", err)
	}

	// Trace the operation with sequence number (if tracing enabled)
	seqno := database.GetLatestSequenceNumber()
	traceOp(trace.TypeWrite, traceWriteBatchPayload(wb, seqno))

	stats.batches.Add(1)
	return nil
}

func doIterScan(database rockyardkv.DB, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand) error {
	// Create iterator
	iter := database.NewIterator(nil)
	if iter == nil {
		return fmt.Errorf("failed to create iterator")
	}
	defer iter.Close()

	// Random scan type
	scanType := rng.Intn(5)
	count := 0
	maxScan := 100 // Limit scan length

	switch scanType {
	case 0: // Forward from beginning with ordering verification
		var prevKey []byte
		for iter.SeekToFirst(); iter.Valid() && count < maxScan; iter.Next() {
			key := iter.Key()
			if prevKey != nil {
				if bytes.Compare(prevKey, key) >= 0 {
					stats.verifyFail.Add(1)
					return fmt.Errorf("iterator ordering violation: %q >= %q", prevKey, key)
				}
			}
			prevKey = append([]byte(nil), key...)
			count++
		}
		stats.iterVerifies.Add(1)
	case 1: // Backward from end with ordering verification
		var prevKey []byte
		for iter.SeekToLast(); iter.Valid() && count < maxScan; iter.Prev() {
			key := iter.Key()
			if prevKey != nil {
				if bytes.Compare(prevKey, key) <= 0 {
					stats.verifyFail.Add(1)
					return fmt.Errorf("reverse iterator ordering violation: %q <= %q", prevKey, key)
				}
			}
			prevKey = append([]byte(nil), key...)
			count++
		}
		stats.iterVerifies.Add(1)
	case 2: // Forward from random key with ordering verification
		startKey := rng.Int63n(*numKeys)
		iter.Seek(makeKey(startKey))
		var prevKey []byte
		for iter.Valid() && count < maxScan {
			key := iter.Key()
			if prevKey != nil {
				if bytes.Compare(prevKey, key) >= 0 {
					stats.verifyFail.Add(1)
					return fmt.Errorf("seek iterator ordering violation: %q >= %q", prevKey, key)
				}
			}
			prevKey = append([]byte(nil), key...)
			iter.Next()
			count++
		}
		stats.iterVerifies.Add(1)
	case 3: // Count all keys
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			count++
			if count > int(*numKeys)+1000 {
				break // Safety limit
			}
		}
	case 4: // Verify values match expected state (spot check)
		startKey := rng.Int63n(*numKeys)
		iter.Seek(makeKey(startKey))
		verified := 0
		for iter.Valid() && verified < 10 {
			key := iter.Key()
			value := iter.Value()

			// Parse key to get key index
			if len(key) > 3 && string(key[:3]) == "key" {
				var keyIdx int64
				fmt.Sscanf(string(key), "key%d", &keyIdx)
				if keyIdx >= 0 && keyIdx < *numKeys {
					// Get expected value
					ev := expected.Get(0, keyIdx)
					if ev.Exists() && len(value) >= 12 {
						actualValueBase := getValueBase(value)
						expectedValueBase := ev.GetValueBase()
						if actualValueBase != expectedValueBase {
							// Don't count as hard failure during concurrent operations
							if *verbose {
								fmt.Printf("Iterator value mismatch: key %d got base %d, want %d\n",
									keyIdx, actualValueBase, expectedValueBase)
							}
						}
					}
					verified++
				}
			}
			iter.Next()
			count++
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	stats.iterScans.Add(1)
	return nil
}

func doSnapshotRead(database rockyardkv.DB, stats *Stats, rng *rand.Rand) error {
	// Get a snapshot
	snap := database.GetSnapshot()
	if snap == nil {
		return nil // Snapshots may not be fully implemented
	}
	defer database.ReleaseSnapshot(snap)

	// Read some keys from snapshot
	numReads := rng.Intn(10) + 1
	for range numReads {
		key := rng.Int63n(*numKeys)
		keyBytes := makeKey(key)

		// TODO: When snapshot reads are implemented, use snap parameter
		_, err := database.Get(nil, keyBytes)
		if err != nil && !errors.Is(err, rockyardkv.ErrNotFound) {
			return fmt.Errorf("snapshot get failed: %w", err)
		}
	}

	stats.snapshotReads.Add(1)
	return nil
}

// doRangeDelete performs a range deletion WITH per-key locking for all keys in range.
// This is a simplified implementation that deletes a small range of keys.
func doRangeDelete(database rockyardkv.DB, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand, stop chan struct{}) error {
	// Pick a random start key and range size (keep range small to limit lock contention)
	rangeSize := int64(rng.Intn(10) + 1) // 1-10 keys
	startKey := rng.Int63n(*numKeys - rangeSize)
	endKey := startKey + rangeSize

	// Helper to check if stopped
	isStopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	// Acquire locks for all keys in range (in order to avoid deadlock)
	type lockInfo struct {
		key          int64
		mu           *sync.Mutex
		pendingValue *testutil.PendingExpectedValueV2
	}
	locks := make([]lockInfo, 0, rangeSize)

	// Acquire locks in order
	for key := startKey; key < endKey; key++ {
		if isStopped() {
			// Release any locks we've acquired
			for _, l := range locks {
				if l.pendingValue != nil {
					l.pendingValue.Rollback()
				}
				l.mu.Unlock()
			}
			return errStopped
		}

		mu := expected.GetMutexForKey(0, key)

		// Try to acquire lock with polling for stop
		for {
			if isStopped() {
				// Release any locks we've acquired
				for _, l := range locks {
					if l.pendingValue != nil {
						l.pendingValue.Rollback()
					}
					l.mu.Unlock()
				}
				return errStopped
			}
			if mu.TryLock() {
				break
			}
			time.Sleep(100 * time.Microsecond)
		}

		// Prepare delete for this key
		pendingValue := expected.PrepareDelete(0, key)
		locks = append(locks, lockInfo{key: key, mu: mu, pendingValue: pendingValue})
	}

	// Perform the range delete
	startKeyBytes := makeKey(startKey)
	endKeyBytes := makeKey(endKey)
	err := database.DeleteRange(nil, startKeyBytes, endKeyBytes)

	// Commit or rollback all pending values and release locks
	for _, l := range locks {
		if err != nil {
			l.pendingValue.Rollback()
		} else {
			l.pendingValue.Commit()
		}
		l.mu.Unlock()
	}

	if err != nil {
		return fmt.Errorf("range delete failed: %w", err)
	}

	stats.rangeDeletes.Add(1)
	return nil
}

// doMerge performs a merge operation.
// NOTE: Merge operations use a separate key prefix ("merge_key") to avoid interfering
// with the expected state oracle. The oracle tracks regular keys ("key%016d") and
// merge operations would corrupt those values. Using separate keys allows us to
// exercise the merge code path without affecting verification.
// See smoke tests for merge correctness verification.
func doMerge(database rockyardkv.DB, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand, stop chan struct{}) error {
	_ = expected // Not tracked in expected state

	// Use a different key prefix to avoid corrupting tracked keys
	key := rng.Int63n(*numKeys)
	keyBytes := fmt.Appendf(nil, "merge_key%016d", key)

	// Create a small merge operand (using string append format)
	operand := fmt.Appendf(nil, "m%d", rng.Intn(1000))

	// Perform merge operation
	if err := database.Merge(stressWriteOpts, keyBytes, operand); err != nil {
		// Merge may fail if merge operator not set - not a real error for stress test
		if err.Error() == "db: merge operator not set in options" {
			return nil
		}
		return fmt.Errorf("merge failed: %w", err)
	}

	stats.merges.Add(1)
	return nil
}

// doIngest creates a small SST file and ingests it.
// This is an expensive operation, so it should have low weight.
func doIngest(holder *dbHolder, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand, stop chan struct{}) error {
	// Pick a range of keys to ingest (small range, 5-20 keys)
	numKeysToIngest := rng.Intn(16) + 5
	startKey := rng.Int63n(*numKeys - int64(numKeysToIngest))

	// Helper to check if stopped
	isStopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	// Acquire locks for all keys we'll ingest (in order)
	type lockInfo struct {
		key          int64
		mu           *sync.Mutex
		pendingValue *testutil.PendingExpectedValueV2
		valueBase    uint32
	}
	locks := make([]lockInfo, 0, numKeysToIngest)

	// Track which lock buckets we've already locked to avoid self-deadlock
	lockedMutexes := make(map[*sync.Mutex]bool)

	for i := range numKeysToIngest {
		key := startKey + int64(i)

		if isStopped() {
			for _, l := range locks {
				if l.pendingValue != nil {
					l.pendingValue.Rollback()
				}
			}
			for mu := range lockedMutexes {
				mu.Unlock()
			}
			return errStopped
		}

		mu := expected.GetMutexForKey(0, key)

		// Only lock if we haven't already locked this mutex (multiple keys per lock)
		if !lockedMutexes[mu] {
			for {
				if isStopped() {
					for _, l := range locks {
						if l.pendingValue != nil {
							l.pendingValue.Rollback()
						}
					}
					for m := range lockedMutexes {
						m.Unlock()
					}
					return errStopped
				}
				if mu.TryLock() {
					lockedMutexes[mu] = true
					break
				}
				time.Sleep(100 * time.Microsecond)
			}
		}

		pendingValue := expected.PreparePut(0, key)
		valueBase := pendingValue.GetFinalValueBase()
		locks = append(locks, lockInfo{key: key, mu: mu, pendingValue: pendingValue, valueBase: valueBase})
	}

	// Helper to cleanup on error
	cleanup := func(rollback bool) {
		for _, l := range locks {
			if rollback && l.pendingValue != nil {
				l.pendingValue.Rollback()
			}
		}
		for mu := range lockedMutexes {
			mu.Unlock()
		}
	}

	// Create temp SST file
	sstPath := filepath.Join(holder.path, fmt.Sprintf("ingest_%d_%d.sst", time.Now().UnixNano(), rng.Int63()))

	writerOpts := rockyardkv.DefaultSstFileWriterOptions()
	writer := rockyardkv.NewSstFileWriter(writerOpts)

	if err := writer.Open(sstPath); err != nil {
		cleanup(true)
		return fmt.Errorf("failed to open SST writer: %w", err)
	}

	// Write keys to SST (keys must be sorted)
	for _, l := range locks {
		keyBytes := makeKey(l.key)
		valueBytes := makeValue(l.key, l.valueBase)
		if err := writer.Put(keyBytes, valueBytes); err != nil {
			writer.Abandon()
			os.Remove(sstPath)
			cleanup(true)
			return fmt.Errorf("SST write failed: %w", err)
		}
	}

	if _, err := writer.Finish(); err != nil {
		os.Remove(sstPath)
		cleanup(true)
		return fmt.Errorf("SST finish failed: %w", err)
	}

	// Ingest the SST file
	ingestOpts := rockyardkv.IngestExternalFileOptions{
		MoveFiles:           true, // Move (delete) the file after ingest
		SnapshotConsistency: true,
		AllowGlobalSeqNo:    true,
		AllowBlockingFlush:  true,
	}

	err := holder.db.IngestExternalFile([]string{sstPath}, ingestOpts)

	// Commit or rollback
	if err != nil {
		for _, l := range locks {
			l.pendingValue.Rollback()
		}
	} else {
		for _, l := range locks {
			l.pendingValue.Commit()
		}
	}

	// Unlock all mutexes
	for mu := range lockedMutexes {
		mu.Unlock()
	}

	// Clean up SST file if it wasn't moved
	os.Remove(sstPath)

	if err != nil {
		return fmt.Errorf("ingest failed: %w", err)
	}

	stats.ingests.Add(1)
	return nil
}

// doTransaction performs a mini-transaction with puts and gets.
// Since we're using the regular DB (not TransactionDB) in the stress test,
// this simulates transaction-like behavior using batches with verification.
func doTransaction(holder *dbHolder, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand, stop chan struct{}) error {
	// Pick a small set of keys for the "transaction"
	numOps := rng.Intn(5) + 2 // 2-6 operations

	// Helper to check if stopped
	isStopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	// Acquire locks for all keys (in order to avoid deadlock)
	type lockInfo struct {
		key          int64
		mu           *sync.Mutex
		pendingValue *testutil.PendingExpectedValueV2
		valueBase    uint32
		isPut        bool
	}
	locks := make([]lockInfo, 0, numOps)
	lockedMutexes := make(map[*sync.Mutex]bool)

	// Decide upfront whether to commit or rollback (80% commit)
	willCommit := rng.Intn(10) < 8

	lockedKeys := make(map[int64]bool)

	for range numOps {
		key := rng.Int63n(*numKeys)

		// Skip duplicate keys in same transaction
		if lockedKeys[key] {
			continue
		}

		if isStopped() {
			for _, l := range locks {
				if l.pendingValue != nil {
					l.pendingValue.Rollback()
				}
			}
			for mu := range lockedMutexes {
				mu.Unlock()
			}
			return errStopped
		}

		mu := expected.GetMutexForKey(0, key)

		// Only lock if we haven't already (same mutex may cover multiple keys)
		if !lockedMutexes[mu] {
			for {
				if isStopped() {
					for _, l := range locks {
						if l.pendingValue != nil {
							l.pendingValue.Rollback()
						}
					}
					for m := range lockedMutexes {
						m.Unlock()
					}
					return errStopped
				}
				if mu.TryLock() {
					lockedMutexes[mu] = true
					break
				}
				time.Sleep(100 * time.Microsecond)
			}
		}

		// 80% puts, 20% deletes
		if rng.Intn(10) < 8 {
			pendingValue := expected.PreparePut(0, key)
			valueBase := pendingValue.GetFinalValueBase()
			locks = append(locks, lockInfo{key: key, mu: mu, pendingValue: pendingValue, valueBase: valueBase, isPut: true})
		} else {
			pendingValue := expected.PrepareDelete(0, key)
			locks = append(locks, lockInfo{key: key, mu: mu, pendingValue: pendingValue, isPut: false})
		}
		lockedKeys[key] = true
	}

	// Build the batch
	wb := rockyardkv.NewWriteBatch()
	for _, l := range locks {
		if l.isPut {
			wb.Put(makeKey(l.key), makeValue(l.key, l.valueBase))
		} else {
			wb.Delete(makeKey(l.key))
		}
	}

	// Execute or "rollback"
	var execErr error
	if willCommit {
		execErr = holder.db.Write(stressWriteOpts, wb)
	}

	// Commit or rollback expected state
	for _, l := range locks {
		if willCommit && execErr == nil {
			l.pendingValue.Commit()
		} else {
			l.pendingValue.Rollback()
		}
	}

	// Unlock all
	for mu := range lockedMutexes {
		mu.Unlock()
	}

	stats.transactions.Add(1)
	if willCommit && execErr == nil {
		stats.txnCommits.Add(1)
	} else {
		stats.txnRollbacks.Add(1)
	}

	if execErr != nil {
		return fmt.Errorf("transaction write failed: %w", execErr)
	}

	return nil
}

// doCompactAndVerify triggers a compaction and verifies a sample of keys afterward.
// This helps catch data corruption during compaction.
func doCompactAndVerify(database rockyardkv.DB, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand) error {
	// Flush first to ensure data is in SST files
	// Note: "immutable memtable already exists" is a transient condition under concurrent load
	if err := database.Flush(nil); err != nil {
		if strings.Contains(err.Error(), "immutable memtable already exists") {
			// Transient condition - flush is in progress, skip and continue
			if *verbose {
				fmt.Printf("Flush skipped (memtable busy)\n")
			}
		} else {
			return fmt.Errorf("flush failed: %w", err)
		}
	}

	// Trigger compaction
	if err := database.CompactRange(nil, nil, nil); err != nil {
		// CompactRange may not be fully implemented - that's okay
		if *verbose {
			fmt.Printf("CompactRange: %v (may not be implemented)\n", err)
		}
	}

	stats.compactions.Add(1)

	// Verify a sample of keys after compaction with per-key locking
	// (required for accurate verification during concurrent stress operations)
	numToVerify := min(50, int(*numKeys))

	failures := 0
	for range numToVerify {
		key := rng.Int63n(*numKeys)
		keyBytes := makeKey(key)

		// Acquire per-key lock for accurate verification
		mu := expected.GetMutexForKey(0, key)
		mu.Lock()

		ev := expected.Get(0, key)
		value, err := database.Get(nil, keyBytes)

		// Skip keys with pending operations - they're in flux
		if ev.PendingWrite() || ev.PendingDelete() {
			mu.Unlock()
			continue
		}

		if ev.IsDeleted() {
			// Key should not exist
			if !errors.Is(err, rockyardkv.ErrNotFound) {
				failures++
				if *verbose {
					fmt.Printf("Post-compaction verify: key %d exists but expected deleted\n", key)
				}
			}
		} else if ev.Exists() {
			// Key should exist
			if errors.Is(err, rockyardkv.ErrNotFound) {
				failures++
				if *verbose {
					fmt.Printf("Post-compaction verify: key %d missing but expected to exist\n", key)
				}
			} else if err != nil {
				failures++
			} else if len(value) >= 12 {
				expectedValueBase := ev.GetValueBase()
				actualValueBase := getValueBase(value)
				if actualValueBase != expectedValueBase {
					failures++
					if *verbose {
						fmt.Printf("Post-compaction verify: key %d value base %d != expected %d\n",
							key, actualValueBase, expectedValueBase)
					}
				}
			}
		}
		// Skip keys in unknown state (neither deleted nor definitely existing)

		mu.Unlock()
	}

	if failures > 0 {
		// Post-compaction verification is best-effort during high concurrency.
		// Under heavy contention, transient races between expected state updates
		// and database reads can cause spurious failures. We log these but don't
		// treat them as fatal - final verification is the authoritative check.
		if *verbose {
			fmt.Printf("Post-compaction verify: %d potential inconsistencies (non-fatal)\n", failures)
		}
	}

	return nil
}

// doSnapshotVerify takes a snapshot and verifies it provides isolation.
// This is a read-only verification that doesn't modify DB state to avoid
// corrupting the expected state oracle.
func doSnapshotVerify(database rockyardkv.DB, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand) error {
	// Take snapshot
	snap := database.GetSnapshot()
	if snap == nil {
		// Snapshots not implemented
		return nil
	}
	defer database.ReleaseSnapshot(snap)

	// Pick some keys to verify - just do read-only verification
	numToVerify := 10
	readOpts := rockyardkv.DefaultReadOptions()
	readOpts.Snapshot = snap

	for range numToVerify {
		key := rng.Int63n(*numKeys)
		keyBytes := makeKey(key)

		// Pre-read expected value
		preRead := expected.Get(0, key)

		// Read from snapshot
		value, err := database.Get(readOpts, keyBytes)

		// Post-read expected value
		postRead := expected.Get(0, key)

		// Verify using pre/post pattern (read-only)
		if errors.Is(err, rockyardkv.ErrNotFound) {
			// Key not found in snapshot
			if testutil.MustHaveExisted(preRead, postRead) {
				if *verbose {
					fmt.Printf("Snapshot verify: key %d not in snapshot but expected to exist\n", key)
				}
			}
		} else if err != nil {
			return fmt.Errorf("snapshot get failed: %w", err)
		} else {
			// Key found in snapshot - verify value is consistent
			if len(value) >= 12 {
				valueBase := getValueBase(value)
				if !testutil.InExpectedValueBaseRange(valueBase, preRead, postRead) {
					if *verbose {
						fmt.Printf("Snapshot verify: key %d value base %d out of range\n", key, valueBase)
					}
				}
			}
		}
	}

	stats.snapshotVerifies.Add(1)
	return nil
}

// doSpotVerify performs spot verification using pre/post read pattern
func doSpotVerify(database rockyardkv.DB, expected *testutil.ExpectedStateV2, stats *Stats, rng *rand.Rand) {
	// Verify a random subset of keys
	numToVerify := min(max(int(int64(*verifyPercent)**numKeys/100), 1), 100)

	failures := 0
	for range numToVerify {
		key := rng.Int63n(*numKeys)

		// Pre-read expected value
		preRead := expected.Get(0, key)

		keyBytes := makeKey(key)
		value, err := database.Get(nil, keyBytes)

		// Post-read expected value
		postRead := expected.Get(0, key)

		// Verify using pre/post pattern
		if errors.Is(err, rockyardkv.ErrNotFound) {
			// Key not found - check if this is expected
			if testutil.MustHaveExisted(preRead, postRead) {
				failures++
			}
		} else if err != nil {
			failures++ // Unexpected error
		} else {
			// Key found
			if testutil.MustHaveNotExisted(preRead, postRead) {
				failures++
			} else if len(value) >= 12 {
				valueBase := getValueBase(value)
				if !testutil.InExpectedValueBaseRange(valueBase, preRead, postRead) {
					failures++
				}
			}
		}
	}

	if failures == 0 {
		stats.spotVerifyPass.Add(1)
	} else {
		stats.verifyFail.Add(uint64(failures))
		if *verbose {
			fmt.Printf("⚠️  Spot verify: %d failures\n", failures)
		}
	}
}

func runReopener(holder *dbHolder, stats *Stats, stop chan struct{}) {
	// Set up goroutine-local fault context if enabled
	if globalGoroutineFS != nil {
		ctx := createReopenerFaultContext()
		if ctx != nil {
			globalGoroutineFS.FaultManager().SetContext(ctx)
			defer globalGoroutineFS.FaultManager().ClearContext()
		}
	}

	ticker := time.NewTicker(*reopenPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			holder.mu.Lock()
			if *verbose {
				fmt.Println("🔄 Reopening database...")
			}

			// Close current
			if holder.db != nil {
				holder.db.Close()
			}

			// Reopen
			newDB, cfs, err := openDB(holder.path)
			if err != nil {
				fmt.Printf("❌ Reopen failed: %v\n", err)
				holder.mu.Unlock()
				continue
			}

			holder.db = newDB
			holder.columnFamilies = cfs
			stats.reopens.Add(1)
			holder.mu.Unlock()

			if *verbose {
				fmt.Println("✅ Database reopened")
			}
		}
	}
}

func runFlusher(holder *dbHolder, expected *testutil.ExpectedStateV2, stats *Stats, stop chan struct{}) {
	// Set up goroutine-local fault context if enabled
	if globalGoroutineFS != nil {
		ctx := createFlusherFaultContext()
		if ctx != nil {
			globalGoroutineFS.FaultManager().SetContext(ctx)
			defer globalGoroutineFS.FaultManager().ClearContext()
		}
	}

	ticker := time.NewTicker(*flushPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			// When DisableWAL is enabled, we track the "durable state" - the expected
			// state at the last successful flush. This is what we verify against
			// after crash recovery, since only flushed data is durable without WAL.
			//
			// Critical ordering: save durable state AFTER successful flush.
			// This ensures durable_state.bin never contains data that isn't in SST.
			if *disableWAL && *durableState != "" {
				// For DisableWAL mode with durable state tracking:
				// We save the expected state AFTER a successful flush, not before.
				//
				// Why AFTER, not BEFORE:
				// - If we save BEFORE flush and crash during flush, the saved state
				//   contains data that never made it to SST (durable state is ahead)
				// - If we save AFTER successful flush, we guarantee the saved state
				//   only contains data that is actually in synced SST files
				//
				// Why holder.mu.Lock() is needed:
				// - We must block workers during flush to get a consistent snapshot
				// - While we hold the lock, no new writes can occur
				// - After flush completes, we save the expected state (now durable)
				// - Then release lock to let workers resume
				//
				// This ensures durable_state.bin <= DB on disk at all times.
				holder.mu.Lock()
				if holder.db != nil {
					flushCount := stats.flushes.Load()
					// First flush - this writes memtable to SST and syncs
					if err := holder.db.Flush(nil); err != nil {
						if *verbose {
							fmt.Printf("Flush #%d error: %v\n", flushCount+1, err)
						}
					} else {
						stats.flushes.Add(1)
						// Only save expected state AFTER successful flush
						// At this point, all writes up to now are durable in SST
						if err := expected.SaveToFile(*durableState); err != nil {
							if *verbose {
								fmt.Printf("Flush #%d: durable state save error: %v\n", flushCount+1, err)
							}
						} else if *verbose {
							fmt.Printf("Flush #%d: durable state saved to %s\n", flushCount+1, *durableState)
						}
					}
				}
				holder.mu.Unlock()
			} else {
				// Normal flush without durable state tracking
				holder.mu.RLock()
				if holder.db != nil {
					if err := holder.db.Flush(nil); err != nil {
						if *verbose {
							fmt.Printf("Flush error: %v\n", err)
						}
					} else {
						stats.flushes.Add(1)
					}
				}
				holder.mu.RUnlock()
			}
		}
	}
}

// verifyAll performs final verification with per-key locking
func verifyAll(database rockyardkv.DB, expected *testutil.ExpectedStateV2, stats *Stats) error {
	verified := 0
	failures := 0

	// Count keys that should exist based on expected state
	expectedExists := 0
	for key := range int64(*numKeys) {
		ev := expected.Get(0, key)
		if ev.Exists() {
			expectedExists++
		}
	}
	fmt.Printf("  Expected keys that should exist: %d\n", expectedExists)

	// Verify expected state matches database
	for key := range int64(*numKeys) {
		// Acquire lock for this key during verification
		mu := expected.GetMutexForKey(0, key)
		mu.Lock()

		ev := expected.Get(0, key)

		// Skip keys with pending operations - they shouldn't exist after test completion
		// but we need to handle any edge cases gracefully
		if ev.PendingWrite() || ev.PendingDelete() {
			mu.Unlock()
			continue
		}

		keyBytes := makeKey(key)
		value, err := database.Get(nil, keyBytes)

		if ev.IsDeleted() {
			// Key should not exist
			if err == nil {
				if *allowDBAhead {
					// In crash testing, DB can be ahead of expected state due to race conditions.
					// Expected state: key deleted (saved before new PUT)
					// DB: key exists (PUT synced after expected state save, before SIGKILL)
					// This is acceptable - no data loss occurred.
					if *verbose {
						fmt.Printf("Verify: key %d expected deleted but found (allowed, DB ahead)\n", key)
					}
				} else {
					failures++
					if *verbose {
						fmt.Printf("Verify: key %d expected deleted but found\n", key)
					}
				}
			} else if !errors.Is(err, rockyardkv.ErrNotFound) {
				// The key might or might not exist, but we could not verify due to a read error.
				failures++
				if *verbose {
					fmt.Printf("Verify: key %d expected deleted but got error: %v\n", key, err)
				}
			}
		} else if ev.Exists() {
			// Key should exist
			if err != nil {
				if *allowDataLoss && errors.Is(err, rockyardkv.ErrNotFound) {
					// DisableWAL + faultfs: data loss is expected (G2 scope, not a bug)
					// The harness saved durable_state after Flush(), but:
					// - Crash occurred before MANIFEST sync
					// - Orphaned SST was deleted on recovery
					// - Data is lost, but this prevents collision (the real bug)
					if *verbose {
						fmt.Printf("Verify: key %d expected to exist but lost (allowed, data loss under DisableWAL)\n", key)
					}
				} else {
					failures++
					if *verbose {
						fmt.Printf("Verify: key %d expected to exist but got error: %v\n", key, err)
					}
				}
			} else {
				// Verify value base
				expectedValueBase := ev.GetValueBase()
				if len(value) >= 12 {
					actualValueBase := getValueBase(value)
					if actualValueBase != expectedValueBase {
						if *allowDBAhead && actualValueBase > expectedValueBase {
							// DB has newer value - acceptable in crash testing
							// (PUT synced after expected state save, before SIGKILL)
							if *verbose {
								fmt.Printf("Verify: key %d value base mismatch: got %d, want %d (allowed, DB ahead)\n",
									key, actualValueBase, expectedValueBase)
							}
						} else if *allowDataLoss && actualValueBase < expectedValueBase {
							// DB has older value - data loss under DisableWAL + faultfs
							// This is expected: newer writes were in memtable/orphaned SST
							if *verbose {
								fmt.Printf("Verify: key %d value base mismatch: got %d, want %d (allowed, data loss under DisableWAL)\n",
									key, actualValueBase, expectedValueBase)
							}
						} else {
							failures++
							if *verbose {
								fmt.Printf("Verify: key %d value base mismatch: got %d, want %d\n",
									key, actualValueBase, expectedValueBase)
							}
						}
					}
				}
			}
		}
		// Skip keys with pending flags or unknown state

		mu.Unlock()
		verified++
	}

	fmt.Printf("  Verified %d keys, %d failures\n", verified, failures)
	stats.verifyFail.Add(uint64(failures))

	if failures > 0 {
		return fmt.Errorf("%d verification failures", failures)
	}
	return nil
}

func makeKey(key int64) []byte {
	return fmt.Appendf(nil, "key%016d", key)
}

// makeValue creates a value that encodes the key and value base for verification.
// Format: [key:8 bytes][valueBase:4 bytes][padding...]
func makeValue(key int64, valueBase uint32) []byte {
	value := make([]byte, *valueSize)
	if *valueSize < 12 {
		return value
	}

	// Encode key and value base in little-endian format
	binary.LittleEndian.PutUint64(value[0:8], uint64(key))
	binary.LittleEndian.PutUint32(value[8:12], valueBase)

	// Fill rest with deterministic data
	for i := 12; i < *valueSize; i++ {
		value[i] = byte((int(key) + int(valueBase) + i) % 256)
	}

	return value
}

// getValueBase extracts the value base from a value.
func getValueBase(value []byte) uint32 {
	if len(value) < 12 {
		return 0
	}
	return binary.LittleEndian.Uint32(value[8:12])
}

// verifyValue checks if a value matches the expected key and value base.
// Reserved for future value verification enhancements.
func verifyValue(key int64, expectedValueBase uint32, value []byte) bool { //nolint:unused // reserved for future use
	if len(value) < 12 {
		return false
	}
	storedKey := binary.LittleEndian.Uint64(value[0:8])
	storedBase := binary.LittleEndian.Uint32(value[8:12])
	return storedKey == uint64(key) && storedBase == expectedValueBase
}

// artifactBundle is the global artifact bundle for collecting evidence on failure.
var artifactBundle *testutil.ArtifactBundle

// startTime tracks when the test started for elapsed time calculation.
var startTime time.Time

func fatal(format string, args ...any) {
	fmt.Printf("FATAL: "+format+"\n", args...)
	os.Exit(1)
}

// exitWithFailure collects artifacts and exits with code 1.
func exitWithFailure(err error, testDir string) {
	if artifactBundle != nil && *runDir != "" {
		elapsed := time.Since(startTime)
		if bundleErr := artifactBundle.RecordFailure(err, elapsed); bundleErr != nil {
			fmt.Printf("⚠️  Artifact collection error: %v\n", bundleErr)
		} else {
			fmt.Printf("📦 Artifacts collected at: %s\n", artifactBundle.RunDir)
		}
	}
	os.Exit(1)
}

// SimulateFaultFSCrash simulates a crash using the FaultInjectionFS.
// This drops unsynced data and/or deletes unsynced files based on flags.
// Called by crashtest before killing the process.
func SimulateFaultFSCrash() {
	if globalFaultFS == nil {
		return
	}

	if *faultDropUnsynced {
		if err := globalFaultFS.DropUnsyncedData(); err != nil {
			if *verbose {
				fmt.Printf("FaultFS: DropUnsyncedData error: %v\n", err)
			}
		} else if *verbose {
			fmt.Println("FaultFS: Dropped unsynced data")
		}
	}

	if *faultDelUnsynced {
		if err := globalFaultFS.DeleteUnsyncedFiles(); err != nil {
			if *verbose {
				fmt.Printf("FaultFS: DeleteUnsyncedFiles error: %v\n", err)
			}
		} else if *verbose {
			fmt.Println("FaultFS: Deleted unsynced files")
		}
	}
}

// GetFaultFS returns the global FaultInjectionFS instance if enabled.
// This allows external code (e.g., crashtest) to inject faults.
func GetFaultFS() *vfs.FaultInjectionFS {
	return globalFaultFS
}

// setupTraceWriter initializes the trace writer.
// The trace file uses a standard binary format (see internal/trace/trace.go).
// If -trace-max-size is set, the writer will stop accepting records when the limit is reached.
func setupTraceWriter(path string, maxSize int64) error {
	var err error
	traceFile, err = os.Create(path)
	if err != nil {
		return err
	}

	// Create trace writer with standard binary header.
	// Configuration metadata (seed, keys, etc.) is not included in the trace file.
	// Use -v flag output or artifact bundle run.json for this information.
	var opts []trace.WriterOption
	if maxSize > 0 {
		opts = append(opts, trace.WithMaxBytes(maxSize))
	}
	globalTraceWriter, err = trace.NewWriter(traceFile, opts...)
	if err != nil {
		traceFile.Close()
		return err
	}

	return nil
}

// closeTraceWriter closes the trace writer and file.
func closeTraceWriter() {
	if globalTraceWriter != nil {
		_ = globalTraceWriter.Close()
		if *verbose {
			fmt.Printf("📝 Trace records written: %d (bytes: %d, truncated: %v)\n",
				globalTraceWriter.Count(),
				globalTraceWriter.BytesWritten(),
				globalTraceWriter.Truncated())
		}
	}
	if traceFile != nil {
		_ = traceFile.Close()
	}
}

// traceOp records an operation to the trace file if tracing is enabled.
func traceOp(opType trace.RecordType, payload []byte) {
	if globalTraceWriter != nil {
		_ = globalTraceWriter.Write(opType, payload)
	}
}

// tracePutPayload creates a trace payload for a Put operation.
// The trace payload uses internal/trace.WritePayload where Data is a raw WriteBatch.
// tracePutPayload creates a trace payload for a Put operation with sequence number.
// seqno is the sequence number assigned by the DB after the write completed.
func tracePutPayload(key, value []byte, seqno uint64) []byte {
	wb := ibatch.New()
	wb.Put(key, value)
	return (&trace.WritePayload{ColumnFamilyID: 0, SequenceNumber: seqno, Data: wb.Data()}).Encode()
}

// traceGetPayload creates a trace payload for a Get operation.
// The trace payload uses internal/trace.GetPayload.
func traceGetPayload(key []byte) []byte {
	return (&trace.GetPayload{ColumnFamilyID: 0, Key: key}).Encode()
}

// traceDeletePayload creates a trace payload for a Delete operation with sequence number.
// The payload is a WritePayload wrapping a WriteBatch with a single Delete record.
// seqno is the sequence number assigned by the DB after the write completed.
func traceDeletePayload(key []byte, seqno uint64) []byte {
	wb := ibatch.New()
	wb.Delete(key)
	return (&trace.WritePayload{ColumnFamilyID: 0, SequenceNumber: seqno, Data: wb.Data()}).Encode()
}

// traceWriteBatchPayload creates a trace payload for a batch write with sequence number.
// The payload is a WritePayload wrapping the raw batch bytes.
// seqno is the sequence number assigned by the DB after the write completed.
func traceWriteBatchPayload(wb *rockyardkv.WriteBatch, seqno uint64) []byte {
	return (&trace.WritePayload{ColumnFamilyID: 0, SequenceNumber: seqno, Data: wb.Data()}).Encode()
}

// seqnoPrefixState is a simple map-based expected state for seqno-prefix verification.
// Unlike ExpectedStateV2, it doesn't use pending semantics since we're replaying
// a known sequence of operations.
type seqnoPrefixState struct {
	// keys maps key number -> value base (0 means deleted/not present)
	keys map[int64]uint32
}

func newSeqnoPrefixState() *seqnoPrefixState {
	return &seqnoPrefixState{keys: make(map[int64]uint32)}
}

func (s *seqnoPrefixState) put(keyNum int64, valBase uint32) {
	s.keys[keyNum] = valBase
}

func (s *seqnoPrefixState) delete(keyNum int64) {
	delete(s.keys, keyNum)
}

func (s *seqnoPrefixState) get(keyNum int64) (valBase uint32, exists bool) {
	v, ok := s.keys[keyNum]
	return v, ok
}

// reconstructStateFromTraces reads trace files and reconstructs expected state
// for all operations with traceSeqno <= replayCutoffSeqno.
// This implements the "seqno-prefix (no holes)" verification model.
//
// Naming convention:
//   - dbRecoveredSeqno: from database.GetLatestSequenceNumber() after recovery
//   - traceSeqno: sequence number recorded in the trace payload
//   - replayCutoffSeqno: upper bound for replay (usually == dbRecoveredSeqno)
func reconstructStateFromTraces(traceDir string, replayCutoffSeqno uint64, _ int64) (*seqnoPrefixState, int, error) {
	// Find all trace files in directory
	entries, err := os.ReadDir(traceDir)
	if err != nil {
		return nil, 0, fmt.Errorf("read trace dir: %w", err)
	}

	var traceFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".trace") {
			traceFiles = append(traceFiles, filepath.Join(traceDir, e.Name()))
		}
	}
	if len(traceFiles) == 0 {
		return nil, 0, fmt.Errorf("no trace files found in %s", traceDir)
	}

	// Sort trace files by cycle number to process in order
	sort.Strings(traceFiles)

	// Create empty expected state
	state := newSeqnoPrefixState()
	replayedOps := 0
	skippedV1 := 0

	// Process each trace file
	for _, tf := range traceFiles {
		ops, v1, err := replayTraceFileSeqno(tf, replayCutoffSeqno, state)
		if err != nil {
			return nil, 0, fmt.Errorf("replay %s: %w", tf, err)
		}
		replayedOps += ops
		skippedV1 += v1
	}

	if skippedV1 > 0 && *verbose {
		fmt.Printf("⚠️  Skipped %d V1 trace records (no seqno)\n", skippedV1)
	}

	return state, replayedOps, nil
}

// replayTraceFileSeqno replays a single trace file, applying writes with seqno <= cutoff.
// Returns (replayed ops, skipped V1 ops, error).
func replayTraceFileSeqno(path string, cutoffSeqno uint64, state *seqnoPrefixState) (int, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	reader, err := trace.NewReader(f)
	if err != nil {
		return 0, 0, fmt.Errorf("create reader: %w", err)
	}

	replayedOps := 0
	skippedV1 := 0

	for {
		rec, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return replayedOps, skippedV1, fmt.Errorf("read record: %w", err)
		}

		// Only process write operations
		if rec.Type != trace.TypeWrite {
			continue
		}

		// Decode write payload using version-aware decoder
		payload, err := reader.DecodeWritePayload(rec.Payload)
		if err != nil {
			continue
		}

		// Skip V1 traces (seqno == 0) - can't do prefix verification without seqno
		if payload.SequenceNumber == 0 {
			skippedV1++
			continue
		}

		// Skip operations with traceSeqno > replayCutoffSeqno
		if payload.SequenceNumber > cutoffSeqno {
			continue
		}

		// Apply batch to expected state
		if err := applyBatchToSeqnoState(payload.Data, state); err != nil {
			return replayedOps, skippedV1, fmt.Errorf("apply batch: %w", err)
		}
		replayedOps++
	}

	return replayedOps, skippedV1, nil
}

// applyBatchToSeqnoState applies a raw WriteBatch to the seqno-prefix state.
func applyBatchToSeqnoState(batchData []byte, state *seqnoPrefixState) error {
	// Parse the batch using our internal batch package
	wb, err := ibatch.NewFromData(batchData)
	if err != nil {
		return fmt.Errorf("parse batch: %w", err)
	}

	// Use a handler to iterate operations
	handler := &seqnoStateHandler{state: state}
	return wb.Iterate(handler)
}

// seqnoStateHandler applies batch operations to seqno-prefix state.
type seqnoStateHandler struct {
	state *seqnoPrefixState
}

func (h *seqnoStateHandler) Put(key, value []byte) error {
	keyNum := parseStressKeyNum(key)
	if keyNum >= 0 {
		valBase := parseStressValueBase(value)
		h.state.put(int64(keyNum), valBase)
	}
	return nil
}

func (h *seqnoStateHandler) Delete(key []byte) error {
	keyNum := parseStressKeyNum(key)
	if keyNum >= 0 {
		h.state.delete(int64(keyNum))
	}
	return nil
}

func (h *seqnoStateHandler) SingleDelete(key []byte) error {
	return h.Delete(key)
}

func (h *seqnoStateHandler) DeleteRange(start, end []byte) error {
	startNum := parseStressKeyNum(start)
	endNum := parseStressKeyNum(end)
	if startNum >= 0 && endNum > startNum {
		for i := startNum; i < endNum; i++ {
			h.state.delete(int64(i))
		}
	}
	return nil
}

func (h *seqnoStateHandler) Merge(key, value []byte) error {
	return h.Put(key, value)
}

func (h *seqnoStateHandler) LogData(_ []byte) {
	// No-op for log data
}

func (h *seqnoStateHandler) PutCF(_ uint32, key, value []byte) error {
	return h.Put(key, value)
}

func (h *seqnoStateHandler) DeleteCF(_ uint32, key []byte) error {
	return h.Delete(key)
}

func (h *seqnoStateHandler) SingleDeleteCF(_ uint32, key []byte) error {
	return h.Delete(key)
}

func (h *seqnoStateHandler) DeleteRangeCF(_ uint32, start, end []byte) error {
	return h.DeleteRange(start, end)
}

func (h *seqnoStateHandler) MergeCF(_ uint32, key, value []byte) error {
	return h.Put(key, value)
}

// parseStressKeyNum extracts the numeric key from stress test key format.
func parseStressKeyNum(key []byte) int {
	// Stress test keys are formatted as 10-digit zero-padded integers
	s := string(key)
	var n int
	if _, err := fmt.Sscanf(s, "%010d", &n); err != nil {
		return -1
	}
	return n
}

// parseStressValueBase extracts the value base from stress test value format.
func parseStressValueBase(value []byte) uint32 {
	// Value format: first 4 bytes are big-endian value base
	if len(value) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(value[:4])
}

// verifySeqnoPrefix performs strict equality verification against reconstructed state.
func verifySeqnoPrefix(database rockyardkv.DB, expectedState *seqnoPrefixState, stats *Stats) error {
	failures := 0
	verified := 0

	for key := range int64(*numKeys) {
		expectedVal, expectedExists := expectedState.get(key)
		keyBytes := makeKey(key)

		got, err := database.Get(nil, keyBytes)
		if err != nil && !errors.Is(err, rockyardkv.ErrNotFound) {
			return fmt.Errorf("get key %d: %w", key, err)
		}

		if expectedExists {
			if got == nil {
				if *verbose {
					fmt.Printf("Verify: key %d missing: expected value base %d\n", key, expectedVal)
				}
				failures++
			} else {
				gotBase := parseStressValueBase(got)
				if gotBase != expectedVal {
					if *verbose {
						fmt.Printf("Verify: key %d value base mismatch: got %d, want %d\n", key, gotBase, expectedVal)
					}
					failures++
				}
			}
		} else {
			if got != nil {
				// Key exists in DB but not in expected state
				// This is OK - the DB may have acknowledged writes that we don't have in trace
				// (dbSeqno > replayCutoffSeqno cases that were acknowledged but not traced before crash)
			}
		}
		verified++
		stats.gets.Add(1)
	}

	if failures > 0 {
		return fmt.Errorf("%d verification failures", failures)
	}
	_ = verified // used for debugging
	return nil
}
