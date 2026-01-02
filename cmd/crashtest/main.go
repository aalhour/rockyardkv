// Crash test orchestrator for RockyardKV.
//
// Use `crashtest` to run write workloads, crash the process, and verify recovery.
// `crashtest` runs `stresstest` in a child process.
// `crashtest` kills the child at a chosen time, then runs a verification pass against the same DB directory.
//
// Use this tool to validate durability and recovery contracts under process death.
// Use `-seed` to make a run reproducible.
// Use `-run-dir` to collect an artifact bundle when verification fails.
//
// Run a basic crash loop:
//
// ```bash
// ./bin/crashtest -cycles=10 -duration=2m -sync
// ```
//
// Use a deterministic crash schedule:
//
// ```bash
// ./bin/crashtest -seed=123 -cycles=3 -crash-schedule="1s,250ms,5s" -sync
// ```
//
// Reference: RocksDB v10.7.5 `tools/db_crashtest.py`.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aalhour/rockyardkv/internal/testutil"
)

var (
	// Test configuration
	duration         = flag.Duration("duration", 10*time.Minute, "Total test duration")
	crashInterval    = flag.Duration("interval", 30*time.Second, "Average time between crashes")
	numCycles        = flag.Int("cycles", 0, "Number of crash cycles (0 = unlimited until duration)")
	dbPath           = flag.String("db", "", "Database path (default: temp directory)")
	keepDB           = flag.Bool("keep", false, "Keep database after test")
	verbose          = flag.Bool("v", false, "Verbose output")
	seed             = flag.Int64("seed", 0, "Random seed (0 for time-based)")
	stressThreads    = flag.Int("threads", 4, "Number of stress test threads")
	stressKeys       = flag.Int64("keys", 10000, "Number of keys in the key space")
	stressValueSize  = flag.Int("value-size", 100, "Size of each value in bytes (stresstest workload)")
	stressSync       = flag.Bool("sync", false, "Sync writes to disk during stress and verification")
	stressDisableWAL = flag.Bool("disable-wal", false, "Disable WAL during stress and verification")
	verifyTimeout    = flag.Duration("verify-timeout", 2*time.Minute, "Verification timeout")
	killMode         = flag.String("kill-mode", "random", "Kill mode: random, sigkill, sigterm")
	minInterval      = flag.Duration("min-interval", 5*time.Second, "Minimum time before crash")

	// Deterministic crash schedule (optional; default is random).
	// If set, overrides the random crash timing for the stress phase.
	//
	// Example:
	//   -crash-schedule="1s,250ms,5s"
	// By default, the schedule is strict: if cycles > entries, the run fails.
	crashSchedule       = flag.String("crash-schedule", "", "Comma-separated list of crash-after durations (per cycle) overriding random crash timing (e.g. \"1s,250ms,5s\")")
	crashScheduleRepeat = flag.Bool("crash-schedule-repeat", false, "When -crash-schedule is shorter than cycles, repeat the last entry instead of failing")

	// Fault injection flags (propagated to stresstest)
	faultFS           = flag.Bool("faultfs", false, "Enable FaultInjectionFS for durability testing")
	faultDropUnsynced = flag.Bool("faultfs-drop-unsynced", false, "Drop unsynced data on simulated crash (requires -faultfs)")
	faultDelUnsynced  = flag.Bool("faultfs-delete-unsynced", false, "Delete unsynced files on simulated crash (requires -faultfs)")

	// Artifact collection
	runDir = flag.String("run-dir", "", "Directory for artifact collection on failure (default: auto-generated)")

	// Trace collection (propagated to stresstest)
	traceDir = flag.String("trace-dir", "", "Directory to write stresstest operation traces (one per crash cycle)")

	// Verification mode for the verification phase (stresstest invocation).
	//
	// - auto: if -trace-dir is set and WAL is enabled, use seqno-prefix verification; otherwise expected-state verification.
	// - expected: use expected-state verification (verify-only + expected-state) with DB-ahead tolerances.
	// - seqno-prefix: use trace-based seqno-prefix verification (requires -trace-dir; requires WAL enabled).
	verifyMode = flag.String("verify-mode", "auto", "Verification mode: auto, expected, seqno-prefix")
)

// TestMode represents the test execution mode
type TestMode int

const (
	ModeStress TestMode = iota
	ModeVerify
)

// Stats tracks crash test statistics
type Stats struct {
	cycles           int
	successfulCrash  int
	successfulVerify int
	failedVerify     int
	errors           int
	startTime        time.Time
}

func main() {
	flag.Parse()

	if *verifyMode != "auto" && *verifyMode != "expected" && *verifyMode != "seqno-prefix" {
		fatal("Invalid -verify-mode: %q (expected: auto, expected, seqno-prefix)", *verifyMode)
	}

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}

	rand.Seed(*seed)

	printBanner()

	// Setup database path
	testDir := setupDBPath()
	defer cleanupDBPath(testDir)

	expectedStateFile := filepath.Join(testDir, "expected_state.bin")

	// Setup artifact bundle for failure collection
	artifactBundle, err := testutil.NewArtifactBundle(*runDir, "crashtest", *seed)
	if err != nil {
		fatal("Failed to create artifact bundle: %v", err)
	}
	artifactBundle.SetDBPath(testDir)
	artifactBundle.SetExpectedStatePath(expectedStateFile)
	artifactBundle.SetFlags(map[string]any{
		"duration":              duration.String(),
		"interval":              crashInterval.String(),
		"cycles":                *numCycles,
		"db":                    testDir,
		"threads":               *stressThreads,
		"keys":                  *stressKeys,
		"value-size":            *stressValueSize,
		"sync":                  *stressSync,
		"disable-wal":           *stressDisableWAL,
		"kill-mode":             *killMode,
		"faultfs":               *faultFS,
		"faultfs-drop-unsynced": *faultDropUnsynced,
		"faultfs-del-unsynced":  *faultDelUnsynced,
	})

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n⚠️  Received interrupt, shutting down...")
		cancel()
	}()

	// Run crash test cycles
	stats := &Stats{startTime: time.Now()}
	testErr := runCrashTestCycles(ctx, testDir, stats)

	// Print final stats
	printStats(stats)

	// Determine final result
	elapsed := time.Since(stats.startTime)
	failed := testErr != nil || stats.failedVerify > 0

	if failed {
		var failErr error
		if testErr != nil {
			failErr = testErr
		} else {
			failErr = fmt.Errorf("verification failures: %d", stats.failedVerify)
		}

		// Collect artifacts on failure
		if bundleErr := artifactBundle.RecordFailure(failErr, elapsed); bundleErr != nil {
			fmt.Printf("⚠️  Artifact collection error: %v\n", bundleErr)
		} else {
			fmt.Printf("📦 Artifacts collected at: %s\n", artifactBundle.RunDir)
		}

		fmt.Printf("\n❌ CRASH TEST FAILED: %v\n", failErr)
		os.Exit(1)
	}

	artifactBundle.RecordSuccess(elapsed)
	fmt.Println("✅ CRASH TEST PASSED")
}

func printBanner() {
	const boxWidth = 74 // inner width between ║ and ║

	line := func(content string) {
		// Pad or truncate content to exactly boxWidth-2 chars (for leading/trailing space)
		padded := fmt.Sprintf(" %-*s", boxWidth-2, content)
		if len(padded) > boxWidth-1 {
			padded = padded[:boxWidth-1]
		}
		fmt.Printf("║%s║\n", padded)
	}

	border := "╔" + repeatChar('═', boxWidth) + "╗"
	middle := "╠" + repeatChar('═', boxWidth) + "╣"
	bottom := "╚" + repeatChar('═', boxWidth) + "╝"

	fmt.Println(border)
	line(center("RockyardKV Crash Test Orchestrator", boxWidth-2))
	fmt.Println(middle)
	line(fmt.Sprintf("Duration: %-10s Interval: %-10s Seed: %d", *duration, *crashInterval, *seed))
	line(fmt.Sprintf("Kill Mode: %-8s Threads: %-4d Keys: %d", *killMode, *stressThreads, *stressKeys))
	fmt.Println(middle)
	line(fmt.Sprintf("Repro: -seed=%d -duration=%s -interval=%s", *seed, *duration, *crashInterval))
	fmt.Println(bottom)
	fmt.Println()
}

func repeatChar(ch rune, n int) string {
	result := make([]rune, n)
	for i := range result {
		result[i] = ch
	}
	return string(result)
}

func center(s string, width int) string {
	if len(s) >= width {
		return s
	}
	pad := (width - len(s)) / 2
	return fmt.Sprintf("%*s%s%*s", pad, "", s, width-len(s)-pad, "")
}

func setupDBPath() string {
	var testDir string
	var err error
	if *dbPath == "" {
		testDir, err = os.MkdirTemp("", "rockyard-crashtest-*")
		if err != nil {
			fatal("Failed to create temp dir: %v", err)
		}
	} else {
		testDir = *dbPath
		// Clean up existing database
		os.RemoveAll(testDir)
		if err := os.MkdirAll(testDir, 0755); err != nil {
			fatal("Failed to create db dir: %v", err)
		}
	}
	return testDir
}

func cleanupDBPath(testDir string) {
	if !*keepDB && *dbPath == "" {
		os.RemoveAll(testDir)
	} else if *keepDB {
		fmt.Printf("📁 Database kept at: %s\n", testDir)
	}
}

func runCrashTestCycles(ctx context.Context, testDir string, stats *Stats) error {
	deadline := time.Now().Add(*duration)
	expectedStateFile := filepath.Join(testDir, "expected_state.bin")

	var schedule []time.Duration
	if *crashSchedule != "" {
		var err error
		schedule, err = parseCrashSchedule(*crashSchedule)
		if err != nil {
			return fmt.Errorf("invalid -crash-schedule: %w", err)
		}
	}

	for {
		// Check if we should stop
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if time.Now().After(deadline) {
			fmt.Println("\n⏱️  Duration limit reached")
			break
		}

		if *numCycles > 0 && stats.cycles >= *numCycles {
			fmt.Printf("\n🔄 Completed %d cycles\n", *numCycles)
			break
		}

		stats.cycles++
		fmt.Printf("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
		fmt.Printf("Cycle %d | Elapsed: %s | Remaining: %s\n",
			stats.cycles,
			time.Since(stats.startTime).Round(time.Second),
			time.Until(deadline).Round(time.Second))
		fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

		// Choose crash interval (scheduled or random).
		crashAt, err := chooseCrashAfter(stats.cycles, schedule)
		if err != nil {
			return err
		}
		if len(schedule) > 0 {
			fmt.Printf("🧭 Will crash after %s (schedule)\n", crashAt.Round(time.Millisecond))
		} else {
			fmt.Printf("🎲 Will crash after %s\n", crashAt.Round(time.Millisecond))
		}

		// Run stress test for a random interval, then kill
		err = runStressAndCrash(ctx, testDir, expectedStateFile, crashAt, stats)
		if err != nil {
			if ctx.Err() != nil {
				return nil // Context cancelled
			}
			stats.errors++
			fmt.Printf("⚠️  Stress phase error: %v\n", err)
			continue
		}
		stats.successfulCrash++

		// Run verification
		fmt.Printf("🔍 Running verification...\n")
		err = runVerification(ctx, testDir, expectedStateFile, stats)
		if err != nil {
			stats.failedVerify++
			fmt.Printf("❌ Verification failed: %v\n", err)
			// On verification failure, we should stop (this is a real bug)
			return err
		}
		stats.successfulVerify++
		fmt.Printf("✓ Verification passed\n")

		// CRITICAL: When DisableWAL is enabled, reset expected_state to match durable_state
		// after each successful verification. This prevents expected state drift:
		// - Unflushed writes are recorded in expected_state but lost after crash
		// - Without reset, next cycle thinks current value is higher than reality
		// - This causes value_base mismatch: expected=N, actual=M where N >> M
		if *stressDisableWAL {
			durableStateFile := expectedStateFile + ".durable"
			if err := copyFile(durableStateFile, expectedStateFile); err != nil {
				// If durable state doesn't exist, that's OK - it means no flushes happened
				if !os.IsNotExist(err) {
					fmt.Printf("⚠️  Failed to reset expected state: %v\n", err)
				}
			}
		}
	}

	// Final verification
	fmt.Printf("\n🔍 Running final verification...\n")
	err := runVerification(ctx, testDir, expectedStateFile, stats)
	if err != nil {
		stats.failedVerify++
		return fmt.Errorf("final verification failed: %w", err)
	}
	stats.successfulVerify++
	fmt.Printf("✓ Final verification passed\n")

	return nil
}

func calculateCrashInterval() time.Duration {
	// Use exponential distribution for random intervals
	// This gives variation while centering around the target
	base := float64(*crashInterval)

	// Random factor between 0.2 and 2.0 (exponential-like)
	factor := 0.2 + rand.Float64()*1.8
	interval := max(
		// Ensure minimum interval
		time.Duration(base*factor), *minInterval)

	return interval
}

func parseCrashSchedule(s string) ([]time.Duration, error) {
	parts := strings.Split(s, ",")
	out := make([]time.Duration, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		d, err := time.ParseDuration(p)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", p, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("duration must be > 0: %q", p)
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty schedule")
	}
	return out, nil
}

func chooseCrashAfter(cycle int, schedule []time.Duration) (time.Duration, error) {
	if len(schedule) == 0 {
		return calculateCrashInterval(), nil
	}
	// cycle is 1-based.
	idx := cycle - 1
	if idx < 0 {
		return 0, fmt.Errorf("internal: invalid cycle %d", cycle)
	}
	if idx < len(schedule) {
		return schedule[idx], nil
	}
	if *crashScheduleRepeat {
		return schedule[len(schedule)-1], nil
	}
	return 0, fmt.Errorf("crash schedule exhausted: cycle=%d schedule_len=%d (set -crash-schedule-repeat to repeat last)", cycle, len(schedule))
}

func runStressAndCrash(ctx context.Context, testDir, expectedStateFile string, crashAfter time.Duration, stats *Stats) error {
	// Derive a reproducible seed for this cycle.
	// Using the base seed + cycle number ensures each cycle is deterministic
	// when the same base seed is provided.
	cycleSeed := *seed + int64(stats.cycles)

	// Build stress command
	stressArgs := []string{
		"-db", testDir,
		"-duration", "10m", // Long duration, we'll kill it
		"-threads", fmt.Sprintf("%d", *stressThreads),
		"-keys", fmt.Sprintf("%d", *stressKeys),
		"-value-size", fmt.Sprintf("%d", *stressValueSize),
		"-seed", fmt.Sprintf("%d", cycleSeed), // Pass derived seed for reproducibility
		"-reopen", "0", // Disable reopens during stress phase
		"-flush", "2s", // Frequent flushes
		"-expected-state", expectedStateFile, // Persistent expected state
		"-save-expected",                   // Save state after operations
		"-save-expected-interval", "100ms", // Frequent saves to minimize race window
		"-v",
	}

	// Optional trace emission for post-mortem analysis.
	// This is strictly a tooling feature (no DB behavior changes).
	if *traceDir != "" {
		if err := os.MkdirAll(*traceDir, 0o755); err != nil {
			return fmt.Errorf("failed to create trace dir: %w", err)
		}
		tracePath := filepath.Join(*traceDir, fmt.Sprintf("cycle_%02d_seed_%d.trace", stats.cycles, cycleSeed))
		stressArgs = append(stressArgs, "-trace-out", tracePath)
		if *verbose {
			fmt.Printf("📝 Trace: %s\n", tracePath)
		}
	}

	if *stressSync {
		stressArgs = append(stressArgs, "-sync")
	}
	if *stressDisableWAL {
		stressArgs = append(stressArgs, "-disable-wal")
		// Track durable state at flush barriers for DisableWAL mode.
		// This allows verification to tolerate unflushed writes being lost.
		durableStateFile := expectedStateFile + ".durable"
		stressArgs = append(stressArgs, "-durable-state", durableStateFile)
	}

	// Propagate fault injection flags to stresstest for durability testing.
	// This enables simulating fsync lies and missing dir sync anomalies.
	if *faultFS {
		stressArgs = append(stressArgs, "-faultfs")
		// Enable crash simulation on SIGTERM so the stresstest can apply
		// FaultInjectionFS effects (drop unsynced data, delete unsynced files)
		// before exiting when we send SIGTERM.
		stressArgs = append(stressArgs, "-faultfs-simulate-crash-on-signal")
	}
	if *faultDropUnsynced {
		stressArgs = append(stressArgs, "-faultfs-drop-unsynced")
	}
	if *faultDelUnsynced {
		stressArgs = append(stressArgs, "-faultfs-delete-unsynced")
	}

	// Create command with context for timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, crashAfter+5*time.Second)
	defer cancel()

	stressBin := getStressBinary()
	cmd := exec.CommandContext(timeoutCtx, stressBin, stressArgs...)

	if *verbose {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	// Start the process
	fmt.Printf("🚀 Starting stress test (PID will follow)...\n")
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start stress test: %w", err)
	}
	fmt.Printf("   PID: %d\n", cmd.Process.Pid)

	// Wait for crash interval
	select {
	case <-time.After(crashAfter):
		// Time to crash!
		fmt.Printf("💥 Sending kill signal after %s\n", crashAfter.Round(time.Millisecond))
		if err := killProcess(cmd.Process); err != nil {
			return fmt.Errorf("failed to send kill signal: %w", err)
		}
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		return ctx.Err()
	}

	// Wait for process to die
	waitErr := cmd.Wait()

	// We expect an error because we killed it
	if waitErr == nil {
		fmt.Printf("⚠️  Process exited normally before kill signal\n")
	} else if *verbose {
		fmt.Printf("   Process exited with: %v\n", waitErr)
	}

	return nil
}

func runVerification(ctx context.Context, testDir, expectedStateFile string, stats *Stats) error {
	// Derive verification seed (same pattern as stress for reproducibility)
	verifySeed := *seed + int64(stats.cycles) + 1000000 // Offset to differentiate from stress

	// Run stress test in the selected verification mode.
	stressArgs := []string{
		"-db", testDir,
		"-duration", "5s", // Short duration for verification
		"-threads", "1", // Single thread for verification
		"-keys", fmt.Sprintf("%d", *stressKeys),
		"-value-size", fmt.Sprintf("%d", *stressValueSize),
		"-seed", fmt.Sprintf("%d", verifySeed), // Pass derived seed for reproducibility
		"-verify-every", "1", // Verify everything
		"-reopen", "0",
		"-v",
	}

	mode := *verifyMode
	if mode == "auto" {
		// Seqno-prefix verification requires traces and WAL.
		if *traceDir != "" && !*stressDisableWAL {
			mode = "seqno-prefix"
		} else {
			mode = "expected"
		}
	}

	switch mode {
	case "expected":
		stressArgs = append(stressArgs,
			"-expected-state", expectedStateFile, // Load persisted expected state
			"-verify-only",
			"-allow-db-ahead", // Allow DB to be ahead of expected state (race condition)
		)
	case "seqno-prefix":
		if *stressDisableWAL {
			return fmt.Errorf("verify-mode=seqno-prefix requires WAL enabled (do not use with -disable-wal)")
		}
		if *traceDir == "" {
			return fmt.Errorf("verify-mode=seqno-prefix requires -trace-dir")
		}
		stressArgs = append(stressArgs,
			"-seqno-prefix-verify",
			"-trace-dir", *traceDir,
		)
	default:
		return fmt.Errorf("internal: unexpected verify mode %q", mode)
	}

	if *stressSync {
		stressArgs = append(stressArgs, "-sync")
	}
	if *stressDisableWAL {
		stressArgs = append(stressArgs, "-disable-wal")
		// Pass durable state file for DisableWAL verification.
		// This allows the verifier to compare against the last flush barrier
		// instead of the full expected state (unflushed writes may be lost).
		durableStateFile := expectedStateFile + ".durable"
		stressArgs = append(stressArgs, "-durable-state", durableStateFile)

		// When DisableWAL + faultfs, data loss is expected.
		// The harness saves durable_state after Flush() returns, but with faultfs:
		// - Crash can occur after flush but before MANIFEST sync
		// - Orphaned SST is deleted on recovery
		// - Data is lost, but this prevents collision (the real bug)
		// We allow this data loss instead of failing verification (G2 scope).
		if *faultFS {
			stressArgs = append(stressArgs, "-allow-data-loss")
		}
	}

	// Propagate fault injection flags for verification as well.
	// Note: During verification, we typically don't drop/delete unsynced data
	// because we want to see if the DB can recover from what actually persisted.
	if *faultFS {
		stressArgs = append(stressArgs, "-faultfs")
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, *verifyTimeout)
	defer cancel()

	stressBin := getStressBinary()
	cmd := exec.CommandContext(timeoutCtx, stressBin, stressArgs...)

	if *verbose {
		// Use Run() with os.Stdout/Stderr in verbose mode
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			return fmt.Errorf("verification failed: %w", err)
		}
		return nil
	}

	// Non-verbose: capture output
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Verification output:\n%s\n", string(output))
		return fmt.Errorf("verification failed: %w", err)
	}

	return nil
}

func killProcess(proc *os.Process) error {
	var sig syscall.Signal

	// When FaultInjectionFS is enabled, we MUST use SIGTERM (not SIGKILL) so
	// the stresstest can apply the fault injection effects (drop unsynced data,
	// delete unsynced files) before exiting. SIGKILL prevents this.
	if *faultFS {
		sig = syscall.SIGTERM
		if *verbose {
			fmt.Printf("   Using SIGTERM (faultfs mode) to allow crash simulation\n")
		}
	} else {
		switch *killMode {
		case "sigterm":
			sig = syscall.SIGTERM
		case "sigkill", "random":
			// Random mode chooses between SIGKILL and SIGTERM
			if *killMode == "random" && rand.Intn(2) == 0 {
				sig = syscall.SIGTERM
			} else {
				sig = syscall.SIGKILL
			}
		default:
			sig = syscall.SIGKILL
		}
	}

	if *verbose {
		fmt.Printf("   Sending %s to PID %d\n", sig, proc.Pid)
	}
	if err := proc.Signal(sig); err != nil {
		return err
	}
	return nil
}

func getStressBinary() string {
	// Always use binaries under bin/*.
	// This keeps the harness reproducible and consistent with `make build`.
	path := "./bin/stresstest"
	if _, err := os.Stat(path); err == nil {
		return path
	}
	fatal("Missing required binary %s. Run: make clean build", path)
	return "" // unreachable
}

func printStats(stats *Stats) {
	const boxWidth = 74

	line := func(content string) {
		padded := fmt.Sprintf(" %-*s", boxWidth-2, content)
		if len(padded) > boxWidth-1 {
			padded = padded[:boxWidth-1]
		}
		fmt.Printf("║%s║\n", padded)
	}

	border := "╔" + repeatChar('═', boxWidth) + "╗"
	middle := "╠" + repeatChar('═', boxWidth) + "╣"
	bottom := "╚" + repeatChar('═', boxWidth) + "╝"

	elapsed := time.Since(stats.startTime)
	fmt.Printf("\n")
	fmt.Println(border)
	line(center("Crash Test Summary", boxWidth-2))
	fmt.Println(middle)
	line(fmt.Sprintf("Total Cycles:             %d", stats.cycles))
	line(fmt.Sprintf("Successful Crashes:       %d", stats.successfulCrash))
	line(fmt.Sprintf("Successful Verifications: %d", stats.successfulVerify))
	line(fmt.Sprintf("Failed Verifications:     %d", stats.failedVerify))
	line(fmt.Sprintf("Errors:                   %d", stats.errors))
	line(fmt.Sprintf("Elapsed Time:             %s", elapsed.Round(time.Second)))
	if stats.cycles > 0 {
		avgCycleTime := elapsed / time.Duration(stats.cycles)
		line(fmt.Sprintf("Avg Cycle Time:           %s", avgCycleTime.Round(time.Millisecond)))
	}
	fmt.Println(bottom)
}

// copyFile copies src to dst, overwriting dst if it exists.
// Used to reset expected_state to durable_state after crash cycles.
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return dstFile.Sync()
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
