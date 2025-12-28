// Trace-based crash recovery tests for RockyardKV.
//
// These tests verify the Option C recovery mechanism:
// 1. Save expected state snapshot before crash window
// 2. Record operations with sequence numbers to trace file
// 3. After crash, replay trace up to DB's recovered seqno
// 4. Verify recovered state matches expectations
//
// Reference: RocksDB v10.7.5
//   - db_stress_tool/expected_state.h (SaveAtAndAfter, Restore)
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/testutil"
)

// TestTraceRecovery_BasicPutCrash tests that trace-based recovery correctly
// reconstructs expected state after a crash.
func TestTraceRecovery_BasicPutCrash(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	recoveryBase := filepath.Join(dir, "recovery")

	// Create initial expected state with some baseline data
	initialState := testutil.NewExpectedStateV2(1000, 1, 4)
	initialState.SyncPut(0, 10, 1) // key 10 = value 1
	initialState.SyncPut(0, 20, 2) // key 20 = value 2
	initialState.SetPersistedSeqno(0)

	// Save initial state as expected state
	expectedStatePath := filepath.Join(dir, "expected_state.bin")
	if err := initialState.SaveToFile(expectedStatePath); err != nil {
		t.Fatalf("Save initial state: %v", err)
	}

	// Open DB and write initial keys
	database := openDBForTest(t, dbPath)
	writeInitialData(t, database, initialState)
	database.Close()

	// Create recovery orchestrator and start recording
	recovery := testutil.NewExpectedStateRecovery(recoveryBase, 1, 1000)
	tw, err := recovery.SaveAtAndAfter(initialState, 0)
	if err != nil {
		t.Fatalf("SaveAtAndAfter: %v", err)
	}

	// Simulate operations that would happen during stress test
	// These are recorded to the trace but may or may not survive crash
	tw.RecordPut(0, 10, 2, 1) // Update key 10: 1 -> 2, seqno 1
	tw.RecordPut(0, 30, 1, 2) // New key 30, seqno 2
	tw.RecordDelete(0, 20, 3) // Delete key 20, seqno 3
	tw.RecordPut(0, 40, 1, 4) // New key 40, seqno 4
	tw.RecordPut(0, 10, 3, 5) // Update key 10: 2 -> 3, seqno 5

	// Stop tracing
	if err := recovery.StopTracing(); err != nil {
		t.Fatalf("StopTracing: %v", err)
	}

	// Test recovery at different "recovered seqno" points
	testCases := []struct {
		name           string
		recoveredSeqno uint64
		expectations   map[int64]struct {
			exists    bool
			valueBase uint32
		}
	}{
		{
			name:           "seqno_0_snapshot_only",
			recoveredSeqno: 0,
			expectations: map[int64]struct {
				exists    bool
				valueBase uint32
			}{
				10: {true, 1},  // Original value
				20: {true, 2},  // Not deleted yet
				30: {false, 0}, // Not created yet
				40: {false, 0}, // Not created yet
			},
		},
		{
			name:           "seqno_2_partial_ops",
			recoveredSeqno: 2,
			expectations: map[int64]struct {
				exists    bool
				valueBase uint32
			}{
				10: {true, 2},  // Updated at seqno 1
				20: {true, 2},  // Not deleted yet (seqno 3)
				30: {true, 1},  // Created at seqno 2
				40: {false, 0}, // Not created yet (seqno 4)
			},
		},
		{
			name:           "seqno_3_with_delete",
			recoveredSeqno: 3,
			expectations: map[int64]struct {
				exists    bool
				valueBase uint32
			}{
				10: {true, 2},  // Updated at seqno 1
				20: {false, 0}, // Deleted at seqno 3
				30: {true, 1},  // Created at seqno 2
				40: {false, 0}, // Not created yet (seqno 4)
			},
		},
		{
			name:           "seqno_5_all_ops",
			recoveredSeqno: 5,
			expectations: map[int64]struct {
				exists    bool
				valueBase uint32
			}{
				10: {true, 3},  // Updated twice: seqno 1 and 5
				20: {false, 0}, // Deleted at seqno 3
				30: {true, 1},  // Created at seqno 2
				40: {true, 1},  // Created at seqno 4
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Restore state for this seqno
			recoveredState, applied, err := recovery.Restore(tc.recoveredSeqno)
			if err != nil {
				t.Fatalf("Restore: %v", err)
			}

			t.Logf("Recovered at seqno %d, applied %d operations", tc.recoveredSeqno, applied)

			// Verify expectations
			for key, exp := range tc.expectations {
				exists := recoveredState.Exists(0, key)
				if exists != exp.exists {
					t.Errorf("Key %d: exists = %v, want %v", key, exists, exp.exists)
					continue
				}
				if exp.exists {
					valueBase := recoveredState.GetValueBase(0, key)
					if valueBase != exp.valueBase {
						t.Errorf("Key %d: valueBase = %d, want %d", key, valueBase, exp.valueBase)
					}
				}
			}
		})
	}
}

// TestTraceRecovery_StressTestIntegration tests the full stresstest integration
// by running stresstest with -trace-recovery flag and simulating a crash.
func TestTraceRecovery_StressTestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	recoveryBase := filepath.Join(dir, "recovery")
	expectedStatePath := filepath.Join(dir, "expected_state.bin")

	// Find stresstest binary
	stresstest := findStresstestBinary(t)

	// Phase 1: Run stresstest with trace recovery, then crash it
	// Note: Using disable-wal mode with faultfs for proper crash simulation.
	// Without faultfs, DB.Close() might persist unflushed data.
	// Note: Only using put/delete/batch operations because trace recovery only
	// traces these ops. Other ops (transactions, merges, etc.) would cause gaps.
	args := []string{
		"-db", dbPath,
		"-keys", "100",
		"-threads", "4",
		"-duration", "3s",
		"-expected-state", expectedStatePath,
		"-save-expected",
		"-trace-recovery", recoveryBase,
		"-disable-wal",
		"-faultfs",
		"-faultfs-drop-unsynced",
		"-faultfs-delete-unsynced",
		"-faultfs-simulate-crash-on-signal",
		"-flush", "500ms", // Flush frequently to ensure some data is durable
		// Only use traced operations
		"-put", "50",
		"-get", "20",
		"-delete", "20",
		"-batch", "10",
		// Disable untraceable operations
		"-iter", "0",
		"-snapshot", "0",
		"-range-delete", "0",
		"-merge", "0",
		"-ingest", "0",
		"-txn", "0",
		"-compact", "0",
		"-snapshot-verify", "0",
		"-cf", "0",
		"-v",
	}

	cmd := exec.Command(stresstest, args...)
	cmd.Dir = dir

	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Start stresstest: %v", err)
	}

	// Let it run for 1 second, then kill it
	time.Sleep(1 * time.Second)

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Logf("SIGTERM failed: %v", err)
	}

	// Wait for process to exit
	_ = cmd.Wait()

	t.Logf("Stresstest output:\n%s", stdout.String())
	if stderr.Len() > 0 {
		t.Logf("Stresstest stderr:\n%s", stderr.String())
	}

	// Phase 2: Verify trace recovery files were created
	recovery := testutil.NewExpectedStateRecovery(recoveryBase, 1, 100)
	if !recovery.HasRecoveryFiles() {
		t.Skip("Trace recovery files not created (stresstest may have exited too quickly)")
	}

	// Phase 3: Open database and get recovered seqno
	database := openDBForTest(t, dbPath)
	recoveredSeqno := database.GetLatestSequenceNumber()
	t.Logf("DB recovered at seqno %d", recoveredSeqno)
	database.Close()

	// Phase 4: Restore expected state using trace
	recoveredState, applied, err := recovery.Restore(recoveredSeqno)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	t.Logf("Trace recovery: applied %d operations", applied)

	// Phase 5: Run stresstest in verify-only mode with trace recovery
	// Note: No faultfs needed for verify-only - just open DB normally
	verifyArgs := []string{
		"-db", dbPath,
		"-keys", "100",
		"-expected-state", expectedStatePath,
		"-trace-recovery", recoveryBase,
		"-disable-wal",
		"-verify-only",
		"-v",
	}

	verifyCmd := exec.Command(stresstest, verifyArgs...)
	verifyCmd.Dir = dir

	var verifyOut strings.Builder
	verifyCmd.Stdout = &verifyOut
	verifyCmd.Stderr = &verifyOut

	if err := verifyCmd.Run(); err != nil {
		t.Logf("Verify output:\n%s", verifyOut.String())
		t.Fatalf("Verification failed: %v", err)
	}

	t.Logf("✅ Trace recovery verification passed")

	// Count keys that exist in recovered state
	existCount := 0
	for key := range int64(100) {
		if recoveredState.Exists(0, key) {
			existCount++
		}
	}
	t.Logf("Recovered state: %d keys exist", existCount)
}

// TestTraceRecovery_Persistence verifies that recovery files survive and can
// be reused across multiple recovery attempts.
func TestTraceRecovery_Persistence(t *testing.T) {
	dir := t.TempDir()
	recoveryBase := filepath.Join(dir, "recovery")

	// Create and populate a recovery state
	initialState := testutil.NewExpectedStateV2(100, 1, 4)
	initialState.SyncPut(0, 1, 10)
	initialState.SyncPut(0, 2, 20)

	recovery1 := testutil.NewExpectedStateRecovery(recoveryBase, 1, 100)
	tw, err := recovery1.SaveAtAndAfter(initialState, 0)
	if err != nil {
		t.Fatalf("SaveAtAndAfter: %v", err)
	}

	// Record some operations
	tw.RecordPut(0, 1, 11, 1)
	tw.RecordPut(0, 3, 30, 2)
	tw.RecordDelete(0, 2, 3)

	if err := recovery1.StopTracing(); err != nil {
		t.Fatalf("StopTracing: %v", err)
	}

	// Create a new recovery orchestrator (simulating process restart)
	recovery2 := testutil.NewExpectedStateRecovery(recoveryBase, 1, 100)

	if !recovery2.HasRecoveryFiles() {
		t.Fatal("Recovery files should persist")
	}

	// Recover at seqno 2
	state, applied, err := recovery2.Restore(2)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if applied != 2 {
		t.Errorf("Applied = %d, want 2", applied)
	}

	// Verify state
	if !state.Exists(0, 1) || state.GetValueBase(0, 1) != 11 {
		t.Errorf("Key 1: got exists=%v valueBase=%d, want exists=true valueBase=11",
			state.Exists(0, 1), state.GetValueBase(0, 1))
	}
	if !state.Exists(0, 2) { // Not yet deleted at seqno 2
		t.Error("Key 2 should still exist at seqno 2")
	}
	if !state.Exists(0, 3) || state.GetValueBase(0, 3) != 30 {
		t.Errorf("Key 3: got exists=%v valueBase=%d, want exists=true valueBase=30",
			state.Exists(0, 3), state.GetValueBase(0, 3))
	}

	// Recover at seqno 3 (key 2 deleted)
	state2, _, err := recovery2.Restore(3)
	if err != nil {
		t.Fatalf("Restore at seqno 3: %v", err)
	}
	if state2.Exists(0, 2) {
		t.Error("Key 2 should be deleted at seqno 3")
	}
}

// Helper functions

func openDBForTest(t *testing.T, path string) db.DB {
	t.Helper()
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := db.Open(path, opts)
	if err != nil {
		t.Fatalf("Open database: %v", err)
	}
	return database
}

func writeInitialData(t *testing.T, database db.DB, state *testutil.ExpectedStateV2) {
	t.Helper()
	// Write data corresponding to the initial state
	for key := range int64(1000) {
		if state.Exists(0, key) {
			valueBase := state.GetValueBase(0, int64(key))
			keyBytes := fmt.Appendf(nil, "key%016d", key)
			valueBytes := makeTestValue(key, valueBase)
			if err := database.Put(nil, keyBytes, valueBytes); err != nil {
				t.Fatalf("Put key %d: %v", key, err)
			}
		}
	}
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func makeTestValue(key int64, valueBase uint32) []byte {
	// Format: [key:8 bytes][valueBase:4 bytes][padding]
	value := make([]byte, 100)
	for i := range 8 {
		value[i] = byte(key >> (i * 8))
	}
	for i := range 4 {
		value[8+i] = byte(valueBase >> (i * 8))
	}
	return value
}

func findStresstestBinary(t *testing.T) string {
	t.Helper()

	// Get the project root by finding go.mod
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}

	// Walk up to find project root (contains go.mod)
	root := wd
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatalf("Could not find project root")
		}
		root = parent
	}

	// Check if binary exists
	binPath := filepath.Join(root, "bin", "stresstest")
	if _, err := os.Stat(binPath); err == nil {
		return binPath
	}

	// Check env
	if p := os.Getenv("STRESSTEST_PATH"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}

	// Build it
	t.Log("Building stresstest binary...")
	outPath := filepath.Join(t.TempDir(), "stresstest")
	cmd := exec.Command("go", "build", "-o", outPath, filepath.Join(root, "cmd", "stresstest"))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("Cannot build stresstest: %v\n%s", err, out)
	}

	return outPath
}
