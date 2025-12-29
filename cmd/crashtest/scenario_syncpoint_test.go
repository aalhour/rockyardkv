//go:build synctest

// Syncpoint-driven whitebox crash tests for RockyardKV.
//
// These tests use sync points (not kill points) to deterministically block
// at specific code boundaries, then crash. Unlike kill points which trigger
// os.Exit(0), sync points block until signaled, allowing precise control
// over crash timing.
//
// Reference: RocksDB v10.7.5
//   - test_util/sync_point.h
//   - test_util/sync_point_impl.cc
//
// Build and run:
//
//	go test -tags synctest -v ./cmd/crashtest/... -run TestScenarioSyncpoint
//
// Environment variables:
//
//	SYNCPOINT_BLOCK=<NAME>   Block at this sync point
//	SYNCPOINT_CHILD=1        Run as child process (internal use)
//	WHITEBOX_ARTIFACT_DIR    Directory for artifact persistence
//	WHITEBOX_ALWAYS_PERSIST  Persist artifacts even on success
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/testutil"
)

// =============================================================================
// Syncpoint Whitebox Test: Flush at DBImpl::Write:BeforeMemtable
// =============================================================================

// TestScenarioSyncpoint_WriteBeforeMemtable verifies that a crash before
// memtable insertion (but after WAL write) recovers the write from WAL.
//
// Sync point: DBImpl::Write:BeforeMemtable
// Invariant: WAL-logged writes survive crash before memtable insertion.
func TestScenarioSyncpoint_WriteBeforeMemtable(t *testing.T) {
	runSyncpointScenario(t, testutil.SPDBWriteMemtable, func(database db.DB) {
		// Write with sync enabled (ensures WAL persistence)
		opts := db.DefaultWriteOptions()
		opts.Sync = true

		if err := database.Put(opts, []byte("syncpoint_key"), []byte("syncpoint_value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		// Crash will occur at DBImpl::Write:BeforeMemtable
	}, func(t *testing.T, database db.DB) {
		// Verify: key should exist (recovered from WAL)
		val, err := database.Get(nil, []byte("syncpoint_key"))
		if err != nil {
			t.Errorf("Key should exist after recovery: %v", err)
			return
		}
		if string(val) != "syncpoint_value" {
			t.Errorf("Value mismatch: got %q, want %q", val, "syncpoint_value")
		}
		t.Log("✅ WAL-logged write recovered after crash before memtable insertion")
	})
}

// TestScenarioSyncpoint_FlushBeforeSST verifies that a crash before SST write
// (during flush) leaves the database in a consistent state with memtable data
// intact (recovered from WAL).
//
// Sync point: FlushJob::Run:BeforeWriteSST
// Invariant: Crash before SST write preserves data in WAL.
func TestScenarioSyncpoint_FlushBeforeSST(t *testing.T) {
	runSyncpointScenario(t, testutil.SPFlushWriteSST, func(database db.DB) {
		// Write data
		opts := db.DefaultWriteOptions()
		opts.Sync = true

		for i := range 10 {
			key := fmt.Sprintf("flush_key_%03d", i)
			val := fmt.Sprintf("flush_val_%03d", i)
			if err := database.Put(opts, []byte(key), []byte(val)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Trigger flush - will crash at SPFlushWriteSST
		if err := database.Flush(nil); err != nil {
			// Expected to not return if crash happens
			t.Fatalf("Flush failed: %v", err)
		}
	}, func(t *testing.T, database db.DB) {
		// Verify: all keys should exist (recovered from WAL)
		for i := range 10 {
			key := fmt.Sprintf("flush_key_%03d", i)
			expectedVal := fmt.Sprintf("flush_val_%03d", i)
			val, err := database.Get(nil, []byte(key))
			if err != nil {
				t.Errorf("Key %s should exist: %v", key, err)
				continue
			}
			if string(val) != expectedVal {
				t.Errorf("Key %s value mismatch: got %q, want %q", key, val, expectedVal)
			}
		}
		t.Log("✅ All keys recovered from WAL after crash before SST write")
	})
}

// =============================================================================
// Syncpoint Harness
// =============================================================================

// syncpointArtifact contains information for reproducing a syncpoint test.
type syncpointArtifact struct {
	TestName      string    `json:"test_name"`
	SyncPoint     string    `json:"sync_point"`
	DBPath        string    `json:"db_path"`
	Timestamp     time.Time `json:"timestamp"`
	GoVersion     string    `json:"go_version"`
	OS            string    `json:"os"`
	Arch          string    `json:"arch"`
	ExitCode      int       `json:"exit_code"`
	ReproCmd      string    `json:"repro_cmd"`
	FailReason    string    `json:"fail_reason"`
	SyncPointHit  bool      `json:"sync_point_hit"`
	ChildDuration string    `json:"child_duration"`
}

// runSyncpointScenario runs a syncpoint-driven crash test.
//
// The test:
// 1. Spawns a child process with SYNCPOINT_BLOCK=<name>
// 2. Child enables sync points and blocks at the named point
// 3. Parent waits briefly then kills the child (simulating crash)
// 4. Parent reopens DB and runs verify function
//
// This differs from kill points:
// - Kill points: child calls os.Exit(0) at the point
// - Sync points: child blocks, parent kills it (SIGKILL)
func runSyncpointScenario(t *testing.T, syncPoint string, childWork func(db.DB), verify func(*testing.T, db.DB)) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping syncpoint scenario in short mode")
	}

	// Check if we're the child process
	if os.Getenv("SYNCPOINT_CHILD") == "1" {
		childDir := os.Getenv("SYNCPOINT_DB_DIR")
		if childDir == "" {
			t.Fatal("SYNCPOINT_DB_DIR not set in child")
		}
		runSyncpointChild(t, childDir, syncPoint, childWork)
		return
	}

	dir := t.TempDir()

	// Parent: spawn child and crash it
	startTime := time.Now()
	exitCode, stdout, stderr := runSyncpointChildProcess(t, dir, syncPoint)
	childDuration := time.Since(startTime)

	// Check if sync point was hit
	syncPointHit := bytes.Contains(stdout.Bytes(), []byte("SYNCPOINT_HIT:"+syncPoint))

	// Persist artifacts if needed
	alwaysPersist := os.Getenv("WHITEBOX_ALWAYS_PERSIST") == "1"
	if exitCode != 0 || !syncPointHit || alwaysPersist {
		failReason := "Success"
		if exitCode != 0 && exitCode != -1 { // -1 is SIGKILL (expected)
			failReason = fmt.Sprintf("Unexpected exit code: %d", exitCode)
		} else if !syncPointHit {
			failReason = "Sync point was NOT hit"
		} else if alwaysPersist {
			failReason = "Success (WHITEBOX_ALWAYS_PERSIST=1)"
		}
		persistSyncpointArtifacts(t, t.Name(), syncPoint, dir, &stdout, &stderr,
			exitCode, failReason, syncPointHit, childDuration)
	}

	// Verify sync point was hit
	if !syncPointHit {
		t.Fatalf("Sync point %s was NOT hit. Child output:\nstdout: %s\nstderr: %s",
			syncPoint, stdout.String(), stderr.String())
	}

	t.Logf("Child blocked at sync point %s, killed after %v", syncPoint, childDuration)

	// Reopen and verify
	database := openDB(t, dir)
	defer database.Close()

	verify(t, database)
}

// runSyncpointChild runs the child process work function.
// Called when SYNCPOINT_CHILD=1 is set.
func runSyncpointChild(t *testing.T, dir, syncPoint string, work func(db.DB)) {
	t.Helper()

	// Enable sync points
	mgr := testutil.EnableSyncPoints()
	defer testutil.DisableSyncPoints()

	// Set callback to signal parent, then block forever waiting for kill
	mgr.SetCallback(syncPoint, func(name string) error {
		// Signal to parent that we hit the sync point
		fmt.Printf("SYNCPOINT_HIT:%s\n", name)
		os.Stdout.Sync()

		// Block forever - parent will kill us with SIGKILL
		// This simulates a crash at exactly this point
		select {}
	})

	// Create/open DB
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer database.Close()

	// Run the work function - callback will block at sync point
	work(database)

	// If we get here, sync point wasn't hit
	fmt.Println("SYNCPOINT_NOT_HIT")
	os.Exit(2)
}

// runSyncpointChildProcess spawns the child and kills it after sync point hit.
func runSyncpointChildProcess(t *testing.T, dir, syncPoint string) (exitCode int, stdout, stderr bytes.Buffer) {
	t.Helper()

	cmd := exec.Command(os.Args[0],
		"-test.run=^"+t.Name()+"$",
		"-test.v",
	)
	cmd.Env = append(os.Environ(),
		"SYNCPOINT_CHILD=1",
		"SYNCPOINT_BLOCK="+syncPoint,
		"SYNCPOINT_DB_DIR="+dir,
	)
	cmd.Dir = dir
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start child: %v", err)
	}

	// Wait for sync point hit or timeout
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	// Give child time to reach sync point
	hitTimeout := 10 * time.Second
	hitDeadline := time.After(hitTimeout)

	// Poll for sync point hit
	for {
		select {
		case err := <-done:
			// Child exited before we killed it
			if err != nil {
				if exitErr, ok := err.(*exec.ExitError); ok {
					exitCode = exitErr.ExitCode()
				} else {
					exitCode = 1
				}
			}
			return
		case <-hitDeadline:
			// Timeout - kill child
			_ = cmd.Process.Signal(syscall.SIGKILL)
			exitCode = -1
			return
		default:
			// Check if sync point was hit
			if bytes.Contains(stdout.Bytes(), []byte("SYNCPOINT_HIT:"+syncPoint)) {
				// Give it a moment to fully block
				time.Sleep(10 * time.Millisecond)
				// Kill child at sync point
				_ = cmd.Process.Signal(syscall.SIGKILL)
				<-done // Wait for process to exit
				exitCode = -1 // SIGKILL
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// persistSyncpointArtifacts saves artifacts for debugging/reproduction.
func persistSyncpointArtifacts(t *testing.T, testName, syncPoint, dbPath string,
	stdout, stderr *bytes.Buffer, exitCode int, failReason string,
	syncPointHit bool, childDuration time.Duration) string {
	t.Helper()

	// Get artifact base directory
	artifactBase := os.Getenv("WHITEBOX_ARTIFACT_DIR")
	if artifactBase == "" {
		artifactBase = filepath.Join(os.TempDir(), "rockyardkv-syncpoint-artifacts")
	}

	// Create unique artifact directory
	timestamp := time.Now().Format("20060102-150405")
	sanitizedTest := filepath.Base(testName)
	// Replace colons in sync point name for filesystem safety
	safeSyncPoint := filepath.Base(syncPoint)
	artifactDir := filepath.Join(artifactBase, fmt.Sprintf("%s-%s-%s", sanitizedTest, safeSyncPoint, timestamp))

	if err := os.MkdirAll(artifactDir, 0755); err != nil {
		t.Logf("Warning: Failed to create artifact directory: %v", err)
		return ""
	}

	// Copy DB directory
	dbCopyPath := filepath.Join(artifactDir, "db")
	if err := copySyncpointDir(dbPath, dbCopyPath); err != nil {
		t.Logf("Warning: Failed to copy DB directory: %v", err)
	}

	// Save stdout
	if stdout.Len() > 0 {
		if err := os.WriteFile(filepath.Join(artifactDir, "stdout.log"), stdout.Bytes(), 0644); err != nil {
			t.Logf("Warning: Failed to write stdout: %v", err)
		}
	}

	// Save stderr
	if stderr.Len() > 0 {
		if err := os.WriteFile(filepath.Join(artifactDir, "stderr.log"), stderr.Bytes(), 0644); err != nil {
			t.Logf("Warning: Failed to write stderr: %v", err)
		}
	}

	// Generate repro command
	reproCmd := fmt.Sprintf("SYNCPOINT_BLOCK=%s go test -tags synctest -v ./cmd/crashtest/... -run '^%s$'",
		syncPoint, testName)

	// Save run.json
	artifact := syncpointArtifact{
		TestName:      testName,
		SyncPoint:     syncPoint,
		DBPath:        dbCopyPath,
		Timestamp:     time.Now(),
		GoVersion:     runtime.Version(),
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		ExitCode:      exitCode,
		ReproCmd:      reproCmd,
		FailReason:    failReason,
		SyncPointHit:  syncPointHit,
		ChildDuration: childDuration.String(),
	}

	if data, err := json.MarshalIndent(artifact, "", "  "); err == nil {
		if err := os.WriteFile(filepath.Join(artifactDir, "run.json"), data, 0644); err != nil {
			t.Logf("Warning: Failed to write run.json: %v", err)
		}
	}

	t.Logf("📁 Artifacts saved to: %s", artifactDir)
	t.Logf("🔄 Repro command: %s", reproCmd)

	return artifactDir
}

// copySyncpointDir copies a directory recursively.
func copySyncpointDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(dstPath, data, info.Mode())
	})
}

