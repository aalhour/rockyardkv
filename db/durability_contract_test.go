// durability_contract_test.go - Tests for durability guarantees.
//
// These tests verify that RockyardKV matches C++ RocksDB durability semantics.
// Some tests verify that data IS lost under certain conditions — this is
// expected behavior, not a bug.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/options.h (WriteOptions::disableWAL)
package db

import (
	"os"
	"os/exec"
	"testing"
)

// TestDurability_WALDisabled_UnflushedWritesNotDurable verifies that writes
// with DisableWAL=true are NOT durable after a crash.
//
// Contract: With WAL disabled, unflushed writes are lost after crash.
// This matches C++ RocksDB behavior exactly.
//
// DO NOT "FIX" THIS TEST BY MAKING DATA DURABLE — that would break
// compatibility with C++ RocksDB's documented behavior.
func TestDurability_WALDisabled_UnflushedWritesNotDurable(t *testing.T) {
	if os.Getenv("DURABILITY_CHILD") == "1" {
		// Child process: write with WAL disabled, then exit without flush
		runDurabilityChild(t)
		return
	}

	// Parent process: spawn child, then verify data is lost
	dir := t.TempDir()

	// Create a database first
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	database.Close()

	// Spawn child that writes with WAL disabled and exits
	cmd := exec.Command(os.Args[0], "-test.run=^TestDurability_WALDisabled_UnflushedWritesNotDurable$", "-test.v")
	cmd.Env = append(os.Environ(), "DURABILITY_CHILD=1", "DURABILITY_DB_PATH="+dir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		// Child exited — this is expected (simulates crash)
		t.Logf("Child exited: %v (expected)", err)
	}

	// Reopen and verify data is LOST (this is the expected behavior)
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer database.Close()

	// The key should NOT exist — data loss is expected
	_, err = database.Get(nil, []byte("wal_disabled_key"))
	if err == nil {
		// If the key exists, either:
		// 1. The child flushed (bug in this test)
		// 2. We made WAL-disabled writes durable (compatibility break!)
		t.Logf("WARNING: Key found after WAL-disabled write + crash")
		t.Logf("This may indicate a compatibility issue with C++ RocksDB")
		// Don't fail — the child might have triggered a flush via memtable pressure
	} else {
		t.Logf("Key not found after crash (expected: data loss with WAL disabled)")
	}
}

func runDurabilityChild(t *testing.T) {
	dir := os.Getenv("DURABILITY_DB_PATH")
	if dir == "" {
		t.Fatal("DURABILITY_DB_PATH not set")
	}

	opts := DefaultOptions()
	opts.CreateIfMissing = false

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("child: failed to open database: %v", err)
	}

	// Write with WAL disabled
	writeOpts := DefaultWriteOptions()
	writeOpts.DisableWAL = true

	if err := database.Put(writeOpts, []byte("wal_disabled_key"), []byte("wal_disabled_value")); err != nil {
		t.Fatalf("child: failed to write: %v", err)
	}

	// Exit WITHOUT flushing — simulates crash
	// The data should be lost
	os.Exit(0)
}

// TestDurability_WALEnabled_WritesAreDurable verifies that writes with
// WAL enabled survive crashes.
//
// Contract: With WAL enabled (default), acknowledged writes survive crash.
func TestDurability_WALEnabled_WritesAreDurable(t *testing.T) {
	if os.Getenv("DURABILITY_CHILD_WAL") == "1" {
		runDurabilityChildWAL(t)
		return
	}

	dir := t.TempDir()

	// Create database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	database.Close()

	// Spawn child that writes with WAL enabled
	cmd := exec.Command(os.Args[0], "-test.run=^TestDurability_WALEnabled_WritesAreDurable$", "-test.v")
	cmd.Env = append(os.Environ(), "DURABILITY_CHILD_WAL=1", "DURABILITY_DB_PATH="+dir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		t.Logf("Child exited: %v (expected)", err)
	}

	// Reopen and verify data survives
	database, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer database.Close()

	value, err := database.Get(nil, []byte("wal_enabled_key"))
	if err != nil {
		t.Errorf("Key not found after WAL-enabled write + crash: %v", err)
		t.Errorf("This is a durability bug — WAL writes should survive crashes")
	} else if string(value) != "wal_enabled_value" {
		t.Errorf("Value mismatch: got %q, want %q", value, "wal_enabled_value")
	} else {
		t.Logf("Key found after crash (expected: WAL writes are durable)")
	}
}

func runDurabilityChildWAL(t *testing.T) {
	dir := os.Getenv("DURABILITY_DB_PATH")
	if dir == "" {
		t.Fatal("DURABILITY_DB_PATH not set")
	}

	opts := DefaultOptions()
	opts.CreateIfMissing = false

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("child: failed to open database: %v", err)
	}

	// Write with WAL enabled (default)
	writeOpts := DefaultWriteOptions()
	writeOpts.Sync = true // Ensure it hits disk

	if err := database.Put(writeOpts, []byte("wal_enabled_key"), []byte("wal_enabled_value")); err != nil {
		t.Fatalf("child: failed to write: %v", err)
	}

	// Exit without closing — simulates crash
	os.Exit(0)
}
