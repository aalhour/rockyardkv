package rockyardkv

// column_family_adversarial_test.go implements adversarial tests for column family isolation.
//
// These tests verify that keys from one column family do not leak into another,
// which was identified as a critical bug in the Red Team audit (Dec 2025).


import (
	"errors"
	"testing"
)

// TestAdversarial_ColumnFamilyIsolation_NoKeyLeakage verifies that keys written
// to one column family are not visible when reading from another column family.
// Contract: Keys in one column family must never be visible from another column family.
func TestAdversarial_ColumnFamilyIsolation_NoKeyLeakage(t *testing.T) {
	dir := t.TempDir()

	// Open database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create column families
	cfOpts := DefaultColumnFamilyOptions()
	cf1, err := db.CreateColumnFamily(cfOpts, "cf1")
	if err != nil {
		t.Fatalf("Failed to create cf1: %v", err)
	}
	cf2, err := db.CreateColumnFamily(cfOpts, "cf2")
	if err != nil {
		t.Fatalf("Failed to create cf2: %v", err)
	}

	// Write different keys to each CF
	writeOpts := DefaultWriteOptions()

	// Default CF gets key "default_key"
	if err := db.Put(writeOpts, []byte("default_key"), []byte("default_value")); err != nil {
		t.Fatalf("Failed to put to default CF: %v", err)
	}

	// CF1 gets key "cf1_key"
	if err := db.PutCF(writeOpts, cf1, []byte("cf1_key"), []byte("cf1_value")); err != nil {
		t.Fatalf("Failed to put to cf1: %v", err)
	}

	// CF2 gets key "cf2_key"
	if err := db.PutCF(writeOpts, cf2, []byte("cf2_key"), []byte("cf2_value")); err != nil {
		t.Fatalf("Failed to put to cf2: %v", err)
	}

	// Force a flush to ensure keys go to SST files
	if err := db.Flush(DefaultFlushOptions()); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Now verify isolation: each CF should only see its own keys
	readOpts := DefaultReadOptions()

	// Test 1: Default CF should only see default_key
	t.Run("DefaultCF", func(t *testing.T) {
		val, err := db.Get(readOpts, []byte("default_key"))
		if err != nil {
			t.Errorf("Expected to find default_key in default CF, got error: %v", err)
		}
		if string(val) != "default_value" {
			t.Errorf("default_key value = %q, want %q", val, "default_value")
		}

		// Should NOT find cf1_key or cf2_key in default CF
		_, err = db.Get(readOpts, []byte("cf1_key"))
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for cf1_key in default CF, got: %v", err)
		}
		_, err = db.Get(readOpts, []byte("cf2_key"))
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for cf2_key in default CF, got: %v", err)
		}
	})

	// Test 2: CF1 should only see cf1_key
	t.Run("CF1", func(t *testing.T) {
		val, err := db.GetCF(readOpts, cf1, []byte("cf1_key"))
		if err != nil {
			t.Errorf("Expected to find cf1_key in cf1, got error: %v", err)
		}
		if string(val) != "cf1_value" {
			t.Errorf("cf1_key value = %q, want %q", val, "cf1_value")
		}

		// Should NOT find default_key or cf2_key in CF1
		_, err = db.GetCF(readOpts, cf1, []byte("default_key"))
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for default_key in cf1, got: %v", err)
		}
		_, err = db.GetCF(readOpts, cf1, []byte("cf2_key"))
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for cf2_key in cf1, got: %v", err)
		}
	})

	// Test 3: CF2 should only see cf2_key
	t.Run("CF2", func(t *testing.T) {
		val, err := db.GetCF(readOpts, cf2, []byte("cf2_key"))
		if err != nil {
			t.Errorf("Expected to find cf2_key in cf2, got error: %v", err)
		}
		if string(val) != "cf2_value" {
			t.Errorf("cf2_key value = %q, want %q", val, "cf2_value")
		}

		// Should NOT find default_key or cf1_key in CF2
		_, err = db.GetCF(readOpts, cf2, []byte("default_key"))
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for default_key in cf2, got: %v", err)
		}
		_, err = db.GetCF(readOpts, cf2, []byte("cf1_key"))
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for cf1_key in cf2, got: %v", err)
		}
	})
}

// TestAdversarial_ColumnFamilyIsolation_IteratorIsolation verifies that iterators
// only see keys from their own column family.
// Contract: Iterators must only yield keys from their own column family.
func TestAdversarial_ColumnFamilyIsolation_IteratorIsolation(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfOpts := DefaultColumnFamilyOptions()
	cf1, err := db.CreateColumnFamily(cfOpts, "cf1")
	if err != nil {
		t.Fatalf("Failed to create cf1: %v", err)
	}

	writeOpts := DefaultWriteOptions()

	// Write to default CF
	for i := range 5 {
		key := []byte{byte('a' + i)}
		if err := db.Put(writeOpts, key, []byte("default")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Write to CF1 with different keys
	for i := range 5 {
		key := []byte{byte('x' + i)}
		if err := db.PutCF(writeOpts, cf1, key, []byte("cf1")); err != nil {
			t.Fatalf("PutCF failed: %v", err)
		}
	}

	// Flush to SST
	if err := db.Flush(DefaultFlushOptions()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Iterator on default CF should only see keys a-e
	t.Run("DefaultIterator", func(t *testing.T) {
		iter := db.NewIterator(nil)
		defer iter.Close()

		var keys []string
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			keys = append(keys, string(iter.Key()))
		}

		if len(keys) != 5 {
			t.Errorf("Expected 5 keys from default CF, got %d: %v", len(keys), keys)
		}
		for _, k := range keys {
			if k[0] < 'a' || k[0] > 'e' {
				t.Errorf("Unexpected key in default CF iterator: %q", k)
			}
		}
	})

	// Iterator on CF1 should only see keys x-|
	t.Run("CF1Iterator", func(t *testing.T) {
		iter := db.NewIteratorCF(nil, cf1)
		defer iter.Close()

		var keys []string
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			keys = append(keys, string(iter.Key()))
		}

		if len(keys) != 5 {
			t.Errorf("Expected 5 keys from cf1, got %d: %v", len(keys), keys)
		}
		for _, k := range keys {
			if k[0] < 'x' {
				t.Errorf("Unexpected key in cf1 iterator: %q", k)
			}
		}
	})
}

// TestAdversarial_ColumnFamilyIsolation_SameKeyDifferentCFs verifies that the
// same key written to different CFs retains separate values.
// Contract: The same key in different CFs must have independent values.
func TestAdversarial_ColumnFamilyIsolation_SameKeyDifferentCFs(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfOpts := DefaultColumnFamilyOptions()
	cf1, err := db.CreateColumnFamily(cfOpts, "cf1")
	if err != nil {
		t.Fatalf("Failed to create cf1: %v", err)
	}

	writeOpts := DefaultWriteOptions()
	readOpts := DefaultReadOptions()

	// Write the SAME key to both CFs with different values
	key := []byte("shared_key")
	if err := db.Put(writeOpts, key, []byte("value_default")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.PutCF(writeOpts, cf1, key, []byte("value_cf1")); err != nil {
		t.Fatalf("PutCF failed: %v", err)
	}

	// Flush to SST
	if err := db.Flush(DefaultFlushOptions()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify each CF has its own value
	val, err := db.Get(readOpts, key)
	if err != nil {
		t.Fatalf("Get from default CF failed: %v", err)
	}
	if string(val) != "value_default" {
		t.Errorf("Default CF: got %q, want %q", val, "value_default")
	}

	val, err = db.GetCF(readOpts, cf1, key)
	if err != nil {
		t.Fatalf("GetCF from cf1 failed: %v", err)
	}
	if string(val) != "value_cf1" {
		t.Errorf("CF1: got %q, want %q", val, "value_cf1")
	}
}
