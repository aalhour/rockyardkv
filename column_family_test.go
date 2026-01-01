// column_family_test.go implements tests for column family.
package rockyardkv

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestColumnFamilyBasic(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Should have default column family
	cfNames := database.ListColumnFamilies()
	if len(cfNames) != 1 {
		t.Fatalf("Expected 1 column family, got %d", len(cfNames))
	}
	if cfNames[0] != DefaultColumnFamilyName {
		t.Fatalf("Expected default column family, got %s", cfNames[0])
	}

	// Create a new column family
	cf1, err := database.CreateColumnFamily(DefaultColumnFamilyOptions(), "cf1")
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	// Verify it was created
	cfNames = database.ListColumnFamilies()
	if len(cfNames) != 2 {
		t.Fatalf("Expected 2 column families, got %d", len(cfNames))
	}

	// Put data in different column families
	if err := database.Put(nil, []byte("key1"), []byte("default_value")); err != nil {
		t.Fatalf("Failed to put in default CF: %v", err)
	}

	if err := database.PutCF(nil, cf1, []byte("key1"), []byte("cf1_value")); err != nil {
		t.Fatalf("Failed to put in cf1: %v", err)
	}

	// Read from different column families - same key, different values
	val, err := database.Get(nil, []byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get from default CF: %v", err)
	}
	if string(val) != "default_value" {
		t.Fatalf("Expected 'default_value', got '%s'", string(val))
	}

	val, err = database.GetCF(nil, cf1, []byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get from cf1: %v", err)
	}
	if string(val) != "cf1_value" {
		t.Fatalf("Expected 'cf1_value', got '%s'", string(val))
	}

	// Delete from one CF, should not affect the other
	if err := database.DeleteCF(nil, cf1, []byte("key1")); err != nil {
		t.Fatalf("Failed to delete from cf1: %v", err)
	}

	// Key should still exist in default CF
	val, err = database.Get(nil, []byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get from default CF after cf1 delete: %v", err)
	}
	if string(val) != "default_value" {
		t.Fatalf("Expected 'default_value', got '%s'", string(val))
	}

	// Key should not exist in cf1
	_, err = database.GetCF(nil, cf1, []byte("key1"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expected ErrNotFound for cf1, got %v", err)
	}
}

func TestColumnFamilyIterator(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Create column families
	cf1, err := database.CreateColumnFamily(DefaultColumnFamilyOptions(), "cf1")
	if err != nil {
		t.Fatalf("Failed to create cf1: %v", err)
	}

	// Add data to default CF
	for i := range 5 {
		key := []byte{'d', byte('0' + i)}
		val := []byte{'D', byte('0' + i)}
		database.Put(nil, key, val)
	}

	// Add data to cf1
	for i := range 3 {
		key := []byte{'c', byte('0' + i)}
		val := []byte{'C', byte('0' + i)}
		database.PutCF(nil, cf1, key, val)
	}

	// Iterate default CF
	iter := database.NewIterator(nil)
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Key()[0] != 'd' {
			t.Errorf("Expected key starting with 'd', got '%c'", iter.Key()[0])
		}
		count++
	}
	if count != 5 {
		t.Errorf("Expected 5 keys in default CF, got %d", count)
	}

	// Iterate cf1
	iter2 := database.NewIteratorCF(nil, cf1)
	defer iter2.Close()

	count = 0
	for iter2.SeekToFirst(); iter2.Valid(); iter2.Next() {
		if iter2.Key()[0] != 'c' {
			t.Errorf("Expected key starting with 'c', got '%c'", iter2.Key()[0])
		}
		count++
	}
	if count != 3 {
		t.Errorf("Expected 3 keys in cf1, got %d", count)
	}
}

func TestColumnFamilyDropCannotDropDefault(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Try to drop default CF - should fail
	defaultCF := database.DefaultColumnFamily()
	err = database.DropColumnFamily(defaultCF)
	if !errors.Is(err, ErrCannotDropDefaultCF) {
		t.Fatalf("Expected ErrCannotDropDefaultCF, got %v", err)
	}
}

func TestColumnFamilyDrop(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Create and drop a CF
	cf1, err := database.CreateColumnFamily(DefaultColumnFamilyOptions(), "cf1")
	if err != nil {
		t.Fatalf("Failed to create cf1: %v", err)
	}

	// Put some data
	database.PutCF(nil, cf1, []byte("key"), []byte("value"))

	// Drop the CF
	if err := database.DropColumnFamily(cf1); err != nil {
		t.Fatalf("Failed to drop cf1: %v", err)
	}

	// CF should no longer be listed
	cfNames := database.ListColumnFamilies()
	for _, name := range cfNames {
		if name == "cf1" {
			t.Fatalf("cf1 should not be listed after drop")
		}
	}

	// Operations on dropped CF should fail
	_, err = database.GetCF(nil, cf1, []byte("key"))
	if !errors.Is(err, ErrColumnFamilyNotFound) {
		t.Fatalf("Expected ErrColumnFamilyNotFound after drop, got %v", err)
	}
}

func TestColumnFamilyCreateDuplicate(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Create a CF
	_, err = database.CreateColumnFamily(DefaultColumnFamilyOptions(), "cf1")
	if err != nil {
		t.Fatalf("Failed to create cf1: %v", err)
	}

	// Try to create it again - should fail
	_, err = database.CreateColumnFamily(DefaultColumnFamilyOptions(), "cf1")
	if !errors.Is(err, ErrColumnFamilyExists) {
		t.Fatalf("Expected ErrColumnFamilyExists, got %v", err)
	}
}

func TestColumnFamilyPersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	// Create database and add data to multiple CFs
	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	cf1, err := database.CreateColumnFamily(DefaultColumnFamilyOptions(), "cf1")
	if err != nil {
		t.Fatalf("Failed to create cf1: %v", err)
	}

	database.Put(nil, []byte("default_key"), []byte("default_value"))
	database.PutCF(nil, cf1, []byte("cf1_key"), []byte("cf1_value"))

	database.Close()

	// Reopen and verify
	// Note: Currently column families are not persisted to MANIFEST,
	// so we just verify default CF data persists
	database, err = Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer database.Close()

	val, err := database.Get(nil, []byte("default_key"))
	if err != nil {
		t.Fatalf("Failed to get default_key after reopen: %v", err)
	}
	if string(val) != "default_value" {
		t.Fatalf("Expected 'default_value', got '%s'", string(val))
	}
}

func TestColumnFamilyHandle(t *testing.T) {
	dir, _ := os.MkdirTemp("", "cftest-*")
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Get default CF handle
	defaultCF := database.DefaultColumnFamily()
	if defaultCF.ID() != DefaultColumnFamilyID {
		t.Errorf("Expected default CF ID %d, got %d", DefaultColumnFamilyID, defaultCF.ID())
	}
	if defaultCF.Name() != DefaultColumnFamilyName {
		t.Errorf("Expected default CF name '%s', got '%s'", DefaultColumnFamilyName, defaultCF.Name())
	}
	if !defaultCF.IsValid() {
		t.Error("Default CF handle should be valid")
	}

	// Create new CF
	cf1, err := database.CreateColumnFamily(DefaultColumnFamilyOptions(), "mycf")
	if err != nil {
		t.Fatalf("Failed to create CF: %v", err)
	}

	if cf1.ID() <= DefaultColumnFamilyID {
		t.Errorf("New CF ID should be > %d, got %d", DefaultColumnFamilyID, cf1.ID())
	}
	if cf1.Name() != "mycf" {
		t.Errorf("Expected CF name 'mycf', got '%s'", cf1.Name())
	}
	if !cf1.IsValid() {
		t.Error("New CF handle should be valid")
	}

	// Drop CF
	database.DropColumnFamily(cf1)
	if cf1.IsValid() {
		t.Error("Dropped CF handle should not be valid")
	}
}

// TestColumnFamilySetCoverage tests internal columnFamilySet methods.
func TestColumnFamilySetCoverage(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Get internal DB
	db := database.(*DBImpl)

	// Test GetByName
	t.Run("GetByName", func(t *testing.T) {
		cfd := db.columnFamilies.GetByName(DefaultColumnFamilyName)
		if cfd == nil {
			t.Error("GetByName(default) returned nil")
		}
		if cfd != nil && cfd.name != DefaultColumnFamilyName {
			t.Errorf("GetByName returned wrong CF: %s", cfd.name)
		}

		// Test non-existent name
		cfd = db.columnFamilies.GetByName("nonexistent")
		if cfd != nil {
			t.Error("GetByName(nonexistent) should return nil")
		}
	})

	// Test Count
	t.Run("Count", func(t *testing.T) {
		count := db.columnFamilies.Count()
		if count != 1 {
			t.Errorf("Count = %d, want 1", count)
		}
	})

	// Test ForEach
	t.Run("ForEach", func(t *testing.T) {
		count := 0
		db.columnFamilies.ForEach(func(cfd *columnFamilyData) {
			count++
		})
		if count != 1 {
			t.Errorf("ForEach visited %d CFs, want 1", count)
		}
	})

	// Test Ref/Unref
	t.Run("RefUnref", func(t *testing.T) {
		cfd := db.columnFamilies.GetDefault()
		initialRefs := cfd.refs

		cfd.Ref()
		if cfd.refs != initialRefs+1 {
			t.Errorf("After Ref: refs = %d, want %d", cfd.refs, initialRefs+1)
		}

		cfd.Unref()
		if cfd.refs != initialRefs {
			t.Errorf("After Unref: refs = %d, want %d", cfd.refs, initialRefs)
		}
	})
}
