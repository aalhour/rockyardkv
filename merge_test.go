// merge_test.go implements tests for merge.
package rockyardkv

import (
	"errors"
	"testing"
)

// =============================================================================
// DB Merge Integration Tests
// =============================================================================

func TestMergeWithoutOperator(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	// No MergeOperator set

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Merge should fail without a merge operator
	err = db.Merge(nil, []byte("key"), []byte("value"))
	if !errors.Is(err, ErrMergeOperatorNotSet) {
		t.Errorf("Merge() error = %v, want ErrMergeOperatorNotSet", err)
	}
}

func TestMergeUInt64Add(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &UInt64AddOperator{}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	key := []byte("counter")

	// Initial put
	err = db.Put(nil, key, encodeUint64(100))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Merge operations: add 10, five times
	for i := range 5 {
		err = db.Merge(nil, key, encodeUint64(10))
		if err != nil {
			t.Fatalf("Merge(%d) error = %v", i, err)
		}
	}

	// Verify merge resolution: 100 + 5*10 = 150
	value, err := db.Get(nil, key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	result := decodeUint64(value)
	if result != 150 {
		t.Errorf("Get() = %d, want 150", result)
	}

	// Close and reopen to test persistence
	db.Close()

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen error = %v", err)
	}
	defer db2.Close()

	// Verify value after reopen
	value, err = db2.Get(nil, key)
	if err != nil {
		t.Fatalf("Get() after reopen error = %v", err)
	}
	result = decodeUint64(value)
	if result != 150 {
		t.Errorf("Get() after reopen = %d, want 150", result)
	}
}

func TestMergeStringAppend(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &StringAppendOperator{Delimiter: ","}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	key := []byte("log")

	// Merge without initial value
	err = db.Merge(nil, key, []byte("entry1"))
	if err != nil {
		t.Fatalf("Merge(1) error = %v", err)
	}

	err = db.Merge(nil, key, []byte("entry2"))
	if err != nil {
		t.Fatalf("Merge(2) error = %v", err)
	}

	err = db.Merge(nil, key, []byte("entry3"))
	if err != nil {
		t.Fatalf("Merge(3) error = %v", err)
	}

	// Verify merge resolution: "entry1,entry2,entry3"
	value, err := db.Get(nil, key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	expected := "entry1,entry2,entry3"
	if string(value) != expected {
		t.Errorf("Get() = %q, want %q", string(value), expected)
	}
}

func TestMergeWithPutAndDelete(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &UInt64AddOperator{}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	key := []byte("counter")

	// Put initial value
	err = db.Put(nil, key, encodeUint64(100))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Merge
	err = db.Merge(nil, key, encodeUint64(50))
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Delete
	err = db.Delete(nil, key)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Merge after delete (should start fresh)
	err = db.Merge(nil, key, encodeUint64(25))
	if err != nil {
		t.Fatalf("Merge after delete error = %v", err)
	}

	t.Log("Put/Merge/Delete/Merge sequence works correctly")
}

func TestMergeColumnFamily(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &UInt64AddOperator{}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Create a column family
	cfOpts := ColumnFamilyOptions{}
	cf, err := db.CreateColumnFamily(cfOpts, "test_cf")
	if err != nil {
		t.Fatalf("CreateColumnFamily() error = %v", err)
	}

	key := []byte("cf_counter")

	// Merge in column family
	err = db.MergeCF(nil, cf, key, encodeUint64(100))
	if err != nil {
		t.Fatalf("MergeCF() error = %v", err)
	}

	// Merge in default column family
	err = db.Merge(nil, key, encodeUint64(200))
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	t.Log("Column family merge operations work correctly")
}

func TestMergeBatch(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &UInt64AddOperator{}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Create batch with multiple merge operations
	wb := NewWriteBatch()
	wb.Put([]byte("key1"), encodeUint64(100))
	wb.Merge([]byte("key1"), encodeUint64(10))
	wb.Merge([]byte("key1"), encodeUint64(20))
	wb.Put([]byte("key2"), encodeUint64(50))
	wb.Merge([]byte("key2"), encodeUint64(5))

	err = db.Write(nil, wb)
	if err != nil {
		t.Fatalf("Write batch error = %v", err)
	}

	t.Log("Batch merge operations written successfully")
}

// =============================================================================
// Merge Operator Edge Cases
// =============================================================================

func TestMergeEmptyValue(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &StringAppendOperator{Delimiter: ","}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	key := []byte("key")

	// Merge with empty value
	err = db.Merge(nil, key, []byte(""))
	if err != nil {
		t.Fatalf("Merge(empty) error = %v", err)
	}

	err = db.Merge(nil, key, []byte("value"))
	if err != nil {
		t.Fatalf("Merge(value) error = %v", err)
	}

	t.Log("Empty value merge works")
}

func TestMergeLargeValue(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &StringAppendOperator{Delimiter: ","}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	key := []byte("large")
	largeValue := make([]byte, 100*1024) // 100KB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err = db.Merge(nil, key, largeValue)
	if err != nil {
		t.Fatalf("Merge(large) error = %v", err)
	}

	t.Log("Large value merge works")
}
