package db_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/aalhour/rockyardkv/db"
)

func TestPrefixSeekBasic(t *testing.T) {
	dir, err := os.MkdirTemp("", "prefix_seek_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.PrefixExtractor = db.NewFixedPrefixExtractor(4) // "user" prefix

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Insert keys with different prefixes
	testData := []struct {
		key   string
		value string
	}{
		{"user0001:name", "Alice"},
		{"user0001:email", "alice@example.com"},
		{"user0002:name", "Bob"},
		{"user0002:email", "bob@example.com"},
		{"order001:id", "12345"},
		{"order001:amount", "100.00"},
		{"user0003:name", "Charlie"},
	}

	for _, kv := range testData {
		if err := database.Put(nil, []byte(kv.key), []byte(kv.value)); err != nil {
			t.Fatalf("Put(%s) error: %v", kv.key, err)
		}
	}

	// Flush to SST
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush error: %v", err)
	}

	// Test 1: Iterate all keys
	iter := database.NewIterator(nil)
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}
	iter.Close()
	if count != 7 {
		t.Errorf("Expected 7 keys, got %d", count)
	}

	// Test 2: Seek to a specific prefix and iterate
	iter = database.NewIterator(nil)
	iter.Seek([]byte("user0001"))
	var userKeys []string
	for ; iter.Valid(); iter.Next() {
		key := string(iter.Key())
		userKeys = append(userKeys, key)
		// Stop when we leave the user prefix
		if len(key) < 4 || key[:4] != "user" {
			break
		}
	}
	iter.Close()

	// Should have found all user keys (5 total)
	expectedUserKeys := 5
	userCount := 0
	for _, k := range userKeys {
		if len(k) >= 4 && k[:4] == "user" {
			userCount++
		}
	}
	if userCount != expectedUserKeys {
		t.Errorf("Expected %d user keys, got %d: %v", expectedUserKeys, userCount, userKeys)
	}
}

func TestPrefixSeekWithUpperBound(t *testing.T) {
	dir, err := os.MkdirTemp("", "prefix_seek_bound_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Insert 10 keys
	for i := range 10 {
		key := fmt.Sprintf("key%02d", i)
		if err := database.Put(nil, []byte(key), []byte("value")); err != nil {
			t.Fatalf("Put(%s) error: %v", key, err)
		}
	}

	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush error: %v", err)
	}

	// Test with upper bound: iterate keys 03-06 (key03 <= key < key07)
	readOpts := db.DefaultReadOptions()
	readOpts.IterateLowerBound = []byte("key03")
	readOpts.IterateUpperBound = []byte("key07")

	iter := database.NewIterator(readOpts)
	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Close()

	expected := []string{"key03", "key04", "key05", "key06"}
	if len(keys) != len(expected) {
		t.Errorf("Expected %d keys, got %d: %v", len(expected), len(keys), keys)
	} else {
		for i, k := range expected {
			if keys[i] != k {
				t.Errorf("Expected key %d to be %q, got %q", i, k, keys[i])
			}
		}
	}
}

func TestPrefixSeekWithPrefixSameAsStart(t *testing.T) {
	dir, err := os.MkdirTemp("", "prefix_same_start_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.PrefixExtractor = db.NewFixedPrefixExtractor(3) // 3-byte prefix

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Insert keys with different prefixes
	keys := []string{
		"aaa1", "aaa2", "aaa3",
		"bbb1", "bbb2",
		"ccc1",
	}
	for _, k := range keys {
		if err := database.Put(nil, []byte(k), []byte("value")); err != nil {
			t.Fatalf("Put(%s) error: %v", k, err)
		}
	}

	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush error: %v", err)
	}

	// Seek to "aaa1" with prefix_same_as_start
	readOpts := db.DefaultReadOptions()
	readOpts.PrefixSameAsStart = true

	iter := database.NewIterator(readOpts)
	var foundKeys []string
	for iter.Seek([]byte("aaa1")); iter.Valid(); iter.Next() {
		foundKeys = append(foundKeys, string(iter.Key()))
	}
	iter.Close()

	// Should only find keys with "aaa" prefix
	expected := []string{"aaa1", "aaa2", "aaa3"}
	if len(foundKeys) != len(expected) {
		t.Errorf("Expected %d keys, got %d: %v", len(expected), len(foundKeys), foundKeys)
	} else {
		for i, k := range expected {
			if foundKeys[i] != k {
				t.Errorf("Expected key %d to be %q, got %q", i, k, foundKeys[i])
			}
		}
	}
}

func TestPrefixSeekEmpty(t *testing.T) {
	dir, err := os.MkdirTemp("", "prefix_seek_empty_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.PrefixExtractor = db.NewCappedPrefixExtractor(4)

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Test seeking in empty database
	iter := database.NewIterator(nil)
	iter.Seek([]byte("test"))
	if iter.Valid() {
		t.Errorf("Expected invalid iterator on empty database")
	}
	iter.Close()

	// Add a key
	if err := database.Put(nil, []byte("abcd1"), []byte("value")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	// Seek to a non-existent prefix
	iter = database.NewIterator(nil)
	iter.Seek([]byte("xyz"))
	if iter.Valid() {
		t.Errorf("Expected invalid iterator when seeking past all keys")
	}
	iter.Close()
}

func TestPrefixSeekReverse(t *testing.T) {
	dir, err := os.MkdirTemp("", "prefix_seek_reverse_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Insert keys
	for i := range 10 {
		key := fmt.Sprintf("key%02d", i)
		if err := database.Put(nil, []byte(key), []byte("value")); err != nil {
			t.Fatalf("Put(%s) error: %v", key, err)
		}
	}

	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush error: %v", err)
	}

	// Test SeekForPrev
	iter := database.NewIterator(nil)
	iter.SeekForPrev([]byte("key05"))
	if !iter.Valid() {
		t.Fatal("Expected valid iterator after SeekForPrev")
	}
	if string(iter.Key()) != "key05" {
		t.Errorf("Expected key05, got %s", iter.Key())
	}

	// Go backwards
	var reverseKeys []string
	for ; iter.Valid(); iter.Prev() {
		reverseKeys = append(reverseKeys, string(iter.Key()))
	}
	iter.Close()

	expected := []string{"key05", "key04", "key03", "key02", "key01", "key00"}
	if len(reverseKeys) != len(expected) {
		t.Errorf("Expected %d keys, got %d: %v", len(expected), len(reverseKeys), reverseKeys)
	} else {
		for i, k := range expected {
			if reverseKeys[i] != k {
				t.Errorf("Expected key %d to be %q, got %q", i, k, reverseKeys[i])
			}
		}
	}
}

func TestPrefixSeekWithBoundsAndDeletions(t *testing.T) {
	dir, err := os.MkdirTemp("", "prefix_bounds_del_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Insert keys
	for i := range 10 {
		key := fmt.Sprintf("key%02d", i)
		if err := database.Put(nil, []byte(key), []byte("value")); err != nil {
			t.Fatalf("Put(%s) error: %v", key, err)
		}
	}

	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush error: %v", err)
	}

	// Delete some keys in the middle
	for i := 3; i <= 5; i++ {
		key := fmt.Sprintf("key%02d", i)
		if err := database.Delete(nil, []byte(key)); err != nil {
			t.Fatalf("Delete(%s) error: %v", key, err)
		}
	}

	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush error: %v", err)
	}

	// Iterate with bounds
	readOpts := db.DefaultReadOptions()
	readOpts.IterateLowerBound = []byte("key02")
	readOpts.IterateUpperBound = []byte("key08")

	iter := database.NewIterator(readOpts)
	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Close()

	// Should have: key02, key06, key07 (skipping deleted 03, 04, 05)
	expected := []string{"key02", "key06", "key07"}
	if len(keys) != len(expected) {
		t.Errorf("Expected %d keys, got %d: %v", len(expected), len(keys), keys)
	} else {
		for i, k := range expected {
			if keys[i] != k {
				t.Errorf("Expected key %d to be %q, got %q", i, k, keys[i])
			}
		}
	}
}
