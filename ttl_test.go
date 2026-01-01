// ttl_test.go implements tests for ttl.
package rockyardkv

import (
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestTTLBasic(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "rockyard-ttl-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Open database with 1 second TTL
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	ttlDB, err := OpenWithTTL(dir, opts, 1*time.Second)
	if err != nil {
		t.Fatalf("Failed to open TTL db: %v", err)
	}
	defer ttlDB.Close()

	// Write a key
	key := []byte("test_key")
	value := []byte("test_value")
	if err := ttlDB.Put(nil, key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Should be readable immediately
	got, err := ttlDB.Get(nil, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(value) {
		t.Errorf("Value = %s, want %s", got, value)
	}

	// Wait for TTL to expire
	time.Sleep(1500 * time.Millisecond)

	// Should return not found
	_, err = ttlDB.Get(nil, key)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound after TTL, got: %v", err)
	}
}

func TestTTLIterator(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "rockyard-ttl-iter-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Open database with 1 second TTL
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	ttlDB, err := OpenWithTTL(dir, opts, 1*time.Second)
	if err != nil {
		t.Fatalf("Failed to open TTL db: %v", err)
	}
	defer ttlDB.Close()

	// Write keys with different creation times
	// Key1 is fresh, key2 will be expired
	now := time.Now()
	ttlDB.PutWithExpiry(nil, []byte("key1"), []byte("value1"), now)
	ttlDB.PutWithExpiry(nil, []byte("key2"), []byte("value2"), now.Add(-2*time.Second)) // Already expired

	// Iterate - should only see key1
	iter := ttlDB.NewIterator(nil)
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
		if string(iter.Key()) != "key1" {
			t.Errorf("Expected key1, got %s", iter.Key())
		}
		if string(iter.Value()) != "value1" {
			t.Errorf("Expected value1, got %s", iter.Value())
		}
	}

	if count != 1 {
		t.Errorf("Expected 1 key (key1), got %d", count)
	}
}

func TestTTLCompactionFilter(t *testing.T) {
	filter := NewTTLCompactionFilter(1 * time.Second)

	if filter.Name() != "TTLCompactionFilter" {
		t.Errorf("Name = %s, want TTLCompactionFilter", filter.Name())
	}

	// Create a value with expired timestamp
	now := time.Now()
	expiredValue := appendTTLTimestamp([]byte("value"), now.Add(-2*time.Second))

	decision, _ := filter.Filter(0, []byte("key"), expiredValue)
	if decision != FilterRemove {
		t.Error("Should remove expired entry")
	}

	// Create a value with fresh timestamp
	freshValue := appendTTLTimestamp([]byte("value"), now)
	decision, _ = filter.Filter(0, []byte("key"), freshValue)
	if decision != FilterKeep {
		t.Error("Should keep fresh entry")
	}

	// Value without timestamp should be kept
	decision, _ = filter.Filter(0, []byte("key"), []byte("notimestamp"))
	if decision != FilterKeep {
		t.Error("Should keep value without timestamp")
	}
}

func TestTTLTimestampHelpers(t *testing.T) {
	originalValue := []byte("hello world")
	now := time.Now()

	// Append timestamp
	withTimestamp := appendTTLTimestamp(originalValue, now)
	if len(withTimestamp) != len(originalValue)+TTLTimestampSize {
		t.Errorf("Length = %d, want %d", len(withTimestamp), len(originalValue)+TTLTimestampSize)
	}

	// Extract timestamp
	extractedTS := extractTTLTimestamp(withTimestamp)
	if extractedTS != now.Unix() {
		t.Errorf("Timestamp = %d, want %d", extractedTS, now.Unix())
	}

	// Strip timestamp
	stripped := stripTTLTimestamp(withTimestamp)
	if string(stripped) != string(originalValue) {
		t.Errorf("Stripped value = %s, want %s", stripped, originalValue)
	}
}

func TestTTLExpiration(t *testing.T) {
	// Test isExpired function
	now := time.Now()
	ttl := 1 * time.Second

	// Fresh timestamp should not be expired
	if isExpired(now.Unix(), ttl) {
		t.Error("Fresh timestamp should not be expired")
	}

	// Old timestamp should be expired
	old := now.Add(-2 * time.Second)
	if !isExpired(old.Unix(), ttl) {
		t.Error("Old timestamp should be expired")
	}

	// Zero TTL means never expires
	if isExpired(old.Unix(), 0) {
		t.Error("Zero TTL should never expire")
	}
}

func TestTTLManyKeys(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "rockyard-ttl-many-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Use a longer TTL for testing to avoid timing issues under load
	ttl := 2 * time.Second
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	ttlDB, err := OpenWithTTL(dir, opts, ttl)
	if err != nil {
		t.Fatalf("Failed to open TTL db: %v", err)
	}
	defer ttlDB.Close()

	// Write many keys
	for i := range 100 {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		if err := ttlDB.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// All keys should be readable immediately after write
	for i := range 100 {
		key := []byte("key" + strconv.Itoa(i))
		_, err := ttlDB.Get(nil, key)
		if err != nil {
			t.Errorf("Key %s should be readable: %v", key, err)
		}
	}

	// Wait for TTL to expire
	time.Sleep(2500 * time.Millisecond)

	// All keys should be expired
	for i := range 100 {
		key := []byte("key" + strconv.Itoa(i))
		_, err := ttlDB.Get(nil, key)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Key %s should be expired", key)
		}
	}
}

func TestTTLDelete(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "rockyard-ttl-delete-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	ttlDB, err := OpenWithTTL(dir, opts, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to open TTL db: %v", err)
	}
	defer ttlDB.Close()

	// Put and delete
	key := []byte("key")
	ttlDB.Put(nil, key, []byte("value"))

	_, err = ttlDB.Get(nil, key)
	if err != nil {
		t.Fatalf("Get after Put failed: %v", err)
	}

	ttlDB.Delete(nil, key)

	_, err = ttlDB.Get(nil, key)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound after Delete, got: %v", err)
	}
}
