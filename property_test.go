// property_test.go implements tests for property.
package rockyardkv

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestGetPropertyBasic(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "rockyard-property-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Open database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer database.Close()

	// Test memtable properties
	t.Run("NumImmutableMemTable", func(t *testing.T) {
		val, ok := database.GetProperty(PropertyNumImmutableMemTable)
		if !ok {
			t.Error("Property should exist")
		}
		if val != "0" && val != "1" {
			t.Errorf("Unexpected value: %s", val)
		}
	})

	t.Run("MemTableFlushPending", func(t *testing.T) {
		val, ok := database.GetProperty(PropertyMemTableFlushPending)
		if !ok {
			t.Error("Property should exist")
		}
		if val != "0" && val != "1" {
			t.Errorf("Unexpected value: %s", val)
		}
	})

	t.Run("CurSizeActiveMemTable", func(t *testing.T) {
		val, ok := database.GetProperty(PropertyCurSizeActiveMemTable)
		if !ok {
			t.Error("Property should exist")
		}
		size, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			t.Errorf("Failed to parse value: %v", err)
		}
		// Should be 0 or small for empty database
		if size > 1024*1024*10 {
			t.Errorf("Unexpectedly large memtable size: %d", size)
		}
	})

	t.Run("NumEntriesActiveMemTable", func(t *testing.T) {
		val, ok := database.GetProperty(PropertyNumEntriesActiveMemTable)
		if !ok {
			t.Error("Property should exist")
		}
		count, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			t.Errorf("Failed to parse value: %v", err)
		}
		if count < 0 {
			t.Errorf("Negative count: %d", count)
		}
	})

	t.Run("CompactionPending", func(t *testing.T) {
		val, ok := database.GetProperty(PropertyCompactionPending)
		if !ok {
			t.Error("Property should exist")
		}
		if val != "0" && val != "1" {
			t.Errorf("Unexpected value: %s", val)
		}
	})

	t.Run("NumSnapshots", func(t *testing.T) {
		val, ok := database.GetProperty(PropertyNumSnapshots)
		if !ok {
			t.Error("Property should exist")
		}
		if val != "0" {
			t.Errorf("Expected 0 snapshots, got: %s", val)
		}
	})

	t.Run("NumColumnFamilies", func(t *testing.T) {
		val, ok := database.GetProperty(PropertyNumColumnFamilies)
		if !ok {
			t.Error("Property should exist")
		}
		count, err := strconv.Atoi(val)
		if err != nil {
			t.Errorf("Failed to parse value: %v", err)
		}
		if count < 1 {
			t.Errorf("Expected at least 1 column family, got: %d", count)
		}
	})

	t.Run("UnknownProperty", func(t *testing.T) {
		_, ok := database.GetProperty("rocksdb.unknown.property")
		if ok {
			t.Error("Unknown property should return false")
		}
	})
}

func TestGetPropertyWithData(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "rockyard-property-data-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Open database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer database.Close()

	// Write some data
	for i := range 100 {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	t.Run("NumEntriesAfterWrites", func(t *testing.T) {
		val, ok := database.GetProperty(PropertyNumEntriesActiveMemTable)
		if !ok {
			t.Error("Property should exist")
		}
		count, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			t.Errorf("Failed to parse value: %v", err)
		}
		if count < 100 {
			t.Errorf("Expected at least 100 entries, got: %d", count)
		}
	})

	t.Run("MemTableSizeAfterWrites", func(t *testing.T) {
		val, ok := database.GetProperty(PropertyCurSizeActiveMemTable)
		if !ok {
			t.Error("Property should exist")
		}
		size, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			t.Errorf("Failed to parse value: %v", err)
		}
		if size == 0 {
			t.Error("Expected non-zero memtable size after writes")
		}
	})

	t.Run("EstimateNumKeys", func(t *testing.T) {
		val, ok := database.GetProperty(PropertyEstimateNumKeys)
		if !ok {
			t.Error("Property should exist")
		}
		count, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			t.Errorf("Failed to parse value: %v", err)
		}
		if count < 100 {
			t.Errorf("Expected at least 100 keys, got: %d", count)
		}
	})
}

func TestGetPropertySnapshots(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "rockyard-property-snap-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Open database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer database.Close()

	// Initially no snapshots
	val, _ := database.GetProperty(PropertyNumSnapshots)
	if val != "0" {
		t.Errorf("Expected 0 snapshots, got: %s", val)
	}

	// Create a snapshot
	snap := database.GetSnapshot()
	val, _ = database.GetProperty(PropertyNumSnapshots)
	if val != "1" {
		t.Errorf("Expected 1 snapshot, got: %s", val)
	}

	// Create another snapshot
	snap2 := database.GetSnapshot()
	val, _ = database.GetProperty(PropertyNumSnapshots)
	if val != "2" {
		t.Errorf("Expected 2 snapshots, got: %s", val)
	}

	// Release first snapshot
	database.ReleaseSnapshot(snap)
	val, _ = database.GetProperty(PropertyNumSnapshots)
	if val != "1" {
		t.Errorf("Expected 1 snapshot after release, got: %s", val)
	}

	// Release second snapshot
	database.ReleaseSnapshot(snap2)
	val, _ = database.GetProperty(PropertyNumSnapshots)
	if val != "0" {
		t.Errorf("Expected 0 snapshots after release, got: %s", val)
	}
}

func TestGetPropertyLevelFiles(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "rockyard-property-level-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Open database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer database.Close()

	// Test level file counts
	for level := range 7 {
		propName := PropertyNumFilesAtLevelPrefix + strconv.Itoa(level)
		val, ok := database.GetProperty(propName)
		if !ok {
			t.Errorf("Property %s should exist", propName)
		}
		count, err := strconv.Atoi(val)
		if err != nil {
			t.Errorf("Failed to parse value for %s: %v", propName, err)
		}
		if count < 0 {
			t.Errorf("Negative file count for level %d: %d", level, count)
		}
	}

	// Test invalid level
	_, ok := database.GetProperty(PropertyNumFilesAtLevelPrefix + "10")
	if ok {
		t.Error("Invalid level should return false")
	}
}

func TestGetPropertyLevelStats(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "rockyard-property-stats-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Open database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer database.Close()

	val, ok := database.GetProperty(PropertyLevelStats)
	if !ok {
		t.Error("LevelStats property should exist")
	}

	// Should contain header
	if !strings.Contains(val, "Level") || !strings.Contains(val, "Files") {
		t.Errorf("LevelStats should contain header: %s", val)
	}

	// Should have 7 level rows
	lines := strings.Split(val, "\n")
	levelLines := 0
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "0") ||
			strings.HasPrefix(strings.TrimSpace(line), "1") ||
			strings.HasPrefix(strings.TrimSpace(line), "2") ||
			strings.HasPrefix(strings.TrimSpace(line), "3") ||
			strings.HasPrefix(strings.TrimSpace(line), "4") ||
			strings.HasPrefix(strings.TrimSpace(line), "5") ||
			strings.HasPrefix(strings.TrimSpace(line), "6") {
			levelLines++
		}
	}
	if levelLines != 7 {
		t.Errorf("Expected 7 level lines, got %d: %s", levelLines, val)
	}
}
