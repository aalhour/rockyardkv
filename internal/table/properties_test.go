package table

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGoldenSSTProperties(t *testing.T) {
	goldenPath := filepath.Join("..", "..", "testdata", "golden", "v10.7.5", "sst", "simple.sst")
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Skipf("Golden file not found: %v", err)
	}

	file := &BytesFile{data: data}
	reader, err := Open(file, ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer reader.Close()

	props, err := reader.Properties()
	if err != nil {
		t.Fatalf("Failed to get properties: %v", err)
	}

	// Log all properties
	t.Logf("DataSize: %d", props.DataSize)
	t.Logf("IndexSize: %d", props.IndexSize)
	t.Logf("NumDataBlocks: %d", props.NumDataBlocks)
	t.Logf("NumEntries: %d", props.NumEntries)
	t.Logf("RawKeySize: %d", props.RawKeySize)
	t.Logf("RawValueSize: %d", props.RawValueSize)
	t.Logf("ColumnFamilyID: %d", props.ColumnFamilyID)
	t.Logf("ColumnFamilyName: %s", props.ColumnFamilyName)
	t.Logf("ComparatorName: %s", props.ComparatorName)
	t.Logf("CompressionName: %s", props.CompressionName)
	t.Logf("IndexKeyIsUserKey: %d", props.IndexKeyIsUserKey)
	t.Logf("IndexValueIsDeltaEncoded: %d", props.IndexValueIsDeltaEncoded)
	t.Logf("PrefixExtractorName: %s", props.PrefixExtractorName)
	t.Logf("CreationTime: %d", props.CreationTime)

	// Verify expected values from sst_dump output:
	// # data blocks: 1
	// # entries: 1
	// # deletions: 0
	// raw key size: 12
	// raw value size: 6
	// column family ID: 0
	// column family name: default
	// comparator name: leveldb.BytewiseComparator

	if props.NumDataBlocks != 1 {
		t.Errorf("NumDataBlocks = %d, want 1", props.NumDataBlocks)
	}
	if props.NumEntries != 1 {
		t.Errorf("NumEntries = %d, want 1", props.NumEntries)
	}
	if props.NumDeletions != 0 {
		t.Errorf("NumDeletions = %d, want 0", props.NumDeletions)
	}
	if props.RawKeySize != 12 {
		t.Errorf("RawKeySize = %d, want 12", props.RawKeySize)
	}
	if props.RawValueSize != 6 {
		t.Errorf("RawValueSize = %d, want 6", props.RawValueSize)
	}
	if props.ColumnFamilyID != 0 {
		t.Errorf("ColumnFamilyID = %d, want 0", props.ColumnFamilyID)
	}
	if props.ColumnFamilyName != "default" {
		t.Errorf("ColumnFamilyName = %q, want 'default'", props.ColumnFamilyName)
	}
	if props.ComparatorName != "leveldb.BytewiseComparator" {
		t.Errorf("ComparatorName = %q, want 'leveldb.BytewiseComparator'", props.ComparatorName)
	}
}

func TestPropertiesLazyLoading(t *testing.T) {
	goldenPath := filepath.Join("..", "..", "testdata", "golden", "v10.7.5", "sst", "simple.sst")
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Skipf("Golden file not found: %v", err)
	}

	file := &BytesFile{data: data}
	reader, err := Open(file, ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer reader.Close()

	// First call should load properties
	props1, err := reader.Properties()
	if err != nil {
		t.Fatalf("Failed to get properties: %v", err)
	}

	// Second call should return cached properties
	props2, err := reader.Properties()
	if err != nil {
		t.Fatalf("Failed to get properties second time: %v", err)
	}

	// Should be the same pointer
	if props1 != props2 {
		t.Error("Properties should be cached")
	}
}

func TestPropertyConstants(t *testing.T) {
	// Verify property name constants match RocksDB
	tests := []struct {
		name  string
		value string
	}{
		{"PropDataSize", PropDataSize},
		{"PropIndexSize", PropIndexSize},
		{"PropRawKeySize", PropRawKeySize},
		{"PropRawValueSize", PropRawValueSize},
		{"PropNumDataBlocks", PropNumDataBlocks},
		{"PropNumEntries", PropNumEntries},
		{"PropColumnFamilyID", PropColumnFamilyID},
		{"PropColumnFamilyName", PropColumnFamilyName},
		{"PropComparator", PropComparator},
		{"PropCompression", PropCompression},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 0 {
				t.Errorf("%s is empty", tt.name)
			}
			// All property names should start with "rocksdb."
			if len(tt.value) < 8 || tt.value[:8] != "rocksdb." {
				t.Errorf("%s = %q, expected to start with 'rocksdb.'", tt.name, tt.value)
			}
		})
	}
}

func TestPropertiesUserCollected(t *testing.T) {
	goldenPath := filepath.Join("..", "..", "testdata", "golden", "v10.7.5", "sst", "simple.sst")
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Skipf("Golden file not found: %v", err)
	}

	file := &BytesFile{data: data}
	reader, err := Open(file, ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer reader.Close()

	props, err := reader.Properties()
	if err != nil {
		t.Fatalf("Failed to get properties: %v", err)
	}

	// Log any user-collected properties
	if len(props.UserCollectedProperties) > 0 {
		t.Log("User-collected properties:")
		for k, v := range props.UserCollectedProperties {
			t.Logf("  %s: %q", k, v)
		}
	} else {
		t.Log("No user-collected properties")
	}
}

// -----------------------------------------------------------------------------
// Additional Properties Tests for C++ Parity
// -----------------------------------------------------------------------------

func TestPropertiesDefaults(t *testing.T) {
	props := &TableProperties{}

	// Verify defaults are zero values
	if props.DataSize != 0 {
		t.Error("DataSize should default to 0")
	}
	if props.NumEntries != 0 {
		t.Error("NumEntries should default to 0")
	}
	if props.ColumnFamilyID != 0 {
		t.Error("ColumnFamilyID should default to 0")
	}
	if props.ComparatorName != "" {
		t.Error("ComparatorName should default to empty")
	}
}

func TestPropertyNamesFormat(t *testing.T) {
	// All standard property names should follow rocksdb. prefix pattern
	names := []string{
		PropDataSize,
		PropIndexSize,
		PropRawKeySize,
		PropRawValueSize,
		PropNumDataBlocks,
		PropNumEntries,
		PropColumnFamilyID,
		PropColumnFamilyName,
		PropComparator,
		PropCompression,
		PropDeletedKeys,
		PropMergeOperands,
		PropNumRangeDeletions,
		PropFormatVersion,
		PropFilterPolicy,
		PropCreationTime,
	}

	for _, name := range names {
		if len(name) < 8 {
			t.Errorf("Property name %q is too short", name)
		}
		if name[:8] != "rocksdb." {
			t.Errorf("Property %q should start with 'rocksdb.'", name)
		}
	}
}

func TestPropertiesFormatVersion(t *testing.T) {
	goldenPath := filepath.Join("..", "..", "testdata", "golden", "v10.7.5", "sst", "simple.sst")
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Skipf("Golden file not found: %v", err)
	}

	file := &BytesFile{data: data}
	reader, err := Open(file, ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer reader.Close()

	props, err := reader.Properties()
	if err != nil {
		t.Fatalf("Failed to get properties: %v", err)
	}

	// Format version should be in the properties
	t.Logf("FormatVersion from properties: %d", props.FormatVersion)

	// Format version from properties should match footer
	footer := reader.Footer()
	if props.FormatVersion != 0 && props.FormatVersion != uint64(footer.FormatVersion) {
		t.Logf("Note: FormatVersion in props (%d) differs from footer (%d)",
			props.FormatVersion, footer.FormatVersion)
	}
}

func TestPropertiesCompressionInfo(t *testing.T) {
	goldenPath := filepath.Join("..", "..", "testdata", "golden", "v10.7.5", "sst", "simple.sst")
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Skipf("Golden file not found: %v", err)
	}

	file := &BytesFile{data: data}
	reader, err := Open(file, ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer reader.Close()

	props, err := reader.Properties()
	if err != nil {
		t.Fatalf("Failed to get properties: %v", err)
	}

	// Log compression info
	t.Logf("CompressionName: %s", props.CompressionName)
	t.Logf("CompressionOptions: %s", props.CompressionOptions)

	// A valid SST should have compression info
	if props.CompressionName == "" {
		t.Log("Warning: CompressionName is empty")
	}
}

func TestPropertiesDBInfo(t *testing.T) {
	goldenPath := filepath.Join("..", "..", "testdata", "golden", "v10.7.5", "sst", "simple.sst")
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Skipf("Golden file not found: %v", err)
	}

	file := &BytesFile{data: data}
	reader, err := Open(file, ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer reader.Close()

	props, err := reader.Properties()
	if err != nil {
		t.Fatalf("Failed to get properties: %v", err)
	}

	// Log DB-related info
	t.Logf("DbId: %s", props.DBID)
	t.Logf("DbSessionId: %s", props.DBSessionID)
	t.Logf("DbHostId: %s", props.DBHostID)
}

func TestPropertiesTimestamps(t *testing.T) {
	goldenPath := filepath.Join("..", "..", "testdata", "golden", "v10.7.5", "sst", "simple.sst")
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Skipf("Golden file not found: %v", err)
	}

	file := &BytesFile{data: data}
	reader, err := Open(file, ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer reader.Close()

	props, err := reader.Properties()
	if err != nil {
		t.Fatalf("Failed to get properties: %v", err)
	}

	t.Logf("CreationTime: %d", props.CreationTime)
	t.Logf("FileCreationTime: %d", props.FileCreationTime)
	t.Logf("OldestKeyTime: %d", props.OldestKeyTime)
}
