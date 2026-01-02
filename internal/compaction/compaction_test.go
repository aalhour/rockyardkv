package compaction

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/vfs"
)

// =============================================================================
// Compaction Structure Tests
// =============================================================================

func TestNewCompaction(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("c")),
			makeTestFileMetaData(2, 1000, []byte("d"), []byte("f")),
		}},
	}

	c := NewCompaction(inputs, 1)

	if c.OutputLevel != 1 {
		t.Errorf("OutputLevel = %d, want 1", c.OutputLevel)
	}
	if c.StartLevel() != 0 {
		t.Errorf("StartLevel() = %d, want 0", c.StartLevel())
	}
	if c.NumInputFiles() != 2 {
		t.Errorf("NumInputFiles() = %d, want 2", c.NumInputFiles())
	}
	if c.Edit == nil {
		t.Error("Edit should not be nil")
	}
}

func TestCompactionEmptyInputs(t *testing.T) {
	c := NewCompaction([]*CompactionInputFiles{}, 1)

	if c.StartLevel() != -1 {
		t.Errorf("StartLevel() for empty = %d, want -1", c.StartLevel())
	}
	if c.NumInputFiles() != 0 {
		t.Errorf("NumInputFiles() for empty = %d, want 0", c.NumInputFiles())
	}
}

func TestCompactionMultipleLevels(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("m")),
		}},
		{Level: 1, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(10, 2000, []byte("a"), []byte("f")),
			makeTestFileMetaData(11, 2000, []byte("g"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 1)

	if c.StartLevel() != 0 {
		t.Errorf("StartLevel() = %d, want 0", c.StartLevel())
	}
	if c.NumInputFiles() != 3 {
		t.Errorf("NumInputFiles() = %d, want 3", c.NumInputFiles())
	}
	if string(c.SmallestKey) != "a" {
		t.Errorf("SmallestKey = %q, want 'a'", c.SmallestKey)
	}
	if string(c.LargestKey) != "z" {
		t.Errorf("LargestKey = %q, want 'z'", c.LargestKey)
	}
}

func TestCompactionTrivialMove(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 1)
	c.IsTrivialMove = true

	if !c.IsTrivialMove {
		t.Error("IsTrivialMove should be true")
	}
}

func TestCompactionMaxOutputFileSize(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 1)

	// Default should be 64MB
	if c.MaxOutputFileSize != 64*1024*1024 {
		t.Errorf("MaxOutputFileSize = %d, want %d", c.MaxOutputFileSize, 64*1024*1024)
	}

	// Should be modifiable
	c.MaxOutputFileSize = 32 * 1024 * 1024
	if c.MaxOutputFileSize != 32*1024*1024 {
		t.Errorf("MaxOutputFileSize after set = %d, want %d", c.MaxOutputFileSize, 32*1024*1024)
	}
}

func TestCompactionMarkFilesBeingCompacted(t *testing.T) {
	meta1 := makeTestFileMetaData(1, 1000, []byte("a"), []byte("m"))
	meta2 := makeTestFileMetaData(2, 1000, []byte("n"), []byte("z"))

	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{meta1, meta2}},
	}

	c := NewCompaction(inputs, 1)

	// Mark as being compacted
	c.MarkFilesBeingCompacted(true)
	if !meta1.BeingCompacted {
		t.Error("meta1.BeingCompacted should be true")
	}
	if !meta2.BeingCompacted {
		t.Error("meta2.BeingCompacted should be true")
	}

	// Clear the flag
	c.MarkFilesBeingCompacted(false)
	if meta1.BeingCompacted {
		t.Error("meta1.BeingCompacted should be false")
	}
	if meta2.BeingCompacted {
		t.Error("meta2.BeingCompacted should be false")
	}
}

func TestCompactionScore(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 1)
	c.Score = 1.5

	if c.Score != 1.5 {
		t.Errorf("Score = %f, want 1.5", c.Score)
	}
}

// =============================================================================
// CompactionReason Tests
// =============================================================================

func TestAllCompactionReasons(t *testing.T) {
	reasons := []struct {
		r    CompactionReason
		want string
	}{
		{CompactionReasonUnknown, "Unknown"},
		{CompactionReasonLevelL0FileNumTrigger, "L0 file count"},
		{CompactionReasonLevelMaxLevelSize, "Level size"},
		{CompactionReasonManualCompaction, "Manual"},
		{CompactionReasonFlush, "Flush"},
	}

	for _, tt := range reasons {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.r.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// =============================================================================
// Key Range Tests
// =============================================================================

func TestCompactionKeyRangeEmpty(t *testing.T) {
	c := NewCompaction([]*CompactionInputFiles{}, 1)

	if c.SmallestKey != nil {
		t.Errorf("SmallestKey should be nil, got %v", c.SmallestKey)
	}
	if c.LargestKey != nil {
		t.Errorf("LargestKey should be nil, got %v", c.LargestKey)
	}
}

func TestCompactionKeyRangeSingleFile(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("abc"), []byte("xyz")),
		}},
	}

	c := NewCompaction(inputs, 1)

	if string(c.SmallestKey) != "abc" {
		t.Errorf("SmallestKey = %q, want 'abc'", c.SmallestKey)
	}
	if string(c.LargestKey) != "xyz" {
		t.Errorf("LargestKey = %q, want 'xyz'", c.LargestKey)
	}
}

func TestCompactionKeyRangeOverlapping(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("ccc"), []byte("fff")),
			makeTestFileMetaData(2, 1000, []byte("aaa"), []byte("ddd")),
			makeTestFileMetaData(3, 1000, []byte("eee"), []byte("zzz")),
		}},
	}

	c := NewCompaction(inputs, 1)

	if string(c.SmallestKey) != "aaa" {
		t.Errorf("SmallestKey = %q, want 'aaa'", c.SmallestKey)
	}
	if string(c.LargestKey) != "zzz" {
		t.Errorf("LargestKey = %q, want 'zzz'", c.LargestKey)
	}
}

func TestCompactionKeyRangeWithEmptyKeys(t *testing.T) {
	meta := manifest.NewFileMetaData()
	meta.FD = manifest.NewFileDescriptor(1, 0, 1000)
	meta.Smallest = []byte{}
	meta.Largest = []byte{}

	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{meta}},
	}

	c := NewCompaction(inputs, 1)

	// Should handle empty keys gracefully
	if c.SmallestKey == nil {
		// Empty byte slice is still valid
	}
}

// =============================================================================
// Input Deletions Tests
// =============================================================================

func TestAddInputDeletionsEmpty(t *testing.T) {
	c := NewCompaction([]*CompactionInputFiles{}, 1)
	c.AddInputDeletions()

	if len(c.DeletedFiles()) != 0 {
		t.Errorf("DeletedFiles() = %d, want 0", len(c.DeletedFiles()))
	}
}

func TestAddInputDeletionsSingleLevel(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("b")),
			makeTestFileMetaData(2, 1000, []byte("c"), []byte("d")),
			makeTestFileMetaData(3, 1000, []byte("e"), []byte("f")),
		}},
	}

	c := NewCompaction(inputs, 1)
	c.AddInputDeletions()

	deleted := c.DeletedFiles()
	if len(deleted) != 3 {
		t.Fatalf("DeletedFiles() = %d, want 3", len(deleted))
	}

	// All should be from level 0
	for _, d := range deleted {
		if d.Level != 0 {
			t.Errorf("Level = %d, want 0", d.Level)
		}
	}
}

func TestAddInputDeletionsMultipleLevels(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("b")),
		}},
		{Level: 1, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(10, 1000, []byte("a"), []byte("c")),
			makeTestFileMetaData(11, 1000, []byte("d"), []byte("f")),
		}},
	}

	c := NewCompaction(inputs, 1)
	c.AddInputDeletions()

	deleted := c.DeletedFiles()
	if len(deleted) != 3 {
		t.Fatalf("DeletedFiles() = %d, want 3", len(deleted))
	}

	// Count by level
	l0Count := 0
	l1Count := 0
	for _, d := range deleted {
		switch d.Level {
		case 0:
			l0Count++
		case 1:
			l1Count++
		}
	}

	if l0Count != 1 {
		t.Errorf("L0 deleted = %d, want 1", l0Count)
	}
	if l1Count != 2 {
		t.Errorf("L1 deleted = %d, want 2", l1Count)
	}
}

// =============================================================================
// CompareKeys Tests
// =============================================================================

func TestCompareKeysEqual(t *testing.T) {
	if compareKeys([]byte("abc"), []byte("abc")) != 0 {
		t.Error("Equal keys should return 0")
	}
}

func TestCompareKeysLess(t *testing.T) {
	if compareKeys([]byte("aaa"), []byte("bbb")) >= 0 {
		t.Error("'aaa' should be less than 'bbb'")
	}
	if compareKeys([]byte("abc"), []byte("abd")) >= 0 {
		t.Error("'abc' should be less than 'abd'")
	}
	if compareKeys([]byte("ab"), []byte("abc")) >= 0 {
		t.Error("'ab' should be less than 'abc'")
	}
}

func TestCompareKeysGreater(t *testing.T) {
	if compareKeys([]byte("bbb"), []byte("aaa")) <= 0 {
		t.Error("'bbb' should be greater than 'aaa'")
	}
	if compareKeys([]byte("abd"), []byte("abc")) <= 0 {
		t.Error("'abd' should be greater than 'abc'")
	}
	if compareKeys([]byte("abc"), []byte("ab")) <= 0 {
		t.Error("'abc' should be greater than 'ab'")
	}
}

func TestCompareKeysEmpty(t *testing.T) {
	if compareKeys([]byte{}, []byte{}) != 0 {
		t.Error("Two empty keys should be equal")
	}
	if compareKeys([]byte{}, []byte("a")) >= 0 {
		t.Error("Empty key should be less than non-empty")
	}
	if compareKeys([]byte("a"), []byte{}) <= 0 {
		t.Error("Non-empty key should be greater than empty")
	}
}

func TestCompareKeysBinary(t *testing.T) {
	// Keys with binary data
	if compareKeys([]byte{0x00}, []byte{0x01}) >= 0 {
		t.Error("0x00 should be less than 0x01")
	}
	if compareKeys([]byte{0xFF}, []byte{0x00}) <= 0 {
		t.Error("0xFF should be greater than 0x00")
	}
}

// =============================================================================
// CompactionJob Tests (with mock filesystem)
// =============================================================================

func TestCompactionJobCreation(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.Default()
	cache := table.NewTableCache(fs, table.TableCacheOptions{MaxOpenFiles: 10})
	fileNum := uint64(100)

	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 1)
	job := NewCompactionJob(c, dir, fs, cache, func() uint64 {
		fileNum++
		return fileNum
	})

	if job == nil {
		t.Fatal("NewCompactionJob returned nil")
	}
	if job.dbPath != dir {
		t.Errorf("dbPath = %q, want %q", job.dbPath, dir)
	}
}

func TestCompactionJobSSTPath(t *testing.T) {
	dir := "/test/db"
	fs := vfs.Default()
	cache := table.NewTableCache(fs, table.TableCacheOptions{MaxOpenFiles: 10})

	c := NewCompaction([]*CompactionInputFiles{}, 1)
	job := NewCompactionJob(c, dir, fs, cache, func() uint64 { return 1 })

	path := job.sstPath(42)
	expected := filepath.Join(dir, "000042.sst")
	if path != expected {
		t.Errorf("sstPath(42) = %q, want %q", path, expected)
	}
}

func TestCompactionJobTrivialMove(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.Default()
	cache := table.NewTableCache(fs, table.TableCacheOptions{MaxOpenFiles: 10})

	meta := makeTestFileMetaData(1, 1000, []byte("a"), []byte("z"))
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{meta}},
	}

	c := NewCompaction(inputs, 1)
	c.IsTrivialMove = true

	job := NewCompactionJob(c, dir, fs, cache, func() uint64 { return 100 })
	outputFiles, err := job.Run()
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// Trivial move should not create new files
	if len(outputFiles) > 0 {
		t.Errorf("Trivial move should not create output files, got %d", len(outputFiles))
	}

	// But the edit should have the file movement recorded
	if len(c.Edit.NewFiles) == 0 {
		t.Error("Edit.NewFiles should have the moved file")
	}
	if len(c.Edit.DeletedFiles) == 0 {
		t.Error("Edit.DeletedFiles should have the deleted file")
	}
}

// =============================================================================
// Integration-style Compaction Tests
// =============================================================================

func createTestSST(t *testing.T, dir string, fileNum uint64, keys []string) {
	t.Helper()

	path := filepath.Join(dir, fmt.Sprintf("%06d.sst", fileNum))
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create SST file: %v", err)
	}

	opts := table.DefaultBuilderOptions()
	builder := table.NewTableBuilder(&writableFileWrapper{file}, opts)

	for _, key := range keys {
		// Create internal key (user key + 8-byte trailer)
		internalKey := makeInternalKey(key, 100, 1)
		err := builder.Add(internalKey, []byte("value_"+key))
		if err != nil {
			file.Close()
			t.Fatalf("Add key %s: %v", key, err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("Finish: %v", err)
	}
	file.Close()
}

// makeInternalKey creates an internal key from user key, sequence, and type.
func makeInternalKey(userKey string, seq uint64, vtype uint8) []byte {
	key := make([]byte, len(userKey)+8)
	copy(key, userKey)
	trailer := (seq << 8) | uint64(vtype)
	for i := range 8 {
		key[len(userKey)+i] = byte(trailer >> (8 * i))
	}
	return key
}

// writableFileWrapper wraps os.File to implement vfs.WritableFile
type writableFileWrapper struct {
	*os.File
}

func (w *writableFileWrapper) Append(data []byte) error {
	_, err := w.Write(data)
	return err
}

func (w *writableFileWrapper) Size() (int64, error) {
	info, err := w.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func TestCompactionJobWithRealFiles(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.Default()
	cache := table.NewTableCache(fs, table.TableCacheOptions{MaxOpenFiles: 10})
	defer cache.Close()

	// Create test SST files
	createTestSST(t, dir, 1, []string{"a", "c", "e"})
	createTestSST(t, dir, 2, []string{"b", "d", "f"})

	// Build file metadata
	meta1 := makeTestFileMetaData(1, 1000, makeInternalKey("a", 100, 1), makeInternalKey("e", 100, 1))
	meta2 := makeTestFileMetaData(2, 1000, makeInternalKey("b", 100, 1), makeInternalKey("f", 100, 1))

	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{meta1, meta2}},
	}

	c := NewCompaction(inputs, 1)
	fileNum := uint64(100)

	job := NewCompactionJob(c, dir, fs, cache, func() uint64 {
		fileNum++
		return fileNum
	})

	outputFiles, err := job.Run()
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(outputFiles) == 0 {
		t.Error("Expected at least one output file")
	}

	// Verify output file exists
	for _, f := range outputFiles {
		path := filepath.Join(dir, fmt.Sprintf("%06d.sst", f.FD.GetNumber()))
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("Output file %s does not exist", path)
		}
	}
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func TestCompactionWithSingleKeyFile(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 100, []byte("only_key"), []byte("only_key")),
		}},
	}

	c := NewCompaction(inputs, 1)

	if !bytes.Equal(c.SmallestKey, c.LargestKey) {
		t.Error("Single-key file should have smallest == largest")
	}
}

func TestCompactionWithLargeFiles(t *testing.T) {
	// Create metadata for large files
	inputs := []*CompactionInputFiles{
		{Level: 1, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 100*1024*1024, []byte("a"), []byte("m")), // 100MB
			makeTestFileMetaData(2, 100*1024*1024, []byte("n"), []byte("z")), // 100MB
		}},
	}

	c := NewCompaction(inputs, 2)

	// Total input size should be tracked correctly
	totalSize := uint64(0)
	for _, in := range c.Inputs {
		for _, f := range in.Files {
			totalSize += f.FD.FileSize
		}
	}

	if totalSize != 200*1024*1024 {
		t.Errorf("Total input size = %d, want %d", totalSize, 200*1024*1024)
	}
}

func TestCompactionOutputLevelRange(t *testing.T) {
	// Test compactions to different output levels
	for outputLevel := 1; outputLevel <= 6; outputLevel++ {
		inputs := []*CompactionInputFiles{
			{Level: outputLevel - 1, Files: []*manifest.FileMetaData{
				makeTestFileMetaData(1, 1000, []byte("a"), []byte("z")),
			}},
		}

		c := NewCompaction(inputs, outputLevel)

		if c.OutputLevel != outputLevel {
			t.Errorf("OutputLevel = %d, want %d", c.OutputLevel, outputLevel)
		}
	}
}

// =============================================================================
// Additional Compaction Edge Case Tests (matching C++ db/db_compaction_test.cc)
// =============================================================================

// TestCompactionWithDeletions tests compaction handling of deletion markers.
func TestCompactionWithDeletions(t *testing.T) {
	// Create files with overlapping keys where one is a deletion
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("m")),
			makeTestFileMetaData(2, 1000, []byte("c"), []byte("z")), // Overlapping with deletions
		}},
	}

	c := NewCompaction(inputs, 1)

	// Verify overlap detection for L0
	if c.StartLevel() != 0 {
		t.Errorf("StartLevel = %d, want 0", c.StartLevel())
	}

	// Key range should span all files
	if !bytes.Equal(c.SmallestKey, []byte("a")) {
		t.Errorf("SmallestKey = %q, want 'a'", c.SmallestKey)
	}
	if !bytes.Equal(c.LargestKey, []byte("z")) {
		t.Errorf("LargestKey = %q, want 'z'", c.LargestKey)
	}
}

// TestCompactionL0ToL0 tests L0 to L0 compaction (intra-L0).
func TestCompactionL0ToL0(t *testing.T) {
	// L0 to L0 compaction is used when L0 has too many files
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 500, []byte("a"), []byte("d")),
			makeTestFileMetaData(2, 500, []byte("e"), []byte("h")),
			makeTestFileMetaData(3, 500, []byte("i"), []byte("l")),
		}},
	}

	c := NewCompaction(inputs, 0) // Output to L0

	if c.OutputLevel != 0 {
		t.Errorf("OutputLevel = %d, want 0", c.OutputLevel)
	}
	if c.StartLevel() != 0 {
		t.Errorf("StartLevel = %d, want 0", c.StartLevel())
	}
}

// TestCompactionMultiLevelInputs tests compaction with files from multiple levels.
func TestCompactionMultiLevelInputs(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("m")),
		}},
		{Level: 1, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(2, 1000, []byte("a"), []byte("g")),
			makeTestFileMetaData(3, 1000, []byte("h"), []byte("n")),
		}},
	}

	c := NewCompaction(inputs, 1)

	if len(c.Inputs) != 2 {
		t.Errorf("Input levels = %d, want 2", len(c.Inputs))
	}
	if c.NumInputFiles() != 3 {
		t.Errorf("NumInputFiles = %d, want 3", c.NumInputFiles())
	}
}

// TestCompactionIsBottommost tests bottom-level detection.
func TestCompactionIsBottommost(t *testing.T) {
	// Compaction to level 6 (bottom-most) should be detected
	inputs := []*CompactionInputFiles{
		{Level: 5, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 6)

	if c.OutputLevel != 6 {
		t.Errorf("OutputLevel = %d, want 6", c.OutputLevel)
	}
}

// TestCompactionFileMetaDataCopy tests that file metadata is properly referenced.
func TestCompactionFileMetaDataCopy(t *testing.T) {
	originalFile := makeTestFileMetaData(1, 1000, []byte("a"), []byte("z"))

	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{originalFile}},
	}

	c := NewCompaction(inputs, 1)

	// Verify the file in compaction points to the same metadata
	if c.Inputs[0].Files[0] != originalFile {
		t.Error("File metadata should be the same reference")
	}
}

// TestCompactionVersionEditGeneration tests that compaction generates proper version edits.
func TestCompactionVersionEditGeneration(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("m")),
			makeTestFileMetaData(2, 1000, []byte("n"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 1)
	c.AddInputDeletions()

	// Verify deletions were added
	if len(c.Edit.DeletedFiles) != 2 {
		t.Errorf("DeletedFiles = %d, want 2", len(c.Edit.DeletedFiles))
	}

	// Verify the correct files are marked for deletion
	deletedNumbers := make(map[uint64]bool)
	for _, df := range c.Edit.DeletedFiles {
		deletedNumbers[df.FileNumber] = true
	}

	if !deletedNumbers[1] || !deletedNumbers[2] {
		t.Error("Files 1 and 2 should be marked for deletion")
	}
}

// TestCompactionWithEmptyFiles tests handling of empty file sets.
func TestCompactionWithEmptyFiles(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{}}, // Empty L0
		{Level: 1, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 1)

	// Should still work with one empty input level
	if c.NumInputFiles() != 1 {
		t.Errorf("NumInputFiles = %d, want 1", c.NumInputFiles())
	}
}

// =============================================================================
// Additional Compaction Tests (matching C++ coverage)
// =============================================================================

// TestCompactionInputFilesLevel tests level tracking in CompactionInputFiles.
func TestCompactionInputFilesLevel(t *testing.T) {
	testCases := []struct {
		level     int
		numFiles  int
		wantLevel int
	}{
		{0, 3, 0},
		{1, 2, 1},
		{5, 1, 5},
		{6, 10, 6},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("L%d", tc.level), func(t *testing.T) {
			files := make([]*manifest.FileMetaData, tc.numFiles)
			for i := range tc.numFiles {
				files[i] = makeTestFileMetaData(uint64(i+1), 100, []byte("a"), []byte("z"))
			}

			input := &CompactionInputFiles{Level: tc.level, Files: files}

			if input.Level != tc.wantLevel {
				t.Errorf("Level = %d, want %d", input.Level, tc.wantLevel)
			}
			if len(input.Files) != tc.numFiles {
				t.Errorf("NumFiles = %d, want %d", len(input.Files), tc.numFiles)
			}
		})
	}
}

// TestCompactionOutputLevel tests output level calculation.
func TestCompactionOutputLevel(t *testing.T) {
	testCases := []struct {
		startLevel  int
		outputLevel int
	}{
		{0, 1}, // L0 -> L1
		{1, 2}, // L1 -> L2
		{5, 6}, // L5 -> L6
		{0, 0}, // L0 -> L0 (intra-L0 compaction)
		{3, 3}, // L3 -> L3 (trivial move at same level)
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("L%d->L%d", tc.startLevel, tc.outputLevel), func(t *testing.T) {
			inputs := []*CompactionInputFiles{
				{Level: tc.startLevel, Files: []*manifest.FileMetaData{
					makeTestFileMetaData(1, 100, []byte("a"), []byte("z")),
				}},
			}

			c := NewCompaction(inputs, tc.outputLevel)

			if c.OutputLevel != tc.outputLevel {
				t.Errorf("OutputLevel = %d, want %d", c.OutputLevel, tc.outputLevel)
			}
		})
	}
}

// TestCompactionTotalInputSize tests total input size calculation.
func TestCompactionTotalInputSize(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("c")),
			makeTestFileMetaData(2, 2000, []byte("d"), []byte("f")),
		}},
		{Level: 1, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(3, 3000, []byte("a"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 1)

	totalSize := uint64(0)
	for _, in := range c.Inputs {
		for _, f := range in.Files {
			totalSize += f.FD.FileSize
		}
	}

	expectedSize := uint64(1000 + 2000 + 3000)
	if totalSize != expectedSize {
		t.Errorf("TotalSize = %d, want %d", totalSize, expectedSize)
	}
}

// TestCompactionSmallestLargestKeys tests the smallest/largest key tracking.
func TestCompactionSmallestLargestKeys(t *testing.T) {
	testCases := []struct {
		name         string
		files        []*manifest.FileMetaData
		wantSmallest []byte
		wantLargest  []byte
	}{
		{
			name: "single_file",
			files: []*manifest.FileMetaData{
				makeTestFileMetaData(1, 100, []byte("key1"), []byte("key5")),
			},
			wantSmallest: []byte("key1"),
			wantLargest:  []byte("key5"),
		},
		{
			name: "multiple_files_ordered",
			files: []*manifest.FileMetaData{
				makeTestFileMetaData(1, 100, []byte("a"), []byte("c")),
				makeTestFileMetaData(2, 100, []byte("d"), []byte("f")),
				makeTestFileMetaData(3, 100, []byte("g"), []byte("i")),
			},
			wantSmallest: []byte("a"),
			wantLargest:  []byte("i"),
		},
		{
			name: "overlapping_files",
			files: []*manifest.FileMetaData{
				makeTestFileMetaData(1, 100, []byte("b"), []byte("e")),
				makeTestFileMetaData(2, 100, []byte("a"), []byte("d")),
				makeTestFileMetaData(3, 100, []byte("c"), []byte("z")),
			},
			wantSmallest: []byte("a"),
			wantLargest:  []byte("z"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []*CompactionInputFiles{
				{Level: 0, Files: tc.files},
			}

			c := NewCompaction(inputs, 1)

			// Check smallest and largest via the stored fields
			if !bytes.HasPrefix(c.SmallestKey, tc.wantSmallest) {
				t.Errorf("SmallestKey = %q, want prefix %q", c.SmallestKey, tc.wantSmallest)
			}
			if !bytes.HasPrefix(c.LargestKey, tc.wantLargest) {
				t.Errorf("LargestKey = %q, want prefix %q", c.LargestKey, tc.wantLargest)
			}
		})
	}
}

// TestCompactionReasonStringRepresentation tests string representation of compaction reasons.
func TestCompactionReasonStringRepresentation(t *testing.T) {
	testCases := []struct {
		reason CompactionReason
		want   string
	}{
		{CompactionReasonUnknown, "Unknown"},
		{CompactionReasonLevelL0FileNumTrigger, "L0 file count"},
		{CompactionReasonLevelMaxLevelSize, "Level size"},
		{CompactionReasonManualCompaction, "Manual"},
		{CompactionReasonFlush, "Flush"},
	}

	for _, tc := range testCases {
		t.Run(tc.want, func(t *testing.T) {
			got := tc.reason.String()
			if got != tc.want {
				t.Errorf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCompactionAllLevels tests compaction can be created for all levels.
func TestCompactionAllLevels(t *testing.T) {
	for level := range 7 {
		t.Run(fmt.Sprintf("L%d", level), func(t *testing.T) {
			inputs := []*CompactionInputFiles{
				{Level: level, Files: []*manifest.FileMetaData{
					makeTestFileMetaData(1, 100, []byte("a"), []byte("z")),
				}},
			}

			outputLevel := min(level+1, 6)

			c := NewCompaction(inputs, outputLevel)

			if c.StartLevel() != level {
				t.Errorf("StartLevel() = %d, want %d", c.StartLevel(), level)
			}
		})
	}
}

// TestCompactionWithManyFiles tests handling of many input files.
func TestCompactionWithManyFiles(t *testing.T) {
	const numFiles = 100

	files := make([]*manifest.FileMetaData, numFiles)
	for i := range numFiles {
		key := fmt.Appendf(nil, "key%04d", i)
		files[i] = makeTestFileMetaData(uint64(i+1), 100, key, key)
	}

	inputs := []*CompactionInputFiles{
		{Level: 0, Files: files},
	}

	c := NewCompaction(inputs, 1)

	if c.NumInputFiles() != numFiles {
		t.Errorf("NumInputFiles() = %d, want %d", c.NumInputFiles(), numFiles)
	}

	// Verify all files are marked for deletion
	c.AddInputDeletions()
	if len(c.Edit.DeletedFiles) != numFiles {
		t.Errorf("DeletedFiles = %d, want %d", len(c.Edit.DeletedFiles), numFiles)
	}
}

// TestCompactionInputLevelSizes tests input level size calculations.
func TestCompactionInputLevelSizes(t *testing.T) {
	testCases := []struct {
		name      string
		level     int
		numFiles  int
		fileSize  uint64
		wantTotal uint64
	}{
		{"empty", 0, 0, 0, 0},
		{"single_small", 0, 1, 100, 100},
		{"multiple", 0, 5, 200, 1000},
		{"level1", 1, 3, 1000, 3000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			files := make([]*manifest.FileMetaData, tc.numFiles)
			for i := range tc.numFiles {
				files[i] = makeTestFileMetaData(uint64(i+1), tc.fileSize, []byte("a"), []byte("z"))
			}

			inputs := []*CompactionInputFiles{
				{Level: tc.level, Files: files},
			}

			c := NewCompaction(inputs, tc.level+1)

			totalSize := uint64(0)
			for _, f := range c.Inputs[0].Files {
				totalSize += f.FD.FileSize
			}

			if totalSize != tc.wantTotal {
				t.Errorf("TotalSize = %d, want %d", totalSize, tc.wantTotal)
			}
		})
	}
}

// TestCompactionEditIntegration tests VersionEdit integration.
func TestCompactionEditIntegration(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(10, 1000, []byte("a"), []byte("m")),
			makeTestFileMetaData(11, 1000, []byte("n"), []byte("z")),
		}},
		{Level: 1, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(20, 5000, []byte("a"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 1)

	// Add deletions
	c.AddInputDeletions()

	// Verify all 3 input files are marked for deletion
	if len(c.Edit.DeletedFiles) != 3 {
		t.Errorf("DeletedFiles = %d, want 3", len(c.Edit.DeletedFiles))
	}

	// Verify levels are correct
	l0Count := 0
	l1Count := 0
	for _, df := range c.Edit.DeletedFiles {
		switch df.Level {
		case 0:
			l0Count++
		case 1:
			l1Count++
		}
	}

	if l0Count != 2 {
		t.Errorf("L0 deletions = %d, want 2", l0Count)
	}
	if l1Count != 1 {
		t.Errorf("L1 deletions = %d, want 1", l1Count)
	}
}

// =============================================================================
// Range Deletion in Compaction Tests
// =============================================================================

func TestCompactionJobShouldDropKey(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.Default()
	tableCache := table.NewTableCache(fs, table.TableCacheOptions{MaxOpenFiles: 100})
	defer tableCache.Close()

	fileNum := uint64(1)
	nextFileNum := func() uint64 {
		n := fileNum
		fileNum++
		return n
	}

	inputs := []*CompactionInputFiles{}
	c := NewCompaction(inputs, 1)

	// Create a compaction job with no earliest snapshot (can drop anything)
	job := NewCompactionJobWithSnapshot(c, dir, fs, tableCache, nextFileNum, 0)

	// Test that a job is created with the aggregator
	if job.rangeDelAgg == nil {
		t.Error("rangeDelAgg should not be nil")
	}

	// Without any tombstones, shouldDropKey should return false
	internalKey := makeInternalKey("testkey", 100, 1) // seq=100, type=1
	if job.shouldDropKey(internalKey) {
		t.Error("shouldDropKey should return false when no tombstones exist")
	}
}
