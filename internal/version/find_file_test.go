package version

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
)

// findFileTestFixture helps set up test scenarios for file finding operations.
// This matches C++ RocksDB's FindLevelFileTest fixture.
type findFileTestFixture struct {
	t      *testing.T
	vset   *VersionSet
	v      *Version
	level  int
	fileNo uint64
}

func newFindFileTestFixture(t *testing.T, numFiles int) *findFileTestFixture {
	opts := DefaultVersionSetOptions(t.TempDir())
	vset := NewVersionSet(opts)
	v := NewVersion(vset, 0)
	return &findFileTestFixture{
		t:      t,
		vset:   vset,
		v:      v,
		level:  1, // Use level 1 for most tests (sorted, non-overlapping)
		fileNo: 1,
	}
}

// Add adds a file with the given key range to the test version.
func (f *findFileTestFixture) Add(smallest, largest string) {
	f.AddWithSeq(smallest, largest, 100, 100)
}

// AddWithSeq adds a file with the given key range and sequence numbers.
func (f *findFileTestFixture) AddWithSeq(smallest, largest string, smallestSeq, largestSeq uint64) {
	meta := &manifest.FileMetaData{
		FD:       manifest.NewFileDescriptor(f.fileNo, 0, 1000),
		Smallest: makeInternalKey(smallest, smallestSeq, 1),
		Largest:  makeInternalKey(largest, largestSeq, 1),
	}
	f.fileNo++
	f.v.files[f.level] = append(f.v.files[f.level], meta)
}

// Overlaps checks if any files overlap with the given key range.
// Uses high sequence numbers for begin and low for end to maximize overlap chance.
func (f *findFileTestFixture) Overlaps(begin, end *string) bool {
	var beginKey, endKey []byte
	if begin != nil {
		// Use high sequence for begin to find files with smaller user keys or same user key
		beginKey = makeInternalKey(*begin, 10000, 1)
	}
	if end != nil {
		// Use low sequence for end to find files with larger user keys or same user key
		endKey = makeInternalKey(*end, 0, 1)
	}
	result := f.v.OverlappingInputs(f.level, beginKey, endKey)
	return len(result) > 0
}

// OverlapsUser checks if any files overlap with the given user key range.
func (f *findFileTestFixture) OverlapsUser(begin, end string) bool {
	return f.Overlaps(&begin, &end)
}

// OverlapsBeginUnbounded checks if any files overlap with [nil, end].
func (f *findFileTestFixture) OverlapsBeginUnbounded(end string) bool {
	return f.Overlaps(nil, &end)
}

// OverlapsEndUnbounded checks if any files overlap with [begin, nil].
func (f *findFileTestFixture) OverlapsEndUnbounded(begin string) bool {
	return f.Overlaps(&begin, nil)
}

// OverlapsBothUnbounded checks if any files overlap with [nil, nil].
func (f *findFileTestFixture) OverlapsBothUnbounded() bool {
	return f.Overlaps(nil, nil)
}

// =============================================================================
// FindLevelFile Tests (matching C++ RocksDB db/version_set_test.cc)
// =============================================================================

// TestFindLevelFileEmpty tests an empty level.
func TestFindLevelFileEmpty(t *testing.T) {
	f := newFindFileTestFixture(t, 0)

	// No files, no overlap
	if f.OverlapsUser("a", "z") {
		t.Error("Expected no overlap for empty level")
	}
	if f.OverlapsBeginUnbounded("z") {
		t.Error("Expected no overlap with unbounded begin")
	}
	if f.OverlapsEndUnbounded("a") {
		t.Error("Expected no overlap with unbounded end")
	}
	if f.OverlapsBothUnbounded() {
		t.Error("Expected no overlap with both unbounded")
	}
}

// TestFindLevelFileSingle tests a level with a single file.
func TestFindLevelFileSingle(t *testing.T) {
	f := newFindFileTestFixture(t, 1)
	f.Add("p", "q")

	// No overlap before file
	if f.OverlapsUser("a", "b") {
		t.Error("Expected no overlap with a-b")
	}
	// No overlap after file
	if f.OverlapsUser("z1", "z2") {
		t.Error("Expected no overlap with z1-z2")
	}

	// Overlap cases
	if !f.OverlapsUser("a", "p") {
		t.Error("Expected overlap with a-p")
	}
	if !f.OverlapsUser("a", "q") {
		t.Error("Expected overlap with a-q")
	}
	if !f.OverlapsUser("a", "z") {
		t.Error("Expected overlap with a-z")
	}
	if !f.OverlapsUser("p", "p1") {
		t.Error("Expected overlap with p-p1")
	}
	if !f.OverlapsUser("p", "q") {
		t.Error("Expected overlap with p-q")
	}
	if !f.OverlapsUser("p", "z") {
		t.Error("Expected overlap with p-z")
	}
	if !f.OverlapsUser("p1", "p2") {
		t.Error("Expected overlap with p1-p2")
	}
	if !f.OverlapsUser("p1", "z") {
		t.Error("Expected overlap with p1-z")
	}
	if !f.OverlapsUser("q", "q") {
		t.Error("Expected overlap with q-q")
	}
	if !f.OverlapsUser("q", "q1") {
		t.Error("Expected overlap with q-q1")
	}
}

// TestFindLevelFileMultiple tests a level with multiple non-overlapping files.
func TestFindLevelFileMultiple(t *testing.T) {
	f := newFindFileTestFixture(t, 4)
	f.Add("150", "200")
	f.Add("200", "250")
	f.Add("300", "350")
	f.Add("400", "450")

	// Before all files
	if f.OverlapsUser("100", "149") {
		t.Error("Expected no overlap with 100-149")
	}

	// After all files
	if f.OverlapsUser("451", "500") {
		t.Error("Expected no overlap with 451-500")
	}

	// In gap between files
	if f.OverlapsUser("251", "299") {
		t.Error("Expected no overlap with 251-299")
	}
	if f.OverlapsUser("351", "399") {
		t.Error("Expected no overlap with 351-399")
	}

	// Overlapping first file
	if !f.OverlapsUser("100", "150") {
		t.Error("Expected overlap with 100-150")
	}
	if !f.OverlapsUser("100", "200") {
		t.Error("Expected overlap with 100-200")
	}

	// Overlapping last file
	if !f.OverlapsUser("400", "500") {
		t.Error("Expected overlap with 400-500")
	}

	// Overlapping middle files
	if !f.OverlapsUser("300", "350") {
		t.Error("Expected overlap with 300-350")
	}
	if !f.OverlapsUser("200", "300") {
		t.Error("Expected overlap with 200-300")
	}

	// Spanning all files
	if !f.OverlapsUser("100", "500") {
		t.Error("Expected overlap with 100-500")
	}
}

// TestFindLevelFileMultipleNullBoundaries tests null boundary handling.
func TestFindLevelFileMultipleNullBoundaries(t *testing.T) {
	f := newFindFileTestFixture(t, 4)
	f.Add("150", "200")
	f.Add("200", "250")
	f.Add("300", "350")
	f.Add("400", "450")

	// Unbounded begin
	if f.OverlapsBeginUnbounded("149") {
		t.Error("Expected no overlap with nil-149")
	}
	if !f.OverlapsBeginUnbounded("150") {
		t.Error("Expected overlap with nil-150")
	}
	if !f.OverlapsBeginUnbounded("199") {
		t.Error("Expected overlap with nil-199")
	}
	if !f.OverlapsBeginUnbounded("200") {
		t.Error("Expected overlap with nil-200")
	}
	if !f.OverlapsBeginUnbounded("201") {
		t.Error("Expected overlap with nil-201")
	}
	if !f.OverlapsBeginUnbounded("400") {
		t.Error("Expected overlap with nil-400")
	}
	if !f.OverlapsBeginUnbounded("800") {
		t.Error("Expected overlap with nil-800")
	}

	// Unbounded end
	if f.OverlapsEndUnbounded("451") {
		t.Error("Expected no overlap with 451-nil")
	}
	if !f.OverlapsEndUnbounded("100") {
		t.Error("Expected overlap with 100-nil")
	}
	if !f.OverlapsEndUnbounded("200") {
		t.Error("Expected overlap with 200-nil")
	}
	if !f.OverlapsEndUnbounded("449") {
		t.Error("Expected overlap with 449-nil")
	}
	if !f.OverlapsEndUnbounded("450") {
		t.Error("Expected overlap with 450-nil")
	}

	// Both unbounded
	if !f.OverlapsBothUnbounded() {
		t.Error("Expected overlap with nil-nil")
	}
}

// TestFindLevelFileOverlapSequenceChecks tests overlap with sequence numbers.
func TestFindLevelFileOverlapSequenceChecks(t *testing.T) {
	f := newFindFileTestFixture(t, 1)
	f.AddWithSeq("200", "200", 5000, 3000)

	if f.OverlapsUser("199", "199") {
		t.Error("Expected no overlap with 199-199")
	}
	if f.OverlapsUser("201", "300") {
		t.Error("Expected no overlap with 201-300")
	}
	if !f.OverlapsUser("200", "200") {
		t.Error("Expected overlap with 200-200")
	}
	if !f.OverlapsUser("190", "200") {
		t.Error("Expected overlap with 190-200")
	}
	if !f.OverlapsUser("200", "210") {
		t.Error("Expected overlap with 200-210")
	}
}

// TestFindLevelFileOverlappingFilesL0 tests L0 with overlapping files.
func TestFindLevelFileOverlappingFilesL0(t *testing.T) {
	f := newFindFileTestFixture(t, 2)
	f.level = 0 // L0 allows overlapping files
	f.Add("150", "600")
	f.Add("400", "500")

	// Before all files
	if f.OverlapsUser("100", "149") {
		t.Error("Expected no overlap with 100-149")
	}
	// After all files
	if f.OverlapsUser("601", "700") {
		t.Error("Expected no overlap with 601-700")
	}

	// Overlap cases
	if !f.OverlapsUser("100", "150") {
		t.Error("Expected overlap with 100-150")
	}
	if !f.OverlapsUser("100", "200") {
		t.Error("Expected overlap with 100-200")
	}
	if !f.OverlapsUser("100", "300") {
		t.Error("Expected overlap with 100-300")
	}
	if !f.OverlapsUser("100", "400") {
		t.Error("Expected overlap with 100-400")
	}
	if !f.OverlapsUser("100", "500") {
		t.Error("Expected overlap with 100-500")
	}
	if !f.OverlapsUser("375", "400") {
		t.Error("Expected overlap with 375-400")
	}
	if !f.OverlapsUser("450", "450") {
		t.Error("Expected overlap with 450-450")
	}
	if !f.OverlapsUser("450", "500") {
		t.Error("Expected overlap with 450-500")
	}
	if !f.OverlapsUser("450", "700") {
		t.Error("Expected overlap with 450-700")
	}
	if !f.OverlapsUser("600", "700") {
		t.Error("Expected overlap with 600-700")
	}
}

// =============================================================================
// OverlappingInputs Tests
// =============================================================================

// TestOverlappingInputsReturnsCorrectFiles tests that OverlappingInputs returns
// the correct files.
func TestOverlappingInputsReturnsCorrectFiles(t *testing.T) {
	f := newFindFileTestFixture(t, 4)
	f.Add("100", "200")
	f.Add("300", "400")
	f.Add("500", "600")
	f.Add("700", "800")

	// Query that overlaps first two files
	begin := makeInternalKey("150", 100, 1)
	end := makeInternalKey("350", 100, 1)
	result := f.v.OverlappingInputs(f.level, begin, end)

	if len(result) != 2 {
		t.Errorf("Expected 2 files, got %d", len(result))
	}

	// Query that overlaps no files
	begin = makeInternalKey("210", 100, 1)
	end = makeInternalKey("290", 100, 1)
	result = f.v.OverlappingInputs(f.level, begin, end)

	if len(result) != 0 {
		t.Errorf("Expected 0 files, got %d", len(result))
	}

	// Query that overlaps all files
	begin = makeInternalKey("000", 100, 1)
	end = makeInternalKey("999", 100, 1)
	result = f.v.OverlappingInputs(f.level, begin, end)

	if len(result) != 4 {
		t.Errorf("Expected 4 files, got %d", len(result))
	}
}

// TestOverlappingInputsNilBoundaries tests OverlappingInputs with nil boundaries.
func TestOverlappingInputsNilBoundaries(t *testing.T) {
	f := newFindFileTestFixture(t, 4)
	f.Add("100", "200")
	f.Add("300", "400")
	f.Add("500", "600")
	f.Add("700", "800")

	// Nil begin - should match from start
	end := makeInternalKey("350", 100, 1)
	result := f.v.OverlappingInputs(f.level, nil, end)
	if len(result) != 2 {
		t.Errorf("Expected 2 files with nil begin, got %d", len(result))
	}

	// Nil end - should match to end
	begin := makeInternalKey("550", 100, 1)
	result = f.v.OverlappingInputs(f.level, begin, nil)
	if len(result) != 2 {
		t.Errorf("Expected 2 files with nil end, got %d", len(result))
	}

	// Both nil - should match all
	result = f.v.OverlappingInputs(f.level, nil, nil)
	if len(result) != 4 {
		t.Errorf("Expected 4 files with both nil, got %d", len(result))
	}
}

// TestOverlappingInputsInvalidLevel tests invalid level handling.
func TestOverlappingInputsInvalidLevel(t *testing.T) {
	f := newFindFileTestFixture(t, 1)
	f.Add("100", "200")

	// Negative level
	result := f.v.OverlappingInputs(-1, nil, nil)
	if result != nil {
		t.Error("Expected nil for negative level")
	}

	// Too high level
	result = f.v.OverlappingInputs(MaxNumLevels, nil, nil)
	if result != nil {
		t.Error("Expected nil for too high level")
	}
}

// =============================================================================
// Version Bytes Tests
// =============================================================================

// TestVersionNumLevelBytesEmpty tests bytes counting with no files.
func TestVersionNumLevelBytesEmpty(t *testing.T) {
	opts := DefaultVersionSetOptions(t.TempDir())
	vset := NewVersionSet(opts)
	v := NewVersion(vset, 0)

	for level := range MaxNumLevels {
		if v.NumLevelBytes(level) != 0 {
			t.Errorf("Level %d: expected 0 bytes, got %d", level, v.NumLevelBytes(level))
		}
	}
}

// TestVersionNumLevelBytesAccurate tests accurate byte counting.
func TestVersionNumLevelBytesAccurate(t *testing.T) {
	opts := DefaultVersionSetOptions(t.TempDir())
	vset := NewVersionSet(opts)
	v := NewVersion(vset, 0)

	// Add files with known sizes
	v.files[0] = []*manifest.FileMetaData{
		{FD: manifest.NewFileDescriptor(1, 0, 1000)},
		{FD: manifest.NewFileDescriptor(2, 0, 2000)},
	}
	v.files[1] = []*manifest.FileMetaData{
		{FD: manifest.NewFileDescriptor(3, 0, 5000)},
	}
	v.files[2] = []*manifest.FileMetaData{
		{FD: manifest.NewFileDescriptor(4, 0, 10000)},
		{FD: manifest.NewFileDescriptor(5, 0, 20000)},
		{FD: manifest.NewFileDescriptor(6, 0, 30000)},
	}

	if v.NumLevelBytes(0) != 3000 {
		t.Errorf("Level 0: expected 3000, got %d", v.NumLevelBytes(0))
	}
	if v.NumLevelBytes(1) != 5000 {
		t.Errorf("Level 1: expected 5000, got %d", v.NumLevelBytes(1))
	}
	if v.NumLevelBytes(2) != 60000 {
		t.Errorf("Level 2: expected 60000, got %d", v.NumLevelBytes(2))
	}
}

// TestVersionNumLevelBytesInvalidLevel tests invalid level handling.
func TestVersionNumLevelBytesInvalidLevel(t *testing.T) {
	opts := DefaultVersionSetOptions(t.TempDir())
	vset := NewVersionSet(opts)
	v := NewVersion(vset, 0)

	if v.NumLevelBytes(-1) != 0 {
		t.Error("Expected 0 for negative level")
	}
	if v.NumLevelBytes(MaxNumLevels) != 0 {
		t.Error("Expected 0 for too high level")
	}
	if v.NumLevelBytes(100) != 0 {
		t.Error("Expected 0 for way too high level")
	}
}

// =============================================================================
// Version File Counting Tests
// =============================================================================

// TestVersionTotalFilesComputation tests total file counting.
func TestVersionTotalFilesComputation(t *testing.T) {
	opts := DefaultVersionSetOptions(t.TempDir())
	vset := NewVersionSet(opts)
	v := NewVersion(vset, 0)

	// Empty version
	if v.TotalFiles() != 0 {
		t.Error("Expected 0 total files for empty version")
	}

	// Add files
	v.files[0] = make([]*manifest.FileMetaData, 3)
	v.files[1] = make([]*manifest.FileMetaData, 5)
	v.files[2] = make([]*manifest.FileMetaData, 10)

	if v.TotalFiles() != 18 {
		t.Errorf("Expected 18 total files, got %d", v.TotalFiles())
	}
}

// TestVersionNumFilesPerLevel tests per-level file counting.
func TestVersionNumFilesPerLevel(t *testing.T) {
	opts := DefaultVersionSetOptions(t.TempDir())
	vset := NewVersionSet(opts)
	v := NewVersion(vset, 0)

	v.files[0] = make([]*manifest.FileMetaData, 3)
	v.files[3] = make([]*manifest.FileMetaData, 7)

	if v.NumFiles(0) != 3 {
		t.Errorf("Level 0: expected 3, got %d", v.NumFiles(0))
	}
	if v.NumFiles(1) != 0 {
		t.Errorf("Level 1: expected 0, got %d", v.NumFiles(1))
	}
	if v.NumFiles(3) != 7 {
		t.Errorf("Level 3: expected 7, got %d", v.NumFiles(3))
	}
	if v.NumFiles(-1) != 0 {
		t.Error("Expected 0 for negative level")
	}
	if v.NumFiles(MaxNumLevels) != 0 {
		t.Error("Expected 0 for too high level")
	}
}
