package compaction

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/version"
)

// makeTestFileMetaData creates a FileMetaData for testing.
func makeTestFileMetaData(fileNum uint64, fileSize uint64, smallest, largest []byte) *manifest.FileMetaData {
	meta := manifest.NewFileMetaData()
	meta.FD = manifest.NewFileDescriptor(fileNum, 0, fileSize)
	meta.Smallest = smallest
	meta.Largest = largest
	return meta
}

// TestLeveledCompactionPickerNeedsCompactionEmpty tests with no files.
func TestLeveledCompactionPickerNeedsCompactionEmpty(t *testing.T) {
	picker := DefaultLeveledCompactionPicker()
	v := version.NewVersion(nil, 1)

	if picker.NeedsCompaction(v) {
		t.Error("Empty version should not need compaction")
	}
}

// TestLeveledCompactionPickerNeedsCompactionL0Trigger tests L0 file count trigger.
func TestLeveledCompactionPickerNeedsCompactionL0Trigger(t *testing.T) {
	picker := DefaultLeveledCompactionPicker()
	picker.L0CompactionTrigger = 4

	// Build a version with L0 files
	opts := version.VersionSetOptions{}
	vset := version.NewVersionSet(opts)
	v := version.NewVersion(vset, 1)

	// Add 3 files - below trigger
	edit := manifest.NewVersionEdit()
	for i := range 3 {
		meta := makeTestFileMetaData(uint64(i+1), 1000, []byte("a"), []byte("z"))
		edit.AddFile(0, meta)
	}

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	if picker.NeedsCompaction(v) {
		t.Error("3 L0 files should not trigger compaction (trigger=4)")
	}

	// Add 1 more file - at trigger
	edit2 := manifest.NewVersionEdit()
	meta4 := makeTestFileMetaData(4, 1000, []byte("a"), []byte("z"))
	edit2.AddFile(0, meta4)

	builder2 := version.NewBuilder(vset, v)
	if err := builder2.Apply(edit2); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder2.SaveTo(vset)

	if !picker.NeedsCompaction(v) {
		t.Error("4 L0 files should trigger compaction (trigger=4)")
	}
}

// TestLeveledCompactionPickerPickL0Compaction tests picking L0 compaction.
func TestLeveledCompactionPickerPickL0Compaction(t *testing.T) {
	picker := DefaultLeveledCompactionPicker()
	picker.L0CompactionTrigger = 2

	opts := version.VersionSetOptions{}
	vset := version.NewVersionSet(opts)
	v := version.NewVersion(vset, 1)

	// Add L0 files
	edit := manifest.NewVersionEdit()
	meta1 := makeTestFileMetaData(1, 1000, []byte("a"), []byte("m"))
	meta2 := makeTestFileMetaData(2, 1000, []byte("n"), []byte("z"))
	edit.AddFile(0, meta1)
	edit.AddFile(0, meta2)

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	c := picker.PickCompaction(v)
	if c == nil {
		t.Fatal("Expected compaction to be picked")
	}

	if c.StartLevel() != 0 {
		t.Errorf("Start level = %d, want 0", c.StartLevel())
	}
	if c.OutputLevel != 1 {
		t.Errorf("Output level = %d, want 1", c.OutputLevel)
	}
	if len(c.Inputs) == 0 {
		t.Error("Expected input files")
	}
	if c.Reason != CompactionReasonLevelL0FileNumTrigger {
		t.Errorf("Reason = %v, want L0FileNumTrigger", c.Reason)
	}
}

// TestLeveledCompactionPickerScoreCalculation tests score calculation.
func TestLeveledCompactionPickerScoreCalculation(t *testing.T) {
	picker := DefaultLeveledCompactionPicker()
	picker.L0CompactionTrigger = 4
	picker.MaxBytesForLevelBase = 100 * 1024 * 1024 // 100MB

	opts := version.VersionSetOptions{}
	vset := version.NewVersionSet(opts)
	v := version.NewVersion(vset, 1)

	// L0 score = numFiles / trigger
	// L1 score = size / maxBytesBase

	// Add 2 L0 files (score = 0.5)
	edit := manifest.NewVersionEdit()
	meta1 := makeTestFileMetaData(1, 1000, []byte("a"), []byte("z"))
	meta2 := makeTestFileMetaData(2, 1000, []byte("a"), []byte("z"))
	edit.AddFile(0, meta1)
	edit.AddFile(0, meta2)

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	score := picker.computeScore(v, 0)
	if score != 0.5 {
		t.Errorf("L0 score = %f, want 0.5", score)
	}

	// Add L1 files with total 50MB (score = 0.5)
	edit2 := manifest.NewVersionEdit()
	meta10 := makeTestFileMetaData(10, 50*1024*1024, []byte("a"), []byte("z"))
	edit2.AddFile(1, meta10)

	builder2 := version.NewBuilder(vset, v)
	if err := builder2.Apply(edit2); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder2.SaveTo(vset)

	score1 := picker.computeScore(v, 1)
	if score1 != 0.5 {
		t.Errorf("L1 score = %f, want 0.5", score1)
	}
}

// TestCompactionReasonString tests the reason string conversion.
func TestCompactionReasonString(t *testing.T) {
	tests := []struct {
		reason CompactionReason
		want   string
	}{
		{CompactionReasonUnknown, "Unknown"},
		{CompactionReasonLevelL0FileNumTrigger, "L0 file count"},
		{CompactionReasonLevelMaxLevelSize, "Level size"},
		{CompactionReasonManualCompaction, "Manual"},
		{CompactionReasonFlush, "Flush"},
	}

	for _, tt := range tests {
		if got := tt.reason.String(); got != tt.want {
			t.Errorf("Reason %v String() = %q, want %q", tt.reason, got, tt.want)
		}
	}
}

// TestCompactionInputDeletions tests adding input deletions to edit.
func TestCompactionInputDeletions(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("a"), []byte("b")),
			makeTestFileMetaData(2, 1000, []byte("c"), []byte("d")),
		}},
		{Level: 1, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(10, 5000, []byte("a"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 1)
	c.AddInputDeletions()

	// Check that all input files are marked for deletion
	deletedFiles := c.DeletedFiles()
	if len(deletedFiles) != 3 {
		t.Errorf("Expected 3 deleted files, got %d", len(deletedFiles))
	}
}

// TestCompactionKeyRange tests key range computation.
func TestCompactionKeyRange(t *testing.T) {
	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(1, 1000, []byte("c"), []byte("e")),
			makeTestFileMetaData(2, 1000, []byte("a"), []byte("b")),
		}},
		{Level: 1, Files: []*manifest.FileMetaData{
			makeTestFileMetaData(10, 5000, []byte("d"), []byte("z")),
		}},
	}

	c := NewCompaction(inputs, 1)

	// Smallest should be "a", largest should be "z"
	if string(c.SmallestKey) != "a" {
		t.Errorf("SmallestKey = %q, want 'a'", c.SmallestKey)
	}
	if string(c.LargestKey) != "z" {
		t.Errorf("LargestKey = %q, want 'z'", c.LargestKey)
	}
}

// TestLeveledCompactionPickerPickLevelCompaction tests picking non-L0 level compaction.
func TestLeveledCompactionPickerPickLevelCompaction(t *testing.T) {
	picker := DefaultLeveledCompactionPicker()
	picker.L0CompactionTrigger = 100   // Disable L0 trigger
	picker.MaxBytesForLevelBase = 1000 // Small base to trigger L1 compaction

	opts := version.VersionSetOptions{}
	vset := version.NewVersionSet(opts)
	v := version.NewVersion(vset, 1)

	// Add L1 files that exceed the threshold
	edit := manifest.NewVersionEdit()
	meta1 := makeTestFileMetaData(10, 2000, []byte("a"), []byte("m"))
	meta2 := makeTestFileMetaData(11, 3000, []byte("n"), []byte("z"))
	edit.AddFile(1, meta1)
	edit.AddFile(1, meta2)

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	// Verify L1 needs compaction (size exceeds base)
	if !picker.NeedsCompaction(v) {
		t.Fatal("L1 should trigger compaction")
	}

	c := picker.PickCompaction(v)
	if c == nil {
		t.Fatal("Expected compaction to be picked")
	}

	if c.StartLevel() != 1 {
		t.Errorf("Start level = %d, want 1", c.StartLevel())
	}
	if c.OutputLevel != 2 {
		t.Errorf("Output level = %d, want 2", c.OutputLevel)
	}
	if len(c.Inputs) == 0 {
		t.Error("Expected input files")
	}
}

// TestLeveledCompactionPickerSkipsCompactingFiles tests that files being compacted are skipped.
func TestLeveledCompactionPickerSkipsCompactingFiles(t *testing.T) {
	picker := DefaultLeveledCompactionPicker()
	picker.L0CompactionTrigger = 100 // Disable L0 trigger
	picker.MaxBytesForLevelBase = 1000

	opts := version.VersionSetOptions{}
	vset := version.NewVersionSet(opts)
	v := version.NewVersion(vset, 1)

	// Add L1 files, one already being compacted
	edit := manifest.NewVersionEdit()
	meta1 := makeTestFileMetaData(10, 5000, []byte("a"), []byte("m"))
	meta1.BeingCompacted = true // This file is already being compacted
	meta2 := makeTestFileMetaData(11, 3000, []byte("n"), []byte("z"))
	edit.AddFile(1, meta1)
	edit.AddFile(1, meta2)

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	c := picker.PickCompaction(v)
	if c == nil {
		t.Fatal("Expected compaction to be picked")
	}

	// Should pick meta2 (not being compacted), not meta1
	inputFiles := c.Inputs[0].Files
	for _, f := range inputFiles {
		if f.FD.GetNumber() == 10 {
			t.Error("Should not pick file 10 which is being compacted")
		}
	}
}

// TestLeveledCompactionPickerNoFilesAvailable tests when all files are being compacted.
func TestLeveledCompactionPickerNoFilesAvailable(t *testing.T) {
	picker := DefaultLeveledCompactionPicker()
	picker.L0CompactionTrigger = 100 // Disable L0 trigger
	picker.MaxBytesForLevelBase = 1000

	opts := version.VersionSetOptions{}
	vset := version.NewVersionSet(opts)
	v := version.NewVersion(vset, 1)

	// Add L1 files, all being compacted
	edit := manifest.NewVersionEdit()
	meta1 := makeTestFileMetaData(10, 5000, []byte("a"), []byte("z"))
	meta1.BeingCompacted = true
	edit.AddFile(1, meta1)

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	c := picker.PickCompaction(v)
	// Should return nil since no files are available
	if c != nil {
		t.Error("Expected nil compaction when all files are being compacted")
	}
}

// TestLeveledCompactionPickerMaxBytesMultiplier tests level size multiplier.
func TestLeveledCompactionPickerMaxBytesMultiplier(t *testing.T) {
	picker := DefaultLeveledCompactionPicker()
	picker.MaxBytesForLevelBase = 100 * 1024 * 1024 // 100 MB
	picker.MaxBytesForLevelMulti = 10

	// L1 max = 100MB
	// L2 max = 100MB * 10 = 1GB
	// L3 max = 100MB * 10 * 10 = 10GB

	opts := version.VersionSetOptions{}
	vset := version.NewVersionSet(opts)
	v := version.NewVersion(vset, 1)

	// Add files to L2 that are under the threshold (< 1GB)
	edit := manifest.NewVersionEdit()
	meta := makeTestFileMetaData(20, 500*1024*1024, []byte("a"), []byte("z")) // 500MB
	edit.AddFile(2, meta)

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	score := picker.computeScore(v, 2)
	expectedScore := float64(500*1024*1024) / float64(100*1024*1024*10) // 0.5
	if score != expectedScore {
		t.Errorf("L2 score = %f, want %f", score, expectedScore)
	}
}

// TestTargetFileSizeForLevel tests target file size calculation.
func TestTargetFileSizeForLevel(t *testing.T) {
	picker := DefaultLeveledCompactionPicker()
	picker.TargetFileSizeBase = 64 * 1024 * 1024 // 64 MB
	picker.TargetFileSizeMulti = 2

	// The function multiplies by TargetFileSizeMulti for each level
	// level 0: base (loop runs 0 times)
	// level 1: base * 2 (loop runs 1 time)
	// level 2: base * 2 * 2 = base * 4 (loop runs 2 times)
	tests := []struct {
		level    int
		expected uint64
	}{
		{0, 64 * 1024 * 1024},   // L0: base
		{1, 128 * 1024 * 1024},  // L1: base * 2
		{2, 256 * 1024 * 1024},  // L2: base * 4
		{3, 512 * 1024 * 1024},  // L3: base * 8
		{4, 1024 * 1024 * 1024}, // L4: base * 16
	}

	for _, tt := range tests {
		got := picker.targetFileSizeForLevel(tt.level)
		if got != tt.expected {
			t.Errorf("targetFileSizeForLevel(%d) = %d, want %d", tt.level, got, tt.expected)
		}
	}
}

// TestCompactionMarkFilesBeingCompactedPicker tests marking files as being compacted via picker.
func TestCompactionMarkFilesBeingCompactedPicker(t *testing.T) {
	meta1 := makeTestFileMetaData(1, 1000, []byte("a"), []byte("b"))
	meta2 := makeTestFileMetaData(2, 1000, []byte("c"), []byte("d"))

	inputs := []*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{meta1, meta2}},
	}

	c := NewCompaction(inputs, 1)

	// Initially not being compacted
	if meta1.BeingCompacted || meta2.BeingCompacted {
		t.Error("Files should not be marked as compacting initially")
	}

	// Mark as being compacted
	c.MarkFilesBeingCompacted(true)
	if !meta1.BeingCompacted || !meta2.BeingCompacted {
		t.Error("Files should be marked as compacting")
	}

	// Unmark
	c.MarkFilesBeingCompacted(false)
	if meta1.BeingCompacted || meta2.BeingCompacted {
		t.Error("Files should not be marked as compacting after unmark")
	}
}

// =============================================================================
// Universal Compaction Picker Tests
// Reference: RocksDB v10.7.5 db/compaction/compaction_picker_universal.cc
// =============================================================================

// TestUniversalCompactionPickerNeedsCompactionEmpty tests with no files.
func TestUniversalCompactionPickerNeedsCompactionEmpty(t *testing.T) {
	picker := NewUniversalCompactionPicker(nil)
	v := version.NewVersion(nil, 1)

	if picker.NeedsCompaction(v) {
		t.Error("Empty version should not need compaction")
	}
}

// TestUniversalCompactionPickerNeedsCompactionBelowMinMergeWidth tests below minimum.
func TestUniversalCompactionPickerNeedsCompactionBelowMinMergeWidth(t *testing.T) {
	opts := DefaultUniversalCompactionOptions()
	opts.MinMergeWidth = 3
	picker := NewUniversalCompactionPicker(opts)

	// Create version with only 2 L0 files
	vsetOpts := version.VersionSetOptions{}
	vset := version.NewVersionSet(vsetOpts)
	v := version.NewVersion(vset, 1)

	edit := manifest.NewVersionEdit()
	for i := range 2 {
		meta := makeTestFileMetaData(uint64(i+1), 1000, []byte("a"), []byte("z"))
		meta.FD.SmallestSeqno = manifest.SequenceNumber(i + 1)
		meta.FD.LargestSeqno = manifest.SequenceNumber(i + 1)
		edit.AddFile(0, meta)
	}

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	if picker.NeedsCompaction(v) {
		t.Error("Should not need compaction with only 2 files and MinMergeWidth=3")
	}
}

// TestUniversalCompactionPickerSizeAmplification tests size amplification trigger.
func TestUniversalCompactionPickerSizeAmplification(t *testing.T) {
	opts := DefaultUniversalCompactionOptions()
	opts.MaxSizeAmplificationPercent = 200 // 2x
	opts.MinMergeWidth = 2
	picker := NewUniversalCompactionPicker(opts)

	vsetOpts := version.VersionSetOptions{}
	vset := version.NewVersionSet(vsetOpts)
	v := version.NewVersion(vset, 1)

	edit := manifest.NewVersionEdit()

	// File 1: newest, small (1000 bytes)
	meta1 := makeTestFileMetaData(1, 1000, []byte("a"), []byte("m"))
	meta1.FD.SmallestSeqno = 100
	meta1.FD.LargestSeqno = 100
	edit.AddFile(0, meta1)

	// File 2: older, large (5000 bytes) - this creates 500% amplification
	meta2 := makeTestFileMetaData(2, 5000, []byte("n"), []byte("z"))
	meta2.FD.SmallestSeqno = 50
	meta2.FD.LargestSeqno = 50
	edit.AddFile(0, meta2)

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	// 5000/1000 * 100 = 500%, which exceeds 200%
	if !picker.NeedsCompaction(v) {
		t.Error("Should need compaction due to size amplification > 200%")
	}

	// Verify it picks compaction
	c := picker.PickCompaction(v)
	if c == nil {
		t.Fatal("PickCompaction returned nil")
	}
	if c.Reason != CompactionReasonUniversalSizeAmplification {
		t.Errorf("Reason = %v, want UniversalSizeAmplification", c.Reason)
	}
}

// TestUniversalCompactionPickerSizeRatio tests size ratio trigger.
func TestUniversalCompactionPickerSizeRatio(t *testing.T) {
	opts := DefaultUniversalCompactionOptions()
	opts.SizeRatio = 10 // Files within 10% size are compacted
	opts.MinMergeWidth = 2
	opts.MaxSizeAmplificationPercent = 1000 // High to avoid triggering
	picker := NewUniversalCompactionPicker(opts)

	vsetOpts := version.VersionSetOptions{}
	vset := version.NewVersionSet(vsetOpts)
	v := version.NewVersion(vset, 1)

	edit := manifest.NewVersionEdit()

	// Create 3 files of similar size (within 10%)
	// File 1: 1000 bytes
	meta1 := makeTestFileMetaData(1, 1000, []byte("a"), []byte("d"))
	meta1.FD.SmallestSeqno = 100
	meta1.FD.LargestSeqno = 100
	edit.AddFile(0, meta1)

	// File 2: 1050 bytes (5% larger)
	meta2 := makeTestFileMetaData(2, 1050, []byte("e"), []byte("h"))
	meta2.FD.SmallestSeqno = 50
	meta2.FD.LargestSeqno = 50
	edit.AddFile(0, meta2)

	// File 3: 1100 bytes (5% larger than meta2)
	meta3 := makeTestFileMetaData(3, 1100, []byte("i"), []byte("z"))
	meta3.FD.SmallestSeqno = 25
	meta3.FD.LargestSeqno = 25
	edit.AddFile(0, meta3)

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	if !picker.NeedsCompaction(v) {
		t.Error("Should need compaction due to size ratio trigger")
	}

	c := picker.PickCompaction(v)
	if c == nil {
		t.Fatal("PickCompaction returned nil")
	}
	if c.Reason != CompactionReasonUniversalSizeRatio {
		t.Errorf("Reason = %v, want UniversalSizeRatio", c.Reason)
	}
}

// TestUniversalCompactionPickerSkipsCompactingFiles tests that compacting files are skipped.
func TestUniversalCompactionPickerSkipsCompactingFiles(t *testing.T) {
	opts := DefaultUniversalCompactionOptions()
	opts.MinMergeWidth = 2
	picker := NewUniversalCompactionPicker(opts)

	vsetOpts := version.VersionSetOptions{}
	vset := version.NewVersionSet(vsetOpts)
	v := version.NewVersion(vset, 1)

	edit := manifest.NewVersionEdit()

	// Create 2 files
	meta1 := makeTestFileMetaData(1, 1000, []byte("a"), []byte("m"))
	meta1.FD.SmallestSeqno = 100
	meta1.FD.LargestSeqno = 100
	edit.AddFile(0, meta1)

	meta2 := makeTestFileMetaData(2, 5000, []byte("n"), []byte("z"))
	meta2.FD.SmallestSeqno = 50
	meta2.FD.LargestSeqno = 50
	meta2.BeingCompacted = true // Mark as being compacted
	edit.AddFile(0, meta2)

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	// Only 1 file available, less than MinMergeWidth
	if picker.NeedsCompaction(v) {
		t.Error("Should not need compaction when files are being compacted")
	}
}

// TestUniversalCompactionPickerDefaultOptions tests default options.
func TestUniversalCompactionPickerDefaultOptions(t *testing.T) {
	opts := DefaultUniversalCompactionOptions()

	if opts.SizeRatio != 1 {
		t.Errorf("SizeRatio = %d, want 1", opts.SizeRatio)
	}
	if opts.MinMergeWidth != 2 {
		t.Errorf("MinMergeWidth = %d, want 2", opts.MinMergeWidth)
	}
	if opts.MaxSizeAmplificationPercent != 200 {
		t.Errorf("MaxSizeAmplificationPercent = %d, want 200", opts.MaxSizeAmplificationPercent)
	}
	if opts.StopStyle != StopStyleTotalSize {
		t.Errorf("StopStyle = %v, want StopStyleTotalSize", opts.StopStyle)
	}
	if opts.AllowTrivialMove {
		t.Error("AllowTrivialMove should be false by default")
	}
}

// TestUniversalCompactionOutputLevel tests that output goes to correct level.
func TestUniversalCompactionOutputLevel(t *testing.T) {
	opts := DefaultUniversalCompactionOptions()
	opts.MaxSizeAmplificationPercent = 100 // Low to trigger compaction
	picker := NewUniversalCompactionPicker(opts)

	vsetOpts := version.VersionSetOptions{}
	vset := version.NewVersionSet(vsetOpts)
	v := version.NewVersion(vset, 1)

	edit := manifest.NewVersionEdit()

	// Create files only in L0
	for i := range 3 {
		meta := makeTestFileMetaData(uint64(i+1), 1000, []byte("a"), []byte("z"))
		meta.FD.SmallestSeqno = manifest.SequenceNumber(100 - i*10)
		meta.FD.LargestSeqno = manifest.SequenceNumber(100 - i*10)
		edit.AddFile(0, meta)
	}

	builder := version.NewBuilder(vset, v)
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	v = builder.SaveTo(vset)

	c := picker.PickCompaction(v)
	if c == nil {
		t.Fatal("PickCompaction returned nil")
	}

	// With only L0 files, output should go to L1
	if c.OutputLevel != 1 {
		t.Errorf("OutputLevel = %d, want 1", c.OutputLevel)
	}
}
