// picker.go implements CompactionPicker for selecting files to compact.
//
// CompactionPicker is an abstract interface for selecting compaction targets.
// Different compaction styles (Level, Universal, FIFO) implement this interface.
//
// Reference: RocksDB v10.7.5
//   - db/compaction/compaction_picker.h
//   - db/compaction/compaction_picker.cc
package compaction

import (
	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/version"
)

// CompactionPicker is responsible for selecting files for compaction.
type CompactionPicker interface {
	// NeedsCompaction returns true if compaction is needed.
	NeedsCompaction(v *version.Version) bool

	// PickCompaction selects files for the next compaction.
	// Returns nil if no compaction is needed.
	PickCompaction(v *version.Version) *Compaction
}

// LeveledCompactionPicker implements leveled compaction strategy.
// This is the default RocksDB compaction style.
type LeveledCompactionPicker struct {
	// Options
	NumLevels             int
	L0CompactionTrigger   int     // Number of L0 files to trigger compaction
	L0StopWritesTrigger   int     // Number of L0 files to stall writes
	MaxBytesForLevelBase  uint64  // Target size for L1
	MaxBytesForLevelMulti float64 // Multiplier for each subsequent level
	TargetFileSizeBase    uint64  // Target file size for L1
	TargetFileSizeMulti   float64 // Multiplier for file size at each level
}

// DefaultLeveledCompactionPicker returns a picker with default settings.
func DefaultLeveledCompactionPicker() *LeveledCompactionPicker {
	return &LeveledCompactionPicker{
		NumLevels:             7,
		L0CompactionTrigger:   4,
		L0StopWritesTrigger:   20,
		MaxBytesForLevelBase:  256 * 1024 * 1024, // 256MB
		MaxBytesForLevelMulti: 10.0,
		TargetFileSizeBase:    64 * 1024 * 1024, // 64MB
		TargetFileSizeMulti:   1.0,
	}
}

// NeedsCompaction returns true if compaction should be triggered.
func (p *LeveledCompactionPicker) NeedsCompaction(v *version.Version) bool {
	// Check L0 file count
	l0Files := v.NumFiles(0)
	if l0Files >= p.L0CompactionTrigger {
		return true
	}

	// Check each level's size
	for level := 1; level < p.NumLevels-1; level++ {
		if p.computeScore(v, level) >= 1.0 {
			return true
		}
	}

	return false
}

// PickCompaction selects the next compaction to perform.
func (p *LeveledCompactionPicker) PickCompaction(v *version.Version) *Compaction {
	// Priority 1: L0 compaction if too many files
	l0Files := v.NumFiles(0)
	if l0Files >= p.L0CompactionTrigger {
		return p.pickL0Compaction(v)
	}

	// Priority 2: Find the level with highest score
	bestLevel := -1
	bestScore := 0.0

	for level := 1; level < p.NumLevels-1; level++ {
		score := p.computeScore(v, level)
		if score > bestScore {
			bestScore = score
			bestLevel = level
		}
	}

	if bestLevel >= 0 && bestScore >= 1.0 {
		return p.pickLevelCompaction(v, bestLevel, bestScore)
	}

	return nil
}

// computeScore calculates the compaction score for a level.
// Score >= 1.0 means compaction is needed.
func (p *LeveledCompactionPicker) computeScore(v *version.Version, level int) float64 {
	if level == 0 {
		// For L0, score is based on file count
		return float64(v.NumFiles(0)) / float64(p.L0CompactionTrigger)
	}

	// For other levels, score is based on size
	levelSize := v.NumLevelBytes(level)
	targetSize := p.targetSizeForLevel(level)

	if targetSize == 0 {
		return 0
	}

	return float64(levelSize) / float64(targetSize)
}

// targetSizeForLevel returns the target size for a level.
func (p *LeveledCompactionPicker) targetSizeForLevel(level int) uint64 {
	if level == 0 {
		return 0 // L0 uses file count, not size
	}

	size := p.MaxBytesForLevelBase
	for i := 1; i < level; i++ {
		size = uint64(float64(size) * p.MaxBytesForLevelMulti)
	}
	return size
}

// targetFileSizeForLevel returns the target file size for a level.
func (p *LeveledCompactionPicker) targetFileSizeForLevel(level int) uint64 {
	size := p.TargetFileSizeBase
	for range level {
		size = uint64(float64(size) * p.TargetFileSizeMulti)
	}
	return size
}

// pickL0Compaction picks a compaction from L0 to L1.
func (p *LeveledCompactionPicker) pickL0Compaction(v *version.Version) *Compaction {
	l0Files := v.Files(0)
	if len(l0Files) == 0 {
		return nil
	}

	// Filter out files that are being compacted
	var availableFiles []*manifest.FileMetaData
	for _, f := range l0Files {
		if !f.BeingCompacted {
			availableFiles = append(availableFiles, f)
		}
	}
	if len(availableFiles) == 0 {
		return nil
	}

	// Start with available L0 files (they may overlap)
	l0Input := &CompactionInputFiles{
		Level: 0,
		Files: make([]*manifest.FileMetaData, len(availableFiles)),
	}
	copy(l0Input.Files, availableFiles)

	// Find the key range covered by L0 files
	var smallest, largest []byte
	for _, f := range availableFiles {
		if smallest == nil || compareKeys(f.Smallest, smallest) < 0 {
			smallest = f.Smallest
		}
		if largest == nil || compareKeys(f.Largest, largest) > 0 {
			largest = f.Largest
		}
	}

	// Find overlapping files in L1 that are not being compacted
	l1Files := v.OverlappingInputs(1, smallest, largest)
	var l1Available []*manifest.FileMetaData
	for _, f := range l1Files {
		if !f.BeingCompacted {
			l1Available = append(l1Available, f)
		}
	}
	l1Input := &CompactionInputFiles{
		Level: 1,
		Files: l1Available,
	}

	inputs := []*CompactionInputFiles{l0Input}
	if len(l1Input.Files) > 0 {
		inputs = append(inputs, l1Input)
	}

	c := NewCompaction(inputs, 1)
	c.Reason = CompactionReasonLevelL0FileNumTrigger
	c.Score = float64(len(l0Files)) / float64(p.L0CompactionTrigger)
	c.MaxOutputFileSize = p.targetFileSizeForLevel(1)

	return c
}

// pickLevelCompaction picks a compaction from level to level+1.
func (p *LeveledCompactionPicker) pickLevelCompaction(v *version.Version, level int, score float64) *Compaction {
	files := v.Files(level)
	if len(files) == 0 {
		return nil
	}

	// Pick the file with the largest size that is not being compacted (simple heuristic)
	var picked *manifest.FileMetaData
	var maxSize uint64
	for _, f := range files {
		if f.BeingCompacted {
			continue
		}
		if f.FD.FileSize > maxSize {
			maxSize = f.FD.FileSize
			picked = f
		}
	}

	if picked == nil {
		return nil
	}

	levelInput := &CompactionInputFiles{
		Level: level,
		Files: []*manifest.FileMetaData{picked},
	}

	// Find overlapping files in level+1 that are not being compacted
	nextLevel := level + 1
	nextLevelFiles := v.OverlappingInputs(nextLevel, picked.Smallest, picked.Largest)
	var nextLevelAvailable []*manifest.FileMetaData
	for _, f := range nextLevelFiles {
		if !f.BeingCompacted {
			nextLevelAvailable = append(nextLevelAvailable, f)
		}
	}
	nextLevelInput := &CompactionInputFiles{
		Level: nextLevel,
		Files: nextLevelAvailable,
	}

	inputs := []*CompactionInputFiles{levelInput}
	if len(nextLevelInput.Files) > 0 {
		inputs = append(inputs, nextLevelInput)
	}

	c := NewCompaction(inputs, nextLevel)
	c.Reason = CompactionReasonLevelMaxLevelSize
	c.Score = score
	c.MaxOutputFileSize = p.targetFileSizeForLevel(nextLevel)

	return c
}
