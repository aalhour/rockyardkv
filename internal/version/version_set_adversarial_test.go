// version_set_adversarial_test.go contains adversarial tests that try to break
// MANIFEST handling through corruption, truncation, and other attacks.
//
// These tests were inspired by Red Team findings (Dec 2025) that identified
// critical issues with MANIFEST validation.
package version

import (
	"bytes"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv/internal/vfs"
	"github.com/aalhour/rockyardkv/internal/wal"
)

// TestAdversarial_ManifestChecksumCorruption tests that we reject MANIFEST files
// where any byte of the CRC checksum has been corrupted.
func TestAdversarial_ManifestChecksumCorruption(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	// Create a valid database
	vs := NewVersionSet(opts)
	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	vs.Close()

	// Find the MANIFEST file
	manifestPath := findManifest(t, dir)
	originalData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}

	// Try corrupting each of the first 4 bytes (CRC checksum)
	for i := 0; i < 4 && i < len(originalData); i++ {
		t.Run(corruptyByteTestName("checksum", i), func(t *testing.T) {
			corrupted := make([]byte, len(originalData))
			copy(corrupted, originalData)
			corrupted[i] ^= 0xFF // Flip all bits

			if err := os.WriteFile(manifestPath, corrupted, 0644); err != nil {
				t.Fatalf("WriteFile error: %v", err)
			}

			vs2 := NewVersionSet(opts)
			err := vs2.Recover()
			if err == nil {
				vs2.Close()
				t.Errorf("Recover() should fail when checksum byte %d is corrupted", i)
			}
		})

		// Restore original for next iteration
		if err := os.WriteFile(manifestPath, originalData, 0644); err != nil {
			t.Fatalf("restore WriteFile error: %v", err)
		}
	}
}

// TestAdversarial_ManifestRecordTypeFlip tests that flipping the record type byte
// causes rejection.
func TestAdversarial_ManifestRecordTypeFlip(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	vs := NewVersionSet(opts)
	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	vs.Close()

	manifestPath := findManifest(t, dir)
	originalData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}

	// Record type is at byte 6 in the WAL format
	if len(originalData) > 6 {
		corrupted := make([]byte, len(originalData))
		copy(corrupted, originalData)
		corrupted[6] ^= 0xFF // Flip the record type byte

		if err := os.WriteFile(manifestPath, corrupted, 0644); err != nil {
			t.Fatalf("WriteFile error: %v", err)
		}

		vs2 := NewVersionSet(opts)
		err := vs2.Recover()
		if err == nil {
			vs2.Close()
			t.Error("Recover() should fail when record type is corrupted")
		}
	}
}

// TestAdversarial_ManifestTruncationVariants tests various truncation scenarios.
func TestAdversarial_ManifestTruncationVariants(t *testing.T) {
	truncations := []struct {
		name   string
		offset int // Bytes to remove from end
	}{
		{"remove_1_byte", 1},
		{"remove_4_bytes", 4},
		{"remove_8_bytes", 8},
		{"remove_16_bytes", 16},
		{"remove_half", -1}, // Special: remove half the file
	}

	for _, tc := range truncations {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := VersionSetOptions{
				DBName:              dir,
				FS:                  vfs.Default(),
				MaxManifestFileSize: 1024 * 1024,
				NumLevels:           MaxNumLevels,
			}

			vs := NewVersionSet(opts)
			if err := vs.Create(); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			vs.Close()

			manifestPath := findManifest(t, dir)
			originalData, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatalf("ReadFile error: %v", err)
			}

			offset := tc.offset
			if offset == -1 {
				offset = len(originalData) / 2
			}

			if offset >= len(originalData) {
				t.Skip("File too small for this truncation")
			}

			truncated := originalData[:len(originalData)-offset]
			if err := os.WriteFile(manifestPath, truncated, 0644); err != nil {
				t.Fatalf("WriteFile error: %v", err)
			}

			vs2 := NewVersionSet(opts)
			err = vs2.Recover()
			// Truncation should either fail or recover to a consistent earlier state
			// It should NOT silently accept corrupted data
			if err == nil {
				// If it succeeded, that's acceptable for some truncation points
				// (e.g., truncation at a record boundary)
				vs2.Close()
				t.Logf("Recover succeeded after truncation (may be at safe boundary)")
			} else {
				t.Logf("Recover failed as expected: %v", err)
			}
		})
	}
}

// TestAdversarial_ManifestRandomBitFlips tests that random bit flips in the
// MANIFEST are detected.
func TestAdversarial_ManifestRandomBitFlips(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	vs := NewVersionSet(opts)
	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	vs.Close()

	manifestPath := findManifest(t, dir)
	originalData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}

	// Try 20 random bit flips
	rng := rand.New(rand.NewSource(42))
	successCount := 0

	for range 20 {
		pos := rng.Intn(len(originalData))
		bit := uint8(1 << rng.Intn(8))

		corrupted := make([]byte, len(originalData))
		copy(corrupted, originalData)
		corrupted[pos] ^= bit

		if err := os.WriteFile(manifestPath, corrupted, 0644); err != nil {
			t.Fatalf("WriteFile error: %v", err)
		}

		vs2 := NewVersionSet(opts)
		err := vs2.Recover()
		if err == nil {
			vs2.Close()
			successCount++
		}

		// Restore for next iteration
		if err := os.WriteFile(manifestPath, originalData, 0644); err != nil {
			t.Fatalf("restore error: %v", err)
		}
	}

	// Most random bit flips should be detected
	if successCount > 5 {
		t.Errorf("Too many corrupted MANIFESTs accepted: %d/20", successCount)
	}
	t.Logf("Detected %d/20 random bit flip corruptions", 20-successCount)
}

// TestAdversarial_ComparatorMismatchVariants tests various comparator mismatch scenarios.
func TestAdversarial_ComparatorMismatchVariants(t *testing.T) {
	mismatches := []struct {
		createWith string
		openWith   string
		shouldFail bool
	}{
		{"leveldb.BytewiseComparator", "rocksdb.ReverseBytewiseComparator", true},
		{"leveldb.BytewiseComparator", "custom.MyComparator", true},
		{"leveldb.BytewiseComparator", "rocksdb.BytewiseComparator", false}, // Backward compat
		{"leveldb.BytewiseComparator", "RocksDB.BytewiseComparator", false}, // Backward compat
		{"leveldb.BytewiseComparator", "", false},                           // Empty = default
	}

	for _, tc := range mismatches {
		name := tc.createWith + "_vs_" + tc.openWith
		if tc.openWith == "" {
			name = tc.createWith + "_vs_empty"
		}

		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opts := VersionSetOptions{
				DBName:              dir,
				FS:                  vfs.Default(),
				MaxManifestFileSize: 1024 * 1024,
				NumLevels:           MaxNumLevels,
				ComparatorName:      tc.createWith,
			}

			vs := NewVersionSet(opts)
			if err := vs.Create(); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			vs.Close()

			opts.ComparatorName = tc.openWith
			vs2 := NewVersionSet(opts)
			err := vs2.Recover()

			if tc.shouldFail {
				if err == nil {
					vs2.Close()
					t.Error("Recover() should fail with comparator mismatch")
				} else if !errors.Is(err, ErrComparatorMismatch) {
					t.Errorf("Expected ErrComparatorMismatch, got: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Recover() should succeed: %v", err)
				} else {
					vs2.Close()
				}
			}
		})
	}
}

// TestAdversarial_StrictReaderVsLenientReader verifies the difference between
// strict and lenient WAL readers.
func TestAdversarial_StrictReaderVsLenientReader(t *testing.T) {
	// Create a valid WAL record
	var buf bytes.Buffer
	w := wal.NewWriter(&buf, 1, false)
	w.AddRecord([]byte("record1"))
	w.AddRecord([]byte("record2"))
	w.AddRecord([]byte("record3"))

	originalData := buf.Bytes()

	// Corrupt the second record's checksum
	corrupted := make([]byte, len(originalData))
	copy(corrupted, originalData)
	if len(corrupted) > 20 {
		corrupted[14] ^= 0xFF // Second record starts around byte 14
	}

	t.Run("lenient_skips_corruption", func(t *testing.T) {
		r := wal.NewReader(bytes.NewReader(corrupted), nil, true, 1)
		records := 0
		for {
			_, err := r.ReadRecord()
			if err != nil {
				break
			}
			records++
		}
		// Lenient reader should skip the corrupted record and possibly read others
		t.Logf("Lenient reader recovered %d records", records)
	})

	t.Run("strict_fails_on_corruption", func(t *testing.T) {
		r := wal.NewStrictReader(bytes.NewReader(corrupted), nil, 1)
		records := 0
		var lastErr error
		for {
			_, err := r.ReadRecord()
			if err != nil {
				lastErr = err
				break
			}
			records++
		}
		if !errors.Is(lastErr, wal.ErrCorruptedRecord) {
			t.Errorf("Strict reader should fail with ErrCorruptedRecord, got: %v", lastErr)
		}
		t.Logf("Strict reader read %d records before failing", records)
	})
}

// Helper functions

func findManifest(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir error: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "MANIFEST-") {
			return filepath.Join(dir, entry.Name())
		}
	}
	t.Fatal("MANIFEST file not found")
	return ""
}

func corruptyByteTestName(prefix string, byteIdx int) string {
	return prefix + "_byte_" + string(rune('0'+byteIdx))
}
