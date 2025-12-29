// version_set.go implements the VersionSet which manages all versions.
//
// VersionSet maintains the set of all versions and handles MANIFEST
// file operations. It provides thread-safe access to the current version.
//
// Reference: RocksDB v10.7.5
//   - db/version_set.h (VersionSet class)
//   - db/version_set.cc
//
// # Whitebox Testing Hooks
//
// This file contains whitebox testing hooks for crash testing (requires -tags crashtest).
// In production builds, these compile to no-ops with zero overhead.
// See docs/testing.md for usage.
package version

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/testutil"
	"github.com/aalhour/rockyardkv/internal/vfs"
	"github.com/aalhour/rockyardkv/internal/wal"
)

// Errors returned by VersionSet operations.
var (
	ErrNotFound          = errors.New("version: not found")
	ErrCorruption        = errors.New("version: corruption")
	ErrInvalidManifest   = errors.New("version: invalid manifest")
	ErrNoCurrentManifest = errors.New("version: no current manifest")
	ErrManifestTooLarge  = errors.New("version: manifest too large")
)

// VersionSetOptions configures the VersionSet.
type VersionSetOptions struct {
	// DBName is the database directory path.
	DBName string

	// FS is the filesystem to use.
	FS vfs.FS

	// MaxManifestFileSize is the maximum size of a MANIFEST file before rotation.
	MaxManifestFileSize uint64

	// NumLevels is the number of levels in the LSM tree.
	NumLevels int

	// ComparatorName is the name of the comparator used by the database.
	// This is validated against the comparator stored in the MANIFEST.
	// If empty, defaults to "leveldb.BytewiseComparator".
	ComparatorName string
}

// ErrComparatorMismatch indicates that the database was created with a different comparator.
var ErrComparatorMismatch = errors.New("version: comparator mismatch")

// DefaultVersionSetOptions returns default options.
func DefaultVersionSetOptions(dbname string) VersionSetOptions {
	return VersionSetOptions{
		DBName:              dbname,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024 * 1024, // 1GB
		NumLevels:           MaxNumLevels,
	}
}

// RecoveredColumnFamily holds information about a column family recovered from MANIFEST.
type RecoveredColumnFamily struct {
	ID   uint32
	Name string
}

// VersionSet manages the set of versions and the MANIFEST file.
type VersionSet struct {
	mu sync.Mutex

	// listMu protects the version linked list (prev/next pointers).
	// This is separate from mu to avoid deadlock when Unref() is called
	// while mu is held (e.g., from LogAndApply).
	listMu sync.Mutex

	opts VersionSetOptions

	// Current version (the newest)
	current *Version

	// Dummy head for version linked list
	dummyVersions Version

	// File numbers
	nextFileNumber        uint64
	manifestFileNumber    uint64
	pendingManifestNumber uint64 //nolint:unused // Reserved for manifest rotation
	lastSequence          uint64
	logNumber             uint64
	prevLogNumber         uint64

	// Version numbering (for debugging)
	currentVersionNumber uint64

	// MANIFEST writer
	manifestFile   vfs.WritableFile
	manifestWriter *wal.Writer

	// Database ID and session ID
	dbID        string //nolint:unused // Reserved for unique DB identification
	dbSessionID string //nolint:unused // Reserved for session tracking

	// Column family info recovered from MANIFEST
	recoveredCFs    []RecoveredColumnFamily
	maxColumnFamily uint32
}

// NewVersionSet creates a new VersionSet.
func NewVersionSet(opts VersionSetOptions) *VersionSet {
	vs := &VersionSet{
		opts:           opts,
		nextFileNumber: 2, // 1 is reserved for MANIFEST
	}

	// Initialize dummy versions linked list
	vs.dummyVersions.prev = &vs.dummyVersions
	vs.dummyVersions.next = &vs.dummyVersions

	return vs
}

// Current returns the current (newest) version.
// The caller should call Ref() on the returned version if they need to keep it.
func (vs *VersionSet) Current() *Version {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.current
}

// NextFileNumber allocates a new file number.
func (vs *VersionSet) NextFileNumber() uint64 {
	return atomic.AddUint64(&vs.nextFileNumber, 1) - 1
}

// NextVersionNumber allocates a new version number.
func (vs *VersionSet) NextVersionNumber() uint64 {
	return atomic.AddUint64(&vs.currentVersionNumber, 1)
}

// CurrentVersionNumber returns the current version number.
func (vs *VersionSet) CurrentVersionNumber() uint64 {
	return atomic.LoadUint64(&vs.currentVersionNumber)
}

// NumLiveVersions returns the number of live versions.
func (vs *VersionSet) NumLiveVersions() int {
	vs.listMu.Lock()
	defer vs.listMu.Unlock()

	count := 0
	for v := vs.dummyVersions.next; v != &vs.dummyVersions; v = v.next {
		count++
	}
	return count
}

// GetManifestFileNumber returns the current MANIFEST file number.
func (vs *VersionSet) GetManifestFileNumber() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.manifestFileNumber
}

// LastSequence returns the last sequence number.
func (vs *VersionSet) LastSequence() uint64 {
	return atomic.LoadUint64(&vs.lastSequence)
}

// SetLastSequence sets the last sequence number.
func (vs *VersionSet) SetLastSequence(seq uint64) {
	atomic.StoreUint64(&vs.lastSequence, seq)
}

// LogNumber returns the current log file number.
func (vs *VersionSet) LogNumber() uint64 {
	return vs.logNumber
}

// ManifestFileNumber returns the current manifest file number.
func (vs *VersionSet) ManifestFileNumber() uint64 {
	return vs.manifestFileNumber
}

// RecoveredColumnFamilies returns the column families recovered from MANIFEST.
// This should be called after Recover() to get the non-default CFs.
func (vs *VersionSet) RecoveredColumnFamilies() []RecoveredColumnFamily {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.recoveredCFs
}

// MaxColumnFamily returns the maximum column family ID seen in the MANIFEST.
func (vs *VersionSet) MaxColumnFamily() uint32 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.maxColumnFamily
}

// Recover reads the MANIFEST file and recovers the database state.
func (vs *VersionSet) Recover() error {
	// Whitebox [synctest]: barrier at Recover start
	_ = testutil.SP(testutil.SPVersionSetRecover)

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Read CURRENT file to find the current MANIFEST
	currentFile := filepath.Join(vs.opts.DBName, "CURRENT")
	data, err := os.ReadFile(currentFile)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNoCurrentManifest
		}
		return err
	}

	manifestName := strings.TrimSpace(string(data))
	if manifestName == "" {
		return ErrInvalidManifest
	}

	// Parse manifest file number from name (format: MANIFEST-NNNNNN)
	if !strings.HasPrefix(manifestName, "MANIFEST-") {
		return ErrInvalidManifest
	}
	numStr := manifestName[len("MANIFEST-"):]
	manifestNum, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return ErrInvalidManifest
	}

	// Read the MANIFEST file
	manifestPath := filepath.Join(vs.opts.DBName, manifestName)
	manifestFile, err := vs.opts.FS.Open(manifestPath)
	if err != nil {
		return err
	}
	defer func() { _ = manifestFile.Close() }()

	// Read all the data
	manifestData, err := io.ReadAll(manifestFile)
	if err != nil {
		return err
	}

	// Parse MANIFEST records with strict checksum validation.
	// Unlike WAL recovery which may tolerate some corruption modes,
	// MANIFEST corruption is always fatal - we cannot trust metadata.
	builder := NewBuilder(vs, nil)
	reader := wal.NewStrictReader(bytes.NewReader(manifestData), nil, manifestNum)

	hasComparator := false
	hasLogNumber := false
	hasNextFileNumber := false
	hasLastSequence := false
	// Track the maximum file number we see in MANIFEST edits so we can avoid reusing
	// file numbers after a crash, even if NextFileNumber was not persisted correctly.
	maxFileNumSeen := manifestNum

	// Track column families during recovery
	// Map from CF ID to name (nil means dropped)
	cfMap := make(map[uint32]string)

	for {
		record, err := reader.ReadRecord()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("manifest read error: %w", err)
		}

		var edit manifest.VersionEdit
		if err := edit.DecodeFrom(record); err != nil {
			return fmt.Errorf("manifest decode error: %w", err)
		}

		// Apply the edit
		if err := builder.Apply(&edit); err != nil {
			return err
		}

		// Track max file numbers referenced by this edit (NewFiles and log numbers).
		for _, nf := range edit.NewFiles {
			if num := nf.Meta.FD.GetNumber(); num > maxFileNumSeen {
				maxFileNumSeen = num
			}
		}
		if edit.HasLogNumber && edit.LogNumber > maxFileNumSeen {
			maxFileNumSeen = edit.LogNumber
		}
		if edit.HasPrevLogNumber && edit.PrevLogNumber > maxFileNumSeen {
			maxFileNumSeen = edit.PrevLogNumber
		}

		// Extract state from edit
		if edit.HasComparator {
			hasComparator = true
			// Validate comparator name matches the one we're using.
			// Allow "leveldb.BytewiseComparator" to match "rocksdb.BytewiseComparator" for backward compat.
			expectedName := vs.opts.ComparatorName
			if expectedName == "" {
				expectedName = "leveldb.BytewiseComparator"
			}
			if !comparatorNamesMatch(edit.Comparator, expectedName) {
				return fmt.Errorf("%w: database uses %q, but opening with %q",
					ErrComparatorMismatch, edit.Comparator, expectedName)
			}
		}
		if edit.HasLogNumber {
			hasLogNumber = true
			vs.logNumber = edit.LogNumber
		}
		if edit.HasPrevLogNumber {
			vs.prevLogNumber = edit.PrevLogNumber
		}
		if edit.HasNextFileNumber {
			hasNextFileNumber = true
			atomic.StoreUint64(&vs.nextFileNumber, edit.NextFileNumber)
		}
		if edit.HasLastSequence {
			hasLastSequence = true
			atomic.StoreUint64(&vs.lastSequence, uint64(edit.LastSequence))
		}

		// Track column family operations
		if edit.HasMaxColumnFamily {
			vs.maxColumnFamily = edit.MaxColumnFamily
		}
		if edit.IsColumnFamilyAdd {
			cfID := edit.ColumnFamily
			if !edit.HasColumnFamily {
				cfID = 0 // Default CF
			}
			cfMap[cfID] = edit.ColumnFamilyName
		}
		if edit.IsColumnFamilyDrop {
			cfID := edit.ColumnFamily
			if !edit.HasColumnFamily {
				cfID = 0
			}
			delete(cfMap, cfID)
		}
	}

	// Build list of recovered column families (excluding default CF which has ID 0)
	vs.recoveredCFs = nil
	for id, name := range cfMap {
		if id != 0 { // Skip default CF
			vs.recoveredCFs = append(vs.recoveredCFs, RecoveredColumnFamily{ID: id, Name: name})
		}
	}

	// Verify we have required fields
	if !hasLogNumber {
		return fmt.Errorf("manifest missing log number")
	}
	// If NextFileNumber is missing (or stale), derive a safe value from what we saw.
	if !hasNextFileNumber {
		atomic.StoreUint64(&vs.nextFileNumber, maxFileNumSeen+1)
	}
	if !hasLastSequence {
		return fmt.Errorf("manifest missing last sequence")
	}
	_ = hasComparator // Optional

	// Ensure NextFileNumber is beyond any file number referenced by recovered state.
	// This prevents reuse/truncation of existing files after recovery.
	if n := atomic.LoadUint64(&vs.nextFileNumber); n <= maxFileNumSeen {
		atomic.StoreUint64(&vs.nextFileNumber, maxFileNumSeen+1)
	}

	// Create the recovered version
	vs.manifestFileNumber = manifestNum
	vs.current = builder.SaveTo(vs)
	vs.current.Ref()
	vs.appendVersion(vs.current)

	return nil
}

// LogAndApply logs a VersionEdit to the MANIFEST and applies it.
func (vs *VersionSet) LogAndApply(edit *manifest.VersionEdit) error {
	// Whitebox [synctest]: barrier at LogAndApply start
	_ = testutil.SP(testutil.SPVersionSetLogAndApply)

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Create new version by applying edit to current
	builder := NewBuilder(vs, vs.current)
	if err := builder.Apply(edit); err != nil {
		return err
	}
	newVersion := builder.SaveTo(vs)

	// Persist NextFileNumber with every edit so recovery never reuses file numbers.
	edit.HasNextFileNumber = true
	edit.NextFileNumber = atomic.LoadUint64(&vs.nextFileNumber)

	// Encode the edit
	encoded := edit.EncodeTo()

	// Write to MANIFEST
	// Track if we created a new MANIFEST so we can update CURRENT after sync.
	// Reference: RocksDB db/version_set.cc ProcessManifestWrites syncs MANIFEST
	// before calling SetCurrentFile to avoid crash window.
	newManifest := false
	if vs.manifestWriter == nil {
		// Create new MANIFEST file
		manifestNum := vs.NextFileNumber()
		manifestPath := vs.manifestFilePath(manifestNum)

		file, err := vs.opts.FS.Create(manifestPath)
		if err != nil {
			return err
		}

		vs.manifestFile = file
		vs.manifestWriter = wal.NewWriter(file, manifestNum, false /* not recyclable */)
		vs.manifestFileNumber = manifestNum
		newManifest = true

		// Write a snapshot of the current state
		snapshotEdit := vs.writeSnapshot()
		snapshotEncoded := snapshotEdit.EncodeTo()
		if _, err := vs.manifestWriter.AddRecord(snapshotEncoded); err != nil {
			return err
		}
	}

	// Whitebox [crashtest]: crash before MANIFEST write — tests partial manifest handling
	testutil.MaybeKill(testutil.KPManifestWrite0)

	// Write the edit
	if _, err := vs.manifestWriter.AddRecord(encoded); err != nil {
		return err
	}

	// Whitebox [crashtest]: crash before MANIFEST sync — tests unsynced manifest
	testutil.MaybeKill(testutil.KPManifestSync0)

	// Sync the manifest file BEFORE updating CURRENT
	if syncer, ok := vs.manifestFile.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			return err
		}
	}

	// Whitebox [crashtest]: crash after MANIFEST sync — CURRENT not yet updated
	testutil.MaybeKill(testutil.KPManifestSync1)

	// Update CURRENT file AFTER MANIFEST is synced (avoids crash window)
	if newManifest {
		// Whitebox [crashtest]: crash before CURRENT update — old manifest still active
		testutil.MaybeKill(testutil.KPCurrentWrite0)

		if err := vs.setCurrentFile(vs.manifestFileNumber); err != nil {
			return err
		}

		// Whitebox [crashtest]: crash after CURRENT update — fully durable
		testutil.MaybeKill(testutil.KPCurrentWrite1)
	}

	// Install the new version
	vs.appendVersion(newVersion)
	newVersion.Ref()
	if vs.current != nil {
		vs.current.Unref()
	}
	vs.current = newVersion

	return nil
}

// SyncManifest ensures the MANIFEST file is synced to disk.
// This is useful before creating checkpoints.
func (vs *VersionSet) SyncManifest() error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.manifestFile == nil {
		return nil
	}

	if syncer, ok := vs.manifestFile.(interface{ Sync() error }); ok {
		return syncer.Sync()
	}
	return nil
}

// writeSnapshot creates a VersionEdit that captures the current state.
func (vs *VersionSet) writeSnapshot() *manifest.VersionEdit {
	edit := &manifest.VersionEdit{
		HasComparator:     true,
		Comparator:        "leveldb.BytewiseComparator",
		HasLogNumber:      true,
		LogNumber:         vs.logNumber,
		HasNextFileNumber: true,
		NextFileNumber:    atomic.LoadUint64(&vs.nextFileNumber),
		HasLastSequence:   true,
		LastSequence:      manifest.SequenceNumber(atomic.LoadUint64(&vs.lastSequence)),
	}

	// Add all files from current version
	if vs.current != nil {
		for level := range MaxNumLevels {
			for _, f := range vs.current.files[level] {
				edit.NewFiles = append(edit.NewFiles, manifest.NewFileEntry{
					Level: level,
					Meta:  f,
				})
			}
		}
	}

	return edit
}

// setCurrentFile writes the CURRENT file pointing to the given manifest.
// Uses the configured VFS and syncs both temp file and directory for durability.
// Reference: RocksDB file/filename.cc SetCurrentFile
func (vs *VersionSet) setCurrentFile(manifestNum uint64) error {
	manifestName := fmt.Sprintf("MANIFEST-%06d", manifestNum)
	tempPath := filepath.Join(vs.opts.DBName, "CURRENT.tmp")
	currentPath := filepath.Join(vs.opts.DBName, "CURRENT")

	// Write to temp file using VFS
	content := manifestName + "\n"
	tempFile, err := vs.opts.FS.Create(tempPath)
	if err != nil {
		return fmt.Errorf("create CURRENT.tmp: %w", err)
	}

	if _, err := tempFile.Write([]byte(content)); err != nil {
		_ = tempFile.Close()            // best-effort cleanup
		_ = vs.opts.FS.Remove(tempPath) // best-effort cleanup
		return fmt.Errorf("write CURRENT.tmp: %w", err)
	}

	// Sync temp file before rename (durability)
	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()            // best-effort cleanup
		_ = vs.opts.FS.Remove(tempPath) // best-effort cleanup
		return fmt.Errorf("sync CURRENT.tmp: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		_ = vs.opts.FS.Remove(tempPath) // best-effort cleanup
		return fmt.Errorf("close CURRENT.tmp: %w", err)
	}

	// Atomic rename using VFS
	if err := vs.opts.FS.Rename(tempPath, currentPath); err != nil {
		_ = vs.opts.FS.Remove(tempPath) // best-effort cleanup
		return fmt.Errorf("rename CURRENT: %w", err)
	}

	// Whitebox [crashtest]: crash before directory sync — CURRENT may not be durable
	testutil.MaybeKill(testutil.KPDirSync0)

	// Sync directory to ensure rename is durable
	if err := vs.opts.FS.SyncDir(vs.opts.DBName); err != nil {
		return fmt.Errorf("sync dir after CURRENT rename: %w", err)
	}

	// Whitebox [crashtest]: crash after directory sync — CURRENT is fully durable
	testutil.MaybeKill(testutil.KPDirSync1)

	return nil
}

// manifestFilePath returns the path to a MANIFEST file.
func (vs *VersionSet) manifestFilePath(num uint64) string {
	return filepath.Join(vs.opts.DBName, fmt.Sprintf("MANIFEST-%06d", num))
}

// appendVersion adds a version to the linked list.
func (vs *VersionSet) appendVersion(v *Version) {
	vs.listMu.Lock()
	defer vs.listMu.Unlock()
	// Insert before dummy head (i.e., at the end of the list)
	v.prev = vs.dummyVersions.prev
	v.next = &vs.dummyVersions
	v.prev.next = v
	v.next.prev = v
}

// Create creates a new database with an initial empty version.
func (vs *VersionSet) Create() error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Create initial version
	vs.current = NewVersion(vs, vs.NextVersionNumber())
	vs.current.Ref()
	vs.appendVersion(vs.current)

	// Create initial MANIFEST
	edit := &manifest.VersionEdit{
		HasComparator:     true,
		Comparator:        "leveldb.BytewiseComparator",
		HasLogNumber:      true,
		LogNumber:         0,
		HasNextFileNumber: true,
		NextFileNumber:    atomic.LoadUint64(&vs.nextFileNumber),
		HasLastSequence:   true,
		LastSequence:      0,
	}

	return vs.logAndApplyLocked(edit)
}

// logAndApplyLocked is the internal version of LogAndApply (caller holds the lock).
func (vs *VersionSet) logAndApplyLocked(edit *manifest.VersionEdit) error {
	// Encode the edit
	encoded := edit.EncodeTo()

	// Create MANIFEST file if needed
	if vs.manifestWriter == nil {
		manifestNum := vs.NextFileNumber()
		manifestPath := vs.manifestFilePath(manifestNum)

		file, err := vs.opts.FS.Create(manifestPath)
		if err != nil {
			return err
		}

		vs.manifestFile = file
		vs.manifestWriter = wal.NewWriter(file, manifestNum, false /* not recyclable */)
		vs.manifestFileNumber = manifestNum
	}

	// Whitebox [crashtest]: crash before MANIFEST write — tests partial manifest
	testutil.MaybeKill(testutil.KPManifestWrite0)

	// Write the edit
	if _, err := vs.manifestWriter.AddRecord(encoded); err != nil {
		return err
	}

	// Whitebox [crashtest]: crash before MANIFEST sync — tests unsynced manifest
	testutil.MaybeKill(testutil.KPManifestSync0)

	// Sync the manifest file
	if syncer, ok := vs.manifestFile.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			return err
		}
	}

	// Whitebox [crashtest]: crash after MANIFEST sync — CURRENT not updated
	testutil.MaybeKill(testutil.KPManifestSync1)

	// Whitebox [crashtest]: crash before CURRENT update — old manifest active
	testutil.MaybeKill(testutil.KPCurrentWrite0)

	// Update CURRENT file
	if err := vs.setCurrentFile(vs.manifestFileNumber); err != nil {
		return err
	}

	// Whitebox [crashtest]: crash after CURRENT update — fully durable
	testutil.MaybeKill(testutil.KPCurrentWrite1)

	return nil
}

// Close closes the VersionSet and releases resources.
func (vs *VersionSet) Close() error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.manifestFile != nil {
		if err := vs.manifestFile.Close(); err != nil {
			return err
		}
		vs.manifestFile = nil
		vs.manifestWriter = nil
	}

	return nil
}

// NumLevelFiles returns the number of files at the given level.
func (vs *VersionSet) NumLevelFiles(level int) int {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.current == nil {
		return 0
	}
	return vs.current.NumFiles(level)
}

// NumLevelBytes returns the total size of files at the given level.
func (vs *VersionSet) NumLevelBytes(level int) uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.current == nil {
		return 0
	}
	return vs.current.NumLevelBytes(level)
}

// comparatorNamesMatch checks if two comparator names are compatible.
// This handles backward compatibility between leveldb and rocksdb names.
func comparatorNamesMatch(diskName, optName string) bool {
	if diskName == optName {
		return true
	}
	// Handle backward compatibility: leveldb.BytewiseComparator == rocksdb.BytewiseComparator
	bytewiseNames := map[string]bool{
		"leveldb.BytewiseComparator":        true,
		"rocksdb.BytewiseComparator":        true,
		"RocksDB.BytewiseComparator":        true,
		"leveldb.ReverseBytewiseComparator": false, // Not compatible with bytewise
	}
	diskIsBytewise := bytewiseNames[diskName]
	optIsBytewise := bytewiseNames[optName]
	return diskIsBytewise && optIsBytewise
}
