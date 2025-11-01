// Package testutil provides test utilities for stress testing and verification.
//
// This file provides file-backed persistence for ExpectedState,
// allowing expected state to survive process restarts for crash testing.
//
// Reference: RocksDB v10.7.5
//   - db_stress_tool/expected_state.h
//   - db_stress_tool/expected_state.cc
package testutil

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	// File format magic number
	expectedStateMagic = uint64(0x524F434B5953544D) // "ROCKYSTM"

	// File format version
	expectedStateVersion = uint32(1)

	// Header size: magic (8) + version (4) + maxKey (8) + numCFs (4) + seqno (8) = 32
	expectedStateHeaderSize = 32
)

// FileExpectedState implements ExpectedState backed by a file.
// It uses memory-mapping for efficient access and persistence.
type FileExpectedState struct {
	mu sync.RWMutex

	// File path
	path string

	// Configuration
	maxKey            int64
	numColumnFamilies int

	// In-memory state (loaded from file)
	values []atomic.Uint32
	seqno  atomic.Uint64

	// Dirty flag for lazy writes
	dirty atomic.Bool
}

// NewFileExpectedState creates a new file-backed expected state.
// If the file exists, it will be loaded. Otherwise, a new file is created.
func NewFileExpectedState(path string, maxKey int64, numCFs int) (*FileExpectedState, error) {
	if numCFs <= 0 {
		numCFs = 1
	}
	if maxKey <= 0 {
		maxKey = 1
	}

	fes := &FileExpectedState{
		path:              path,
		maxKey:            maxKey,
		numColumnFamilies: numCFs,
	}

	// Check if file exists
	if _, err := os.Stat(path); err == nil {
		// Load existing file
		if err := fes.load(); err != nil {
			return nil, fmt.Errorf("failed to load expected state: %w", err)
		}
	} else {
		// Create new state
		fes.values = make([]atomic.Uint32, maxKey*int64(numCFs))
		if err := fes.save(); err != nil {
			return nil, fmt.Errorf("failed to create expected state file: %w", err)
		}
	}

	return fes, nil
}

// load reads the expected state from the file.
func (fes *FileExpectedState) load() error {
	fes.mu.Lock()
	defer fes.mu.Unlock()

	file, err := os.Open(fes.path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	// Read header
	header := make([]byte, expectedStateHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Verify magic
	magic := binary.LittleEndian.Uint64(header[0:8])
	if magic != expectedStateMagic {
		return fmt.Errorf("invalid magic number: expected %x, got %x", expectedStateMagic, magic)
	}

	// Read version
	version := binary.LittleEndian.Uint32(header[8:12])
	if version != expectedStateVersion {
		return fmt.Errorf("unsupported version: %d", version)
	}

	// Read configuration
	storedMaxKey := int64(binary.LittleEndian.Uint64(header[12:20]))
	storedNumCFs := int(binary.LittleEndian.Uint32(header[20:24]))
	storedSeqno := binary.LittleEndian.Uint64(header[24:32])

	// Verify configuration matches
	if storedMaxKey != fes.maxKey || storedNumCFs != fes.numColumnFamilies {
		return fmt.Errorf("configuration mismatch: file has maxKey=%d, numCFs=%d; expected maxKey=%d, numCFs=%d",
			storedMaxKey, storedNumCFs, fes.maxKey, fes.numColumnFamilies)
	}

	// Read values
	totalSlots := fes.maxKey * int64(fes.numColumnFamilies)
	fes.values = make([]atomic.Uint32, totalSlots)

	valueData := make([]byte, totalSlots*4)
	if _, err := io.ReadFull(file, valueData); err != nil {
		return fmt.Errorf("failed to read values: %w", err)
	}

	for i := range totalSlots {
		fes.values[i].Store(binary.LittleEndian.Uint32(valueData[i*4 : (i+1)*4]))
	}

	fes.seqno.Store(storedSeqno)
	fes.dirty.Store(false)

	return nil
}

// save writes the expected state to the file.
func (fes *FileExpectedState) save() error {
	fes.mu.RLock()
	defer fes.mu.RUnlock()

	return fes.saveUnlocked()
}

// saveUnlocked saves without acquiring lock (caller must hold lock).
func (fes *FileExpectedState) saveUnlocked() error {
	// Ensure directory exists
	dir := filepath.Dir(fes.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create temp file for atomic write
	tmpPath := fes.path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	// Write header
	header := make([]byte, expectedStateHeaderSize)
	binary.LittleEndian.PutUint64(header[0:8], expectedStateMagic)
	binary.LittleEndian.PutUint32(header[8:12], expectedStateVersion)
	binary.LittleEndian.PutUint64(header[12:20], uint64(fes.maxKey))
	binary.LittleEndian.PutUint32(header[20:24], uint32(fes.numColumnFamilies))
	binary.LittleEndian.PutUint64(header[24:32], fes.seqno.Load())

	if _, err := file.Write(header); err != nil {
		_ = file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write values
	totalSlots := fes.maxKey * int64(fes.numColumnFamilies)
	valueData := make([]byte, totalSlots*4)
	for i := range totalSlots {
		binary.LittleEndian.PutUint32(valueData[i*4:(i+1)*4], fes.values[i].Load())
	}

	if _, err := file.Write(valueData); err != nil {
		_ = file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write values: %w", err)
	}

	// Sync and close
	if err := file.Sync(); err != nil {
		_ = file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to sync: %w", err)
	}
	_ = file.Close()

	// Atomic rename
	if err := os.Rename(tmpPath, fes.path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename: %w", err)
	}

	fes.dirty.Store(false)
	return nil
}

// Sync forces the expected state to be written to disk.
func (fes *FileExpectedState) Sync() error {
	if !fes.dirty.Load() {
		return nil
	}
	return fes.save()
}

// Close saves and closes the expected state file.
func (fes *FileExpectedState) Close() error {
	return fes.save()
}

// getIndex returns the index into the values array for the given CF and key.
func (fes *FileExpectedState) getIndex(cf int, key int64) int64 {
	if cf < 0 || cf >= fes.numColumnFamilies {
		return -1
	}
	if key < 0 || key >= fes.maxKey {
		return -1
	}
	return int64(cf)*fes.maxKey + key
}

// Get returns the expected state for a key.
func (fes *FileExpectedState) Get(cf int, key int64) ValueState {
	idx := fes.getIndex(cf, key)
	if idx < 0 {
		return ValueStateUnknown
	}
	return ValueState(fes.values[idx].Load())
}

// Put records that a key was written with a specific value ID.
func (fes *FileExpectedState) Put(cf int, key int64, valueID uint32) {
	idx := fes.getIndex(cf, key)
	if idx < 0 {
		return
	}
	fes.values[idx].Store(uint32(ValueStateExists) + valueID)
	fes.seqno.Add(1)
	fes.dirty.Store(true)
}

// Delete records that a key was deleted.
func (fes *FileExpectedState) Delete(cf int, key int64) {
	idx := fes.getIndex(cf, key)
	if idx < 0 {
		return
	}
	fes.values[idx].Store(uint32(ValueStateDeleted))
	fes.seqno.Add(1)
	fes.dirty.Store(true)
}

// IsDeleted returns true if the key is expected to be deleted.
func (fes *FileExpectedState) IsDeleted(cf int, key int64) bool {
	return fes.Get(cf, key) == ValueStateDeleted
}

// Exists returns true if the key is expected to exist.
func (fes *FileExpectedState) Exists(cf int, key int64) bool {
	state := fes.Get(cf, key)
	return state >= ValueStateExists
}

// GetValueID returns the expected value ID for a key.
func (fes *FileExpectedState) GetValueID(cf int, key int64) (uint32, bool) {
	state := fes.Get(cf, key)
	if state < ValueStateExists {
		return 0, false
	}
	return uint32(state) - uint32(ValueStateExists), true
}

// Seqno returns the current sequence number.
func (fes *FileExpectedState) Seqno() uint64 {
	return fes.seqno.Load()
}

// Clear resets all state to unknown.
func (fes *FileExpectedState) Clear() {
	fes.mu.Lock()
	defer fes.mu.Unlock()

	for i := range fes.values {
		fes.values[i].Store(0)
	}
	fes.seqno.Store(0)
	fes.dirty.Store(true)
}

// Path returns the file path.
func (fes *FileExpectedState) Path() string {
	return fes.path
}

// MaxKey returns the maximum key.
func (fes *FileExpectedState) MaxKey() int64 {
	return fes.maxKey
}

// NumColumnFamilies returns the number of column families.
func (fes *FileExpectedState) NumColumnFamilies() int {
	return fes.numColumnFamilies
}

// Reload reloads the expected state from the file.
// This is useful after a crash to restore the state.
func (fes *FileExpectedState) Reload() error {
	return fes.load()
}

// ExpectedStateInterface defines the common interface for expected state implementations.
type ExpectedStateInterface interface {
	Get(cf int, key int64) ValueState
	Put(cf int, key int64, valueID uint32)
	Delete(cf int, key int64)
	IsDeleted(cf int, key int64) bool
	Exists(cf int, key int64) bool
	GetValueID(cf int, key int64) (uint32, bool)
	Seqno() uint64
	Clear()
}

// Verify that both implementations satisfy the interface
var _ ExpectedStateInterface = (*ExpectedState)(nil)
var _ ExpectedStateInterface = (*FileExpectedState)(nil)
