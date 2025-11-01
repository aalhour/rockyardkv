// Package testutil provides test utilities for stress testing and verification.
//
// ExpectedState maintains a shadow copy of expected database state for
// verification during stress testing.
//
// Reference: RocksDB v10.7.5
//   - db_stress_tool/expected_state.h
//   - db_stress_tool/expected_state.cc
package testutil

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"sync/atomic"
)

// Errors for persistent expected state
var (
	errInvalidFile        = errors.New("expected_state: invalid file format")
	errInvalidMagic       = errors.New("expected_state: invalid magic number")
	errUnsupportedVersion = errors.New("expected_state: unsupported version")
)

// File operation helpers (thin wrappers for testing)
var (
	createFile    = os.Create
	removeFile    = os.Remove
	renameFile    = os.Rename
	readFileBytes = os.ReadFile
)

// ExpectedState tracks the expected state of a database for verification.
// It maintains expected values for keys and can verify against actual DB state.
//
// The state uses a compact representation:
// - Each key maps to a 32-bit value that encodes:
//   - Value existence (deleted or present)
//   - A value identifier that can be verified
//
// This allows efficient tracking of millions of keys.
type ExpectedState struct {
	mu sync.RWMutex

	// Number of column families
	numColumnFamilies int

	// Maximum key (exclusive) - keys are 0 to maxKey-1
	maxKey int64

	// values[cf * maxKey + key] = expected value state
	// Layout: [CF0 keys...][CF1 keys...]...
	values []atomic.Uint32

	// Global sequence number for ordering
	seqno atomic.Uint64
}

// ValueState represents the state of a key's value.
type ValueState uint32

const (
	// ValueStateUnknown means the key's state is unknown (never written).
	ValueStateUnknown ValueState = 0

	// ValueStateDeleted means the key has been deleted.
	ValueStateDeleted ValueState = 1

	// ValueStateExists means the key exists with value (state - 2) as the value ID.
	// Value IDs start at 0, so state 2 = value 0, state 3 = value 1, etc.
	ValueStateExists ValueState = 2
)

// NewExpectedState creates a new ExpectedState for tracking key-value state.
// maxKey is the maximum key value (exclusive).
// numCFs is the number of column families to track.
func NewExpectedState(maxKey int64, numCFs int) *ExpectedState {
	if numCFs <= 0 {
		numCFs = 1
	}
	if maxKey <= 0 {
		maxKey = 1
	}

	totalSlots := maxKey * int64(numCFs)
	values := make([]atomic.Uint32, totalSlots)

	return &ExpectedState{
		numColumnFamilies: numCFs,
		maxKey:            maxKey,
		values:            values,
	}
}

// getIndex returns the index into the values array for the given CF and key.
func (es *ExpectedState) getIndex(cf int, key int64) int64 {
	if cf < 0 || cf >= es.numColumnFamilies {
		return -1
	}
	if key < 0 || key >= es.maxKey {
		return -1
	}
	return int64(cf)*es.maxKey + key
}

// Get returns the expected state for a key.
func (es *ExpectedState) Get(cf int, key int64) ValueState {
	idx := es.getIndex(cf, key)
	if idx < 0 {
		return ValueStateUnknown
	}
	return ValueState(es.values[idx].Load())
}

// Put records that a key was written with a specific value ID.
// valueID is a small identifier that can be encoded in the actual value
// for later verification.
func (es *ExpectedState) Put(cf int, key int64, valueID uint32) {
	idx := es.getIndex(cf, key)
	if idx < 0 {
		return
	}
	es.values[idx].Store(uint32(ValueStateExists) + valueID)
	es.seqno.Add(1)
}

// Delete records that a key was deleted.
func (es *ExpectedState) Delete(cf int, key int64) {
	idx := es.getIndex(cf, key)
	if idx < 0 {
		return
	}
	es.values[idx].Store(uint32(ValueStateDeleted))
	es.seqno.Add(1)
}

// IsDeleted returns true if the key is expected to be deleted.
func (es *ExpectedState) IsDeleted(cf int, key int64) bool {
	return es.Get(cf, key) == ValueStateDeleted
}

// Exists returns true if the key is expected to exist (not deleted, not unknown).
func (es *ExpectedState) Exists(cf int, key int64) bool {
	state := es.Get(cf, key)
	return state >= ValueStateExists
}

// GetValueID returns the expected value ID for a key.
// Returns (valueID, true) if the key exists, or (0, false) if deleted/unknown.
func (es *ExpectedState) GetValueID(cf int, key int64) (uint32, bool) {
	state := es.Get(cf, key)
	if state < ValueStateExists {
		return 0, false
	}
	return uint32(state) - uint32(ValueStateExists), true
}

// Seqno returns the current sequence number (number of operations).
func (es *ExpectedState) Seqno() uint64 {
	return es.seqno.Load()
}

// Clear resets all state to unknown.
func (es *ExpectedState) Clear() {
	es.mu.Lock()
	defer es.mu.Unlock()

	for i := range es.values {
		es.values[i].Store(0)
	}
	es.seqno.Store(0)
}

// PendingExpectedValue represents a value that is being written but not yet committed.
// This is used for tracking in-flight operations during crash recovery testing.
type PendingExpectedValue struct {
	state         *ExpectedState
	cf            int
	key           int64
	originalState ValueState
	committed     bool
}

// PreparePut creates a pending put operation.
// The original state is saved so it can be restored on rollback.
func (es *ExpectedState) PreparePut(cf int, key int64, valueID uint32) *PendingExpectedValue {
	idx := es.getIndex(cf, key)
	if idx < 0 {
		return nil
	}

	original := ValueState(es.values[idx].Load())

	return &PendingExpectedValue{
		state:         es,
		cf:            cf,
		key:           key,
		originalState: original,
		committed:     false,
	}
}

// PrepareDelete creates a pending delete operation.
func (es *ExpectedState) PrepareDelete(cf int, key int64) *PendingExpectedValue {
	idx := es.getIndex(cf, key)
	if idx < 0 {
		return nil
	}

	original := ValueState(es.values[idx].Load())

	return &PendingExpectedValue{
		state:         es,
		cf:            cf,
		key:           key,
		originalState: original,
		committed:     false,
	}
}

// Commit commits the pending operation.
func (pev *PendingExpectedValue) Commit(valueID uint32, isDelete bool) {
	if pev.committed {
		return
	}
	pev.committed = true

	if isDelete {
		pev.state.Delete(pev.cf, pev.key)
	} else {
		pev.state.Put(pev.cf, pev.key, valueID)
	}
}

// Rollback restores the original state.
func (pev *PendingExpectedValue) Rollback() {
	if pev.committed {
		return
	}

	idx := pev.state.getIndex(pev.cf, pev.key)
	if idx < 0 {
		return
	}

	pev.state.values[idx].Store(uint32(pev.originalState))
}

// VerificationResult holds the result of verifying expected state against actual state.
type VerificationResult struct {
	Verified   int64 // Number of keys verified
	Mismatches int64 // Number of mismatches found
	Errors     []VerificationError
}

// VerificationError describes a single verification failure.
type VerificationError struct {
	CF          int
	Key         int64
	Expected    ValueState
	ExpectedID  uint32
	ActualFound bool
	ActualValue []byte
	Message     string
}

// GenerateValue generates a value that encodes the key and valueID for later verification.
// Format: [key:8][valueID:4][padding...]
func GenerateValue(key int64, valueID uint32, valueSize int) []byte {
	if valueSize < 12 {
		valueSize = 12
	}
	value := make([]byte, valueSize)
	binary.LittleEndian.PutUint64(value[0:8], uint64(key))
	binary.LittleEndian.PutUint32(value[8:12], valueID)
	// Fill rest with deterministic pattern
	for i := 12; i < valueSize; i++ {
		value[i] = byte((int(key) + int(valueID) + i) % 256)
	}
	return value
}

// VerifyValue checks if a value matches the expected key and valueID.
func VerifyValue(key int64, expectedValueID uint32, value []byte) bool {
	if len(value) < 12 {
		return false
	}
	storedKey := binary.LittleEndian.Uint64(value[0:8])
	storedID := binary.LittleEndian.Uint32(value[8:12])
	return storedKey == uint64(key) && storedID == expectedValueID
}

// ExpectedStateManager manages saving and restoring expected state.
// This is useful for crash recovery testing.
type ExpectedStateManager struct {
	state     *ExpectedState
	snapshots []*ExpectedStateSnapshot
	mu        sync.Mutex
}

// ExpectedStateSnapshot is a point-in-time copy of expected state.
type ExpectedStateSnapshot struct {
	seqno  uint64
	values map[int64]uint32 // Only stores non-zero values for efficiency
}

// NewExpectedStateManager creates a new manager for the given state.
func NewExpectedStateManager(state *ExpectedState) *ExpectedStateManager {
	return &ExpectedStateManager{
		state:     state,
		snapshots: make([]*ExpectedStateSnapshot, 0),
	}
}

// TakeSnapshot creates a snapshot of the current expected state.
func (m *ExpectedStateManager) TakeSnapshot() {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap := &ExpectedStateSnapshot{
		seqno:  m.state.seqno.Load(),
		values: make(map[int64]uint32),
	}

	// Only store non-zero values
	for i := range m.state.values {
		val := m.state.values[i].Load()
		if val != 0 {
			snap.values[int64(i)] = val
		}
	}

	m.snapshots = append(m.snapshots, snap)
}

// RestoreLatestSnapshot restores the most recent snapshot.
func (m *ExpectedStateManager) RestoreLatestSnapshot() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.snapshots) == 0 {
		return false
	}

	snap := m.snapshots[len(m.snapshots)-1]

	// Clear all values
	for i := range m.state.values {
		m.state.values[i].Store(0)
	}

	// Restore snapshot values
	for idx, val := range snap.values {
		if idx >= 0 && idx < int64(len(m.state.values)) {
			m.state.values[idx].Store(val)
		}
	}

	m.state.seqno.Store(snap.seqno)
	return true
}

// ClearSnapshots removes all snapshots.
func (m *ExpectedStateManager) ClearSnapshots() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshots = m.snapshots[:0]
}

// NumSnapshots returns the number of saved snapshots.
func (m *ExpectedStateManager) NumSnapshots() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.snapshots)
}

// ============================================================================
// Persistent ExpectedState Support
// ============================================================================

// persistentHeader is the file format header for persistent expected state.
// Format:
//
//	Magic (8 bytes): "EXSTATE1"
//	Version (4 bytes): 1
//	NumCFs (4 bytes): number of column families
//	MaxKey (8 bytes): max key value
//	Seqno (8 bytes): sequence number
//	Data: (numCFs * maxKey * 4 bytes): value states as uint32
const persistentMagic = "EXSTATE1"
const persistentVersion = 1

// SaveToFile saves the expected state to a file.
// This allows crash testing to persist state across process crashes.
func (es *ExpectedState) SaveToFile(path string) error {
	es.mu.RLock()
	defer es.mu.RUnlock()

	// Calculate file size
	headerSize := 8 + 4 + 4 + 8 + 8 // magic + version + numCFs + maxKey + seqno
	dataSize := len(es.values) * 4
	totalSize := headerSize + dataSize

	data := make([]byte, totalSize)
	offset := 0

	// Magic
	copy(data[offset:], persistentMagic)
	offset += 8

	// Version
	binary.LittleEndian.PutUint32(data[offset:], persistentVersion)
	offset += 4

	// NumCFs
	binary.LittleEndian.PutUint32(data[offset:], uint32(es.numColumnFamilies))
	offset += 4

	// MaxKey
	binary.LittleEndian.PutUint64(data[offset:], uint64(es.maxKey))
	offset += 8

	// Seqno
	binary.LittleEndian.PutUint64(data[offset:], es.seqno.Load())
	offset += 8

	// Data
	for i := range es.values {
		binary.LittleEndian.PutUint32(data[offset:], es.values[i].Load())
		offset += 4
	}

	return writeFile(path, data)
}

// LoadExpectedStateFromFile loads the expected state from a file.
// Returns a new ExpectedState with the loaded data.
func LoadExpectedStateFromFile(path string) (*ExpectedState, error) {
	data, err := readFile(path)
	if err != nil {
		return nil, err
	}

	if len(data) < 32 {
		return nil, errInvalidFile
	}

	offset := 0

	// Magic
	magic := string(data[offset : offset+8])
	if magic != persistentMagic {
		return nil, errInvalidMagic
	}
	offset += 8

	// Version
	version := binary.LittleEndian.Uint32(data[offset:])
	if version != persistentVersion {
		return nil, errUnsupportedVersion
	}
	offset += 4

	// NumCFs
	numCFs := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	// MaxKey
	maxKey := int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	// Seqno
	seqno := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// Calculate expected data size
	expectedDataSize := numCFs * int(maxKey) * 4
	if len(data)-offset != expectedDataSize {
		return nil, errInvalidFile
	}

	// Create state
	es := NewExpectedState(maxKey, numCFs)
	es.seqno.Store(seqno)

	// Load data
	for i := range es.values {
		es.values[i].Store(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
	}

	return es, nil
}

// writeFile is a helper to write data atomically.
func writeFile(path string, data []byte) error {
	tmpPath := path + ".tmp"
	f, err := createFile(tmpPath)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = removeFile(tmpPath) // Best-effort cleanup
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = removeFile(tmpPath) // Best-effort cleanup
		return err
	}

	if err := f.Close(); err != nil {
		_ = removeFile(tmpPath) // Best-effort cleanup
		return err
	}

	return renameFile(tmpPath, path)
}

// readFile is a helper to read a file.
func readFile(path string) ([]byte, error) {
	return readFileBytes(path)
}
