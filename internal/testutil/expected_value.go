// Package testutil provides test utilities for stress testing and verification.
//
// ExpectedValue implements RocksDB-compatible expected value tracking with
// pending operation flags and concurrent access support.
//
// Reference: RocksDB v10.7.5
//   - db_stress_tool/expected_value.h
package testutil

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// ExpectedValue represents the expected state of a key using a packed 32-bit format.
// This matches RocksDB's ExpectedValue implementation.
//
// Bit layout:
//   - Bits 0-14: Value base (the actual value identifier, 0-32767)
//   - Bit 15: Pending write flag (1 = write in progress)
//   - Bits 16-29: Deletion counter (number of times deleted, 0-16383)
//   - Bit 30: Pending delete flag (1 = delete in progress)
//   - Bit 31: Is deleted flag (1 = currently deleted)
type ExpectedValue uint32

// Constants for bit manipulation
const (
	valueBaseMask     uint32 = 0x7fff     // Bits 0-14
	valueBaseDelta    uint32 = 1          // Increment for value base
	pendingWriteMask  uint32 = 1 << 15    // Bit 15
	delCounterMask    uint32 = 0x3fff0000 // Bits 16-29
	delCounterDelta   uint32 = 1 << 16    // Increment for deletion counter
	pendingDeleteMask uint32 = 1 << 30    // Bit 30
	deletedMask       uint32 = 1 << 31    // Bit 31
)

// NewExpectedValue creates an ExpectedValue in the deleted state (initial state).
func NewExpectedValue() ExpectedValue {
	return ExpectedValue(deletedMask) // Start as deleted
}

// NewExpectedValueFromRaw creates an ExpectedValue from a raw uint32.
func NewExpectedValueFromRaw(raw uint32) ExpectedValue {
	return ExpectedValue(raw)
}

// Raw returns the raw uint32 value.
func (ev ExpectedValue) Raw() uint32 {
	return uint32(ev)
}

// GetValueBase returns the value base (bits 0-14).
func (ev ExpectedValue) GetValueBase() uint32 {
	return uint32(ev) & valueBaseMask
}

// NextValueBase returns the next value base (wraps around at valueBaseMask).
func (ev ExpectedValue) NextValueBase() uint32 {
	current := ev.GetValueBase()
	next := current + valueBaseDelta
	return next & valueBaseMask
}

// SetValueBase sets the value base.
func (ev *ExpectedValue) SetValueBase(valueBase uint32) {
	*ev = ExpectedValue((uint32(*ev) & ^valueBaseMask) | (valueBase & valueBaseMask))
}

// PendingWrite returns true if a write is pending.
func (ev ExpectedValue) PendingWrite() bool {
	return (uint32(ev) & pendingWriteMask) != 0
}

// SetPendingWrite sets the pending write flag.
func (ev *ExpectedValue) SetPendingWrite() {
	*ev = ExpectedValue(uint32(*ev) | pendingWriteMask)
}

// ClearPendingWrite clears the pending write flag.
func (ev *ExpectedValue) ClearPendingWrite() {
	*ev = ExpectedValue(uint32(*ev) & ^pendingWriteMask)
}

// GetDelCounter returns the deletion counter (bits 16-29).
func (ev ExpectedValue) GetDelCounter() uint32 {
	return (uint32(ev) & delCounterMask) >> 16
}

// NextDelCounter returns the next deletion counter (wraps around).
func (ev ExpectedValue) NextDelCounter() uint32 {
	current := ev.GetDelCounter()
	return (current + 1) & (delCounterMask >> 16)
}

// SetDelCounter sets the deletion counter.
func (ev *ExpectedValue) SetDelCounter(counter uint32) {
	*ev = ExpectedValue((uint32(*ev) & ^delCounterMask) | ((counter << 16) & delCounterMask))
}

// PendingDelete returns true if a delete is pending.
func (ev ExpectedValue) PendingDelete() bool {
	return (uint32(ev) & pendingDeleteMask) != 0
}

// SetPendingDelete sets the pending delete flag.
func (ev *ExpectedValue) SetPendingDelete() {
	*ev = ExpectedValue(uint32(*ev) | pendingDeleteMask)
}

// ClearPendingDelete clears the pending delete flag.
func (ev *ExpectedValue) ClearPendingDelete() {
	*ev = ExpectedValue(uint32(*ev) & ^pendingDeleteMask)
}

// IsDeleted returns true if the key is deleted.
func (ev ExpectedValue) IsDeleted() bool {
	return (uint32(ev) & deletedMask) != 0
}

// SetDeleted sets the deleted flag.
func (ev *ExpectedValue) SetDeleted() {
	*ev = ExpectedValue(uint32(*ev) | deletedMask)
}

// ClearDeleted clears the deleted flag.
func (ev *ExpectedValue) ClearDeleted() {
	*ev = ExpectedValue(uint32(*ev) & ^deletedMask)
}

// Exists returns true if the key exists (not deleted, no pending operations).
func (ev ExpectedValue) Exists() bool {
	return !ev.IsDeleted() && !ev.PendingWrite() && !ev.PendingDelete()
}

// GetFinalValueBase returns the value base that will be set after pending write completes.
// If no pending write, returns current value base.
func (ev ExpectedValue) GetFinalValueBase() uint32 {
	if ev.PendingWrite() {
		return ev.NextValueBase()
	}
	return ev.GetValueBase()
}

// GetFinalDelCounter returns the deletion counter that will be set after pending delete completes.
// If no pending delete, returns current counter.
func (ev ExpectedValue) GetFinalDelCounter() uint32 {
	if ev.PendingDelete() {
		return ev.NextDelCounter()
	}
	return ev.GetDelCounter()
}

// Put prepares or commits a put operation.
// If pending=true, sets the pending write flag but doesn't update value.
// If pending=false, increments value base and clears deleted flag.
func (ev *ExpectedValue) Put(pending bool) {
	if pending {
		ev.SetPendingWrite()
	} else {
		ev.SetValueBase(ev.NextValueBase())
		ev.ClearPendingWrite()
		ev.ClearDeleted()
	}
}

// Delete prepares or commits a delete operation.
// If pending=true, sets the pending delete flag.
// If pending=false, increments deletion counter and sets deleted flag.
// Returns true if the key existed before delete (for SingleDelete verification).
func (ev *ExpectedValue) Delete(pending bool) bool {
	existed := !ev.IsDeleted()
	if pending {
		ev.SetPendingDelete()
	} else {
		ev.SetDelCounter(ev.NextDelCounter())
		ev.ClearPendingDelete()
		ev.SetDeleted()
	}
	return existed
}

// SyncPut atomically sets the value base (for external sync operations).
func (ev *ExpectedValue) SyncPut(valueBase uint32) {
	ev.SetValueBase(valueBase)
	ev.ClearPendingWrite()
	ev.ClearDeleted()
}

// SyncPendingPut completes a pending put operation.
func (ev *ExpectedValue) SyncPendingPut() {
	ev.SetValueBase(ev.NextValueBase())
	ev.ClearPendingWrite()
	ev.ClearDeleted()
}

// SyncDelete atomically marks as deleted.
func (ev *ExpectedValue) SyncDelete() {
	ev.SetDelCounter(ev.NextDelCounter())
	ev.ClearPendingDelete()
	ev.SetDeleted()
}

// -------------------------------------------------------------------------------------------------
// PendingExpectedValueV2 - RAII-style wrapper for pending operations (new version)
// -------------------------------------------------------------------------------------------------

// PendingExpectedValueV2 represents an expected value undergoing a pending operation.
// After creation, either Commit or Rollback must be called before it goes out of scope.
// This matches RocksDB's PendingExpectedValue class.
type PendingExpectedValueV2 struct {
	valuePtr           *atomic.Uint32
	origValue          ExpectedValue
	finalValue         ExpectedValue
	pendingStateClosed bool
}

// NewPendingExpectedValueV2 creates a new pending expected value.
func NewPendingExpectedValueV2(valuePtr *atomic.Uint32, origValue, finalValue ExpectedValue) *PendingExpectedValueV2 {
	return &PendingExpectedValueV2{
		valuePtr:           valuePtr,
		origValue:          origValue,
		finalValue:         finalValue,
		pendingStateClosed: false,
	}
}

// Commit commits the pending operation, storing the final value.
func (pev *PendingExpectedValueV2) Commit() {
	if pev.pendingStateClosed {
		return
	}
	pev.pendingStateClosed = true

	// Memory barrier to prevent reordering
	// Store final value
	pev.valuePtr.Store(pev.finalValue.Raw())
}

// Rollback restores the original value.
func (pev *PendingExpectedValueV2) Rollback() {
	if pev.pendingStateClosed {
		return
	}
	pev.pendingStateClosed = true

	// Memory barrier to prevent reordering
	// Restore original value
	pev.valuePtr.Store(pev.origValue.Raw())
}

// PermitUnclosedPendingState allows the pending state to be unclosed (for special cases).
func (pev *PendingExpectedValueV2) PermitUnclosedPendingState() {
	pev.pendingStateClosed = true
}

// GetFinalValueBase returns the final value base.
func (pev *PendingExpectedValueV2) GetFinalValueBase() uint32 {
	return pev.finalValue.GetValueBase()
}

// IsClosed returns true if the pending state has been closed.
func (pev *PendingExpectedValueV2) IsClosed() bool {
	return pev.pendingStateClosed
}

// -------------------------------------------------------------------------------------------------
// ExpectedValueHelper - Helper functions for verification
// -------------------------------------------------------------------------------------------------

// ExpectedValueHelper provides utilities for verification.
type ExpectedValueHelper struct{}

// MustHaveNotExisted returns true if the key must have not existed during the entire read operation.
// This means the key was deleted before the read AND no write happened during the read.
func MustHaveNotExisted(preRead, postRead ExpectedValue) bool {
	preDeleted := preRead.IsDeleted()
	preValueBase := preRead.GetValueBase()
	postFinalValueBase := postRead.GetFinalValueBase()

	// Key was deleted before AND no write happened during the read
	noWriteDuring := preValueBase == postFinalValueBase
	return preDeleted && noWriteDuring
}

// MustHaveExisted returns true if the key must have existed during the entire read operation.
// This means the key existed before the read AND no delete happened during the read.
func MustHaveExisted(preRead, postRead ExpectedValue) bool {
	preNotDeleted := !preRead.IsDeleted()
	preDelCounter := preRead.GetDelCounter()
	postFinalDelCounter := postRead.GetFinalDelCounter()

	// Key existed before AND no delete happened during the read
	noDeleteDuring := preDelCounter == postFinalDelCounter
	return preNotDeleted && noDeleteDuring
}

// InExpectedValueBaseRange returns true if the value base falls within the expected range.
// This handles the case where the value base may have changed during the read.
func InExpectedValueBaseRange(valueBase uint32, preRead, postRead ExpectedValue) bool {
	if valueBase > valueBaseMask {
		return false // Invalid value base
	}

	preValueBase := preRead.GetValueBase()
	postFinalValueBase := postRead.GetFinalValueBase()

	if preValueBase <= postFinalValueBase {
		// Normal case: no wraparound
		return preValueBase <= valueBase && valueBase <= postFinalValueBase
	}
	// Wraparound case
	return valueBase <= postFinalValueBase || preValueBase <= valueBase
}

// -------------------------------------------------------------------------------------------------
// ExpectedStateV2 - Thread-safe expected state with per-key locking
// -------------------------------------------------------------------------------------------------

// ExpectedStateV2 tracks expected state with per-key locking for concurrent access.
// This is based on RocksDB's ExpectedState with SharedState key_locks_.
type ExpectedStateV2 struct {
	maxKey            int64
	numColumnFamilies int
	log2KeysPerLock   uint32

	// values[cf * maxKey + key] = atomic expected value
	values []atomic.Uint32

	// keyLocks[cf][key >> log2KeysPerLock] = mutex for key range
	keyLocks [][]sync.Mutex

	// Global sequence number
	persistedSeqno atomic.Uint64
}

// NewExpectedStateV2 creates a new ExpectedStateV2.
func NewExpectedStateV2(maxKey int64, numCFs int, log2KeysPerLock uint32) *ExpectedStateV2 {
	if numCFs <= 0 {
		numCFs = 1
	}
	if maxKey <= 0 {
		maxKey = 1
	}
	if log2KeysPerLock > 20 {
		log2KeysPerLock = 2 // Default to 4 keys per lock
	}

	totalSlots := maxKey * int64(numCFs)
	values := make([]atomic.Uint32, totalSlots)

	// Initialize all values to deleted state
	for i := range values {
		values[i].Store(deletedMask)
	}

	// Calculate number of locks per CF
	numLocks := maxKey >> log2KeysPerLock
	if maxKey&((1<<log2KeysPerLock)-1) != 0 {
		numLocks++ // Round up
	}

	// Create lock arrays
	keyLocks := make([][]sync.Mutex, numCFs)
	for cf := range numCFs {
		keyLocks[cf] = make([]sync.Mutex, numLocks)
	}

	return &ExpectedStateV2{
		maxKey:            maxKey,
		numColumnFamilies: numCFs,
		log2KeysPerLock:   log2KeysPerLock,
		values:            values,
		keyLocks:          keyLocks,
	}
}

// getIndex returns the index into the values array.
func (es *ExpectedStateV2) getIndex(cf int, key int64) int64 {
	if cf < 0 || cf >= es.numColumnFamilies {
		return -1
	}
	if key < 0 || key >= es.maxKey {
		return -1
	}
	return int64(cf)*es.maxKey + key
}

// GetMutexForKey returns the mutex for the given key.
func (es *ExpectedStateV2) GetMutexForKey(cf int, key int64) *sync.Mutex {
	if cf < 0 || cf >= es.numColumnFamilies {
		return nil
	}
	if key < 0 || key >= es.maxKey {
		return nil
	}
	lockIdx := key >> es.log2KeysPerLock
	return &es.keyLocks[cf][lockIdx]
}

// lockAllKeyLocks locks all key locks in a stable order.
//
// This is intentionally heavy-weight and is meant for crash-test oracle persistence.
// It ensures we never persist ExpectedValue entries with pending flags set mid-operation.
//
//nolint:unused // Reserved for crash-test oracle persistence feature
func (es *ExpectedStateV2) lockAllKeyLocks() {
	for cf := range len(es.keyLocks) {
		for i := range len(es.keyLocks[cf]) {
			es.keyLocks[cf][i].Lock()
		}
	}
}

//nolint:unused // Reserved for crash-test oracle persistence feature
func (es *ExpectedStateV2) unlockAllKeyLocks() {
	for cf := len(es.keyLocks) - 1; cf >= 0; cf-- {
		for i := len(es.keyLocks[cf]) - 1; i >= 0; i-- {
			es.keyLocks[cf][i].Unlock()
		}
	}
}

// Get returns the current expected value for a key (atomic read).
func (es *ExpectedStateV2) Get(cf int, key int64) ExpectedValue {
	idx := es.getIndex(cf, key)
	if idx < 0 {
		return NewExpectedValue() // Return deleted state
	}
	return ExpectedValue(es.values[idx].Load())
}

// PreparePut creates a pending put operation.
// The caller should hold the key lock.
func (es *ExpectedStateV2) PreparePut(cf int, key int64) *PendingExpectedValueV2 {
	idx := es.getIndex(cf, key)
	if idx < 0 {
		return nil
	}

	origValue := ExpectedValue(es.values[idx].Load())

	// Create final value (with pending write flag set now, will be cleared on commit)
	finalValue := origValue
	finalValue.Put(false) // Apply the put (increment value base, clear deleted)

	// Set pending write flag in the current value
	withPending := origValue
	withPending.SetPendingWrite()
	es.values[idx].Store(withPending.Raw())

	return NewPendingExpectedValueV2(&es.values[idx], origValue, finalValue)
}

// PrepareDelete creates a pending delete operation.
// The caller should hold the key lock.
func (es *ExpectedStateV2) PrepareDelete(cf int, key int64) *PendingExpectedValueV2 {
	idx := es.getIndex(cf, key)
	if idx < 0 {
		return nil
	}

	origValue := ExpectedValue(es.values[idx].Load())

	// Create final value
	finalValue := origValue
	finalValue.Delete(false) // Apply the delete (increment del counter, set deleted)

	// Set pending delete flag in the current value
	withPending := origValue
	withPending.SetPendingDelete()
	es.values[idx].Store(withPending.Raw())

	return NewPendingExpectedValueV2(&es.values[idx], origValue, finalValue)
}

// Exists returns true if the key is expected to exist.
func (es *ExpectedStateV2) Exists(cf int, key int64) bool {
	return es.Get(cf, key).Exists()
}

// GetValueBase returns the current value base for a key.
func (es *ExpectedStateV2) GetValueBase(cf int, key int64) uint32 {
	return es.Get(cf, key).GetValueBase()
}

// Clear resets all state to deleted.
func (es *ExpectedStateV2) Clear() {
	for i := range es.values {
		es.values[i].Store(deletedMask)
	}
	es.persistedSeqno.Store(0)
}

// ClearColumnFamily resets all keys in a column family to deleted.
func (es *ExpectedStateV2) ClearColumnFamily(cf int) {
	if cf < 0 || cf >= es.numColumnFamilies {
		return
	}

	start := int64(cf) * es.maxKey
	end := start + es.maxKey

	for i := start; i < end; i++ {
		es.values[i].Store(deletedMask)
	}
}

// SetPersistedSeqno sets the persisted sequence number.
func (es *ExpectedStateV2) SetPersistedSeqno(seqno uint64) {
	es.persistedSeqno.Store(seqno)
}

// GetPersistedSeqno returns the persisted sequence number.
func (es *ExpectedStateV2) GetPersistedSeqno() uint64 {
	return es.persistedSeqno.Load()
}

// MaxKey returns the maximum key.
func (es *ExpectedStateV2) MaxKey() int64 {
	return es.maxKey
}

// NumColumnFamilies returns the number of column families.
func (es *ExpectedStateV2) NumColumnFamilies() int {
	return es.numColumnFamilies
}

// ============================================================================
// Persistent ExpectedStateV2 Support
// ============================================================================

// persistentMagicV2 is the file format magic for ExpectedStateV2 (8 bytes).
const persistentMagicV2 = "EXSTATE2"

// SaveToFile saves the expected state to a file.
// This allows crash testing to persist state across process crashes.
func (es *ExpectedStateV2) SaveToFile(path string) error {
	// Calculate file size
	headerSize := 8 + 4 + 4 + 8 + 4 + 8 // magic(8) + version(4) + numCFs(4) + maxKey(8) + log2KeysPerLock(4) + seqno(8)
	dataSize := len(es.values) * 4
	totalSize := headerSize + dataSize

	data := make([]byte, totalSize)
	offset := 0

	// Magic (padded to 8 bytes)
	copy(data[offset:], persistentMagicV2)
	offset += 8

	// Version
	putUint32(data[offset:], persistentVersion)
	offset += 4

	// NumCFs
	putUint32(data[offset:], uint32(es.numColumnFamilies))
	offset += 4

	// MaxKey
	putUint64(data[offset:], uint64(es.maxKey))
	offset += 8

	// log2KeysPerLock
	putUint32(data[offset:], es.log2KeysPerLock)
	offset += 4

	// Seqno
	putUint64(data[offset:], es.persistedSeqno.Load())
	offset += 8

	// Data
	for i := range es.values {
		// Persist a conservative snapshot: pending flags represent in-flight operations.
		// If a key has a pending operation, we DON'T KNOW if the DB operation will complete
		// before the crash. To be safe:
		// - If pendingWriteMask is set: save as if the write will complete (clear deleted flag)
		// - If pendingDeleteMask is set: save as if the delete will complete (set deleted flag)
		// This ensures consistency: if the DB op completes before crash, we match.
		// If it doesn't, we'll have a false positive during verify, but that's safer than
		// a false negative (missing corruption).
		v := es.values[i].Load()
		ev := ExpectedValue(v)

		if ev.PendingWrite() {
			// Assume the pending write will complete: clear deleted flag
			v &^= (pendingWriteMask | pendingDeleteMask | deletedMask)
		} else if ev.PendingDelete() {
			// Assume the pending delete will complete: set deleted flag
			v &^= (pendingWriteMask | pendingDeleteMask)
			v |= deletedMask
		} else {
			// No pending operation, just strip the flags (they should be 0 anyway)
			v &^= (pendingWriteMask | pendingDeleteMask)
		}

		putUint32(data[offset:], v)
		offset += 4
	}

	return writeFileV2(path, data)
}

// LoadExpectedStateV2FromFile loads the expected state from a file.
// Returns a new ExpectedStateV2 with the loaded data.
func LoadExpectedStateV2FromFile(path string) (*ExpectedStateV2, error) {
	data, err := readFileBytes(path)
	if err != nil {
		return nil, err
	}

	if len(data) < 36 {
		return nil, errInvalidFileV2
	}

	offset := 0

	// Magic
	magic := string(data[offset : offset+8])
	if magic != persistentMagicV2 {
		return nil, errInvalidMagicV2
	}
	offset += 8

	// Version
	version := getUint32(data[offset:])
	if version != persistentVersion {
		return nil, errUnsupportedVersionV2
	}
	offset += 4

	// NumCFs
	numCFs := int(getUint32(data[offset:]))
	offset += 4

	// MaxKey
	maxKey := int64(getUint64(data[offset:]))
	offset += 8

	// log2KeysPerLock
	log2KeysPerLock := getUint32(data[offset:])
	offset += 4

	// Seqno
	seqno := getUint64(data[offset:])
	offset += 8

	// Calculate expected data size
	expectedDataSize := numCFs * int(maxKey) * 4
	if len(data)-offset != expectedDataSize {
		return nil, errInvalidFileV2
	}

	// Create state
	es := NewExpectedStateV2(maxKey, numCFs, log2KeysPerLock)
	es.persistedSeqno.Store(seqno)

	// Load data
	for i := range es.values {
		es.values[i].Store(getUint32(data[offset:]))
		offset += 4
	}

	return es, nil
}

// Errors for persistent expected state V2
var (
	errInvalidFileV2        = errors.New("expected_state_v2: invalid file format")
	errInvalidMagicV2       = errors.New("expected_state_v2: invalid magic number")
	errUnsupportedVersionV2 = errors.New("expected_state_v2: unsupported version")
)

// Helper functions for binary encoding
func putUint32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

func getUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func putUint64(b []byte, v uint64) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
}

func getUint64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

// writeFileV2 writes data atomically.
func writeFileV2(path string, data []byte) error {
	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}

	// Make the rename durable.
	// This matters for crash testing, where the process may be SIGKILLed at any point.
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	return nil
}
