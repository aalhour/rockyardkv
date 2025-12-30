package campaign

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// KnownFailure represents a previously seen failure fingerprint.
type KnownFailure struct {
	Fingerprint string `json:"fingerprint"`
	Instance    string `json:"instance"`
	FirstSeen   string `json:"first_seen"`
	Count       int    `json:"count"`
	Description string `json:"description,omitempty"`
}

// KnownFailures tracks failure fingerprints for deduplication.
type KnownFailures struct {
	mu       sync.RWMutex
	failures map[string]*KnownFailure
	path     string
}

// NewKnownFailures creates a new known failures tracker.
// If path is non-empty, failures are persisted to disk.
func NewKnownFailures(path string) *KnownFailures {
	kf := &KnownFailures{
		failures: make(map[string]*KnownFailure),
		path:     path,
	}
	if path != "" {
		kf.load()
	}
	return kf
}

// load reads known failures from disk.
func (kf *KnownFailures) load() {
	data, err := os.ReadFile(kf.path)
	if err != nil {
		return // File doesn't exist yet
	}

	var failures []*KnownFailure
	if err := json.Unmarshal(data, &failures); err != nil {
		return
	}

	for _, f := range failures {
		kf.failures[f.Fingerprint] = f
	}
}

// save writes known failures to disk.
func (kf *KnownFailures) save() error {
	if kf.path == "" {
		return nil
	}

	failures := make([]*KnownFailure, 0, len(kf.failures))
	for _, f := range kf.failures {
		failures = append(failures, f)
	}

	data, err := json.MarshalIndent(failures, "", "  ")
	if err != nil {
		return err
	}

	// Ensure directory exists
	if dir := filepath.Dir(kf.path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	return os.WriteFile(kf.path, data, 0o644)
}

// IsDuplicate returns true if the fingerprint has been seen before.
func (kf *KnownFailures) IsDuplicate(fingerprint string) bool {
	kf.mu.RLock()
	defer kf.mu.RUnlock()
	_, exists := kf.failures[fingerprint]
	return exists
}

// Record adds or updates a failure fingerprint.
// Returns true if this is a new (not duplicate) failure.
func (kf *KnownFailures) Record(fingerprint, instance, timestamp string) bool {
	kf.mu.Lock()
	defer kf.mu.Unlock()

	if existing, ok := kf.failures[fingerprint]; ok {
		existing.Count++
		_ = kf.save()
		return false // duplicate
	}

	kf.failures[fingerprint] = &KnownFailure{
		Fingerprint: fingerprint,
		Instance:    instance,
		FirstSeen:   timestamp,
		Count:       1,
	}
	_ = kf.save()
	return true // new failure
}

// Count returns the number of known failure fingerprints.
func (kf *KnownFailures) Count() int {
	kf.mu.RLock()
	defer kf.mu.RUnlock()
	return len(kf.failures)
}

// All returns all known failures.
func (kf *KnownFailures) All() []*KnownFailure {
	kf.mu.RLock()
	defer kf.mu.RUnlock()

	result := make([]*KnownFailure, 0, len(kf.failures))
	for _, f := range kf.failures {
		result = append(result, f)
	}
	return result
}
