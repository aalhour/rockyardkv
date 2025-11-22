package db

import (
	"bytes"
	"testing"
)

func TestFixedPrefixExtractor(t *testing.T) {
	tests := []struct {
		name      string
		prefixLen int
		key       []byte
		wantIn    bool
		wantPfx   []byte
	}{
		{"exact_length", 4, []byte("test"), true, []byte("test")},
		{"longer_key", 4, []byte("testing"), true, []byte("test")},
		{"shorter_key", 4, []byte("abc"), false, []byte("abc")},
		{"empty_key", 4, []byte{}, false, []byte{}},
		{"prefix_len_1", 1, []byte("abc"), true, []byte("a")},
		{"prefix_len_0_defaults_to_1", 0, []byte("abc"), true, []byte("a")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewFixedPrefixExtractor(tt.prefixLen)

			if e.Name() != "rocksdb.FixedPrefix" {
				t.Errorf("Name() = %q, want %q", e.Name(), "rocksdb.FixedPrefix")
			}

			gotIn := e.InDomain(tt.key)
			if gotIn != tt.wantIn {
				t.Errorf("InDomain(%q) = %v, want %v", tt.key, gotIn, tt.wantIn)
			}

			gotPfx := e.Transform(tt.key)
			if !bytes.Equal(gotPfx, tt.wantPfx) {
				t.Errorf("Transform(%q) = %q, want %q", tt.key, gotPfx, tt.wantPfx)
			}
		})
	}
}

func TestCappedPrefixExtractor(t *testing.T) {
	tests := []struct {
		name    string
		capLen  int
		key     []byte
		wantPfx []byte
	}{
		{"longer_key", 4, []byte("testing"), []byte("test")},
		{"exact_length", 4, []byte("test"), []byte("test")},
		{"shorter_key", 4, []byte("abc"), []byte("abc")},
		{"empty_key", 4, []byte{}, []byte{}},
		{"cap_len_1", 1, []byte("abc"), []byte("a")},
		{"cap_len_0_defaults_to_1", 0, []byte("abc"), []byte("a")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewCappedPrefixExtractor(tt.capLen)

			if e.Name() != "rocksdb.CappedPrefix" {
				t.Errorf("Name() = %q, want %q", e.Name(), "rocksdb.CappedPrefix")
			}

			// CappedPrefix always returns true for InDomain
			if !e.InDomain(tt.key) {
				t.Errorf("InDomain(%q) = false, want true", tt.key)
			}

			gotPfx := e.Transform(tt.key)
			if !bytes.Equal(gotPfx, tt.wantPfx) {
				t.Errorf("Transform(%q) = %q, want %q", tt.key, gotPfx, tt.wantPfx)
			}
		})
	}
}

func TestNoopPrefixExtractor(t *testing.T) {
	e := NewNoopPrefixExtractor()

	if e.Name() != "rocksdb.Noop" {
		t.Errorf("Name() = %q, want %q", e.Name(), "rocksdb.Noop")
	}

	keys := [][]byte{
		{},
		[]byte("a"),
		[]byte("testing"),
		[]byte("very_long_key_with_lots_of_characters"),
	}

	for _, key := range keys {
		if !e.InDomain(key) {
			t.Errorf("InDomain(%q) = false, want true", key)
		}
		gotPfx := e.Transform(key)
		if !bytes.Equal(gotPfx, key) {
			t.Errorf("Transform(%q) = %q, want %q", key, gotPfx, key)
		}
	}
}

func TestPrefixExtractorInterface(t *testing.T) {
	// Verify all implementations satisfy the interface
	extractors := []PrefixExtractor{
		NewFixedPrefixExtractor(4),
		NewCappedPrefixExtractor(4),
		NewNoopPrefixExtractor(),
	}

	for _, e := range extractors {
		// Just verify they implement the interface
		_ = e.Name()
		_ = e.Transform([]byte("test"))
		_ = e.InDomain([]byte("test"))
	}
}

func TestPrefixExtractorWithUserKeys(t *testing.T) {
	// Test prefix extraction with realistic user key patterns
	e := NewFixedPrefixExtractor(8) // user: prefix

	type testCase struct {
		key    string
		wantIn bool
	}

	tests := []testCase{
		{"user:123:name", true},
		{"user:123:email", true},
		{"user:456:name", true},
		{"order:1", false}, // shorter than prefix len
		{"customer:99:id", true},
	}

	for _, tt := range tests {
		gotIn := e.InDomain([]byte(tt.key))
		if gotIn != tt.wantIn {
			t.Errorf("InDomain(%q) = %v, want %v", tt.key, gotIn, tt.wantIn)
		}

		if gotIn {
			pfx := e.Transform([]byte(tt.key))
			if len(pfx) != 8 {
				t.Errorf("Transform(%q) len = %d, want 8", tt.key, len(pfx))
			}
		}
	}

	// Keys with same prefix should extract the same prefix
	key1 := []byte("user:123:name")
	key2 := []byte("user:123:email")
	pfx1 := e.Transform(key1)
	pfx2 := e.Transform(key2)
	if !bytes.Equal(pfx1, pfx2) {
		t.Errorf("Transform(%q) = %q, Transform(%q) = %q, want equal prefixes",
			key1, pfx1, key2, pfx2)
	}
}
