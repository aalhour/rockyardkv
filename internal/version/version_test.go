package version

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
)

func TestVersionNew(t *testing.T) {
	v := NewVersion(nil, 1)

	if v.VersionNumber() != 1 {
		t.Errorf("VersionNumber() = %d, want 1", v.VersionNumber())
	}

	if v.TotalFiles() != 0 {
		t.Errorf("TotalFiles() = %d, want 0", v.TotalFiles())
	}

	if v.NumLevels() != MaxNumLevels {
		t.Errorf("NumLevels() = %d, want %d", v.NumLevels(), MaxNumLevels)
	}
}

func TestVersionRefUnref(t *testing.T) {
	v := NewVersion(nil, 1)

	v.Ref()
	v.Ref()
	v.Unref()
	v.Unref()

	// Should not panic or crash
}

func TestVersionNumFiles(t *testing.T) {
	v := NewVersion(nil, 1)

	// Empty version
	for level := range MaxNumLevels {
		if got := v.NumFiles(level); got != 0 {
			t.Errorf("NumFiles(%d) = %d, want 0", level, got)
		}
	}

	// Invalid levels
	if got := v.NumFiles(-1); got != 0 {
		t.Errorf("NumFiles(-1) = %d, want 0", got)
	}
	if got := v.NumFiles(MaxNumLevels); got != 0 {
		t.Errorf("NumFiles(%d) = %d, want 0", MaxNumLevels, got)
	}
}

func TestVersionNumLevelBytes(t *testing.T) {
	v := NewVersion(nil, 1)

	// Add some files
	v.files[0] = []*manifest.FileMetaData{
		{FD: manifest.FileDescriptor{FileSize: 100}},
		{FD: manifest.FileDescriptor{FileSize: 200}},
	}
	v.files[1] = []*manifest.FileMetaData{
		{FD: manifest.FileDescriptor{FileSize: 1000}},
	}

	if got := v.NumLevelBytes(0); got != 300 {
		t.Errorf("NumLevelBytes(0) = %d, want 300", got)
	}
	if got := v.NumLevelBytes(1); got != 1000 {
		t.Errorf("NumLevelBytes(1) = %d, want 1000", got)
	}
	if got := v.NumLevelBytes(2); got != 0 {
		t.Errorf("NumLevelBytes(2) = %d, want 0", got)
	}
}

func TestVersionTotalFiles(t *testing.T) {
	v := NewVersion(nil, 1)

	// Add files to different levels
	v.files[0] = []*manifest.FileMetaData{{}, {}}
	v.files[1] = []*manifest.FileMetaData{{}}
	v.files[3] = []*manifest.FileMetaData{{}, {}, {}}

	if got := v.TotalFiles(); got != 6 {
		t.Errorf("TotalFiles() = %d, want 6", got)
	}
}

func TestVersionFiles(t *testing.T) {
	v := NewVersion(nil, 1)

	// Add files
	files := []*manifest.FileMetaData{
		{FD: manifest.NewFileDescriptor(1, 0, 100)},
		{FD: manifest.NewFileDescriptor(2, 0, 200)},
	}
	v.files[0] = files

	got := v.Files(0)
	if len(got) != 2 {
		t.Errorf("Files(0) length = %d, want 2", len(got))
	}

	// Invalid level
	if got := v.Files(-1); got != nil {
		t.Errorf("Files(-1) = %v, want nil", got)
	}
	if got := v.Files(MaxNumLevels); got != nil {
		t.Errorf("Files(%d) = %v, want nil", MaxNumLevels, got)
	}
}

func TestCompareInternalKey(t *testing.T) {
	tests := []struct {
		name string
		a    []byte
		b    []byte
		want int // -1, 0, or 1
	}{
		{
			name: "equal keys",
			a:    makeInternalKey("key", 100, 1),
			b:    makeInternalKey("key", 100, 1),
			want: 0,
		},
		{
			name: "different user keys, a < b",
			a:    makeInternalKey("aaa", 100, 1),
			b:    makeInternalKey("bbb", 100, 1),
			want: -1,
		},
		{
			name: "different user keys, a > b",
			a:    makeInternalKey("bbb", 100, 1),
			b:    makeInternalKey("aaa", 100, 1),
			want: 1,
		},
		{
			name: "same user key, higher seq first (a has higher seq)",
			a:    makeInternalKey("key", 200, 1),
			b:    makeInternalKey("key", 100, 1),
			want: -1, // Higher seq comes first
		},
		{
			name: "same user key, lower seq second",
			a:    makeInternalKey("key", 100, 1),
			b:    makeInternalKey("key", 200, 1),
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareInternalKey(tt.a, tt.b)
			if (got < 0 && tt.want >= 0) || (got > 0 && tt.want <= 0) || (got == 0 && tt.want != 0) {
				t.Errorf("compareInternalKey() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBytesCompare(t *testing.T) {
	tests := []struct {
		a, b []byte
		want int
	}{
		{[]byte{}, []byte{}, 0},
		{[]byte{1}, []byte{}, 1},
		{[]byte{}, []byte{1}, -1},
		{[]byte{1, 2, 3}, []byte{1, 2, 3}, 0},
		{[]byte{1, 2, 3}, []byte{1, 2, 4}, -1},
		{[]byte{1, 2, 4}, []byte{1, 2, 3}, 1},
		{[]byte{1, 2}, []byte{1, 2, 3}, -1},
		{[]byte{1, 2, 3}, []byte{1, 2}, 1},
	}

	for _, tt := range tests {
		got := bytesCompare(tt.a, tt.b)
		if (got < 0 && tt.want >= 0) || (got > 0 && tt.want <= 0) || (got == 0 && tt.want != 0) {
			t.Errorf("bytesCompare(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

// makeInternalKey creates an internal key from user key, sequence number, and value type.
func makeInternalKey(userKey string, seq uint64, vtype uint8) []byte {
	key := make([]byte, len(userKey)+8)
	copy(key, userKey)
	trailer := (seq << 8) | uint64(vtype)
	for i := range 8 {
		key[len(userKey)+i] = byte(trailer >> (8 * i))
	}
	return key
}
