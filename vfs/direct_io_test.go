// Package vfs tests for Direct I/O functionality.
//
// Reference: RocksDB v10.7.5
//   env/env_test.cc - Tests for Direct I/O functionality (lines 1112-2098)

package vfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestAlignmentHelpers(t *testing.T) {
	tests := []struct {
		name      string
		value     int
		alignment int
		isAligned bool
		alignUp   int
		alignDown int
	}{
		{
			name:      "aligned value",
			value:     4096,
			alignment: 4096,
			isAligned: true,
			alignUp:   4096,
			alignDown: 4096,
		},
		{
			name:      "unaligned value - small",
			value:     100,
			alignment: 4096,
			isAligned: false,
			alignUp:   4096,
			alignDown: 0,
		},
		{
			name:      "unaligned value - large",
			value:     5000,
			alignment: 4096,
			isAligned: false,
			alignUp:   8192,
			alignDown: 4096,
		},
		{
			name:      "zero value",
			value:     0,
			alignment: 4096,
			isAligned: true,
			alignUp:   0,
			alignDown: 0,
		},
		{
			name:      "512 byte alignment",
			value:     1024,
			alignment: 512,
			isAligned: true,
			alignUp:   1024,
			alignDown: 1024,
		},
		{
			name:      "zero alignment",
			value:     100,
			alignment: 0,
			isAligned: true,
			alignUp:   100,
			alignDown: 100,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsAligned(tc.value, tc.alignment); got != tc.isAligned {
				t.Errorf("IsAligned(%d, %d) = %v, want %v", tc.value, tc.alignment, got, tc.isAligned)
			}
			if got := AlignUp(tc.value, tc.alignment); got != tc.alignUp {
				t.Errorf("AlignUp(%d, %d) = %d, want %d", tc.value, tc.alignment, got, tc.alignUp)
			}
			if got := AlignDown(tc.value, tc.alignment); got != tc.alignDown {
				t.Errorf("AlignDown(%d, %d) = %d, want %d", tc.value, tc.alignment, got, tc.alignDown)
			}
		})
	}
}

func TestAlignedBuffer(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		buf := NewAlignedBuffer(4096, 4096)

		if buf.Len() != 4096 {
			t.Errorf("Len() = %d, want 4096", buf.Len())
		}
		if buf.Cap() != 4096 {
			t.Errorf("Cap() = %d, want 4096", buf.Cap())
		}
		if buf.Alignment() != 4096 {
			t.Errorf("Alignment() = %d, want 4096", buf.Alignment())
		}
	})

	t.Run("default alignment", func(t *testing.T) {
		buf := NewAlignedBuffer(1024, 0)

		if buf.Alignment() != DefaultBlockSize {
			t.Errorf("Alignment() = %d, want %d", buf.Alignment(), DefaultBlockSize)
		}
	})

	t.Run("resize", func(t *testing.T) {
		buf := NewAlignedBuffer(4096, 4096)

		if err := buf.Resize(2048); err != nil {
			t.Fatalf("Resize() failed: %v", err)
		}
		if buf.Len() != 2048 {
			t.Errorf("Len() = %d, want 2048", buf.Len())
		}

		// Resize beyond capacity should fail
		if err := buf.Resize(8192); err == nil {
			t.Error("Resize() should fail when exceeding capacity")
		}
	})

	t.Run("clear", func(t *testing.T) {
		buf := NewAlignedBuffer(4096, 4096)
		buf.Clear()

		if buf.Len() != 0 {
			t.Errorf("Len() = %d, want 0 after Clear()", buf.Len())
		}
	})
}

func TestDirectIOHelper(t *testing.T) {
	h := NewDirectIOHelper(4096)

	t.Run("alignment check", func(t *testing.T) {
		if !h.IsOffsetAndLengthAligned(0, 4096) {
			t.Error("IsOffsetAndLengthAligned(0, 4096) should return true")
		}
		if !h.IsOffsetAndLengthAligned(4096, 8192) {
			t.Error("IsOffsetAndLengthAligned(4096, 8192) should return true")
		}
		if h.IsOffsetAndLengthAligned(100, 4096) {
			t.Error("IsOffsetAndLengthAligned(100, 4096) should return false")
		}
		if h.IsOffsetAndLengthAligned(4096, 100) {
			t.Error("IsOffsetAndLengthAligned(4096, 100) should return false")
		}
	})

	t.Run("align offset", func(t *testing.T) {
		if got := h.AlignOffset(5000); got != 4096 {
			t.Errorf("AlignOffset(5000) = %d, want 4096", got)
		}
		if got := h.AlignOffset(0); got != 0 {
			t.Errorf("AlignOffset(0) = %d, want 0", got)
		}
	})

	t.Run("align length", func(t *testing.T) {
		if got := h.AlignLength(5000); got != 8192 {
			t.Errorf("AlignLength(5000) = %d, want 8192", got)
		}
		if got := h.AlignLength(4096); got != 4096 {
			t.Errorf("AlignLength(4096) = %d, want 4096", got)
		}
	})

	t.Run("finished tracking", func(t *testing.T) {
		h2 := NewDirectIOHelper(4096)
		h2.AddFinished(1000)
		h2.AddFinished(2000)
		if got := h2.Finished(); got != 3000 {
			t.Errorf("Finished() = %d, want 3000", got)
		}
	})
}

func TestFileOptions(t *testing.T) {
	t.Run("valid options", func(t *testing.T) {
		opts := FileOptions{
			UseDirectReads:  true,
			UseDirectWrites: true,
		}
		if err := opts.Validate(); err != nil {
			t.Errorf("Validate() failed: %v", err)
		}
	})

	t.Run("invalid - direct reads with mmap reads", func(t *testing.T) {
		opts := FileOptions{
			UseDirectReads: true,
			UseMmapReads:   true,
		}
		if err := opts.Validate(); err == nil {
			t.Error("Validate() should fail with both direct and mmap reads")
		}
	})

	t.Run("invalid - direct writes with mmap writes", func(t *testing.T) {
		opts := FileOptions{
			UseDirectWrites: true,
			UseMmapWrites:   true,
		}
		if err := opts.Validate(); err == nil {
			t.Error("Validate() should fail with both direct and mmap writes")
		}
	})

	t.Run("get block size", func(t *testing.T) {
		opts := FileOptions{}
		if got := opts.GetBlockSize(); got != DefaultBlockSize {
			t.Errorf("GetBlockSize() = %d, want %d", got, DefaultBlockSize)
		}

		opts.BlockSize = 512
		if got := opts.GetBlockSize(); got != 512 {
			t.Errorf("GetBlockSize() = %d, want 512", got)
		}
	})
}

func TestDirectIOFS(t *testing.T) {
	dir := t.TempDir()
	fs := NewDirectIOFS()

	t.Run("is direct IO supported", func(t *testing.T) {
		// This should return true on Linux/macOS
		supported := fs.IsDirectIOSupported()
		t.Logf("Direct I/O supported: %v", supported)
	})

	t.Run("get block size", func(t *testing.T) {
		bs := fs.GetBlockSize(dir)
		if bs <= 0 {
			t.Errorf("GetBlockSize() = %d, want > 0", bs)
		}
		t.Logf("Block size for %s: %d", dir, bs)
	})

	t.Run("create and write with direct IO", func(t *testing.T) {
		if !fs.IsDirectIOSupported() {
			t.Skip("Direct I/O not supported on this platform")
		}

		path := filepath.Join(dir, "direct_write_test.dat")
		opts := FileOptions{
			UseDirectWrites: true,
		}

		f, err := fs.CreateWithOptions(path, opts)
		if err != nil {
			t.Fatalf("CreateWithOptions() failed: %v", err)
		}
		defer f.Close()

		if !f.UseDirectIO() {
			t.Error("UseDirectIO() should return true")
		}

		alignment := f.GetRequiredBufferAlignment()
		if alignment < 512 {
			t.Errorf("GetRequiredBufferAlignment() = %d, want >= 512", alignment)
		}

		// Write aligned data
		data := make([]byte, 4096)
		for i := range data {
			data[i] = byte(i % 256)
		}

		n, err := f.Write(data)
		if err != nil {
			t.Fatalf("Write() failed: %v", err)
		}
		if n != len(data) {
			t.Errorf("Write() = %d, want %d", n, len(data))
		}

		if err := f.Sync(); err != nil {
			t.Fatalf("Sync() failed: %v", err)
		}
	})

	t.Run("read with direct IO", func(t *testing.T) {
		if !fs.IsDirectIOSupported() {
			t.Skip("Direct I/O not supported on this platform")
		}

		// First write a file normally
		path := filepath.Join(dir, "direct_read_test.dat")
		data := make([]byte, 4096)
		for i := range data {
			data[i] = byte(i % 256)
		}
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatalf("WriteFile() failed: %v", err)
		}

		// Read it back with Direct I/O
		opts := FileOptions{
			UseDirectReads: true,
		}

		f, err := fs.OpenWithOptions(path, opts)
		if err != nil {
			t.Fatalf("OpenWithOptions() failed: %v", err)
		}
		defer f.Close()

		if !f.UseDirectIO() {
			t.Error("UseDirectIO() should return true")
		}

		readBuf := make([]byte, 4096)
		n, err := f.Read(readBuf)
		if err != nil {
			t.Fatalf("Read() failed: %v", err)
		}
		if n != len(data) {
			t.Errorf("Read() = %d, want %d", n, len(data))
		}

		if !bytes.Equal(readBuf, data) {
			t.Error("Read data doesn't match written data")
		}
	})

	t.Run("random access with direct IO", func(t *testing.T) {
		if !fs.IsDirectIOSupported() {
			t.Skip("Direct I/O not supported on this platform")
		}

		// First write a file normally
		path := filepath.Join(dir, "direct_random_test.dat")
		data := make([]byte, 8192)
		for i := range data {
			data[i] = byte(i % 256)
		}
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatalf("WriteFile() failed: %v", err)
		}

		// Read it back with Direct I/O random access
		opts := FileOptions{
			UseDirectReads: true,
		}

		f, err := fs.OpenRandomAccessWithOptions(path, opts)
		if err != nil {
			t.Fatalf("OpenRandomAccessWithOptions() failed: %v", err)
		}
		defer f.Close()

		if !f.UseDirectIO() {
			t.Error("UseDirectIO() should return true")
		}

		// Read from offset 4096
		readBuf := make([]byte, 4096)
		n, err := f.ReadAt(readBuf, 4096)
		if err != nil {
			t.Fatalf("ReadAt() failed: %v", err)
		}
		if n != 4096 {
			t.Errorf("ReadAt() = %d, want 4096", n)
		}

		if !bytes.Equal(readBuf, data[4096:8192]) {
			t.Error("Read data doesn't match expected data at offset 4096")
		}
	})

	t.Run("fallback to normal IO", func(t *testing.T) {
		path := filepath.Join(dir, "normal_io_test.dat")
		opts := FileOptions{
			UseDirectWrites: false,
			UseDirectReads:  false,
		}

		f, err := fs.CreateWithOptions(path, opts)
		if err != nil {
			t.Fatalf("CreateWithOptions() failed: %v", err)
		}

		if f.UseDirectIO() {
			t.Error("UseDirectIO() should return false for normal I/O")
		}

		f.Close()
	})
}

func TestWrapWithDirectIO(t *testing.T) {
	t.Run("wrap non-DirectIOFS", func(t *testing.T) {
		fs := Default()
		dfs := WrapWithDirectIO(fs)

		// Should not support Direct I/O (it's a fallback wrapper)
		if dfs.IsDirectIOSupported() {
			t.Error("IsDirectIOSupported() should return false for wrapped FS")
		}
	})

	t.Run("wrap DirectIOFS returns same", func(t *testing.T) {
		dfs := NewDirectIOFS()
		wrapped := WrapWithDirectIO(dfs)

		// Should be the same object
		if wrapped != dfs {
			t.Error("WrapWithDirectIO() should return the same DirectIOFS")
		}
	})
}

// Benchmark aligned vs unaligned operations
func BenchmarkAlignUp(b *testing.B) {
	for b.Loop() {
		AlignUp(5000, 4096)
	}
}

func BenchmarkAlignDown(b *testing.B) {
	for b.Loop() {
		AlignDown(5000, 4096)
	}
}

func BenchmarkIsAligned(b *testing.B) {
	for b.Loop() {
		IsAligned(5000, 4096)
	}
}
