package main

import (
	"os"
	"strings"
)

// toolEnv builds an environment for invoking a C++ oracle tool (ldb/sst_dump),
// ensuring the tool directory is on the dynamic linker path and optionally adding
// a dependency library directory via ROCKSDB_DEPS_LIBDIR.
//
// On macOS this allows resolving libsnappy/lz4/zstd dylibs when they are not in a
// default search path. On Linux it similarly extends LD_LIBRARY_PATH.
func toolEnv(toolDir string) []string {
	env := os.Environ()

	depsDir := strings.TrimSpace(os.Getenv("ROCKSDB_DEPS_LIBDIR"))

	if toolDir != "" {
		env = append(env,
			"DYLD_LIBRARY_PATH="+joinPathList(toolDir, depsDir, os.Getenv("DYLD_LIBRARY_PATH")),
			"LD_LIBRARY_PATH="+joinPathList(toolDir, depsDir, os.Getenv("LD_LIBRARY_PATH")),
		)
	}

	return env
}

func joinPathList(parts ...string) string {
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, ":")
}
