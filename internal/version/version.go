// Package version reports what this binary is, for the control-plane handshake
// and the health endpoint. The value comes from the VCS stamp the Go toolchain
// embeds automatically, so nothing has to be threaded through the build flags.
package version

import (
	"runtime/debug"
	"sync"
)

const unknown = "unknown"

var (
	once   sync.Once
	cached string
)

// String returns a short build identifier: the commit the binary was built
// from, suffixed with "+dirty" when the tree had uncommitted changes, or
// "unknown" when the build carried no VCS stamp.
func String() string {
	once.Do(func() { cached = read() })
	return cached
}

func read() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return unknown
	}
	rev, modified := "", false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	if rev == "" {
		return unknown
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if modified {
		return rev + "+dirty"
	}
	return rev
}
