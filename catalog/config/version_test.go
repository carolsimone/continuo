package config

import (
	"runtime/debug"
	"testing"
)

// The version an export's header names comes from the build: the main
// module's version when the build stamped one, otherwise the VCS revision
// (marked when the tree was modified), otherwise "unknown".
func TestVersion_ComesFromTheBuild(t *testing.T) {
	setting := func(pairs ...[2]string) []debug.BuildSetting {
		out := make([]debug.BuildSetting, 0, len(pairs))
		for _, kv := range pairs {
			out = append(out, debug.BuildSetting{Key: kv[0], Value: kv[1]})
		}
		return out
	}
	cases := []struct {
		name string
		info *debug.BuildInfo
		ok   bool
		want string
	}{
		{"no build info", nil, false, "unknown"},
		{"a module version", &debug.BuildInfo{Main: debug.Module{Version: "v0.16.0"}, Settings: setting([2]string{"vcs.revision", "abc123"})}, true, "v0.16.0"},
		{"a development build at a revision", &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}, Settings: setting([2]string{"vcs.revision", "abc123"}, [2]string{"vcs.modified", "false"})}, true, "abc123"},
		{"a modified tree", &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}, Settings: setting([2]string{"vcs.revision", "abc123"}, [2]string{"vcs.modified", "true"})}, true, "abc123+dirty"},
		{"a development build without VCS", &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}}, true, "unknown"},
		{"no version and no VCS", &debug.BuildInfo{}, true, "unknown"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := versionOf(c.info, c.ok); got != c.want {
				t.Fatalf("versionOf = %q, want %q", got, c.want)
			}
		})
	}
	if Version() == "" {
		t.Fatal("Version() is empty")
	}
}
