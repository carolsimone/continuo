package config

import (
	"runtime/debug"
	"testing"
)

// The version an export's header names comes from the build: the main
// module's version when the build stamped one, otherwise the VCS revision
// (marked when the tree was modified), otherwise "unknown".
func TestVersion_ComesFromTheBuild(t *testing.T) {
	setting := func(kv ...string) []debug.BuildSetting {
		var out []debug.BuildSetting
		for i := 0; i < len(kv); i += 2 {
			out = append(out, debug.BuildSetting{Key: kv[i], Value: kv[i+1]})
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
		{"a module version", &debug.BuildInfo{Main: debug.Module{Version: "v0.16.0"}, Settings: setting("vcs.revision", "abc123")}, true, "v0.16.0"},
		{"a development build at a revision", &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}, Settings: setting("vcs.revision", "abc123", "vcs.modified", "false")}, true, "abc123"},
		{"a modified tree", &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}, Settings: setting("vcs.revision", "abc123", "vcs.modified", "true")}, true, "abc123+dirty"},
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
