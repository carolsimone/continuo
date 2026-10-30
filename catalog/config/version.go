package config

import "runtime/debug"

// Version is the version of the catalog build that runs, as an export's
// header names it: the main module's version when the build stamped one,
// otherwise the VCS revision it was built from (with "+dirty" when the tree
// was modified), otherwise "unknown".
func Version() string {
	info, ok := debug.ReadBuildInfo()
	return versionOf(info, ok)
}

func versionOf(info *debug.BuildInfo, ok bool) string {
	if !ok || info == nil {
		return "unknown"
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	var revision, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if revision == "" {
		return "unknown"
	}
	if modified == "true" {
		return revision + "+dirty"
	}
	return revision
}
