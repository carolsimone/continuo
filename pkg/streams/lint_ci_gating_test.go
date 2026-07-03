package streams_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestLintCIModulesCoversWorkspace guards the CI lint gate against silent gaps.
// scripts/lint-go.sh --ci lints only the modules listed in
// scripts/lint-ci-modules.txt; that list must stay in lockstep with the
// authoritative module set — every go.work member plus the cli module, which is
// a separate module outside the workspace by design (see CLAUDE.md). Without
// this guard a newly added service would be linted locally by `make lint-go`
// yet silently skipped by CI until someone remembered to enroll it.
func TestLintCIModulesCoversWorkspace(t *testing.T) {
	root := repoRootFromTest(t)

	want := workspaceModules(t, root)
	want = append(want, "cli") // outside go.work by design
	sort.Strings(want)

	got := gatedModules(t, root)
	sort.Strings(got)

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("scripts/lint-ci-modules.txt is out of sync with the workspace.\n"+
			" gated: %v\n want : %v\n"+
			"Update scripts/lint-ci-modules.txt (and make sure the module is lint-clean: bash scripts/lint-go.sh <module>).",
			got, want)
	}
}

// workspaceModules returns the go.work member paths (e.g. "state", "tests/e2e").
func workspaceModules(t *testing.T, root string) []string {
	t.Helper()
	path := filepath.Join(root, "go.work")
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is built from repoRootFromTest() plus a fixed literal segment, not external input
	if err != nil {
		t.Fatal(err)
	}
	var mods []string
	inUse := false
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "use ("):
			inUse = true
		case inUse && line == ")":
			inUse = false
		case inUse && strings.HasPrefix(line, "./"):
			mods = append(mods, strings.TrimPrefix(line, "./"))
		case strings.HasPrefix(line, "use ./"):
			mods = append(mods, strings.TrimPrefix(line, "use ./"))
		}
	}
	if len(mods) == 0 {
		t.Fatal("no modules parsed from go.work")
	}
	return mods
}

// gatedModules returns the modules enrolled in the CI lint gate.
func gatedModules(t *testing.T, root string) []string {
	t.Helper()
	path := filepath.Join(root, "scripts", "lint-ci-modules.txt")
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is built from repoRootFromTest() plus fixed literal segments, not external input
	if err != nil {
		t.Fatal(err)
	}
	var mods []string
	for _, line := range strings.Split(string(data), "\n") {
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line != "" {
			mods = append(mods, line)
		}
	}
	return mods
}
