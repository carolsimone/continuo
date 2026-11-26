package deploycheck

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// fixtureRepo builds a throwaway git repository holding list as its
// retired-names.yaml and each of files (path → content), all tracked.
func fixtureRepo(t *testing.T, list string, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	write := func(rel, content string) {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(retiredNamesFile, list)
	for rel, content := range files {
		write(rel, content)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "-A"}} {
		cmd := exec.Command("git", append([]string{"-C", dir, "-c", "init.defaultBranch=main"}, args...)...) //nolint:gosec // G204: git with fixed arguments over the test's own temporary directory
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	return dir
}

// findingsIn runs the guard over a fixture repository and returns each
// finding as the line the test reports.
func findingsIn(t *testing.T, list string, files map[string]string) []string {
	t.Helper()
	found, err := checkRetiredNames(fixtureRepo(t, list, files))
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, f := range found {
		lines = append(lines, f.String())
	}
	return lines
}

func assertFindings(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("findings:\n  got  %q\n  want %q", got, want)
	}
}

const frobnicateList = `names:
  - pattern: frobnicate
    use: widget
`

func TestRetiredNames_EmptyListPasses(t *testing.T) {
	got := findingsIn(t, "names: []\n", map[string]string{"src/a.go": "frobnicate()\n"})
	assertFindings(t, got)
}

func TestRetiredNames_PlantedNameFailsNamingFileLineNameAndWord(t *testing.T) {
	got := findingsIn(t, frobnicateList, map[string]string{
		"src/a.go": "package a\n\nfunc f() { frobnicate() }\n",
	})
	assertFindings(t, got, `src/a.go:3: "frobnicate" is a retired name; use "widget"`)
}

func TestRetiredNames_RespectsWordBoundaries(t *testing.T) {
	got := findingsIn(t, frobnicateList, map[string]string{
		"src/words.txt": strings.Join([]string{
			"frobnicated",      // a longer word
			"refrobnicate",     // a longer word
			"frobnicate_all",   // snake_case identifier
			"frobnicateAll",    // camelCase identifier
			"Frobnicate",       // another case
			"run frobnicate-x", // hyphenated: matches
			"(frobnicate).",    // punctuation: matches
			"a.frobnicate b",   // dotted: matches
			"frobnicate\r",     // CRLF line ending: matches
		}, "\n"),
	})
	assertFindings(t, got,
		`src/words.txt:6: "frobnicate" is a retired name; use "widget"`,
		`src/words.txt:7: "frobnicate" is a retired name; use "widget"`,
		`src/words.txt:8: "frobnicate" is a retired name; use "widget"`,
		`src/words.txt:9: "frobnicate" is a retired name; use "widget"`,
	)
}

func TestRetiredNames_CaseInsensitivePatternAndAlternation(t *testing.T) {
	list := `names:
  - pattern: (?i)gizmo runs?|gizmo_id
    use: gadget run
`
	got := findingsIn(t, list, map[string]string{
		"src/a.md": "A Gizmo run and two GIZMO RUNS, a gizmo_id, a gizmo runner.\n",
	})
	assertFindings(t, got,
		`src/a.md:1: "Gizmo run" is a retired name; use "gadget run"`,
		`src/a.md:1: "GIZMO RUNS" is a retired name; use "gadget run"`,
		`src/a.md:1: "gizmo_id" is a retired name; use "gadget run"`,
	)
}

func TestRetiredNames_FindingsOfOneLineInColumnOrder(t *testing.T) {
	list := `names:
  - pattern: zeta
    use: last
  - pattern: alpha
    use: first
`
	got := findingsIn(t, list, map[string]string{"a.txt": "alpha zeta alpha\n"})
	assertFindings(t, got,
		`a.txt:1: "alpha" is a retired name; use "first"`,
		`a.txt:1: "zeta" is a retired name; use "last"`,
		`a.txt:1: "alpha" is a retired name; use "first"`,
	)
}

func TestRetiredNames_GlossaryAvoidLinePasses(t *testing.T) {
	got := findingsIn(t, frobnicateList, map[string]string{
		"GLOSSARY.md": "**Widget**:\nA widget.\n_Avoid_: frobnicate, gizmo\n",
		"catalog/GLOSSARY.md": "**Widget**:\n_Avoid_: frobnicate\n" +
			"Still a frobnicate here.\n",
		"docs/notes.md": "_Avoid_: frobnicate\n",
	})
	assertFindings(t, got,
		// A glossary's other lines are checked.
		`catalog/GLOSSARY.md:3: "frobnicate" is a retired name; use "widget"`,
		// An _Avoid_: line outside a glossary is checked.
		`docs/notes.md:1: "frobnicate" is a retired name; use "widget"`,
	)
}

func TestRetiredNames_ReleasedChangelogSectionPasses(t *testing.T) {
	changelog := strings.Join([]string{
		"# Changelog",
		"Intro naming frobnicate.", // line 2: before any section, checked
		"## [Unreleased]",
		"### Breaking",
		"- frobnicate is now widget.", // line 5: unreleased, checked
		"## [0.2.0] - 2026-11-17",
		"- Added frobnicate.", // released, exempt
		"### Changed",
		"- frobnicate again.", // released, exempt
		"## [0.1.0] - 2026-11-01",
		"- frobnicate.", // released, exempt
	}, "\n")
	notes := strings.Join([]string{
		"# Release notes",
		"About frobnicate.", // line 2: checked
		"## 0.2.0 · 2026-11-17 · `MINOR`",
		"frobnicate shipped.", // released, exempt
	}, "\n")
	got := findingsIn(t, frobnicateList, map[string]string{
		"deploy/continuo/CHANGELOG.md": changelog,
		"docs/release-notes.md":        notes,
		// The same headings in any other file exempt nothing.
		"docs/other.md": "## [0.2.0] - 2026-11-17\nfrobnicate\n",
	})
	assertFindings(t, got,
		`deploy/continuo/CHANGELOG.md:2: "frobnicate" is a retired name; use "widget"`,
		`deploy/continuo/CHANGELOG.md:5: "frobnicate" is a retired name; use "widget"`,
		`docs/other.md:2: "frobnicate" is a retired name; use "widget"`,
		`docs/release-notes.md:2: "frobnicate" is a retired name; use "widget"`,
	)
}

func TestRetiredNames_PathLimitedEntryFailsOnlyInsideItsPaths(t *testing.T) {
	list := `names:
  - pattern: attempt
    use: try
    paths: [agent-remediation, "ui/src/**/remediation*", docs/one.md]
`
	text := "an attempt\n"
	got := findingsIn(t, list, map[string]string{
		"agent-remediation/service/runner/runner.go":  text, // under a directory path
		"ui/src/client/pages/remediation-tab.tsx":     text, // a glob over the file
		"ui/src/client/pages/remediation/view.tsx":    text, // a glob over its directory
		"ui/src/remediation.ts":                       text, // ** matches no segment too
		"docs/one.md":                                 text, // a file path
		"run-controller/service/attempt.go":           text, // outside every path
		"agent-remediation-extra/a.go":                text, // a longer directory name
		"ui/src/client/pages/runs/remediation-x/a.md": text, // a glob over a parent directory
		"ui/tests/remediation.test.ts":                text, // outside the glob's root
		"docs/one.md.bak":                             text, // a longer file name
	})
	assertFindings(t, got,
		`agent-remediation/service/runner/runner.go:1: "attempt" is a retired name; use "try"`,
		`docs/one.md:1: "attempt" is a retired name; use "try"`,
		`ui/src/client/pages/remediation-tab.tsx:1: "attempt" is a retired name; use "try"`,
		`ui/src/client/pages/remediation/view.tsx:1: "attempt" is a retired name; use "try"`,
		`ui/src/client/pages/runs/remediation-x/a.md:1: "attempt" is a retired name; use "try"`,
		`ui/src/remediation.ts:1: "attempt" is a retired name; use "try"`,
	)
}

// The guard wraps each pattern in word boundaries, so a pattern that ends on
// a quote can only match before a word character, which a struct tag never
// has. The repository's list must still catch a struct tag naming a
// component under the key service, and leave a tag of another key alone.
func TestRetiredNames_RepositoryListCatchesTheServiceStructTag(t *testing.T) {
	names, err := parseRetiredNames([]byte(readRepoFile(t, retiredNamesFile)))
	if err != nil {
		t.Fatal(err)
	}
	src := "package a\n\ntype c struct {\n\tA string `yaml:\"service\"`\n\tB string `json:\"service\"`\n\tC string `yaml:\"services\"`\n\tD string `json:\"service_id\"`\n}\n"
	var got []string
	for _, f := range scanForRetiredNames("src/a.go", []byte(src), names) {
		got = append(got, f.String())
	}
	assertFindings(t, got,
		`src/a.go:4: "yaml:\"service" is a retired name; use "yaml:\"component\", json:\"component\""`,
		`src/a.go:5: "json:\"service" is a retired name; use "yaml:\"component\", json:\"component\""`,
	)
}

func TestRetiredNames_ListItselfAndBinaryFilesPass(t *testing.T) {
	got := findingsIn(t, frobnicateList+"# frobnicate in a comment\n", map[string]string{
		"img/logo.png": "\x89PNG\x00frobnicate",
	})
	assertFindings(t, got)
}

func TestRetiredNames_UntrackedFilePasses(t *testing.T) {
	dir := fixtureRepo(t, frobnicateList, map[string]string{"a.txt": "fine\n"})
	if err := os.WriteFile(filepath.Join(dir, "scratch.txt"), []byte("frobnicate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	found, err := checkRetiredNames(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Errorf("an untracked file was checked: %v", found)
	}
}

func TestRetiredNames_RefusesABadList(t *testing.T) {
	cases := map[string]struct{ list, want string }{
		"empty file":      {"", "no names key"},
		"no names key":    {"other: []\n", "field other not found"},
		"names is null":   {"names:\n", "no names key"},
		"unknown key":     {"names:\n  - pattern: a\n    use: b\n    path: [x]\n", "field path not found"},
		"no pattern":      {"names:\n  - use: b\n", "no pattern"},
		"no word to use":  {"names:\n  - pattern: a\n", "no word to use"},
		"bad pattern":     {"names:\n  - pattern: a(\n    use: b\n", "missing closing )"},
		"empty match":     {"names:\n  - pattern: a?\n    use: b\n", "matches the empty string"},
		"absolute path":   {"names:\n  - pattern: a\n    use: b\n    paths: [/src]\n", "not a relative path"},
		"dot path":        {"names:\n  - pattern: a\n    use: b\n    paths: [./src]\n", "not a relative path"},
		"unclean path":    {"names:\n  - pattern: a\n    use: b\n    paths: [src/../ui]\n", "not a clean path"},
		"empty path":      {"names:\n  - pattern: a\n    use: b\n    paths: [\"\"]\n", "empty"},
		"untracked path":  {"names:\n  - pattern: a\n    use: b\n    paths: [nowhere]\n", `names[0] (a): path "nowhere" names no tracked file`},
		"unmatched glob":  {"names:\n  - pattern: a\n    use: b\n    paths: [\"src/*.py\"]\n", `names[0] (a): path "src/*.py" names no tracked file`},
		"second document": {"names: []\n---\nnames: []\n", "more than one YAML document"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := checkRetiredNames(fixtureRepo(t, c.list, map[string]string{"src/a.go": "package a\n"}))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want one containing %q", err, c.want)
			}
		})
	}
}

// The code-forms pattern AGENTS.md gives as an example catches the name, its
// plural and its snake_case and camelCase identifiers, and nothing longer.
func TestRetiredNames_CodeFormsPatternOfAgentsMd(t *testing.T) {
	list := "names:\n  - pattern: attempts?|attempt_\\w+|[aA]ttempt[A-Z]\\w*\n    use: try\n"
	got := findingsIn(t, list, map[string]string{
		"a.go": "attempt attempts attempt_id attemptCount AttemptStatus attempted Attempts\n",
	})
	assertFindings(t, got,
		`a.go:1: "attempt" is a retired name; use "try"`,
		`a.go:1: "attempts" is a retired name; use "try"`,
		`a.go:1: "attempt_id" is a retired name; use "try"`,
		`a.go:1: "attemptCount" is a retired name; use "try"`,
		`a.go:1: "AttemptStatus" is a retired name; use "try"`,
	)
}

// The example entry of retired-names.yaml's header, with (?i) over every
// alternative, catches the phrase and its identifiers in any case.
func TestRetiredNames_HeaderExamplePattern(t *testing.T) {
	list := "names:\n  - pattern: (?i)widget runs?|widget_?run\\w*\n    use: gadget run\n"
	got := findingsIn(t, list, map[string]string{
		"a.go": "Widget runs, widget_run_id, widgetRunner, WidgetRun, widgets\n",
	})
	assertFindings(t, got,
		`a.go:1: "Widget runs" is a retired name; use "gadget run"`,
		`a.go:1: "widget_run_id" is a retired name; use "gadget run"`,
		`a.go:1: "widgetRunner" is a retired name; use "gadget run"`,
		`a.go:1: "WidgetRun" is a retired name; use "gadget run"`,
	)
}
