package streams_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// benchCounterRe finds the pattern scripts/bench/outage.sh counts abandoned
// messages by: its `grep -cE '<pattern>'` over the consumers' logs.
var benchCounterRe = regexp.MustCompile(`grep -cE '([^']+)'`)

// pythonDeadLetterLogRe matches topology-controller's LOG_DEAD_LETTERED
// assignment: a plain double-quoted literal, without escapes, on one line.
var pythonDeadLetterLogRe = regexp.MustCompile(`(?m)^LOG_DEAD_LETTERED\s*=\s*"([^"\\\n]*)"\s*$`)

// TestDeadLetterLogLineMatchesBenchOutageCounter pins the log lines of the
// stream consumers to the counter scripts/bench/outage.sh reads them with. The
// script counts the messages the consumers abandon by grepping their logs, so
// each consumer must log exactly one line the pattern matches per
// dead-lettered message, and no other line it matches. The Go consumer
// (pkg/redis) logs logDeadLettered once per dead letter and logInfraPause once
// per infrastructure pause; topology-controller logs LOG_DEAD_LETTERED, which
// must be the same text. The test reads the repository's sources, so it runs
// where the whole checkout is present (make guards), not in a service's dev
// container, which mounts only some directories.
func TestDeadLetterLogLineMatchesBenchOutageCounter(t *testing.T) {
	root := repoRootFromTest(t)
	pattern := benchOutageCounter(t, root)

	redisDir := filepath.Join(root, "pkg", "redis")
	goLines := goStringConstants(t, redisDir, "logDeadLettered", "logInfraPause")
	deadLettered, infraPause := goLines["logDeadLettered"], goLines["logInfraPause"]

	if !pattern.MatchString(deadLettered) {
		t.Errorf("the bench counter %q does not match the Go dead-letter log line %q", pattern, deadLettered)
	}
	if pattern.MatchString(infraPause) {
		t.Errorf("the bench counter %q matches the Go infrastructure-pause log line %q", pattern, infraPause)
	}
	if matches := goStringLiteralsMatching(t, redisDir, pattern); len(matches) != 1 {
		t.Errorf("only the logDeadLettered constant of pkg/redis may match the bench counter %q; matches: %v", pattern, matches)
	}

	pythonDir := filepath.Join(root, "topology-controller", "adapters", "redis")
	pythonLine := pythonDeadLetterLog(t, filepath.Join(pythonDir, "consumer.py"))
	if pythonLine != deadLettered {
		t.Errorf("topology-controller's LOG_DEAD_LETTERED is %q, want the Go consumer's %q", pythonLine, deadLettered)
	}
	if matches := pythonSourceMatches(t, pythonDir, pattern); len(matches) != 1 {
		t.Errorf("only the LOG_DEAD_LETTERED constant of topology-controller/adapters/redis may match the bench counter %q; matches: %v", pattern, matches)
	}
}

// benchOutageCounter returns the pattern scripts/bench/outage.sh greps for.
func benchOutageCounter(t *testing.T, root string) *regexp.Regexp {
	t.Helper()
	path := filepath.Join(root, "scripts", "bench", "outage.sh")
	script, err := os.ReadFile(path) //nolint:gosec // G304: path is built from the repo root and fixed segments
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	m := benchCounterRe.FindSubmatch(script)
	if m == nil {
		t.Fatalf("%s no longer counts abandoned messages with grep -cE '<pattern>'", path)
	}
	pattern, err := regexp.Compile(string(m[1]))
	if err != nil {
		t.Fatalf("%s: the counter pattern %q is not a valid regexp: %v", path, m[1], err)
	}
	return pattern
}

// nonTestGoFiles returns the .go files of dir that are not tests.
func nonTestGoFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatalf("glob %s: %v", dir, err)
	}
	files = slices.DeleteFunc(files, func(f string) bool { return strings.HasSuffix(f, "_test.go") })
	if len(files) == 0 {
		t.Fatalf("no non-test Go files in %s", dir)
	}
	return files
}

// goStringConstants returns the value of each named string constant declared in
// the non-test files of dir. Every name must be declared exactly once, as a
// string literal.
func goStringConstants(t *testing.T, dir string, names ...string) map[string]string {
	t.Helper()
	found := map[string]string{}
	fset := token.NewFileSet()
	for _, path := range nonTestGoFiles(t, dir) {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, id := range vs.Names {
					if !slices.Contains(names, id.Name) {
						continue
					}
					if _, dup := found[id.Name]; dup {
						t.Fatalf("%s: constant %s is declared more than once in %s", path, id.Name, dir)
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						t.Fatalf("%s: constant %s must be a string literal", path, id.Name)
					}
					value, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatalf("%s: unquote %s: %v", path, id.Name, err)
					}
					found[id.Name] = value
				}
			}
		}
	}
	for _, name := range names {
		if _, ok := found[name]; !ok {
			t.Fatalf("no constant %s in the non-test Go files of %s", name, dir)
		}
	}
	return found
}

// goStringLiteralsMatching returns "file:line" for every string literal in the
// non-test files of dir that pattern matches.
func goStringLiteralsMatching(t *testing.T, dir string, pattern *regexp.Regexp) []string {
	t.Helper()
	var matches []string
	fset := token.NewFileSet()
	for _, path := range nonTestGoFiles(t, dir) {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Fatalf("%s: unquote %s: %v", path, lit.Value, err)
			}
			if pattern.MatchString(value) {
				matches = append(matches, fmt.Sprintf("%s:%d", filepath.Base(path), fset.Position(lit.Pos()).Line))
			}
			return true
		})
	}
	return matches
}

// pythonDeadLetterLog returns the string assigned to LOG_DEAD_LETTERED in path.
func pythonDeadLetterLog(t *testing.T, path string) string {
	t.Helper()
	src, err := os.ReadFile(path) //nolint:gosec // G304: path is built from the repo root and fixed segments
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	m := pythonDeadLetterLogRe.FindSubmatch(src)
	if m == nil {
		t.Fatalf("%s: no LOG_DEAD_LETTERED = \"...\" assignment", path)
	}
	return string(m[1])
}

// pythonSourceMatches returns "file:line" for every line of the .py files in
// dir that pattern matches.
func pythonSourceMatches(t *testing.T, dir string, pattern *regexp.Regexp) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.py"))
	if err != nil {
		t.Fatalf("glob %s: %v", dir, err)
	}
	if len(files) == 0 {
		t.Fatalf("no Python files in %s", dir)
	}
	var matches []string
	for _, path := range files {
		src, err := os.ReadFile(path) //nolint:gosec // G304: path comes from filepath.Glob over a fixed repo directory
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			if pattern.MatchString(line) {
				matches = append(matches, fmt.Sprintf("%s:%d", filepath.Base(path), i+1))
			}
		}
	}
	return matches
}
