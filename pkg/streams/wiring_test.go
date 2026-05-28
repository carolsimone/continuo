package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// servicesWithMainGo lists service-root main.go paths (relative to repo root)
// that must reference only pkg/streams constants — never string literals — for
// stream names or consumer groups.
var servicesWithMainGo = []string{
	"state/main.go",
	"orchestrator/main.go",
	"executor-controller/main.go",
	"k8s-controller/main.go",
	"release-controller/main.go",
}

// streamLiteralRe matches versioned stream literals (e.g. "node.updated:v1").
var streamLiteralRe = regexp.MustCompile(`^[a-z][a-z.]+:v[0-9]+$`)

// groupLiteralRe matches kebab-case identifiers plausibly used as consumer-group names.
var groupLiteralRe = regexp.MustCompile(`^[a-z][a-z0-9]+(-[a-z0-9]+)+$`)

// repoRootFromTest walks up until it finds go.work.
func repoRootFromTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repo root not found from %s", wd)
		}
		dir = parent
	}
}

// TestNoStreamOrGroupLiteralsInMains walks every service main.go and asserts
// that no string literal matches the stream/group regexes.
//
// PERMISSIVE: in Phase 3 this is skipped because the services still hold
// literals before their refactors land. Phase 11 unskips it.
func TestNoStreamOrGroupLiteralsInMains(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()
	for _, rel := range servicesWithMainGo {
		path := filepath.Join(root, rel)
		f, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			bl, ok := n.(*ast.BasicLit)
			if !ok || bl.Kind != token.STRING {
				return true
			}
			val := strings.Trim(bl.Value, `"`)
			if streamLiteralRe.MatchString(val) {
				t.Errorf("%s:%d: stream-name literal %q — use pkg/streams constant", path, fset.Position(bl.Pos()).Line, val)
			}
			if groupLiteralRe.MatchString(val) && (strings.Contains(val, "group") ||
				strings.Contains(val, "consumer") ||
				looksLikeServicePrefixedGroup(val)) {
				t.Errorf("%s:%d: group-name literal %q — use pkg/streams constant", path, fset.Position(bl.Pos()).Line, val)
			}
			return true
		})
	}
}

func looksLikeServicePrefixedGroup(s string) bool {
	for _, p := range []string{"state-", "orchestrator-", "executor-", "k8s-", "manifest-"} {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}
