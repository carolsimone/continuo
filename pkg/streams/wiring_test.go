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
	"remediation/main.go",
	"agent-remediation/main.go",
}

// streamLiteralRe matches versioned stream literals (e.g. "node.updated:v1",
// "run.entries.dispatch_failed:v1"). Underscores appear in segment names such
// as single_node_run, so they are part of the character class.
var streamLiteralRe = regexp.MustCompile(`^[a-z][a-z._]+:v[0-9]+$`)

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
	for _, p := range []string{"state-", "orchestrator-", "executor-", "k8s-", "manifest-", "agent-remediation-", "remediation-"} {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// streamLiteralScanDirs lists the production directories that must reference
// pkg/streams constants for stream names rather than inline literals. These are
// the seams where a versioned-stream string is most likely to be hand-written:
// Redis stream bindings (the message_processing.stream_name dedup value) and
// the message handlers (the dedup message_type argument).
var streamLiteralScanDirs = []string{
	"state/adapters/redis",
	"state/service/handlers",
	"state/internal/grpc/handlers",
	"orchestrator/adapters/redis",
	"orchestrator/service/handlers",
	"executor-controller/adapters/redis",
	"executor-controller/service/handlers",
	"k8s-controller/adapters/redis",
	"k8s-controller/service/handlers",
	"release-controller/adapters/redis",
	"release-controller/service/handlers",
}

// TestNoStreamLiteralsInProductionGoFiles walks every production (non-test) Go
// file under the binding/handler directories and asserts that no string literal
// matches the versioned-stream regex. This is the repo-wide guard that catches
// inlined names like "node.updated:v1" outside service main.go files.
//
// Test files (*_test.go) are out of scope: a contract test legitimately asserts
// that a constant resolves to its exact wire value (e.g. that
// streams.RunEntriesDispatchFailedV1 == "run.entries.dispatch_failed:v1"), and
// such an assertion needs the literal. Generated code (streams.gen.go) is the
// single source of the literals and lives in this package, not the scan dirs.
func TestNoStreamLiteralsInProductionGoFiles(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()

	for _, rel := range streamLiteralScanDirs {
		dir := filepath.Join(root, rel)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			// Not every service has every directory; skip absent ones.
			continue
		}
		walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, path, nil, parser.AllErrors)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				bl, ok := n.(*ast.BasicLit)
				if !ok || bl.Kind != token.STRING {
					return true
				}
				val := strings.Trim(bl.Value, `"`)
				if streamLiteralRe.MatchString(val) {
					t.Errorf("%s:%d: stream-name literal %q — use a pkg/streams constant",
						path, fset.Position(bl.Pos()).Line, val)
				}
				return true
			})
			return nil
		})
		if walkErr != nil {
			t.Fatalf("walk %s: %v", dir, walkErr)
		}
	}
}
