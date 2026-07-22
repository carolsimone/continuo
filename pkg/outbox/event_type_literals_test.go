package outbox_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// forbiddenEventTypeLiterals is the set of outbox event_type routing-key values
// that must never appear as a bare string literal in a publisher switch or a
// producer's outbox INSERT. Each value has a named per-service constant
// (orchestrator/domain, executor-controller/domain/event, k8s-controller/domain/event);
// the literal must be referenced through that constant so the emit site and the
// publisher share one source of truth.
var forbiddenEventTypeLiterals = map[string]bool{
	"node_ready_for_execution":    true,
	"cascade_task_skipped":        true,
	"run_entries_dispatched":      true,
	"run_entries_dispatch_failed": true,
	"release_promoted":            true,
	"task_status_updated":         true,
	"node_deployed":               true,
	"node_updated":                true,
	"task_execution_recorded":     true,
	"task_retry":                  true,
	"task_failed":                 true,
	"node_status_updated":         true,
	"check_delayed":               true,
}

// eventTypeScanDirs are the publisher adapters and producer packages that route
// or emit outbox rows. Their event_type strings must come from a constant, never
// a literal. The constant declarations themselves live in the services' domain/
// packages, which are deliberately not scanned here.
var eventTypeScanDirs = []string{
	"orchestrator/adapters/publisher",
	"orchestrator/service/handlers",
	"executor-controller/adapters/publisher",
	"executor-controller/service/deployer",
	"k8s-controller/adapters/publisher",
	"k8s-controller/service/handlers",
}

// repoRootFromEventTypeTest walks up until it finds go.work.
func repoRootFromEventTypeTest(t *testing.T) string {
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

// TestOutboxEventTypesUseConstants fails if any scanned production file contains
// a string literal equal to a known outbox event_type value. Such a literal must
// be replaced with its per-service EventType* constant so the emit site and the
// publisher cannot silently drift apart.
//
// Test files (*_test.go) are out of scope: a publisher contract test legitimately
// asserts that a constant resolves to its exact wire value, and such an assertion
// needs the literal — the same exemption pkg/streams/wiring_test.go makes.
func TestOutboxEventTypesUseConstants(t *testing.T) {
	root := repoRootFromEventTypeTest(t)
	fset := token.NewFileSet()

	for _, rel := range eventTypeScanDirs {
		dir := filepath.Join(root, rel)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Fatalf("scan dir %s does not exist", rel)
		}
		walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
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
				if forbiddenEventTypeLiterals[val] {
					t.Errorf("%s:%d: outbox event_type literal %q — use the per-service EventType* constant",
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
