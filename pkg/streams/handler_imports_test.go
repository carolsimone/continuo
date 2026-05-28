package streams_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// handlerDirs lists application-layer handler packages (relative to repo root)
// that must not import any adapters/* package. The dependency arrow runs
// adapter → port, never application → adapter.
var handlerDirs = []string{
	"k8s-controller/service/handlers",
	"orchestrator/service/handlers",
	"state/service/handlers",
	"executor-controller/service/handlers",
	"release-controller/service/handlers",
}

// TestServiceHandlersDoNotImportAdapters parses every .go file under each
// handler dir and asserts no import path contains "/adapters/".
func TestServiceHandlersDoNotImportAdapters(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()
	for _, rel := range handlerDirs {
		dir := filepath.Join(root, rel)
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if !strings.HasSuffix(d.Name(), ".go") {
				return nil
			}
			// Test files may import adapters to wire concrete implementations for
			// integration tests; only production handler code is guarded.
			if strings.HasSuffix(d.Name(), "_test.go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, imp := range f.Imports {
				p := strings.Trim(imp.Path.Value, `"`)
				if strings.Contains(p, "/adapters/") {
					t.Errorf("%s: application handler imports adapter package %q — declare the collaborator as a port (domain/repository or service/ports) and have the adapter implement it", path, p)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk dir %s: %v", dir, err)
		}
	}
}
