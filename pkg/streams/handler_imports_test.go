package streams_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// handlerDirs lists application-layer packages (relative to repo root) that
// must not import any adapters/* package, nor any gRPC/proto wire types. The
// dependency arrow runs adapter → port, never application → adapter. This covers
// request handlers as well as other application services — dispatchers,
// domain-service helpers, the dispatch watchdog — that orchestrate via ports.
//
// Whole `service` subtrees are listed where the entire application layer is
// guarded (orchestrator); narrower entries are used where only specific
// application packages exist under a service's tree.
var handlerDirs = []string{
	"k8s-controller/service/handlers",
	"orchestrator/service",
	"state/service/handlers",
	"executor-controller/service/handlers",
	"executor-controller/service/deployer",
	"executor-controller/service/validation",
	"release-controller/service/handlers",
	"agent-runner/service/chat",
}

// forbiddenAppImports are import-path fragments the application layer must not
// reference directly: concrete adapters, and the gRPC/proto wire types that are
// an adapter concern. Application code reaches these only through ports.
var forbiddenAppImports = []struct {
	fragment string
	why      string
}{
	{"/adapters/", "declare the collaborator as a port (domain/repository or service/ports) and have the adapter implement it"},
	{"google.golang.org/grpc", "gRPC is an adapter concern — speak a domain-typed port and translate wire types in adapters/grpc"},
	{"/proto/", "generated proto wire types belong in adapters — define a domain-typed port and map them in the adapter"},
}

// TestServiceHandlersDoNotImportAdapters parses every .go file under each
// application-layer dir and asserts none imports an adapter package or a
// gRPC/proto wire-type package.
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
			// Test files may import adapters/wire types to wire concrete
			// implementations for integration tests; only production code is guarded.
			if strings.HasSuffix(d.Name(), "_test.go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, imp := range f.Imports {
				p := strings.Trim(imp.Path.Value, `"`)
				for _, bad := range forbiddenAppImports {
					if strings.Contains(p, bad.fragment) {
						t.Errorf("%s: application code imports %q — %s", path, p, bad.why)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk dir %s: %v", dir, err)
		}
	}
}
