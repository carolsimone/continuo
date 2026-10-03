package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// TestMainsThatRunConsumersNameTheirService discovers every service's main.go
// by glob and fails when one that refers to *pkgredis.StreamConsumer never
// calls SetService(config.ServiceName). A consumer refuses to start without a
// service name (it is the producer of every dead letter the consumer writes),
// so a missing call would only surface when the pod boots. Passing the
// config.ServiceName constant, rather than a literal, keeps one spelling of the
// name per service.
func TestMainsThatRunConsumersNameTheirService(t *testing.T) {
	root := repoRootFromTest(t)
	mains, err := filepath.Glob(filepath.Join(root, "*", "main.go"))
	if err != nil {
		t.Fatalf("glob main.go: %v", err)
	}
	if len(mains) == 0 {
		t.Fatal("no service main.go files found one level under the repo root")
	}

	fset := token.NewFileSet()
	for _, path := range mains {
		f, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}

		runsConsumers, namesService := false, false
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				if pkg, ok := x.X.(*ast.Ident); ok && pkg.Name == "pkgredis" && x.Sel.Name == "StreamConsumer" {
					runsConsumers = true
				}
			case *ast.CallExpr:
				sel, ok := x.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "SetService" || len(x.Args) != 1 {
					return true
				}
				if arg, ok := x.Args[0].(*ast.SelectorExpr); ok {
					if pkg, ok := arg.X.(*ast.Ident); ok && pkg.Name == "config" && arg.Sel.Name == "ServiceName" {
						namesService = true
					}
				}
			}
			return true
		})

		if runsConsumers && !namesService {
			t.Errorf("%s runs stream consumers but never calls SetService(config.ServiceName)", path)
		}
	}
}
