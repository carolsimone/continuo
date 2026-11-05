package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"testing"
)

const pkgDBImportPath = "github.com/carolsimone/continuo/pkg/db"

// TestServiceMainsOpenPostgresThroughTheStartupWait fails when a service's
// main.go calls pkg/db.Open, which makes one attempt, instead of pkg/db.Connect.
// A new pod's connections are refused until the NetworkPolicy in front of
// Postgres admits its IP, a second or two after it starts, so a service that
// gives up on the first refusal crashes on every deploy and Kubernetes then
// holds its restart back. Every service's main.go is discovered by glob, so a
// new service is covered the day it lands; a one-shot tool under cmd/ is not a
// service and may use Open, since failing at once is what its operator wants.
//
// It also fails when main.go reaches Connect before AddStartupGate. The wait
// lasts up to five minutes, and a liveness probe restarts a pod that has not
// answered for about 35 seconds, so the health server and its startup gate come
// up first: /livez answers throughout and readiness stays false until startup
// ends.
func TestServiceMainsOpenPostgresThroughTheStartupWait(t *testing.T) {
	root := repoRootFromTest(t)
	mains, err := filepath.Glob(filepath.Join(root, "*", "main.go"))
	if err != nil {
		t.Fatalf("glob main.go: %v", err)
	}
	if len(mains) == 0 {
		t.Fatal("no service main.go files found one level under the repo root")
	}

	fset := token.NewFileSet()
	connecting := 0
	for _, path := range mains {
		f, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		name := importedPackageName(t, f, pkgDBImportPath)
		if name == "" {
			continue
		}
		var gate, connect token.Pos
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if sel.Sel.Name == "AddStartupGate" && !gate.IsValid() {
				gate = call.Pos()
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != name {
				return true
			}
			switch sel.Sel.Name {
			case "Open":
				t.Errorf("%s: a service must open Postgres with pkg/db.Connect, which waits through a refused connection, not pkg/db.Open", fset.Position(call.Pos()))
			case "Connect":
				connect = call.Pos()
				connecting++
			}
			return true
		})
		if connect.IsValid() && (!gate.IsValid() || gate > connect) {
			t.Errorf("%s: start the health server and its startup gate (AddStartupGate) before pkg/db.Connect, or the liveness probe restarts the pod during the wait", fset.Position(connect))
		}
	}
	if connecting == 0 {
		t.Fatal("no service main.go calls pkg/db.Connect, so this guard checked nothing")
	}
}

// importedPackageName returns the name f refers to the package at path by, or
// "" when f does not import it.
func importedPackageName(t *testing.T, f *ast.File, path string) string {
	t.Helper()
	for _, spec := range f.Imports {
		got, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatalf("unquote import %s: %v", spec.Path.Value, err)
		}
		if got != path {
			continue
		}
		if spec.Name != nil {
			return spec.Name.Name
		}
		return filepath.Base(path)
	}
	return ""
}
