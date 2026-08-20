package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// forbiddenGoStartOwners are the fragments of a variable name that identify a
// blocking server whose only stopper is a shutdown handler registered with
// lifecycleManager.RegisterShutdownHandler. Its Start method (ListenAndServe,
// Serve, ...) blocks until that shutdown handler runs Shutdown/GracefulStop —
// and the shutdown handlers only run in step 3 of the ordered sequence,
// *after* the drain in step 2 has already returned. Wrapping such a call in
// lifecycleManager.Go(...) makes the tracked goroutine unable to return until
// the drain's own WaitGroup.Wait() has already returned, so every shutdown
// burns the full SHUTDOWN_GRACE timeout and logs a drain-timeout warning —
// with no functional test failing, since the process still shuts down
// eventually. This guard catches the wiring mistake at parse time instead.
var forbiddenGoStartOwners = []string{"healthServer", "grpcServer", "httpServer"}

// TestLifecycleGoNeverWrapsAServerStart parses every service main.go and fails
// if a call to lifecycleManager.Go(...) — any receiver, method named Go — has
// a function-literal body that calls Start() on a variable whose name
// contains "healthServer", "grpcServer", or "httpServer". Those servers block
// in Start until their own shutdown handler runs in the close-infra step, so
// they must keep running as an untracked `go func() { ... }()`, not a tracked
// goroutine the drain step waits on.
func TestLifecycleGoNeverWrapsAServerStart(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()
	for _, rel := range servicesWithMainGo {
		path := filepath.Join(root, rel)
		f, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Go" {
				return true
			}
			for _, arg := range call.Args {
				lit, ok := arg.(*ast.FuncLit)
				if !ok {
					continue
				}
				ast.Inspect(lit.Body, func(inner ast.Node) bool {
					innerCall, ok := inner.(*ast.CallExpr)
					if !ok {
						return true
					}
					innerSel, ok := innerCall.Fun.(*ast.SelectorExpr)
					if !ok || innerSel.Sel.Name != "Start" {
						return true
					}
					owner := ownerName(innerSel.X)
					for _, bad := range forbiddenGoStartOwners {
						if strings.Contains(owner, bad) {
							t.Errorf(
								"%s:%d: %s.Start() is wrapped in a lifecycleManager.Go(...) tracked goroutine — "+
									"this server blocks until its shutdown handler runs in step 3 (close infra), "+
									"which happens after step 2's drain already returned, so wg.Wait() would block "+
									"on a goroutine that can never finish before the wait itself finishes; run it as "+
									"an untracked `go func() { ... }()` instead",
								path, fset.Position(innerCall.Pos()).Line, owner,
							)
						}
					}
					return true
				})
			}
			return true
		})
	}
}

// ownerName extracts the identifier name a method is called on, whether the
// receiver is a bare identifier (healthServer.Start()) or a field selector
// (s.healthServer.Start()).
func ownerName(x ast.Expr) string {
	switch e := x.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	default:
		return ""
	}
}
