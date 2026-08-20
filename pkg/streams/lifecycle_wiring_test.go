package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// TestLifecycleGoNeverWrapsAServerStart discovers every service's main.go by
// globbing one level under the repo root (state/main.go, agent-runner/main.go,
// ...) rather than reading from a maintained list, so a new service is
// covered the day it lands. For each file it fails if a tracked goroutine —
// a call to Go(...) with method name "Go", any receiver — invokes Start on a
// variable that is ALSO closed by a RegisterShutdownHandler(...) closure
// elsewhere in the same file.
//
// That combination is the deadlock this guard exists to prevent: a server
// whose Start blocks until its own registered shutdown handler calls
// Shutdown/Close/Stop can never return before the drain step that precedes
// the shutdown-handler step (shutdown handlers run in step 3, close infra,
// strictly after step 2's drain). Tracking such a call in Go(...) makes
// wg.Wait() wait on a goroutine that cannot finish before the wait itself
// finishes, so every shutdown burns the full SHUTDOWN_GRACE timeout and logs
// a drain-timeout warning — with no functional test failing, since the
// process still shuts down eventually.
//
// Detecting the actual invariant (registered as a shutdown target), rather
// than matching known variable-name substrings like "healthServer", also
// catches an arbitrarily named server variable (e.g. "srv") and a future
// service this guard has never seen before.
func TestLifecycleGoNeverWrapsAServerStart(t *testing.T) {
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

		stoppable := registeredShutdownIdents(f)
		if len(stoppable) == 0 {
			// No RegisterShutdownHandler calls in this file: the invariant
			// this guard checks (tracked-and-also-registered-as-a-shutdown-
			// target) cannot arise here.
			continue
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
					if owner == "" || !stoppable[owner] {
						return true
					}
					t.Errorf(
						"%s:%d: %s.Start() is wrapped in a lifecycleManager.Go(...) tracked goroutine, "+
							"but %s is also closed by a RegisterShutdownHandler(...) in this file — its "+
							"Start blocks until that handler runs in step 3 (close infra), which happens "+
							"after step 2's drain already returned, so wg.Wait() would block on a goroutine "+
							"that can never finish before the wait itself finishes; run it as an untracked "+
							"`go func() { ... }()` instead",
						path, fset.Position(innerCall.Pos()).Line, owner, owner,
					)
					return true
				})
			}
			return true
		})
	}
}

// registeredShutdownIdents returns the set of identifier names referenced
// anywhere inside every RegisterShutdownHandler(...) closure body in f. This
// is the "stoppable" set for the file: a variable named here is closed only
// in step 3 (close infra), after the drain, so its Start must never be
// tracked by Go(...) in the same file.
func registeredShutdownIdents(f *ast.File) map[string]bool {
	idents := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "RegisterShutdownHandler" {
			return true
		}
		for _, arg := range call.Args {
			lit, ok := arg.(*ast.FuncLit)
			if !ok {
				continue
			}
			ast.Inspect(lit.Body, func(inner ast.Node) bool {
				if id, ok := inner.(*ast.Ident); ok {
					idents[id.Name] = true
				}
				return true
			})
		}
		return true
	})
	return idents
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
