package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// skipAllowlist names test files whose unconditional t.Skip* is deliberate,
// with the reason. Every other skip must be a testing.Short() opt-out or go
// through pkg/testdeps, so an unreachable dependency fails under
// REQUIRE_TEST_DEPS=1 instead of turning a suite into a silent green.
var skipAllowlist = map[string]string{
	"agent-chat/adapters/anthropic/provider_integration_test.go": "calls the paid Anthropic API; needs a secret CI does not hold",
	"agent-remediation/adapters/packaging/cli_packager_test.go":  "continuo-runtime ships only in the agent-remediation image; a dedicated in-container CI step runs this package",
}

// skipGuardSkipDirs are not scanned: tests/e2e is a separate harness with its
// own stack and gating, and vendored or generated trees hold no service tests.
var skipGuardSkipDirs = map[string]bool{
	"e2e": true, "node_modules": true, ".git": true, ".worktrees": true, "vendor": true,
}

// TestIntegrationTestsNeverSkipSilently parses every *_test.go and rejects a
// t.Skip/Skipf/SkipNow call that is neither behind a testing.Short() check nor
// allowlisted. A test that skips when its database is unreachable reports
// success for code it never ran; pkg/testdeps.Unavailable skips on a bare
// checkout but fails when the shared test entrypoint sets REQUIRE_TEST_DEPS.
func TestIntegrationTestsNeverSkipSilently(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipGuardSkipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "tests/e2e/") {
			return nil
		}
		if _, ok := skipAllowlist[rel]; ok {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Errorf("parse %s: %v", rel, perr)
			return nil
		}
		for _, bad := range unguardedSkips(f) {
			t.Errorf("%s:%d: %s — use testdeps.Unavailable (github.com/carolsimone/continuo/pkg/testdeps) for a missing dependency, or guard with testing.Short()",
				rel, fset.Position(bad.Pos()).Line, exprString(bad))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestSkipGuardFlagsUnguardedSkips pins the detector itself so it cannot rot
// into a guard that never fires.
func TestSkipGuardFlagsUnguardedSkips(t *testing.T) {
	src := `package x
import "testing"
func a(t *testing.T) { t.Skip("db down") }
func b(t *testing.T) { t.Skipf("db down %v", 1) }
func c(t *testing.T) { if err := dial(); err != nil { t.SkipNow() } }
func d(t *testing.T) { if testing.Short() { t.Skip("slow") } }
func e(t *testing.T) { if testing.Short() || other() { t.Skip("slow") } }
func f(t *testing.T) { if !testing.Short() { } else { t.Skip("x") } }
func g(m *model) { m.Skip("domain method") }
`
	f, err := parser.ParseFile(token.NewFileSet(), "x_test.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(unguardedSkips(f)); got != 3 {
		t.Fatalf("flagged %d skips, want 3 (funcs a, b, c only)", got)
	}
}

// unguardedSkips returns t/tb/b .Skip*, .SkipNow calls not nested in an `if`
// whose condition mentions testing.Short.
func unguardedSkips(f *ast.File) []*ast.CallExpr {
	var out []*ast.CallExpr
	var walk func(n ast.Node, short bool)
	walk = func(n ast.Node, short bool) {
		ast.Inspect(n, func(c ast.Node) bool {
			switch x := c.(type) {
			case *ast.IfStmt:
				inner := short || mentionsTestingShort(x.Cond)
				if x.Init != nil {
					walk(x.Init, short)
				}
				walk(x.Cond, short)
				walk(x.Body, inner)
				if x.Else != nil {
					walk(x.Else, inner)
				}
				return false
			case *ast.CallExpr:
				if !short && isTestingSkip(x) {
					out = append(out, x)
				}
			}
			return true
		})
	}
	walk(f, false)
	return out
}

func isTestingSkip(c *ast.CallExpr) bool {
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	recv, ok := sel.X.(*ast.Ident)
	if !ok || (recv.Name != "t" && recv.Name != "tb" && recv.Name != "b") {
		return false
	}
	switch sel.Sel.Name {
	case "Skip", "Skipf", "SkipNow":
		return true
	}
	return false
}

func mentionsTestingShort(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if s, ok := n.(*ast.SelectorExpr); ok {
			if id, ok := s.X.(*ast.Ident); ok && id.Name == "testing" && s.Sel.Name == "Short" {
				found = true
			}
		}
		return !found
	})
	return found
}

func exprString(c *ast.CallExpr) string {
	if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
		return "t." + sel.Sel.Name + "(…)"
	}
	return "skip call"
}
