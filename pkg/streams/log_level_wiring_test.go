package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// slogHandlerConstructors are the log/slog functions that build a handler from
// HandlerOptions.
var slogHandlerConstructors = map[string]bool{
	"NewJSONHandler": true,
	"NewTextHandler": true,
}

// TestServiceMainsLogAtTheConfiguredLevel discovers every service's main.go
// one level under the repo root and fails when one builds a log/slog handler
// whose HandlerOptions.Level is not a pkgconfig.LoadLogLevel(...) call, or
// builds no handler at all. LOG_LEVEL reaches every pod from the chart's
// global.logLevel; a main that fixes its own level, or passes no options,
// logs at that level whatever the install asked for.
func TestServiceMainsLogAtTheConfiguredLevel(t *testing.T) {
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
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		handlers := 0
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !slogHandlerConstructors[sel.Sel.Name] {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "slog" {
				return true
			}
			handlers++
			if len(call.Args) != 2 || !levelIsLoadLogLevel(call.Args[1]) {
				t.Errorf("%s:%d: slog.%s is built without HandlerOptions{Level: pkgconfig.LoadLogLevel(v)}, so the service ignores LOG_LEVEL",
					path, fset.Position(call.Pos()).Line, sel.Sel.Name)
			}
			return true
		})
		if handlers == 0 {
			t.Errorf("%s builds no slog handler; build the service logger in main with HandlerOptions{Level: pkgconfig.LoadLogLevel(v)}", path)
		}
	}
}

// levelIsLoadLogLevel reports whether opts is a &slog.HandlerOptions{...}
// literal whose Level field is a call to LoadLogLevel.
func levelIsLoadLogLevel(opts ast.Expr) bool {
	if u, ok := opts.(*ast.UnaryExpr); ok && u.Op == token.AND {
		opts = u.X
	}
	lit, ok := opts.(*ast.CompositeLit)
	if !ok {
		return false
	}
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); !ok || key.Name != "Level" {
			continue
		}
		call, ok := kv.Value.(*ast.CallExpr)
		if !ok {
			return false
		}
		fn, ok := call.Fun.(*ast.SelectorExpr)
		return ok && fn.Sel.Name == "LoadLogLevel"
	}
	return false
}

// TestChartLogLevelsMatchTheServices pins the level list the chart's
// continuo.logLevel helper (deploy/continuo/templates/_helpers.tpl) accepts to
// the LOG_LEVEL names the services accept. A name the chart rendered and the
// services refused would stop every service at startup; one the services
// accept and the chart refuses could not be installed.
func TestChartLogLevelsMatchTheServices(t *testing.T) {
	path := filepath.Join(repoRootFromTest(t), "deploy", "continuo", "templates", "_helpers.tpl")
	src, err := os.ReadFile(path) //nolint:gosec // G304: path is built from the repo root and fixed segments
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	list := regexp.MustCompile(`(?s)define "continuo\.logLevel".*?\$accepted := list ([^\n]+?) -\}\}`).FindSubmatch(src)
	if list == nil {
		t.Fatalf("%s declares no continuo.logLevel helper with an $accepted list", path)
	}
	var got []string
	for _, m := range regexp.MustCompile(`"([a-z]+)"`).FindAllSubmatch(list[1], -1) {
		got = append(got, string(m[1]))
	}
	if want := pkgconfig.AcceptedLogLevels(); !slices.Equal(got, want) {
		t.Fatalf("continuo.logLevel accepts %v, want %v (pkg/config.AcceptedLogLevels, in order)", got, want)
	}
}

// TestTopologyControllerLogLevelsMatchTheGoServices pins the LOG_LEVEL names
// topology-controller accepts (_LOG_LEVELS in its config module) to
// pkgconfig.AcceptedLogLevels, so one install-wide value is honoured by every
// service or refused by every one.
func TestTopologyControllerLogLevelsMatchTheGoServices(t *testing.T) {
	path := filepath.Join(repoRootFromTest(t), "topology-controller", "config", "config.py")
	src, err := os.ReadFile(path) //nolint:gosec // G304: path is built from the repo root and fixed segments
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	block := regexp.MustCompile(`(?s)_LOG_LEVELS = \{(.*?)\n\}`).FindSubmatch(src)
	if block == nil {
		t.Fatalf("%s declares no _LOG_LEVELS mapping", path)
	}
	var got []string
	for _, m := range regexp.MustCompile(`"([a-z]+)":`).FindAllSubmatch(block[1], -1) {
		got = append(got, string(m[1]))
	}
	if want := pkgconfig.AcceptedLogLevels(); !slices.Equal(got, want) {
		t.Fatalf("topology-controller _LOG_LEVELS names %v, want %v (pkg/config.AcceptedLogLevels, in order)", got, want)
	}
}
