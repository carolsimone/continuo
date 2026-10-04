package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestMainsThatRunConsumersNameTheirService discovers every service's main.go
// by glob and fails when one that refers to the pkg/redis StreamConsumer, under
// any import name, never calls SetService(config.ServiceName). A consumer
// refuses to start without a service name (it is the producer of every dead
// letter the consumer writes), so a missing call would only surface when the
// pod boots. Passing the config.ServiceName constant, rather than a literal,
// keeps one spelling of the name per service. The services whose main.go runs
// consumers must be exactly those that declare a ServiceName, so the guard
// fails rather than passes when it stops recognising consumer wiring.
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
	var running []string
	for _, path := range mains {
		f, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		runsConsumers, namesService := consumerWiring(f)
		if runsConsumers {
			running = append(running, filepath.Base(filepath.Dir(path)))
		}
		if runsConsumers && !namesService {
			t.Errorf("%s runs stream consumers but never calls SetService(config.ServiceName)", path)
		}
	}

	var declaring []string
	for rel := range goServiceNames(t, root) {
		declaring = append(declaring, strings.Split(filepath.ToSlash(rel), "/")[0])
	}
	slices.Sort(running)
	slices.Sort(declaring)
	if !slices.Equal(running, declaring) {
		t.Errorf("mains detected running stream consumers %v, want the services that declare a ServiceName %v", running, declaring)
	}
}

// TestRunsConsumersWithoutServiceName_Detector pins the detector the guard
// above relies on, so a change to it cannot silently stop flagging a main.
func TestRunsConsumersWithoutServiceName_Detector(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want bool
	}{
		{"aliased import without SetService", `package main
import rds "github.com/carolsimone/continuo/pkg/redis"
func run(c *rds.StreamConsumer) {}`, true},
		{"aliased import with SetService", `package main
import (
	rds "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/state/config"
)
func run(c *rds.StreamConsumer) { c.SetService(config.ServiceName) }`, false},
		{"unaliased import without SetService", `package main
import "github.com/carolsimone/continuo/pkg/redis"
func run(c *redis.StreamConsumer) {}`, true},
		{"consumer constructed without naming the type", `package main
import rds "github.com/carolsimone/continuo/pkg/redis"
func run() { _ = rds.NewStreamConsumer(nil, "", "", nil, nil) }`, true},
		{"another package's StreamConsumer", `package main
import pkgredis "example.com/other/redis"
func run(c *pkgredis.StreamConsumer) {}`, false},
		{"no consumers", `package main
import rds "github.com/carolsimone/continuo/pkg/redis"
var _ = rds.WaitForRedis`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := parser.ParseFile(token.NewFileSet(), "main.go", tc.src, parser.AllErrors)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := runsConsumersWithoutServiceName(f); got != tc.want {
				t.Errorf("runsConsumersWithoutServiceName = %v, want %v", got, tc.want)
			}
		})
	}
}

// pkgRedisImportPath is the import path of the shared stream-consumer package.
const pkgRedisImportPath = "github.com/carolsimone/continuo/pkg/redis"

// importedName returns the name the file refers to the package at path by:
// the import's explicit name, or else the last element of the path. It returns
// "" when the file does not import the package.
func importedName(f *ast.File, path string) string {
	for _, spec := range f.Imports {
		if strings.Trim(spec.Path.Value, `"`) != path {
			continue
		}
		if spec.Name != nil {
			return spec.Name.Name
		}
		return path[strings.LastIndex(path, "/")+1:]
	}
	return ""
}

// runsConsumersWithoutServiceName reports whether the file refers to the
// pkg/redis StreamConsumer, by its type or its constructor, under whatever
// name the file imports pkg/redis as, without ever calling
// SetService(config.ServiceName).
func runsConsumersWithoutServiceName(f *ast.File) bool {
	runsConsumers, namesService := consumerWiring(f)
	return runsConsumers && !namesService
}

// consumerWiring reports whether the file refers to the pkg/redis
// StreamConsumer by its type or its constructor, and whether it calls
// SetService(config.ServiceName).
func consumerWiring(f *ast.File) (runsConsumers, namesService bool) {
	redisPkg := importedName(f, pkgRedisImportPath)
	if redisPkg == "" || redisPkg == "_" {
		return false, false
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.SelectorExpr:
			if pkg, ok := x.X.(*ast.Ident); ok && pkg.Name == redisPkg &&
				(x.Sel.Name == "StreamConsumer" || x.Sel.Name == "NewStreamConsumer") {
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
	return runsConsumers, namesService
}
