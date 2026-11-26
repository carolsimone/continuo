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

// TestMainsThatRunConsumersNameTheirComponent discovers every component's main.go
// by glob and fails when one that refers to the pkg/redis StreamConsumer, under
// any import name, never calls SetComponent(config.ComponentName). A consumer
// refuses to start without a component name (it is the producer of every dead
// letter the consumer writes), so a missing call would only surface when the
// pod boots. Passing the config.ComponentName constant, rather than a literal,
// keeps one spelling of the name per component. The components whose main.go runs
// consumers must be exactly those that declare a ComponentName, so the guard
// fails rather than passes when it stops recognising consumer wiring.
func TestMainsThatRunConsumersNameTheirComponent(t *testing.T) {
	root := repoRootFromTest(t)
	mains, err := filepath.Glob(filepath.Join(root, "*", "main.go"))
	if err != nil {
		t.Fatalf("glob main.go: %v", err)
	}
	if len(mains) == 0 {
		t.Fatal("no component main.go files found one level under the repo root")
	}

	fset := token.NewFileSet()
	var running []string
	for _, path := range mains {
		f, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		runsConsumers, namesComponent := consumerWiring(f)
		if runsConsumers {
			running = append(running, filepath.Base(filepath.Dir(path)))
		}
		if runsConsumers && !namesComponent {
			t.Errorf("%s runs stream consumers but never calls SetComponent(config.ComponentName)", path)
		}
	}

	var declaring []string
	for rel := range goComponentNames(t, root) {
		declaring = append(declaring, strings.Split(filepath.ToSlash(rel), "/")[0])
	}
	slices.Sort(running)
	slices.Sort(declaring)
	if !slices.Equal(running, declaring) {
		t.Errorf("mains detected running stream consumers %v, want the components that declare a ComponentName %v", running, declaring)
	}
}

// TestRunsConsumersWithoutComponentName_Detector pins the detector the guard
// above relies on, so a change to it cannot silently stop flagging a main.
func TestRunsConsumersWithoutComponentName_Detector(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want bool
	}{
		{"aliased import without SetComponent", `package main
import rds "github.com/carolsimone/continuo/pkg/redis"
func run(c *rds.StreamConsumer) {}`, true},
		{"aliased import with SetComponent", `package main
import (
	rds "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/run-controller/config"
)
func run(c *rds.StreamConsumer) { c.SetComponent(config.ComponentName) }`, false},
		{"unaliased import without SetComponent", `package main
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
			if got := runsConsumersWithoutComponentName(f); got != tc.want {
				t.Errorf("runsConsumersWithoutComponentName = %v, want %v", got, tc.want)
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

// runsConsumersWithoutComponentName reports whether the file refers to the
// pkg/redis StreamConsumer, by its type or its constructor, under whatever
// name the file imports pkg/redis as, without ever calling
// SetComponent(config.ComponentName).
func runsConsumersWithoutComponentName(f *ast.File) bool {
	runsConsumers, namesComponent := consumerWiring(f)
	return runsConsumers && !namesComponent
}

// consumerWiring reports whether the file refers to the pkg/redis
// StreamConsumer by its type or its constructor, and whether it calls
// SetComponent(config.ComponentName).
func consumerWiring(f *ast.File) (runsConsumers, namesComponent bool) {
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
			if !ok || sel.Sel.Name != "SetComponent" || len(x.Args) != 1 {
				return true
			}
			if arg, ok := x.Args[0].(*ast.SelectorExpr); ok {
				if pkg, ok := arg.X.(*ast.Ident); ok && pkg.Name == "config" && arg.Sel.Name == "ComponentName" {
					namesComponent = true
				}
			}
		}
		return true
	})
	return runsConsumers, namesComponent
}
