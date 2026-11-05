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

const testcontainersImportPath = "github.com/testcontainers/testcontainers-go"

// TestTestcontainerStartsRegisterTheirTeardownFirst fails on the two ways a test
// leaks a container where nothing reaps it (Ryuk is off on macOS with colima):
//
//   - The statement after testcontainers.GenericContainer does not touch the
//     container. A container whose wait strategy fails comes back together with
//     the error, so a teardown registered after the error check never runs. The
//     next statement must register it: testcontainers.CleanupContainer, a defer,
//     or recording the container for a TestMain to terminate.
//   - A TestMain both defers and calls os.Exit. os.Exit skips deferred calls, so
//     a container terminated in a defer stays running after every run. TestMain
//     calls os.Exit on the result of a function that holds the defers instead.
//
// Every test file in the repository that imports testcontainers is checked, so a
// new suite is covered the day it lands.
func TestTestcontainerStartsRegisterTheirTeardownFirst(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()
	starts := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && (strings.HasPrefix(d.Name(), ".") || d.Name() == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		imports, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		name := importedPackageName(t, imports, testcontainersImportPath)
		if name == "" {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		// The package at this path is named testcontainers, not after the path's
		// last element, unless the file renames it.
		if name == filepath.Base(testcontainersImportPath) {
			name = "testcontainers"
		}
		starts += checkContainerStarts(t, fset, f, name)
		checkTestMainDoesNotDeferAndExit(t, fset, f)
		return nil
	})
	if err != nil {
		t.Fatalf("walk the repository: %v", err)
	}
	if starts == 0 {
		t.Fatal("found no testcontainers.GenericContainer call, so this guard checked nothing")
	}
}

// checkContainerStarts reports each GenericContainer call in f whose next
// statement does not use the container, and returns how many calls it found.
func checkContainerStarts(t *testing.T, fset *token.FileSet, f *ast.File, testcontainers string) int {
	t.Helper()
	starts := 0
	ast.Inspect(f, func(n ast.Node) bool {
		block, ok := n.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i, stmt := range block.List {
			container, isStart := startedContainer(stmt, testcontainers)
			if !isStart {
				continue
			}
			starts++
			if i+1 == len(block.List) || !usesIdent(block.List[i+1], container) {
				t.Errorf("%s: register the teardown of %q right after GenericContainer, before checking the error: a container whose wait failed comes back with the error", fset.Position(stmt.Pos()), container)
			}
		}
		return true
	})
	return starts
}

// startedContainer returns the variable a `c, err := testcontainers.GenericContainer(...)`
// statement assigns the container to.
func startedContainer(stmt ast.Stmt, testcontainers string) (string, bool) {
	assign, ok := stmt.(*ast.AssignStmt)
	if !ok || len(assign.Lhs) == 0 || len(assign.Rhs) != 1 {
		return "", false
	}
	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok {
		return "", false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "GenericContainer" {
		return "", false
	}
	if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != testcontainers {
		return "", false
	}
	target, ok := assign.Lhs[0].(*ast.Ident)
	if !ok {
		return "", false
	}
	return target.Name, true
}

func usesIdent(n ast.Node, name string) bool {
	used := false
	ast.Inspect(n, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == name {
			used = true
		}
		return !used
	})
	return used
}

// checkTestMainDoesNotDeferAndExit reports a TestMain whose own body, closures
// aside, both defers a call and calls os.Exit.
func checkTestMainDoesNotDeferAndExit(t *testing.T, fset *token.FileSet, f *ast.File) {
	t.Helper()
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "TestMain" || fn.Recv != nil || fn.Body == nil {
			continue
		}
		var deferred, exits bool
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.FuncLit:
				return false
			case *ast.DeferStmt:
				deferred = true
			case *ast.CallExpr:
				if sel, ok := node.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Exit" {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "os" {
						exits = true
					}
				}
			}
			return true
		})
		if deferred && exits {
			t.Errorf("%s: TestMain defers a call and calls os.Exit, which skips it, so a container it terminates in a defer leaks on every run; move the body into a function that returns the exit code", fset.Position(fn.Pos()))
		}
	}
}
