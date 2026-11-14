package config

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// guardedDirs hold the remediation steps and the SQL parser adapter, relative
// to this package. The warehouse's dialect reaches them from WAREHOUSE_ENGINE
// through Config.SQLDialect; a dialect written as a literal there would parse
// every install's SQL as the author's warehouse.
var guardedDirs = []string{"../service", "../adapters/sqlparse"}

// dialectLiterals reports every string literal in the Go file src, other than
// in a test file, that equals one of dialects, as "file:line: literal".
func dialectLiterals(t *testing.T, fset *token.FileSet, path string, src any, dialects map[string]bool) []string {
	t.Helper()
	f, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(lit.Value)
		if err != nil {
			return true
		}
		if dialects[value] {
			out = append(out, fset.Position(lit.Pos()).String()+": "+lit.Value)
		}
		return true
	})
	return out
}

func dialectValues() map[string]bool {
	out := make(map[string]bool, len(engineDialects))
	for _, d := range engineDialects {
		out[d] = true
	}
	return out
}

// TestNoDialectNameInRemediationSteps: no production Go file under the steps
// or the SQL parser adapter writes a dialect of engineDialects as a string
// literal. Tests may name a dialect.
func TestNoDialectNameInRemediationSteps(t *testing.T) {
	dialects := dialectValues()
	fset := token.NewFileSet()
	var offenders []string
	for _, dir := range guardedDirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			offenders = append(offenders, dialectLiterals(t, fset, path, nil, dialects)...)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	if len(offenders) > 0 {
		t.Errorf("a SQL dialect is written in step or adapter code; take it from Config.SQLDialect instead:\n%s",
			strings.Join(offenders, "\n"))
	}
}

// TestDialectGuardCatchesALiteral: the guard's check finds a dialect literal
// and leaves a dialect read from a variable alone.
func TestDialectGuardCatchesALiteral(t *testing.T) {
	dialects := dialectValues()
	for d := range dialects {
		src := "package p\n\nfunc f(parse func(string)) { parse(" + strconv.Quote(d) + ") }\n"
		if got := dialectLiterals(t, token.NewFileSet(), "p.go", src, dialects); len(got) != 1 {
			t.Errorf("dialect %q: found %v, want one literal", d, got)
		}
	}
	clean := "package p\n\nfunc f(parse func(string), dialect string) { parse(dialect) }\n"
	if got := dialectLiterals(t, token.NewFileSet(), "p.go", clean, dialects); len(got) != 0 {
		t.Errorf("found %v in code that reads the dialect from a variable", got)
	}
}
