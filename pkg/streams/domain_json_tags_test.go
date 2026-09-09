package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// domainModules lists every module whose domain/ tree is guarded. Each entry is
// a module directory relative to the repo root; the guard walks "<module>/domain".
// topology-controller is Python and holds no Go files, so it is not listed.
var domainModules = []string{
	"state",
	"orchestrator",
	"executor-controller",
	"k8s-controller",
	"release-controller",
	"remediation",
	"agent-remediation",
	"agent-chat",
	"pkg",
}

// TestNoJSONTagsInDomainPackages fails on any `json:` struct tag under any
// service's domain/ tree (or pkg/domain). Serialization is an adapter concern: a
// json-tagged domain struct couples the domain to a wire or persistence format.
// Move the tags to a DTO in the owning adapter (or a top-level serialization)
// package with toDomain/fromDomain mappers, and route the marshal/unmarshal
// through them. This mirrors the stream-literal and handler-import AST guards in
// this package.
func TestNoJSONTagsInDomainPackages(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()

	for _, m := range domainModules {
		domainDir := filepath.Join(root, m, "domain")
		if _, err := os.Stat(domainDir); err != nil {
			continue // a module without a domain/ tree
		}
		err := filepath.WalkDir(domainDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				field, ok := n.(*ast.Field)
				if !ok || field.Tag == nil {
					return true
				}
				tag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
				if _, has := tag.Lookup("json"); has {
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s:%d: domain struct field carries a json tag — move it to an adapter/serialization DTO with to/from-domain mappers",
						rel, fset.Position(field.Pos()).Line)
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", domainDir, err)
		}
	}
}
