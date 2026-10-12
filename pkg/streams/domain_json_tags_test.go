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
	"execution-controller",
	"release-controller",
	"remediation",
	"agent-remediation",
	"agent-chat",
	"dead-letter-controller",
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

// forbiddenDomainImportPrefixes lists the serialization-framework import paths a
// domain package must not depend on. A type-level dependency on one of these
// (e.g. a json.RawMessage field, a proto message embedded in an aggregate)
// couples the domain to a wire/persistence format just as a json struct tag
// does, but the tag guard above — which inspects struct tags only — cannot see
// it. An entry ending in "/" matches that path and anything under it; an entry
// without a trailing slash matches exactly.
var forbiddenDomainImportPrefixes = []string{
	"encoding/json",
	"encoding/xml",
	"google.golang.org/protobuf/",
	"github.com/golang/protobuf/",
}

// TestNoSerializationImportsInDomainPackages fails on any import of a
// serialization framework under a service's domain/ tree (or pkg/domain).
// Serialization is an adapter concern: move the marshal/unmarshal to a DTO in
// the owning adapter (or a top-level serialization package) with to/from-domain
// mappers, and keep the domain type free of the format. This complements the
// struct-tag guard above by catching type-level leaks it cannot see.
func TestNoSerializationImportsInDomainPackages(t *testing.T) {
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
			f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			for _, imp := range f.Imports {
				p := strings.Trim(imp.Path.Value, `"`)
				if forbiddenDomainImport(p) {
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s:%d: domain package imports serialization framework %q — move marshal/unmarshal to an adapter/serialization DTO with to/from-domain mappers",
						rel, fset.Position(imp.Pos()).Line, p)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", domainDir, err)
		}
	}
}

// forbiddenDomainImport reports whether importPath matches any entry in
// forbiddenDomainImportPrefixes (exact match, or prefix match for entries ending
// in "/").
func forbiddenDomainImport(importPath string) bool {
	for _, pfx := range forbiddenDomainImportPrefixes {
		if strings.HasSuffix(pfx, "/") {
			if strings.HasPrefix(importPath, pfx) {
				return true
			}
		} else if importPath == pfx {
			return true
		}
	}
	return false
}

// TestNoObjectStoreImportsInDomainOrServicePackages fails on any Go file under
// a module's domain/ or service/ tree that imports pkg/objectstore or a
// package of the AWS SDK. Object storage is infrastructure: a service declares
// the port it needs in service/ports (or domain/repository), phrased in its
// own vocabulary, and an adapter implements it over a pkg/objectstore role.
// Test files are guarded too: a handler test that reaches for the store is the
// same coupling.
func TestNoObjectStoreImportsInDomainOrServicePackages(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()
	checked := 0
	for _, m := range domainModules {
		for _, layer := range []string{"domain", "service"} {
			dir := filepath.Join(root, m, layer)
			if _, err := os.Stat(dir); err != nil {
				continue // a module without this layer
			}
			err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.IsDir() || !strings.HasSuffix(d.Name(), ".go") {
					return nil
				}
				f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
				if perr != nil {
					t.Fatalf("parse %s: %v", path, perr)
				}
				checked++
				for _, p := range objectStoreImportsIn(f) {
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s: %s/ package imports %q — object storage is an adapter concern; declare a port in service/ports and implement it in adapters/ over a pkg/objectstore role",
						rel, layer, p)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("walk %s: %v", dir, err)
			}
		}
	}
	if checked == 0 {
		t.Fatal("guard inspected no domain or service files — the discovery in domainModules is broken")
	}
}

// TestObjectStoreImportGuardFlagsViolations pins the import detector so it
// cannot rot into a guard that never fires.
func TestObjectStoreImportGuardFlagsViolations(t *testing.T) {
	src := `package x
import (
	"context"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2-extra/x"
	"github.com/carolsimone/continuo/pkg/objectstorage"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
)
`
	f, err := parser.ParseFile(token.NewFileSet(), "x.go", src, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"github.com/carolsimone/continuo/pkg/objectstore",
		"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest",
		"github.com/aws/aws-sdk-go-v2/service/s3",
		"github.com/aws/aws-sdk-go-v2/aws",
	}
	if got := objectStoreImportsIn(f); !reflect.DeepEqual(got, want) {
		t.Fatalf("flagged %v, want %v", got, want)
	}
}

// TestAdaptersDependOnObjectStoreRoles fails on any Go file under a module's
// adapters/ tree that names the concrete objectstore.Client. An adapter
// depends on the narrowest role pkg/objectstore declares (Reader, Writer,
// PrefixDeleter or ReadWriter): it reaches no operation it does not use, and
// its unit tests substitute objectstoretest.Memory. Only a service's main
// builds the Client and passes it in as those roles. Test files are guarded
// too: an adapter's credential-wiring test passes objectstore.New's result
// straight to the constructor without naming its type.
func TestAdaptersDependOnObjectStoreRoles(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()
	checked := 0
	for _, m := range domainModules {
		dir := filepath.Join(root, m, "adapters")
		if _, err := os.Stat(dir); err != nil {
			continue // a module without adapters
		}
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(d.Name(), ".go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			checked++
			rel, _ := filepath.Rel(root, path)
			for _, pos := range concreteObjectStoreClientUses(f) {
				t.Errorf("%s:%d: names objectstore.Client — depend on the objectstore role the adapter uses (Reader, Writer, PrefixDeleter or ReadWriter); only main builds the Client",
					rel, fset.Position(pos).Line)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	if checked == 0 {
		t.Fatal("guard inspected no adapter files — the discovery in domainModules is broken")
	}
}

// TestObjectStoreRoleGuardFlagsTheConcreteClient pins the role detector so it
// cannot rot into a guard that never fires.
func TestObjectStoreRoleGuardFlagsTheConcreteClient(t *testing.T) {
	src := `package x

import (
	"context"

	store "github.com/carolsimone/continuo/pkg/objectstore"
)

type a struct{ c *store.Client }

func b(r store.Reader) {}

func c(ctx context.Context) { _, _ = store.New(ctx, store.Config{}) }

var d = (*store.Client)(nil)
`
	f, err := parser.ParseFile(token.NewFileSet(), "x.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(concreteObjectStoreClientUses(f)); got != 2 {
		t.Fatalf("flagged %d uses, want 2 (the field of a and var d)", got)
	}

	// A file that does not import pkg/objectstore is never flagged, whatever
	// its own identifiers are called.
	other := `package x

func f(objectstore struct{ Client int }) int { return objectstore.Client }
`
	g, err := parser.ParseFile(token.NewFileSet(), "y.go", other, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(concreteObjectStoreClientUses(g)); got != 0 {
		t.Fatalf("flagged %d uses in a file without the import, want 0", got)
	}
}

// objectStoreImportRoots are the import paths no domain or service package may
// depend on: the shared object-store package and the AWS SDK. A root matches
// itself and every package under it, never a sibling path that merely shares
// its prefix.
var objectStoreImportRoots = []string{
	"github.com/carolsimone/continuo/pkg/objectstore",
	"github.com/aws/aws-sdk-go-v2",
}

// objectStoreImportsIn returns, in source order, the import paths of f that
// name the shared object-store package, the AWS SDK, or a package under either.
func objectStoreImportsIn(f *ast.File) []string {
	var out []string
	for _, imp := range f.Imports {
		p := strings.Trim(imp.Path.Value, `"`)
		for _, root := range objectStoreImportRoots {
			if p == root || strings.HasPrefix(p, root+"/") {
				out = append(out, p)
				break
			}
		}
	}
	return out
}

// concreteObjectStoreClientUses returns the position of every reference f
// makes to objectstore.Client, under whatever name f imports
// pkg/objectstore. A file that does not import the package names nothing.
func concreteObjectStoreClientUses(f *ast.File) []token.Pos {
	name := ""
	for _, imp := range f.Imports {
		if strings.Trim(imp.Path.Value, `"`) != "github.com/carolsimone/continuo/pkg/objectstore" {
			continue
		}
		name = "objectstore"
		if imp.Name != nil {
			name = imp.Name.Name
		}
	}
	if name == "" || name == "_" {
		return nil
	}
	var out []token.Pos
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == name && sel.Sel.Name == "Client" {
			out = append(out, sel.Pos())
		}
		return true
	})
	return out
}
