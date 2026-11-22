package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"testing"
)

// TestManifestKindsMatchTopologyController pins the release kinds
// topology-controller reads (ManifestKind in its domain/model.py) to the kinds
// release-controller accepts on a new release (the cases of ParseManifestKind
// in its domain/release/manifest_kind.go). A kind one side accepts and the
// other does not would be requested by release-controller and failed by
// topology-controller, or the reverse. The kind python is not in the list: only
// a stored row holds it, and ParseManifestKind refuses it.
func TestManifestKindsMatchTopologyController(t *testing.T) {
	root := repoRootFromTest(t)
	got := pythonManifestKinds(t, filepath.Join(root, "topology-controller", "domain", "model.py"))
	want := goParseableManifestKinds(t, filepath.Join(root, "release-controller", "domain", "release", "manifest_kind.go"))
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("topology-controller ManifestKind has %v, release-controller ParseManifestKind accepts %v", got, want)
	}
	if len(want) == 0 {
		t.Fatal("no manifest kinds found: the pattern this test reads no longer matches the sources")
	}
}

// pythonManifestKinds returns the values of the members of the ManifestKind
// class in the python file at path.
func pythonManifestKinds(t *testing.T, path string) []string {
	t.Helper()
	src, err := os.ReadFile(path) //nolint:gosec // G304: path is built from the repo root and fixed segments
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	class := regexp.MustCompile(`(?s)class ManifestKind\(StrEnum\):(.*?)\n\n\n`).FindSubmatch(src)
	if class == nil {
		t.Fatalf("%s declares no class ManifestKind(StrEnum)", path)
	}
	var kinds []string
	for _, m := range regexp.MustCompile(`(?m)^    [A-Z_]+ = "([a-z-]+)"$`).FindAllSubmatch(class[1], -1) {
		kinds = append(kinds, string(m[1]))
	}
	return kinds
}

// goParseableManifestKinds returns the values of the constants that name a
// case of the switch in ParseManifestKind and are returned as a valid kind: the
// first clause of the switch, whose cases are the accepted kinds.
func goParseableManifestKinds(t *testing.T, path string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	consts := map[string]string{}
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, name := range vs.Names {
				if i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("%s: constant %s: %v", path, name.Name, err)
				}
				consts[name.Name] = value
			}
		}
	}
	var kinds []string
	found := false
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "ParseManifestKind" {
			continue
		}
		found = true
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sw, ok := n.(*ast.SwitchStmt)
			if !ok || len(sw.Body.List) == 0 {
				return true
			}
			first := sw.Body.List[0].(*ast.CaseClause)
			for _, expr := range first.List {
				id, ok := expr.(*ast.Ident)
				if !ok {
					t.Fatalf("%s: a case of ParseManifestKind is not a named constant", path)
				}
				value, known := consts[id.Name]
				if !known {
					t.Fatalf("%s: ParseManifestKind accepts %s, which is not a string constant of the file", path, id.Name)
				}
				kinds = append(kinds, value)
			}
			return false
		})
	}
	if !found {
		t.Fatalf("%s declares no ParseManifestKind", path)
	}
	return kinds
}
