package model_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

func TestParseNodeType_PythonCsv(t *testing.T) {
	got, err := model.ParseNodeType("python-csv")
	if err != nil {
		t.Fatalf("ParseNodeType(python-csv): %v", err)
	}
	if got != model.NodeTypePythonCsv {
		t.Fatalf("got %q", got)
	}
}

func TestIsPython(t *testing.T) {
	cases := map[model.NodeType]bool{
		model.NodeTypeDbtModel:    false,
		model.NodeTypeDbtSeed:     false,
		model.NodeTypeDbtSnapshot: false,
		model.NodeTypePythonModel: true,
		model.NodeTypePythonCsv:   true,
	}
	for nt, want := range cases {
		if nt.IsPython() != want {
			t.Errorf("IsPython(%q) = %v, want %v", nt, !want, want)
		}
	}
}

// TestEveryNodeTypeIsClassified is the guard the spec requires: adding a
// NodeType constant without deciding its family must fail here, so the four
// call sites that branch on family cannot be silently wrong for a new kind.
func TestEveryNodeTypeIsClassified(t *testing.T) {
	for _, nt := range model.AllNodeTypes {
		isDbt := nt == model.NodeTypeDbtModel || nt == model.NodeTypeDbtSeed ||
			nt == model.NodeTypeDbtSnapshot
		if nt.IsPython() == isDbt {
			t.Errorf("NodeType %q is in neither or both families", nt)
		}
	}
	if len(model.AllNodeTypes) != 5 {
		t.Errorf("AllNodeTypes has %d entries; update it AND the family "+
			"classification when adding a NodeType", len(model.AllNodeTypes))
	}
}

// TestAllNodeTypesMatchesDeclaredConstants parses model.go and counts the
// const specs whose declared type is NodeType, then asserts that count
// equals len(AllNodeTypes). This catches "declared a NodeType constant but
// forgot to add it to AllNodeTypes" at compile-test time, rather than
// leaving a silently unclassified node type to surface at runtime.
func TestAllNodeTypesMatchesDeclaredConstants(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "model.go", nil, 0)
	if err != nil {
		t.Fatalf("parse model.go: %v", err)
	}

	count := 0
	for _, decl := range f.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			continue
		}
		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok || valueSpec.Type == nil {
				continue
			}
			ident, ok := valueSpec.Type.(*ast.Ident)
			if !ok || ident.Name != "NodeType" {
				continue
			}
			count += len(valueSpec.Names)
		}
	}

	if count != len(model.AllNodeTypes) {
		t.Errorf("model.go declares %d NodeType constants but AllNodeTypes has %d entries; "+
			"keep them in sync", count, len(model.AllNodeTypes))
	}
}
