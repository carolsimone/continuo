package model_test

import (
	"strings"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

func TestParseNodeType_EveryDeclaredValueParses(t *testing.T) {
	for _, nt := range model.NodeTypes() {
		got, err := model.ParseNodeType(string(nt))
		if err != nil || got != nt {
			t.Errorf("ParseNodeType(%q) = %q, %v", nt, got, err)
		}
	}
}

func TestParseNodeType_RetiredPythonModelIsRejectedWithValidList(t *testing.T) {
	_, err := model.ParseNodeType("python-model")
	if err == nil {
		t.Fatal("python-model must not parse")
	}
	for _, want := range []string{`"python-model"`, "python-node", "python-csv"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q must mention %s", err, want)
		}
	}
}

func TestParseNodeType_EmptyIsRejected(t *testing.T) {
	if _, err := model.ParseNodeType(""); err == nil {
		t.Fatal("empty node_type must not parse")
	}
}

func TestIsPython(t *testing.T) {
	cases := map[model.NodeType]bool{
		model.NodeTypeDbtModel:    false,
		model.NodeTypeDbtSeed:     false,
		model.NodeTypeDbtSnapshot: false,
		model.NodeTypeDbtTest:     false,
		model.NodeTypePythonNode:  true,
		model.NodeTypePythonCsv:   true,
	}
	if len(cases) != len(model.NodeTypes()) {
		t.Fatalf("this table covers %d node types, the catalog declares %d — add the new one", len(cases), len(model.NodeTypes()))
	}
	for nt, want := range cases {
		if nt.IsPython() != want {
			t.Errorf("IsPython(%q) = %v, want %v", nt, !want, want)
		}
	}
}

// TestEveryNodeTypeIsClassified: every NodeType falls into exactly one family —
// a relation-producing dbt kind, a python kind, or the validation-only dbt-test.
func TestEveryNodeTypeIsClassified(t *testing.T) {
	for _, nt := range model.NodeTypes() {
		isDbtRelation := nt.Runtime() == model.NodeRuntimeDbt && nt != model.NodeTypeDbtTest
		isTest := nt == model.NodeTypeDbtTest
		families := 0
		for _, in := range []bool{isDbtRelation, nt.IsPython(), isTest} {
			if in {
				families++
			}
		}
		if families != 1 {
			t.Errorf("NodeType %q is in %d families, want exactly 1", nt, families)
		}
	}
}
