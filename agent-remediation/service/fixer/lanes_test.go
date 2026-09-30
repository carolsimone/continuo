package fixer

import (
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// TestFixerLanes_CoverEveryPythonNodeType pins that a new python kind cannot
// silently fall through to a dbt lane by omission: every python NodeType must
// have an entry in both pythonParseLanes and pythonValidationLanes.
func TestFixerLanes_CoverEveryPythonNodeType(t *testing.T) {
	for _, nt := range pkg_model.NodeTypes() {
		if !nt.IsPython() {
			continue
		}
		if _, ok := pythonParseLanes[nt]; !ok {
			t.Errorf("python node type %q has no parse lane", nt)
		}
		if _, ok := pythonValidationLanes[nt]; !ok {
			t.Errorf("python node type %q has no validation lane", nt)
		}
	}
}

// TestFor_PicksTheDeclaredLanes verifies For resolves each source/node-type
// pair to the lane declared in pythonParseLanes / pythonValidationLanes,
// falling back to the dbt lane for a non-python node type.
func TestFor_PicksTheDeclaredLanes(t *testing.T) {
	cases := []struct {
		source, nodeType string
		want             Fixer
	}{
		{sourceParse, string(pkg_model.NodeTypePythonNode), pythonParseFixer{}},
		{sourceParse, string(pkg_model.NodeTypePythonCsv), parseFixer{}},
		{sourceParse, string(pkg_model.NodeTypePythonApi), parseFixer{}},
		{sourceParse, string(pkg_model.NodeTypeDbtModel), parseFixer{}},
		{sourceValidation, string(pkg_model.NodeTypePythonNode), pythonValidationFixer{}},
		{sourceValidation, string(pkg_model.NodeTypePythonCsv), csvValidationFixer{}},
		{sourceValidation, string(pkg_model.NodeTypePythonApi), pythonValidationFixer{}},
		{sourceValidation, string(pkg_model.NodeTypeDbtModel), validationFixer{}},
	}
	for _, c := range cases {
		got, err := For(c.source, c.nodeType)
		if err != nil || got != c.want {
			t.Errorf("For(%q, %q) = %T, %v; want %T", c.source, c.nodeType, got, err, c.want)
		}
	}
}
