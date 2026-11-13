package model_test

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

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
		model.NodeTypeDbtModel:        false,
		model.NodeTypeDbtSeed:         false,
		model.NodeTypeDbtSnapshot:     false,
		model.NodeTypeDbtTest:         false,
		model.NodeTypePythonNode:      true,
		model.NodeTypePythonCsv:       true,
		model.NodeTypePythonApi:       true,
		model.NodeTypeUnknownExternal: false,
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
// a relation-producing dbt kind, a python kind, the validation-only dbt-test, or
// the unknown-external kind no node builds.
func TestEveryNodeTypeIsClassified(t *testing.T) {
	for _, nt := range model.NodeTypes() {
		isDbtRelation := nt.Runtime() == model.NodeRuntimeDbt && nt != model.NodeTypeDbtTest
		isTest := nt == model.NodeTypeDbtTest
		families := 0
		for _, in := range []bool{isDbtRelation, nt.IsPython(), isTest, nt.IsExternal()} {
			if in {
				families++
			}
		}
		if families != 1 {
			t.Errorf("NodeType %q is in %d families, want exactly 1", nt, families)
		}
	}
}

func TestIsExternal(t *testing.T) {
	for _, nt := range model.NodeTypes() {
		assert.Equal(t, nt == model.NodeTypeUnknownExternal, nt.IsExternal(), nt)
	}
	assert.Equal(t, model.NodeRuntimeExternal, model.NodeTypeUnknownExternal.Runtime())
	assert.False(t, model.NodeTypeUnknownExternal.SupportsFullRefresh())
	assert.False(t, model.NodeTypeUnknownExternal.AllowsSecretRef())
}

type dormantCase struct {
	NodeType string `json:"node_type"`
	Schedule string `json:"schedule"`
	Dormant  bool   `json:"dormant"`
}

// dormant_cases.json is shared with topology-controller's tests, which read a
// copy of it, so the Go and the Python rule answer the same questions.
func loadDormantCases(t *testing.T) []dormantCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/dormant_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []dormantCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

func TestIsDormant_FollowsTheSharedFixture(t *testing.T) {
	cases := loadDormantCases(t)
	covered := map[model.NodeType]bool{}
	for _, c := range cases {
		covered[model.NodeType(c.NodeType)] = true
		assert.Equal(t, c.Dormant, model.IsDormant(model.NodeType(c.NodeType), c.Schedule),
			"IsDormant(%q, %q)", c.NodeType, c.Schedule)
	}
	for _, nt := range model.NodeTypes() {
		assert.True(t, covered[nt], "dormant_cases.json has no case for %s", nt)
	}
}

func TestBuiltNodeTypes_AreTheDbtAndPythonKindsExceptTests(t *testing.T) {
	built := model.BuiltNodeTypes()
	for _, nt := range model.NodeTypes() {
		wantBuilt := (nt.Runtime() == model.NodeRuntimeDbt || nt.Runtime() == model.NodeRuntimePython) &&
			nt != model.NodeTypeDbtTest
		assert.Equal(t, wantBuilt, nt.IsBuilt(), "IsBuilt(%q)", nt)
		assert.Equal(t, wantBuilt, slices.Contains(built, nt), "BuiltNodeTypes contains %q", nt)
	}
	assert.NotContains(t, built, model.NodeTypeUnknownExternal)
	assert.NotContains(t, built, model.NodeTypeDbtTest)
	assert.ElementsMatch(t, []model.NodeType{
		model.NodeTypeDbtModel, model.NodeTypeDbtSeed, model.NodeTypeDbtSnapshot,
		model.NodeTypePythonNode, model.NodeTypePythonCsv, model.NodeTypePythonApi,
	}, built)
	assert.False(t, model.NodeType("no-such-type").IsBuilt())
}

func TestBuiltNodeTypes_FollowsDeclarationOrder(t *testing.T) {
	var want []model.NodeType
	for _, nt := range model.NodeTypes() {
		if nt.IsBuilt() {
			want = append(want, nt)
		}
	}
	assert.Equal(t, want, model.BuiltNodeTypes())
}

func TestCanonicalNodeType_ReadsIngestedAsUnknownExternal(t *testing.T) {
	assert.Equal(t, model.NodeTypeUnknownExternal, model.CanonicalNodeType("ingested"))
	assert.Equal(t, model.NodeTypeDbtModel, model.CanonicalNodeType("dbt-model"))
	assert.Equal(t, model.NodeTypeUnknownExternal, model.CanonicalNodeType("unknown-external"))
	assert.Equal(t, model.NodeType("x"), model.CanonicalNodeType("x"))
	assert.Equal(t, model.NodeType(""), model.CanonicalNodeType(""))
}
