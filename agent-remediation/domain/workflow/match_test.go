package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatch_Selects(t *testing.T) {
	dbtValidation := Match{Cluster: ClusterSingle, Source: "validation",
		NodeTypes: []string{"dbt-model", "dbt-seed", "dbt-snapshot", NodeTypeNone}}
	assert.True(t, dbtValidation.Selects(ClusterSingle, "validation", "dbt-model"))
	assert.True(t, dbtValidation.Selects(ClusterSingle, "validation", "")) // "none"
	assert.False(t, dbtValidation.Selects(ClusterSingle, "validation", "python-node"))
	assert.False(t, dbtValidation.Selects(ClusterSingle, "validation", "dbt-test"))
	assert.False(t, dbtValidation.Selects(ClusterSharedUpstream, "validation", "dbt-model"))
	assert.True(t, Match{Cluster: ClusterSharedUpstream}.Selects(ClusterSharedUpstream, "compile", ""))
}

func TestMatch_Overlaps(t *testing.T) {
	py := Match{Cluster: ClusterSingle, Source: "validation", NodeTypes: []string{"python-node", "python-api"}}
	csv := Match{Cluster: ClusterSingle, Source: "validation", NodeTypes: []string{"python-csv"}}
	anyValidation := Match{Cluster: ClusterSingle, Source: "validation"}
	assert.False(t, py.Overlaps(csv))
	assert.True(t, py.Overlaps(anyValidation))
	assert.False(t, py.Overlaps(Match{Cluster: ClusterSingle, Source: "parse", NodeTypes: []string{"python-node"}}))
	assert.False(t, py.Overlaps(Match{Cluster: ClusterSharedUpstream}))
}

func TestMatch_NoneIsNotAnEmptyList(t *testing.T) {
	onlyPython := Match{Cluster: ClusterSingle, NodeTypes: []string{"python-node"}}
	assert.False(t, onlyPython.Selects(ClusterSingle, "validation", ""))
	assert.True(t, Match{Cluster: ClusterSingle, NodeTypes: []string{NodeTypeNone}}.Selects(ClusterSingle, "validation", ""))
	assert.False(t, Match{Cluster: ClusterSingle, NodeTypes: []string{NodeTypeNone}}.Selects(ClusterSingle, "validation", "dbt-model"))
}

func TestSet_TypeFor_NeverReturnsATypeTheFlowDoesNotList(t *testing.T) {
	single := Match{Cluster: ClusterSingle, Source: "validation"}
	set := Set{Types: map[string]Type{
		"a": {Name: "a", Match: Match{Cluster: ClusterSingle, Source: "compile"}},
		"b": {Name: "b", Match: Match{Cluster: ClusterSharedUpstream}},
		"c": {Name: "c", Match: single},
	}}

	listsAB := Flow{Name: "f", Types: []string{"a", "b"}}
	_, ok := set.TypeFor(listsAB, ClusterSingle, "validation", "dbt-model")
	assert.False(t, ok, "type c matches the failure but the flow does not list it")

	got, ok := set.TypeFor(listsAB, ClusterSingle, "compile", "dbt-model")
	assert.True(t, ok)
	assert.Equal(t, "a", got.Name)

	listsABC := Flow{Name: "f", Types: []string{"a", "b", "c"}}
	got, ok = set.TypeFor(listsABC, ClusterSingle, "validation", "")
	assert.True(t, ok)
	assert.Equal(t, "c", got.Name)

	_, ok = set.TypeFor(Flow{Name: "f", Types: []string{"missing"}}, ClusterSingle, "validation", "")
	assert.False(t, ok, "a name the set does not define selects nothing")
}
