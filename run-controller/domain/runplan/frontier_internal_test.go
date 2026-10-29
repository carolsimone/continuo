package runplan

import (
	"testing"

	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failureShape is the e2e failure schedule's top: stg.a and stg.b feed stg.c,
// stg.c feeds stg.d, and stg.a feeds stg.g.
func failureShape() topology.Graph {
	n := func(k topology.NodeKey) topology.Node { return topology.Node{Key: k, NodeType: "dbt-model"} }
	e := func(up, down topology.NodeKey) topology.Edge { return topology.Edge{Upstream: up, Downstream: down} }
	return topology.NewGraph(
		[]topology.Node{n("stg.a"), n("stg.b"), n("stg.c"), n("stg.d"), n("stg.g")},
		[]topology.Edge{e("stg.a", "stg.c"), e("stg.b", "stg.c"), e("stg.c", "stg.d"), e("stg.a", "stg.g")},
	)
}

func set(keys ...topology.NodeKey) map[topology.NodeKey]bool {
	out := make(map[topology.NodeKey]bool, len(keys))
	for _, k := range keys {
		out[k] = true
	}
	return out
}

func TestDagPlan_ReadyAreThePendingTasksWithoutAPendingUpstream(t *testing.T) {
	plan := dagPlan(failureShape(), set("stg.a", "stg.b", "stg.c", "stg.d", "stg.g"), nil)
	assert.Equal(t, []PlannedTask{{Key: "stg.a"}, {Key: "stg.b"}, {Key: "stg.c"}, {Key: "stg.d"}, {Key: "stg.g"}}, plan.Tasks)
	assert.Equal(t, []topology.NodeKey{"stg.a", "stg.b"}, plan.Ready)
}

// A task whose every upstream is inherited has nothing to wait for, so it is
// dispatched when the run is created; a task whose upstream is pending waits.
func TestDagPlan_InheritedUpstreamsDoNotHoldATaskBack(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	plan := dagPlan(failureShape(), set("stg.c", "stg.d", "stg.g"), map[topology.NodeKey]uuid.UUID{"stg.a": a, "stg.b": b})
	assert.Equal(t, []PlannedTask{
		{Key: "stg.a", Inherited: true, InheritedFrom: a},
		{Key: "stg.b", Inherited: true, InheritedFrom: b},
		{Key: "stg.c"},
		{Key: "stg.d"},
		{Key: "stg.g"},
	}, plan.Tasks)
	assert.Equal(t, []topology.NodeKey{"stg.c", "stg.g"}, plan.Ready)
}

// An upstream that is no task of the run is never going to succeed in it, so
// it does not hold its downstream back.
func TestDagPlan_AnUpstreamOutsideThePlanDoesNotHoldATaskBack(t *testing.T) {
	plan := dagPlan(failureShape(), set("stg.c", "stg.d"), nil)
	assert.Equal(t, []topology.NodeKey{"stg.c"}, plan.Ready)
}

func TestDagPlan_AKeyBothPendingAndInheritedIsPending(t *testing.T) {
	plan := dagPlan(failureShape(), set("stg.a"), map[topology.NodeKey]uuid.UUID{"stg.a": uuid.New()})
	assert.Equal(t, []PlannedTask{{Key: "stg.a"}}, plan.Tasks)
	assert.Equal(t, []topology.NodeKey{"stg.a"}, plan.Ready)
}

func TestIsReady_OnlyAPendingTaskCanBeReady(t *testing.T) {
	assert.False(t, isReady(failureShape(), "stg.a", set("stg.c")))
	assert.True(t, isReady(failureShape(), "stg.c", set("stg.c")))
}

func TestFlatPlan_EveryDistinctKeyIsAReadyTask(t *testing.T) {
	plan := flatPlan([]topology.NodeKey{"stg.c", "stg.a", "stg.c"})
	assert.Equal(t, []PlannedTask{{Key: "stg.a"}, {Key: "stg.c"}}, plan.Tasks)
	assert.Equal(t, []topology.NodeKey{"stg.a", "stg.c"}, plan.Ready)
	assert.True(t, plan.HasPending())
}

func TestCheckNodeTypes_RefusesTheFirstUndeclaredTypeInKeyOrder(t *testing.T) {
	g := topology.NewGraph([]topology.Node{
		{Key: "stg.a", NodeType: "dbt-model"},
		{Key: "stg.b", NodeType: "dbt-macro"},
		{Key: "stg.c", NodeType: ""},
	}, nil)

	require.NoError(t, checkNodeTypes(g, []topology.NodeKey{"stg.a", "raw.ghost"}))
	err := checkNodeTypes(g, []topology.NodeKey{"stg.c", "stg.a", "stg.b"})
	require.ErrorIs(t, err, ErrInvalidNodeType)
	assert.Contains(t, err.Error(), `stg.b has node type "dbt-macro"`)
}
