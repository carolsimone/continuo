package runplan_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// e2e-schedule pulls in its three seeds from the seed schedule; they are the
// only nodes with no upstream, so they are dispatched first.
func TestWholeDAG_RunDispatchesTheNodesWithoutUpstreamFirst(t *testing.T) {
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(scheduleGraph(e2eDAG, "e2e-schedule"))
	require.NoError(t, err)
	assert.Equal(t, planOf(pending(
		"seed_table_1", "seed_table_2", "seed_table_3",
		"table_a", "table_b", "table_c", "table_d", "table_e", "table_f", "table_g", "table_h", "table_i", "table_j",
	), "seed_table_1", "seed_table_2", "seed_table_3"), plan)
}

func TestWholeDAG_RunOfTheFailureSchedule(t *testing.T) {
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(scheduleGraph(e2eDAG, "e2e-schedule-failure"))
	require.NoError(t, err)
	assert.Equal(t, planOf(pending(
		"ftable_a", "ftable_b", "ftable_c", "ftable_d", "ftable_e", "ftable_f", "ftable_g", "ftable_h",
	), "ftable_a", "ftable_b"), plan)
}

func TestWholeDAG_BuildPlansLikeRun(t *testing.T) {
	g := scheduleGraph(e2eDAG, "e2e-schedule-failure")
	run, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)
	build, err := runplan.WholeDAG{Operation: model.OperationBuild}.Select(g)
	require.NoError(t, err)
	assert.Equal(t, run, build)
}

// A test run tests only the nodes with a known, positive count, and none
// waits for another; e2e-schedule's seeds count, though they belong to the
// seed schedule.
func TestWholeDAG_TestFansOutOverTheNodesWithTests(t *testing.T) {
	for _, schedule := range []string{"seed", "e2e-schedule"} {
		t.Run(schedule, func(t *testing.T) {
			plan, err := runplan.WholeDAG{Operation: model.OperationTest}.Select(scheduleGraph(e2eDAG, schedule))
			require.NoError(t, err)
			assert.Equal(t, planOf(pending("seed_table_1", "seed_table_3"), "seed_table_1", "seed_table_3"), plan)
		})
	}
}

func TestWholeDAG_TestLeavesOutUnknownAndZeroCounts(t *testing.T) {
	g := topology.NewGraph([]topology.Node{
		{Key: key("unknown"), NodeType: "dbt-model"},
		{Key: key("zero"), NodeType: "dbt-model", TestCount: count(0)},
		{Key: key("three"), NodeType: "dbt-model", TestCount: count(3)},
	}, []topology.Edge{{Upstream: key("unknown"), Downstream: key("three")}})
	plan, err := runplan.WholeDAG{Operation: model.OperationTest}.Select(g)
	require.NoError(t, err)
	assert.Equal(t, planOf(pending("three"), "three"), plan)
}

func TestWholeDAG_TestWithoutTestsIsRefused(t *testing.T) {
	_, err := runplan.WholeDAG{Operation: model.OperationTest}.Select(scheduleGraph(e2eDAG, "e2e-schedule-failure"))
	assert.ErrorIs(t, err, runplan.ErrNoTests)
}

func TestWholeDAG_AnEmptyScheduleIsRefused(t *testing.T) {
	empty := scheduleGraph(e2eDAG, "no-such-schedule")
	for _, op := range []model.Operation{model.OperationRun, model.OperationBuild, model.OperationTest} {
		_, err := runplan.WholeDAG{Operation: op}.Select(empty)
		assert.ErrorIs(t, err, runplan.ErrEmptySelection, "operation %q", op)
	}
}

// An undeclared node type is refused before anything is dispatched, wherever
// the node sits; a test run refuses it only on a node it would test.
func TestWholeDAG_AnUndeclaredNodeTypeIsRefused(t *testing.T) {
	g := topology.NewGraph([]topology.Node{
		{Key: key("ok"), NodeType: "dbt-model", TestCount: count(1)},
		{Key: key("odd"), NodeType: "dbt-macro", TestCount: count(0)},
	}, []topology.Edge{{Upstream: key("ok"), Downstream: key("odd")}})

	_, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.ErrorIs(t, err, runplan.ErrInvalidNodeType)
	assert.Contains(t, err.Error(), `e2e_schema.odd has node type "dbt-macro"`)

	plan, err := runplan.WholeDAG{Operation: model.OperationTest}.Select(g)
	require.NoError(t, err)
	assert.Equal(t, planOf(pending("ok"), "ok"), plan)
}

// An unknown-external node has no schedule, so the graph of a schedule holds none. A
// graph that holds one is planned without it, as a schedule that does not
// name it: the node is no task, and what reads it is ready at creation.
func TestWholeDAG_LeavesAnUnknownExternalNodeOut(t *testing.T) {
	schedule := scheduleGraph(e2eDAG, "e2e-schedule")
	g := withUnknownExternalUpstream(schedule, "seed_table_1", "table_a")
	for _, op := range []model.Operation{model.OperationRun, model.OperationBuild, model.OperationTest} {
		got, err := runplan.WholeDAG{Operation: op}.Select(g)
		require.NoError(t, err, "operation %q", op)
		want, err := runplan.WholeDAG{Operation: op}.Select(schedule)
		require.NoError(t, err, "operation %q", op)
		assert.Equal(t, want, got, "operation %q", op)
	}
}

func TestWholeDAG_AGraphOfUnknownExternalNodesOnlyIsEmpty(t *testing.T) {
	g := topology.NewGraph([]topology.Node{unknownExternalNode()}, nil)
	for _, op := range []model.Operation{model.OperationRun, model.OperationBuild, model.OperationTest} {
		_, err := runplan.WholeDAG{Operation: op}.Select(g)
		assert.ErrorIs(t, err, runplan.ErrEmptySelection, "operation %q", op)
	}
}
