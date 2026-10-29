package runplan_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sourceGraph is what the planner passes a rerun: the source release
// restricted to the source run's nodes.
func sourceGraph(dag []fixtureNode, src runplan.Source) topology.Graph {
	return releaseGraph(dag).Restrict(sourceKeys(src))
}

// The e2e-schedule-failure run: the failures and the tasks they skipped run again,
// and of those only the failures are ready, since each reads only inherited
// tasks.
func TestRerun_ReExecutesTheFailuresAndTheirDescendants(t *testing.T) {
	src := failedFailureRun()
	plan, err := runplan.Rerun{Source: src}.Select(sourceGraph(e2eDAG, src))
	require.NoError(t, err)
	assert.Equal(t, planOf(append(
		inherited("ftable_a", "ftable_b", "ftable_c", "ftable_d"),
		pending("ftable_e", "ftable_f", "ftable_g", "ftable_h")...,
	), "ftable_e", "ftable_g"), plan)
}

// A task whose upstreams are all inherited has nothing to wait for: ftable_c
// is dispatched when the rerun is created, and its descendants wait for it.
func TestRerun_ANodeWhoseUpstreamsAreAllInheritedIsReadyAtCreation(t *testing.T) {
	src := sourceRun(model.OperationRun,
		[]string{"ftable_a", "ftable_b", "ftable_g", "ftable_h"},
		[]string{"ftable_c", "ftable_d", "ftable_e", "ftable_f"})
	plan, err := runplan.Rerun{Source: src}.Select(sourceGraph(e2eDAG, src))
	require.NoError(t, err)
	assert.Equal(t, planOf(append(
		inherited("ftable_a", "ftable_b", "ftable_g", "ftable_h"),
		pending("ftable_c", "ftable_d", "ftable_e", "ftable_f")...,
	), "ftable_c"), plan)
}

func TestRerun_OfTheLayeredSchedule(t *testing.T) {
	src := sourceRun(model.OperationBuild,
		[]string{"seed_table_1", "seed_table_2", "seed_table_3", "table_a", "table_b", "table_c", "table_d", "table_e", "table_f", "table_h"},
		[]string{"table_g", "table_i", "table_j"})
	plan, err := runplan.Rerun{Source: src}.Select(sourceGraph(e2eDAG, src))
	require.NoError(t, err)
	assert.Equal(t, planOf(append(
		inherited("seed_table_1", "seed_table_2", "seed_table_3", "table_a", "table_b", "table_c", "table_d", "table_e", "table_f", "table_h"),
		pending("table_g", "table_i", "table_j")...,
	), "table_g"), plan)
}

// The rerun reads only the source's own tasks: a node of the release the
// source never ran does not join it, however wide the slice it is given.
func TestRerun_KeepsToTheSourceTasks(t *testing.T) {
	src := failedFailureRun()
	release := releaseGraph(changed(e2eDAG, nil, fixtureNode{"ftable_i", "service-2", "e2e-schedule-failure", []string{"ftable_e"}}))
	plan, err := runplan.Rerun{Source: src}.Select(release)
	require.NoError(t, err)
	want, err := runplan.Rerun{Source: src}.Select(sourceGraph(e2eDAG, src))
	require.NoError(t, err)
	assert.Equal(t, want, plan)
	assert.NotContains(t, plan.Tasks, runplan.PlannedTask{Key: key("ftable_i")})
}

// A carried-over task points at the task that last ran the node: a source
// task that was itself inherited passes its own root on.
func TestRerun_AnInheritedTaskKeepsItsRoot(t *testing.T) {
	src := failedFailureRun()
	root := uuid.NewSHA1(uuid.NameSpaceURL, []byte("continuo/earlier-run/ftable_a"))
	src.Tasks[0].InheritedFrom = root
	require.Equal(t, key("ftable_a"), src.Tasks[0].Key)

	plan, err := runplan.Rerun{Source: src}.Select(sourceGraph(e2eDAG, src))
	require.NoError(t, err)
	assert.Equal(t, runplan.PlannedTask{Key: key("ftable_a"), Inherited: true, InheritedFrom: root}, plan.Tasks[0])
	assert.Equal(t, runplan.PlannedTask{Key: key("ftable_b"), Inherited: true, InheritedFrom: sourceTaskID("ftable_b")}, plan.Tasks[1])
}

func TestRerun_ASourceWithoutAnUnsucceededTaskHasNothingToRerun(t *testing.T) {
	src := sourceRun(model.OperationRun, []string{"ftable_a", "ftable_b", "ftable_c"}, nil)
	_, err := runplan.Rerun{Source: src}.Select(sourceGraph(e2eDAG, src))
	assert.ErrorIs(t, err, runplan.ErrNothingToRerun)

	_, err = runplan.Rerun{Source: runplan.Source{}}.Select(releaseGraph(e2eDAG))
	assert.ErrorIs(t, err, runplan.ErrNothingToRerun)
}

func TestRerun_OfATestRunIsRefused(t *testing.T) {
	src := sourceRun(model.OperationTest, nil, []string{"seed_table_1"})
	_, err := runplan.Rerun{Source: src}.Select(sourceGraph(e2eDAG, src))
	assert.ErrorIs(t, err, runplan.ErrRerunOfTestUnsupported)
}

// A source task whose node its release does not hold cannot be reproduced,
// so the rerun is refused rather than run without it.
func TestRerun_ASourceTaskOutsideItsReleaseIsNotFound(t *testing.T) {
	src := failedFailureRun()
	src.Tasks = append(src.Tasks, runplan.SourceTask{Key: key("ftable_z"), TaskID: sourceTaskID("ftable_z")})
	_, err := runplan.Rerun{Source: src}.Select(releaseGraph(e2eDAG))
	require.ErrorIs(t, err, runplan.ErrTargetNotFound)
	assert.Contains(t, err.Error(), "e2e_schema.ftable_z")
}

// An undeclared node type is refused on a task the rerun executes; on a task
// it carries over it is never dispatched, so it is not.
func TestRerun_AnUndeclaredNodeTypeIsRefusedOnlyOnATaskItExecutes(t *testing.T) {
	src := failedFailureRun()
	_, err := runplan.Rerun{Source: src}.Select(retyped(sourceGraph(e2eDAG, src), "ftable_f", "dbt-macro"))
	assert.ErrorIs(t, err, runplan.ErrInvalidNodeType)

	_, err = runplan.Rerun{Source: src}.Select(retyped(sourceGraph(e2eDAG, src), "ftable_d", "dbt-macro"))
	assert.NoError(t, err)
}
