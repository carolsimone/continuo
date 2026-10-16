package runplan_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRebase_OnAnUnchangedReleaseSelectsWhatARerunSelects(t *testing.T) {
	src := failedFailureRun()
	plan, err := runplan.Rebase{Source: src}.Select(scheduleGraph(e2eDAG, "e2e-schedule-failure"))
	require.NoError(t, err)
	assert.Equal(t, planOf(append(
		inherited("ftable_a", "ftable_b", "ftable_c", "ftable_d"),
		pending("ftable_e", "ftable_f", "ftable_g", "ftable_h")...,
	), "ftable_e", "ftable_g"), plan)
}

// The live release dropped ftable_h and added ftable_i (reads ftable_d) and
// ftable_j (reads ftable_e). The dropped node leaves the run; the new nodes
// join it, ftable_i ready at once since ftable_d is inherited, ftable_j
// waiting for ftable_e.
func TestRebase_LeavesOutDroppedNodesAndRunsNewOnes(t *testing.T) {
	live := changed(e2eDAG, []string{"ftable_h"},
		fixtureNode{"ftable_i", "service-2", "e2e-schedule-failure", []string{"ftable_d"}},
		fixtureNode{"ftable_j", "service-3", "e2e-schedule-failure", []string{"ftable_e"}},
	)
	pv, err := runplan.Rebase{Source: failedFailureRun()}.Preview(scheduleGraph(live, "e2e-schedule-failure"))
	require.NoError(t, err)
	assert.Equal(t, planOf(append(
		inherited("ftable_a", "ftable_b", "ftable_c", "ftable_d"),
		pending("ftable_e", "ftable_f", "ftable_g", "ftable_i", "ftable_j")...,
	), "ftable_e", "ftable_g", "ftable_i"), pv.Plan)
	assert.Equal(t, counts{FailedOrSkipped: 3, New: 2, Inherited: 4}, countsOf(t, pv),
		"the dropped ftable_h is neither run nor inherited")
}

// A new node runs, and so does every source success below it: a node that
// reads a table the run builds anew must be built again after it. ftable_d
// waits for the new ftable_x; ftable_f waits for both.
func TestRebase_ANewUpstreamReRunsTheSuccessesBelowIt(t *testing.T) {
	live := changed(e2eDAG, []string{"ftable_d"},
		fixtureNode{"ftable_x", "service-1", "e2e-schedule-failure", nil},
		fixtureNode{"ftable_d", "service-2", "e2e-schedule-failure", []string{"ftable_c", "ftable_x"}},
	)
	pv, err := runplan.Rebase{Source: failedFailureRun()}.Preview(scheduleGraph(live, "e2e-schedule-failure"))
	require.NoError(t, err)
	assert.Equal(t, planOf(append(
		inherited("ftable_a", "ftable_b", "ftable_c"),
		pending("ftable_d", "ftable_e", "ftable_f", "ftable_g", "ftable_h", "ftable_x")...,
	), "ftable_e", "ftable_g", "ftable_x"), pv.Plan)
	assert.Equal(t, counts{FailedOrSkipped: 4, New: 1, Downstream: 1, Inherited: 3}, countsOf(t, pv))
}

// The source's seeds come from the seed schedule; they are carried over like
// any other success.
func TestRebase_OfTheLayeredSchedule(t *testing.T) {
	src := sourceRun(model.OperationRun,
		[]string{"seed_table_1", "seed_table_2", "seed_table_3", "table_a", "table_b", "table_c", "table_d", "table_e", "table_f", "table_h"},
		[]string{"table_g", "table_i", "table_j"})
	plan, err := runplan.Rebase{Source: src}.Select(scheduleGraph(e2eDAG, "e2e-schedule"))
	require.NoError(t, err)
	assert.Equal(t, planOf(append(
		inherited("seed_table_1", "seed_table_2", "seed_table_3", "table_a", "table_b", "table_c", "table_d", "table_e", "table_f", "table_h"),
		pending("table_g", "table_i", "table_j")...,
	), "table_g"), plan)
}

func TestRebase_AnInheritedTaskKeepsItsRoot(t *testing.T) {
	src := failedFailureRun()
	root := uuid.NewSHA1(uuid.NameSpaceURL, []byte("continuo/earlier-run/ftable_b"))
	src.Tasks[1].InheritedFrom = root
	require.Equal(t, key("ftable_b"), src.Tasks[1].Key)

	plan, err := runplan.Rebase{Source: src}.Select(scheduleGraph(e2eDAG, "e2e-schedule-failure"))
	require.NoError(t, err)
	assert.Equal(t, runplan.PlannedTask{Key: key("ftable_b"), Inherited: true, InheritedFrom: root}, plan.Tasks[1])
}

// The only failure's node is gone from the live release and nothing is new,
// so there is nothing left to execute.
func TestRebase_ADroppedFailureLeavesNothingToRerun(t *testing.T) {
	src := sourceRun(model.OperationRun,
		[]string{"ftable_a", "ftable_b", "ftable_c", "ftable_d", "ftable_e", "ftable_f", "ftable_g"},
		[]string{"ftable_h"})
	live := changed(e2eDAG, []string{"ftable_h"})
	_, err := runplan.Rebase{Source: src}.Select(scheduleGraph(live, "e2e-schedule-failure"))
	assert.ErrorIs(t, err, runplan.ErrNothingToRerun)
}

func TestRebase_ASourceWhereEveryTaskSucceededHasNothingToRerun(t *testing.T) {
	src := sourceRun(model.OperationRun,
		[]string{"ftable_a", "ftable_b", "ftable_c", "ftable_d", "ftable_e", "ftable_f", "ftable_g", "ftable_h"}, nil)
	_, err := runplan.Rebase{Source: src}.Select(scheduleGraph(e2eDAG, "e2e-schedule-failure"))
	assert.ErrorIs(t, err, runplan.ErrNothingToRerun)
}

func TestRebase_OfATestRunIsRefused(t *testing.T) {
	src := sourceRun(model.OperationTest, nil, []string{"seed_table_1"})
	_, err := runplan.Rebase{Source: src}.Select(scheduleGraph(e2eDAG, "seed"))
	assert.ErrorIs(t, err, runplan.ErrRerunOfTestUnsupported)
}

func TestRebase_AnUndeclaredNodeTypeOnANewNodeIsRefused(t *testing.T) {
	live := changed(e2eDAG, nil, fixtureNode{"ftable_i", "service-2", "e2e-schedule-failure", []string{"ftable_d"}})
	g := retyped(scheduleGraph(live, "e2e-schedule-failure"), "ftable_i", "dbt-macro")
	_, err := runplan.Rebase{Source: failedFailureRun()}.Select(g)
	require.ErrorIs(t, err, runplan.ErrInvalidNodeType)
	assert.Contains(t, err.Error(), "e2e_schema.ftable_i")
}

// A single-node run's schedule name ("single-node-run-…") names no live
// schedule, so its live graph is empty: a rebase never widens one node's run,
// a full refresh say, into a schedule's.
func TestRebase_ASingleNodeRunHasNothingToRebase(t *testing.T) {
	src := sourceRun(model.OperationFullRefresh, nil, []string{"table_d"})
	_, err := runplan.Rebase{Source: src}.Select(scheduleGraph(e2eDAG, "single-node-run-1a2b3c4d"))
	assert.ErrorIs(t, err, runplan.ErrNothingToRerun)
}
