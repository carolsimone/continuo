package runplan_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleNode_SelectsTheNodeReady(t *testing.T) {
	for _, op := range []model.Operation{model.OperationRun, model.OperationBuild, model.OperationFullRefresh} {
		plan, err := runplan.SingleNode{Key: key("table_d"), ServiceName: "service-3", Operation: op}.Select(releaseGraph(e2eDAG))
		require.NoError(t, err, "operation %q", op)
		assert.Equal(t, planOf(pending("table_d"), "table_d"), plan, "operation %q", op)
	}
}

func TestSingleNode_ANodeOutsideTheReleaseOrTheServiceIsNotFound(t *testing.T) {
	g := releaseGraph(e2eDAG)
	_, err := runplan.SingleNode{Key: key("table_z"), ServiceName: "service-3"}.Select(g)
	assert.ErrorIs(t, err, runplan.ErrTargetNotFound)

	_, err = runplan.SingleNode{Key: key("table_d"), ServiceName: "service-1"}.Select(g)
	require.ErrorIs(t, err, runplan.ErrTargetNotFound)
	assert.Contains(t, err.Error(), `e2e_schema.table_d of service "service-1"`)
}

// Only a known, positive data-test count is testable: seed_table_2 has a known
// 0, and a node whose count is unknown cannot be shown to have a test.
func TestSingleNode_TestNeedsAKnownPositiveCount(t *testing.T) {
	g := releaseGraph(e2eDAG)
	plan, err := runplan.SingleNode{Key: key("seed_table_1"), ServiceName: "service-1", Operation: model.OperationTest}.Select(g)
	require.NoError(t, err)
	assert.Equal(t, planOf(pending("seed_table_1"), "seed_table_1"), plan)

	_, err = runplan.SingleNode{Key: key("seed_table_2"), ServiceName: "service-1", Operation: model.OperationTest}.Select(g)
	assert.ErrorIs(t, err, runplan.ErrNoTests)

	_, err = runplan.SingleNode{Key: key("legacy"), ServiceName: "service-1", Operation: model.OperationTest}.Select(withNode("legacy", "dbt-model", nil))
	assert.ErrorIs(t, err, runplan.ErrNoTests)
}

func TestSingleNode_FullRefreshNeedsATypeThatSupportsIt(t *testing.T) {
	_, err := runplan.SingleNode{Key: key("seed_table_1"), ServiceName: "service-1", Operation: model.OperationFullRefresh}.Select(releaseGraph(e2eDAG))
	require.NoError(t, err, "a dbt seed can be rebuilt from scratch")

	for _, nodeType := range []string{"dbt-snapshot", "python-node", "python-csv", "python-api"} {
		_, err := runplan.SingleNode{Key: key("n"), ServiceName: "service-1", Operation: model.OperationFullRefresh}.Select(withNode("n", nodeType, count(0)))
		require.ErrorIs(t, err, runplan.ErrFullRefreshUnsupported, nodeType)
		assert.Contains(t, err.Error(), nodeType)
	}
}

func TestSingleNode_AnUndeclaredNodeTypeIsRefused(t *testing.T) {
	_, err := runplan.SingleNode{Key: key("n"), ServiceName: "service-1"}.Select(withNode("n", "dbt-macro", count(0)))
	assert.ErrorIs(t, err, runplan.ErrInvalidNodeType)
}

// An unknown-external node is a table no node builds, so it is never a task. A run
// that names its key is refused as a run of an unknown-external node whatever the
// operation, and before the service it names is compared or its data tests
// and rebuildability are looked at: the service of an unknown-external node is empty,
// so a request that names one would otherwise be a node of another service.
func TestSingleNode_RefusesAnUnknownExternalNode(t *testing.T) {
	g := withUnknownExternalUpstream(releaseGraph(e2eDAG), "table_a")
	for _, service := range []string{"", "service-1"} {
		for _, op := range []model.Operation{model.OperationRun, model.OperationBuild, model.OperationTest, model.OperationFullRefresh} {
			_, err := runplan.SingleNode{Key: unknownExternalProbe, ServiceName: service, Operation: op}.Select(g)
			require.ErrorIs(t, err, runplan.ErrExternalNode, "service %q, operation %q", service, op)
			assert.NotErrorIs(t, err, runplan.ErrTargetNotFound, "service %q, operation %q", service, op)
			assert.Contains(t, err.Error(), "raw.ingest_probe")
		}
	}
}

// A key the release does not hold is still not found: the unknown-external check
// reads the node the lookup found.
func TestSingleNode_AnUnknownExternalKeyOfAnotherReleaseIsNotFound(t *testing.T) {
	_, err := runplan.SingleNode{Key: unknownExternalProbe, ServiceName: ""}.Select(releaseGraph(e2eDAG))
	assert.ErrorIs(t, err, runplan.ErrTargetNotFound)
}

// A dormant node is on no schedule, and a run of the node alone executes it:
// the plan holds its one task, ready at creation, whatever it reads.
func TestSingleNode_SelectsADormantNode(t *testing.T) {
	g := releaseGraph(dormantDAG)
	n, ok := g.Node(key("dormant_d"))
	require.True(t, ok)
	require.True(t, n.IsDormant(), "the fixture node is dormant")

	for _, op := range []model.Operation{model.OperationRun, model.OperationBuild, model.OperationFullRefresh} {
		plan, err := runplan.SingleNode{Key: key("dormant_d"), ServiceName: "service-3", Operation: op}.Select(g)
		require.NoError(t, err, "operation %q", op)
		assert.Equal(t, planOf(pending("dormant_d"), "dormant_d"), plan, "operation %q", op)
	}
}
