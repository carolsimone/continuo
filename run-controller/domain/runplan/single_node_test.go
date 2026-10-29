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
