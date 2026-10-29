package runplan_test

import (
	"testing"

	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeeds_EverySeedIsAReadyTask(t *testing.T) {
	plan, err := runplan.Seeds{Keys: keys("seed_table_3", "seed_table_1", "seed_table_3")}.Select(releaseGraph(e2eDAG))
	require.NoError(t, err)
	assert.Equal(t, planOf(pending("seed_table_1", "seed_table_3"), "seed_table_1", "seed_table_3"), plan)
}

func TestSeeds_AnEmptySetIsRefused(t *testing.T) {
	_, err := runplan.Seeds{}.Select(releaseGraph(e2eDAG))
	assert.ErrorIs(t, err, runplan.ErrEmptySelection)
}

// A seed the release does not hold fails the whole selection rather than
// shrinking it: the reconcile asked for every one of them.
func TestSeeds_ASeedOutsideTheReleaseIsNotFound(t *testing.T) {
	_, err := runplan.Seeds{Keys: keys("seed_table_1", "seed_table_9")}.Select(releaseGraph(e2eDAG))
	require.ErrorIs(t, err, runplan.ErrTargetNotFound)
	assert.Contains(t, err.Error(), "e2e_schema.seed_table_9")
}

func TestSeeds_AnUndeclaredNodeTypeIsRefused(t *testing.T) {
	_, err := runplan.Seeds{Keys: keys("n")}.Select(withNode("n", "dbt-macro", count(0)))
	assert.ErrorIs(t, err, runplan.ErrInvalidNodeType)
}
