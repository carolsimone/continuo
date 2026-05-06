package neo4jinfra_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestQueryRepo builds an OrchestratorQueryRepository against the live Neo4j and returns
// it together with a cleanup function that wipes any nodes created by the test.
// It intentionally re-uses the env conventions already in use by run_repository_test.go
// (NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD with the docker-compose defaults).
func newTestQueryRepo(t *testing.T) (*neo4jinfra.OrchestratorQueryRepository, neo4jinfra.Neo4jClient, func()) {
	t.Helper()
	client := newTestClient(t) // already defined in run_repository_test.go
	repo := neo4jinfra.NewOrchestratorQueryRepository(client, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	cleanup := func() {
		ctx := context.Background()
		session := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer session.Close(ctx)
		_, _ = session.Run(ctx, "MATCH (r:Run {test_marker: $m}) DETACH DELETE r", map[string]any{"m": t.Name()})
	}
	return repo, client, cleanup
}

func seedRunWithGeneration(t *testing.T, ctx context.Context, client neo4jinfra.Neo4jClient, runID string, gen *int64) {
	t.Helper()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)
	var query string
	params := map[string]any{"run_id": runID, "marker": t.Name()}
	if gen == nil {
		query = `CREATE (r:Run {run_id: $run_id, schedule_name: 'sched', test_marker: $marker})`
	} else {
		query = `CREATE (r:Run {run_id: $run_id, schedule_name: 'sched', topology_generation: $gen, test_marker: $marker})`
		params["gen"] = *gen
	}
	_, err := session.Run(ctx, query, params)
	require.NoError(t, err)
}

func TestOrchestratorQueryRepository_GetRunTopologyGeneration_ReturnsValue(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s-1", t.Name())
	gen := int64(7)
	seedRunWithGeneration(t, ctx, client, runID, &gen)

	got, err := repo.GetRunTopologyGeneration(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, int64(7), got)
}

func TestOrchestratorQueryRepository_GetRunTopologyGeneration_ReturnsZeroWhenUnset(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s-2", t.Name())
	seedRunWithGeneration(t, ctx, client, runID, nil) // property absent

	got, err := repo.GetRunTopologyGeneration(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), got)
}

func TestOrchestratorQueryRepository_GetRunTopologyGeneration_ReturnsZeroWhenRunMissing(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, _, cleanup := newTestQueryRepo(t)
	defer cleanup()

	ctx := context.Background()
	got, err := repo.GetRunTopologyGeneration(ctx, "nonexistent-run")
	require.NoError(t, err)
	assert.Equal(t, int64(0), got)
}

func TestOrchestratorQueryRepository_ListActiveRuns_OnlyUnfinalized(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()

	ctx := context.Background()

	// Seed: one unfinalized run + one finalized run.
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := session.Run(ctx, `
        CREATE (r1:Run {run_id: 'active-1', schedule_name: 'sched-a', topology_generation: 5, test_marker: $m})
        CREATE (r2:Run {run_id: 'finalized-1', schedule_name: 'sched-b', topology_generation: 4,
                       completed_at: datetime(), terminal_status: 'SUCCEEDED', test_marker: $m})
    `, map[string]any{"m": t.Name()})
	require.NoError(t, err)
	session.Close(ctx)

	runs, err := repo.ListActiveRuns(ctx)
	require.NoError(t, err)
	// Filter to runs created by this test (cleanup window).
	filtered := make([]*domain.ActiveRun, 0)
	for _, r := range runs {
		if r.RunID == "active-1" || r.RunID == "finalized-1" {
			filtered = append(filtered, r)
		}
	}
	require.Len(t, filtered, 1)
	assert.Equal(t, "active-1", filtered[0].RunID)
	assert.Equal(t, "sched-a", filtered[0].ScheduleName)
	assert.Equal(t, int64(5), filtered[0].TopologyGeneration)
}

func TestOrchestratorQueryRepository_ListActiveRuns_ZeroForUnsetGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()

	ctx := context.Background()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := session.Run(ctx, `
        CREATE (r:Run {run_id: 'active-no-gen', schedule_name: 'sched-c', test_marker: $m})
    `, map[string]any{"m": t.Name()})
	require.NoError(t, err)
	session.Close(ctx)

	runs, err := repo.ListActiveRuns(ctx)
	require.NoError(t, err)
	var found *domain.ActiveRun
	for _, r := range runs {
		if r.RunID == "active-no-gen" {
			found = r
			break
		}
	}
	require.NotNil(t, found)
	assert.Equal(t, int64(0), found.TopologyGeneration)
}
