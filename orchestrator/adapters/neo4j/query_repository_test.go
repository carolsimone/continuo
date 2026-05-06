// File: orchestrator/adapters/neo4j/query_repository_test.go
package neo4jinfra_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestQueryRepo builds a QueryRepository against the live Neo4j and returns
// it together with a cleanup function that wipes any nodes created by the test.
// It intentionally re-uses the env conventions already in use by run_repository_test.go
// (NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD with the docker-compose defaults).
func newTestQueryRepo(t *testing.T) (*neo4jinfra.QueryRepository, neo4jinfra.Neo4jClient, func()) {
	t.Helper()
	client := newTestClient(t) // already defined in run_repository_test.go
	repo := neo4jinfra.NewQueryRepository(client, slog.New(slog.NewTextHandler(os.Stderr, nil)))
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

func TestQueryRepository_GetRunTopologyGeneration_ReturnsValue(t *testing.T) {
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

func TestQueryRepository_GetRunTopologyGeneration_ReturnsZeroWhenUnset(t *testing.T) {
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

func TestQueryRepository_GetRunTopologyGeneration_ReturnsZeroWhenRunMissing(t *testing.T) {
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
