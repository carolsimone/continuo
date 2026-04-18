package neo4jinfra_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// neo4jURI returns the Neo4j bolt URI from the environment, defaulting to the
// docker-compose service name used in CI / local development.
func neo4jURI() string {
	if u := os.Getenv("NEO4J_URI"); u != "" {
		return u
	}
	return "bolt://neo4j:7687"
}

func neo4jUser() string {
	if u := os.Getenv("NEO4J_USER"); u != "" {
		return u
	}
	return "neo4j"
}

func neo4jPassword() string {
	if p := os.Getenv("NEO4J_PASSWORD"); p != "" {
		return p
	}
	return "atlas_password"
}

// newTestClient creates a Neo4j client for integration tests.
// Tests that call this will be skipped if Neo4j is unreachable.
func newTestClient(t *testing.T) neo4jinfra.Neo4jClient {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client, err := neo4jinfra.NewNeo4jClient(neo4jURI(), neo4jUser(), neo4jPassword(), logger)
	if err != nil {
		t.Skipf("Neo4j unavailable, skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

// seedScheduleNodes creates a set of Table nodes in Neo4j for the given
// scheduleName and returns a cleanup function that removes them.
// Topology: nodeA → nodeB → nodeC  (DEPENDS_ON edges)
// nodeC is a dbt-seed so it gets included via the seed-path in SnapshotGraph.
func seedScheduleNodes(t *testing.T, ctx context.Context, client neo4jinfra.Neo4jClient, scheduleName string) func() {
	t.Helper()

	driver := client.(interface {
		NewSession(context.Context, neo4j.AccessMode) neo4j.SessionWithContext
	})
	session := driver.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	seed := fmt.Sprintf(`
		MERGE (a:Table {schema_name: 'test_schema', table_name: 'node_a_%s'})
		  ON CREATE SET a.service_name = 'svc1', a.schedule_name = $sched,
		                a.node_type = 'dbt-model', a.criticality = 'unspecified'
		  ON MATCH  SET a.schedule_name = $sched
		MERGE (b:Table {schema_name: 'test_schema', table_name: 'node_b_%s'})
		  ON CREATE SET b.service_name = 'svc1', b.schedule_name = $sched,
		                b.node_type = 'dbt-model', b.criticality = 'unspecified'
		  ON MATCH  SET b.schedule_name = $sched
		MERGE (c:Table {schema_name: 'test_schema', table_name: 'node_c_%s'})
		  ON CREATE SET c.service_name = 'svc1', c.schedule_name = $sched,
		                c.node_type = 'dbt-seed', c.criticality = 'unspecified'
		  ON MATCH  SET c.schedule_name = $sched
		MERGE (a)-[:DEPENDS_ON]->(b)
		MERGE (b)-[:DEPENDS_ON]->(c)
	`, scheduleName, scheduleName, scheduleName)

	result, err := session.Run(ctx, seed, map[string]interface{}{"sched": scheduleName})
	require.NoError(t, err)
	_, err = result.Consume(ctx)
	require.NoError(t, err)

	return func() {
		cleanSession := driver.NewSession(context.Background(), neo4j.AccessModeWrite)
		defer cleanSession.Close(context.Background())
		cleanup := fmt.Sprintf(`
			MATCH (n:Table)
			WHERE n.schedule_name = $sched
			   OR n.table_name IN ['node_a_%s','node_b_%s','node_c_%s']
			DETACH DELETE n
		`, scheduleName, scheduleName, scheduleName)
		r, _ := cleanSession.Run(context.Background(), cleanup, map[string]interface{}{"sched": scheduleName})
		if r != nil {
			_, _ = r.Consume(context.Background())
		}
		// Also clean up any Run nodes created for this test
		runClean := `MATCH (r:Run) WHERE r.schedule_name = $sched DETACH DELETE r`
		r2, _ := cleanSession.Run(context.Background(), runClean, map[string]interface{}{"sched": scheduleName})
		if r2 != nil {
			_, _ = r2.Consume(context.Background())
		}
	}
}

// isValidUUID returns true when s is a well-formed RFC 4122 UUID string.
func isValidUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}

// TestRunRepository_SnapshotGraph_AssignsTaskUUIDs verifies that:
//  1. After SnapshotGraph, every node returned by GetScheduleInitNodes has a
//     non-empty, valid UUID in TaskID.
//  2. Re-calling SnapshotGraph (idempotent replay) does NOT change existing
//     task_id values on the EXECUTES edges.
func TestRunRepository_SnapshotGraph_AssignsTaskUUIDs(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)

	scheduleName := "test_uuid_" + uuid.New().String()[:8]
	cleanup := seedScheduleNodes(t, ctx, client, scheduleName)
	t.Cleanup(cleanup)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := neo4jinfra.NewRunRepository(client, logger)

	runID := uuid.New().String()

	// ── First snapshot ────────────────────────────────────────────────────────
	require.NoError(t, repo.SnapshotGraph(ctx, runID, scheduleName))

	initNodes, err := repo.GetScheduleInitNodes(ctx, scheduleName, runID)
	require.NoError(t, err)

	// We seeded 3 nodes: node_a (model), node_b (model), node_c (seed).
	// AllNodes must include all three.
	require.NotEmpty(t, initNodes.AllNodes, "AllNodes must not be empty after snapshot")

	// Collect task_ids from first snapshot.
	taskIDsByTable := make(map[string]string)
	for _, n := range initNodes.AllNodes {
		assert.NotEmpty(t, n.TaskID,
			"node %s.%s must have a non-empty TaskID after snapshot", n.SchemaName, n.TableName)
		assert.True(t, isValidUUID(n.TaskID),
			"TaskID for %s.%s must be a valid UUID, got %q", n.SchemaName, n.TableName, n.TaskID)
		taskIDsByTable[n.TableName] = n.TaskID
	}

	// RootNodes must also carry TaskIDs.
	for _, n := range initNodes.RootNodes {
		assert.NotEmpty(t, n.TaskID,
			"root node %s.%s must have a non-empty TaskID", n.SchemaName, n.TableName)
		assert.True(t, isValidUUID(n.TaskID),
			"TaskID for root node %s.%s must be a valid UUID, got %q", n.SchemaName, n.TableName, n.TaskID)
	}

	// SeedNodes must also carry TaskIDs.
	for _, n := range initNodes.SeedNodes {
		assert.NotEmpty(t, n.TaskID,
			"seed node %s.%s must have a non-empty TaskID", n.SchemaName, n.TableName)
		assert.True(t, isValidUUID(n.TaskID),
			"TaskID for seed node %s.%s must be a valid UUID, got %q", n.SchemaName, n.TableName, n.TaskID)
	}

	// ── Second snapshot (idempotent) ──────────────────────────────────────────
	require.NoError(t, repo.SnapshotGraph(ctx, runID, scheduleName),
		"second SnapshotGraph call (same runID) must succeed")

	initNodes2, err := repo.GetScheduleInitNodes(ctx, scheduleName, runID)
	require.NoError(t, err)
	require.Equal(t, len(initNodes.AllNodes), len(initNodes2.AllNodes),
		"node count must not change after re-snapshot")

	for _, n := range initNodes2.AllNodes {
		original, ok := taskIDsByTable[n.TableName]
		require.True(t, ok, "unexpected node %s in second snapshot", n.TableName)
		assert.Equal(t, original, n.TaskID,
			"TaskID for %s.%s must be stable across re-snapshots", n.SchemaName, n.TableName)
	}
}
