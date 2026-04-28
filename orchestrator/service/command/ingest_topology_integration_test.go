package command_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	pginfra "github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/service/command"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func commandTestNeo4jURI() string {
	if u := os.Getenv("NEO4J_URI"); u != "" {
		return u
	}
	return "bolt://neo4j:7687"
}

func commandTestNeo4jUser() string {
	if u := os.Getenv("NEO4J_USER"); u != "" {
		return u
	}
	return "neo4j"
}

func commandTestNeo4jPassword() string {
	if p := os.Getenv("NEO4J_PASSWORD"); p != "" {
		return p
	}
	return "atlas_password"
}

func newCommandTestNeo4jClient(t *testing.T) neo4jinfra.Neo4jClient {
	t.Helper()

	client, err := neo4jinfra.NewNeo4jClient(
		commandTestNeo4jURI(),
		commandTestNeo4jUser(),
		commandTestNeo4jPassword(),
		newTestLogger(),
	)
	if err != nil {
		t.Skipf("Neo4j unavailable, skipping integration test: %v", err)
	}

	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func newCommandTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "postgres"
	}
	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}
	db, err := sqlx.Connect("postgres", fmt.Sprintf(
		"host=%s port=%s dbname=continuo_orchestrator user=continuo_svc password=continuo sslmode=disable",
		host, port,
	))
	if err != nil {
		t.Skipf("Postgres unavailable: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestIngestTopology_RetiresNodesMissingFromLatestManifestSnapshot(t *testing.T) {
	ctx := context.Background()
	client := newCommandTestNeo4jClient(t)
	pgDB := newCommandTestDB(t)

	scheduleName := "reconcile_sched_" + uuid.New().String()[:8]
	keepTable := "keep_table_" + uuid.New().String()[:8]
	staleTable := "stale_table_" + uuid.New().String()[:8]

	t.Cleanup(func() {
		session := client.NewSession(context.Background(), neo4j.AccessModeWrite)
		defer session.Close(context.Background())

		result, err := session.Run(context.Background(), `
			MATCH (n:Table)
			WHERE n.schedule_name = $schedule_name
			   OR n.table_name IN [$keep_table, $stale_table]
			DETACH DELETE n
		`, map[string]any{
			"schedule_name": scheduleName,
			"keep_table":    keepTable,
			"stale_table":   staleTable,
		})
		if err == nil {
			_, _ = result.Consume(context.Background())
		}

		result, err = session.Run(context.Background(), `
			MATCH (r:Run {schedule_name: $schedule_name})
			DETACH DELETE r
		`, map[string]any{"schedule_name": scheduleName})
		if err == nil {
			_, _ = result.Consume(context.Background())
		}

		// Clean up TopologyRoot singleton for generation assertions
		result, err = session.Run(context.Background(), `
			MATCH (root:TopologyRoot {id: 'singleton'})
			DETACH DELETE root
		`, nil)
		if err == nil {
			_, _ = result.Consume(context.Background())
		}

		// Reset topology_generation for test isolation
		_, _ = pgDB.ExecContext(context.Background(), `UPDATE topology_state SET topology_generation = 0`)
	})

	// Reset topology_generation before the test
	_, err := pgDB.ExecContext(ctx, `UPDATE topology_state SET topology_generation = 0`)
	require.NoError(t, err)

	stateRepo := pginfra.NewTopologyStateRepository(pgDB)
	handler := command.NewIngestTopologyHandler(
		newFakeUnitOfWork(),
		neo4jinfra.NewTopologyRepository(client, newTestLogger()),
		stateRepo,
		newTestLogger(),
	)
	queryRepo := neo4jinfra.NewQueryRepository(client, newTestLogger())
	runRepo := neo4jinfra.NewRunRepository(client, newTestLogger())

	initialLoad := domainCmd.IngestTopologyCmd{
		Nodes: []domainCmd.TopologyNodePayload{
			{
				ServiceName:     "svc-a",
				SchemaName:      "public",
				TableName:       keepTable,
				Owner:           "team-a",
				ScheduleName:    scheduleName,
				Criticality:     "CORE",
				NodeType:        "dbt-model",
				ManifestVersion: "v1",
				ImageTag:        "sha256:aaa",
			},
			{
				ServiceName:     "svc-a",
				SchemaName:      "public",
				TableName:       staleTable,
				Owner:           "team-a",
				ScheduleName:    scheduleName,
				Criticality:     "CORE",
				NodeType:        "dbt-model",
				ManifestVersion: "v1",
				ImageTag:        "sha256:aaa",
			},
		},
	}

	require.NoError(t, handler.Handle(ctx, initialLoad, "manifest-msg-initial-"+scheduleName))

	graph, err := queryRepo.GetScheduleGraph(ctx, scheduleName)
	require.NoError(t, err)
	require.Len(t, graph.Nodes, 2, "initial manifest load should expose both nodes")

	historicalRunID := uuid.New().String()
	require.NoError(t, runRepo.SnapshotGraph(ctx, historicalRunID, scheduleName))

	updatedLoad := domainCmd.IngestTopologyCmd{
		Nodes: []domainCmd.TopologyNodePayload{
			{
				ServiceName:     "svc-a",
				SchemaName:      "public",
				TableName:       keepTable,
				Owner:           "team-a",
				ScheduleName:    scheduleName,
				Criticality:     "CORE",
				NodeType:        "dbt-model",
				ManifestVersion: "v2",
				ImageTag:        "sha256:bbb",
			},
		},
	}

	require.NoError(t, handler.Handle(ctx, updatedLoad, "manifest-msg-updated-"+scheduleName))

	graph, err = queryRepo.GetScheduleGraph(ctx, scheduleName)
	require.NoError(t, err)
	require.Len(t, graph.Nodes, 1, "schedule graph must reflect only the latest manifest snapshot")
	assert.Equal(t, keepTable, graph.Nodes[0].TableName)

	runID := uuid.New().String()
	require.NoError(t, runRepo.SnapshotGraph(ctx, runID, scheduleName))

	initNodes, err := runRepo.GetScheduleInitNodes(ctx, scheduleName, runID)
	require.NoError(t, err)
	require.Len(t, initNodes.AllNodes, 1, "new runs must not snapshot nodes removed from the latest manifest")
	assert.Equal(t, keepTable, initNodes.AllNodes[0].TableName)

	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	result2, err := session.Run(ctx, `
		MATCH (n:Table {schema_name: 'public', table_name: $table_name})
		RETURN COALESCE(n.active, true) AS active
	`, map[string]any{"table_name": staleTable})
	require.NoError(t, err)
	require.True(t, result2.Next(ctx), "stale table node should still exist for historical runs")

	activeRaw, _ := result2.Record().Get("active")
	active, ok := activeRaw.(bool)
	require.True(t, ok, fmt.Sprintf("expected active flag to be boolean, got %T", activeRaw))
	assert.False(t, active, "stale table must be retired from the current topology")

	// Verify TopologyRoot was created with correct service_metadata and topology_generation=2
	rootSession := client.NewSession(ctx, neo4j.AccessModeRead)
	defer rootSession.Close(ctx)

	res, err := rootSession.Run(ctx, `
		MATCH (r:TopologyRoot {id: 'singleton'})
		RETURN r.service_metadata AS sm, r.topology_generation AS gen
	`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx), "TopologyRoot singleton must exist")
	gen, _ := res.Record().Get("gen")
	assert.Equal(t, int64(2), gen.(int64), "two manifest ingestions = generation 2")
	sm, _ := res.Record().Get("sm")
	assert.Contains(t, sm.(string), `"manifest_version"`)
}
