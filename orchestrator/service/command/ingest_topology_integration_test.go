package command_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/service/command"
	"github.com/google/uuid"
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

func TestIngestTopology_RetiresNodesMissingFromLatestManifestSnapshot(t *testing.T) {
	ctx := context.Background()
	client := newCommandTestNeo4jClient(t)

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
	})

	handler := command.NewIngestTopologyHandler(
		newFakeUnitOfWork(),
		neo4jinfra.NewTopologyRepository(client, newTestLogger()),
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

	result, err := session.Run(ctx, `
		MATCH (n:Table {schema_name: 'public', table_name: $table_name})
		RETURN COALESCE(n.active, true) AS active
	`, map[string]any{"table_name": staleTable})
	require.NoError(t, err)
	require.True(t, result.Next(ctx), "stale table node should still exist for historical runs")

	activeRaw, _ := result.Record().Get("active")
	active, ok := activeRaw.(bool)
	require.True(t, ok, fmt.Sprintf("expected active flag to be boolean, got %T", activeRaw))
	assert.False(t, active, "stale table must be retired from the current topology")
}
