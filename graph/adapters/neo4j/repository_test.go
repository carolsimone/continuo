package neo4j_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/graph/adapters/neo4j"
	"github.com/carolsimone/continuo/graph/domain/model"
	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestNeo4jClient(t *testing.T) neo4j.Neo4jClient {
	t.Helper()
	uri := os.Getenv("NEO4J_URI")
	if uri == "" {
		uri = "bolt://neo4j:7687"
	}
	user := os.Getenv("NEO4J_USER")
	if user == "" {
		user = "neo4j"
	}
	password := os.Getenv("NEO4J_PASSWORD")
	if password == "" {
		password = "atlas_password"
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client, err := neo4j.NewNeo4jClient(uri, user, password, logger)
	if err != nil {
		t.Skipf("neo4j not available: %v", err)
	}
	t.Cleanup(func() { client.Close(context.Background()) })
	return client
}

// cleanupSchedule removes all test nodes for a given schedule_name.
func cleanupSchedule(t *testing.T, client neo4j.Neo4jClient, scheduleName string) {
	t.Helper()
	ctx := context.Background()
	session := client.NewSession(ctx, neo4jdriver.AccessModeWrite)
	defer session.Close(ctx)
	_, err := session.Run(ctx,
		"MATCH (n:Table {schedule_name: $sn}) DETACH DELETE n",
		map[string]interface{}{"sn": scheduleName})
	if err != nil {
		t.Logf("cleanup warning: %v", err)
	}
}

func cleanupRun(t *testing.T, client neo4j.Neo4jClient, runID string) {
	t.Helper()
	ctx := context.Background()
	session := client.NewSession(ctx, neo4jdriver.AccessModeWrite)
	defer session.Close(ctx)
	_, err := session.Run(ctx,
		"MATCH (r:Run {run_id: $rid}) DETACH DELETE r",
		map[string]interface{}{"rid": runID})
	if err != nil {
		t.Logf("cleanup run warning: %v", err)
	}
}

func createTestRun(t *testing.T, client neo4j.Neo4jClient, runID, scheduleName string) {
	t.Helper()
	ctx := context.Background()
	session := client.NewSession(ctx, neo4jdriver.AccessModeWrite)
	defer session.Close(ctx)
	_, err := session.Run(ctx,
		"CREATE (:Run {run_id: $rid, schedule_name: $sn, created_at: datetime()})",
		map[string]interface{}{"rid": runID, "sn": scheduleName})
	require.NoError(t, err)
}

func getTestRun(t *testing.T, client neo4j.Neo4jClient, runID string) map[string]interface{} {
	t.Helper()
	ctx := context.Background()
	session := client.NewSession(ctx, neo4jdriver.AccessModeRead)
	defer session.Close(ctx)
	result, err := session.Run(ctx, `
		MATCH (r:Run {run_id: $rid})
		RETURN r.terminal_status AS terminal_status, toString(r.completed_at) AS completed_at
	`, map[string]interface{}{"rid": runID})
	require.NoError(t, err)
	record, err := result.Single(ctx)
	require.NoError(t, err)
	terminalStatus, _ := record.Get("terminal_status")
	completedAt, _ := record.Get("completed_at")
	return map[string]interface{}{
		"terminal_status": terminalStatus,
		"completed_at":    completedAt,
	}
}

// createTopologyNode creates a bare Table node without run-scoped execution state.
func createTopologyNode(t *testing.T, client neo4j.Neo4jClient, scheduleName, schema, table string) {
	t.Helper()
	ctx := context.Background()
	session := client.NewSession(ctx, neo4jdriver.AccessModeWrite)
	defer session.Close(ctx)
	query := `
		MERGE (n:Table {schedule_name: $sn, schema: $schema, table_name: $table})
		SET n.service_name = 'svc', n.node_type = 'dbt-model', n.manifest_version = 'v1'
	`
	_, err := session.Run(ctx, query, map[string]interface{}{
		"sn":     scheduleName,
		"schema": schema,
		"table":  table,
	})
	require.NoError(t, err)
}

// setExecutesStatus creates a run snapshot edge and assigns execution status for that run.
func setExecutesStatus(t *testing.T, client neo4j.Neo4jClient, runID, scheduleName, schema, table, status string) {
	t.Helper()
	ctx := context.Background()
	session := client.NewSession(ctx, neo4jdriver.AccessModeWrite)
	defer session.Close(ctx)
	query := `
		MERGE (run:Run {run_id: $rid})
		ON CREATE SET run.schedule_name = $sn, run.created_at = datetime()
		WITH run
		MATCH (t:Table {schedule_name: $sn, schema: $schema, table_name: $table})
		MERGE (run)-[e:EXECUTES]->(t)
		SET e.status = $status, e.manifest_version = COALESCE(t.manifest_version, '')
	`
	_, err := session.Run(ctx, query, map[string]interface{}{
		"rid":    runID,
		"sn":     scheduleName,
		"schema": schema,
		"table":  table,
		"status": status,
	})
	require.NoError(t, err)
}

// createTestNode creates a Table node in Neo4j with an optional status.
func createTestNode(t *testing.T, client neo4j.Neo4jClient, scheduleName, schema, table, status string) {
	t.Helper()
	ctx := context.Background()
	session := client.NewSession(ctx, neo4jdriver.AccessModeWrite)
	defer session.Close(ctx)
	query := `
		MERGE (n:Table {schedule_name: $sn, schema: $schema, table_name: $table})
		SET n.service_name = 'svc', n.node_type = 'dbt-model', n.status = $status
	`
	_, err := session.Run(ctx, query, map[string]interface{}{
		"sn":     scheduleName,
		"schema": schema,
		"table":  table,
		"status": status,
	})
	require.NoError(t, err)
}

// createDependsOn creates an edge: child -[:DEPENDS_ON]-> parent
func createDependsOn(t *testing.T, client neo4j.Neo4jClient, scheduleName, childSchema, childTable, parentSchema, parentTable string) {
	t.Helper()
	ctx := context.Background()
	session := client.NewSession(ctx, neo4jdriver.AccessModeWrite)
	defer session.Close(ctx)
	query := `
		MATCH (child:Table {schedule_name: $sn, schema: $cs, table_name: $ct})
		MATCH (parent:Table {schedule_name: $sn, schema: $ps, table_name: $pt})
		MERGE (child)-[:DEPENDS_ON]->(parent)
	`
	_, err := session.Run(ctx, query, map[string]interface{}{
		"sn": scheduleName,
		"cs": childSchema, "ct": childTable,
		"ps": parentSchema, "pt": parentTable,
	})
	require.NoError(t, err)
}

func TestGetReadyDownstream_WithRunID(t *testing.T) {
	client := newTestNeo4jClient(t)
	repo := neo4j.NewGraphRepository(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	sn := "ready-run-test"
	runID := "run-ready-001"
	cleanupSchedule(t, client, sn)
	cleanupRun(t, client, runID)
	t.Cleanup(func() {
		cleanupSchedule(t, client, sn)
		cleanupRun(t, client, runID)
	})

	createTopologyNode(t, client, sn, "public", "table_a")
	createTopologyNode(t, client, sn, "public", "table_b")
	createDependsOn(t, client, sn, "public", "table_b", "public", "table_a")

	setExecutesStatus(t, client, runID, sn, "public", "table_a", "SUCCEEDED")
	setExecutesStatus(t, client, runID, sn, "public", "table_b", "PENDING")

	nodes, err := repo.GetReadyDownstream(ctx, sn, "public", "table_a", runID)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Equal(t, "table_b", nodes[0].TableName)
}

func TestGetReadyDownstream_BlockedByNewNode(t *testing.T) {
	client := newTestNeo4jClient(t)
	repo := neo4j.NewGraphRepository(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	sn := "blocked-run-test"
	runID := "run-blocked-001"
	cleanupSchedule(t, client, sn)
	cleanupRun(t, client, runID)
	t.Cleanup(func() {
		cleanupSchedule(t, client, sn)
		cleanupRun(t, client, runID)
	})

	createTopologyNode(t, client, sn, "public", "table_a")
	createTopologyNode(t, client, sn, "public", "table_b")
	createTopologyNode(t, client, sn, "public", "new_node")
	createDependsOn(t, client, sn, "public", "table_b", "public", "table_a")
	createDependsOn(t, client, sn, "public", "table_b", "public", "new_node")

	setExecutesStatus(t, client, runID, sn, "public", "table_a", "SUCCEEDED")
	setExecutesStatus(t, client, runID, sn, "public", "table_b", "PENDING")

	nodes, err := repo.GetReadyDownstream(ctx, sn, "public", "table_a", runID)
	require.NoError(t, err)
	assert.Empty(t, nodes, "table_b must remain blocked because new_node is not in the snapshot")
}

func TestGetTransitiveDownstream_ReturnsFAILEDAndNonSUCCEEDEDNodes(t *testing.T) {
	client := newTestNeo4jClient(t)
	scheduleName := fmt.Sprintf("test-rerun-%s", t.Name())
	defer cleanupSchedule(t, client, scheduleName)

	// DAG: A -> B -> C  (B and C depend on A; C depends on B)
	// A = SUCCEEDED (start node), B = FAILED, C = PENDING (no status set)
	createTestNode(t, client, scheduleName, "s", "A", "SUCCEEDED")
	createTestNode(t, client, scheduleName, "s", "B", "FAILED")
	createTestNode(t, client, scheduleName, "s", "C", "")
	createDependsOn(t, client, scheduleName, "s", "B", "s", "A")
	createDependsOn(t, client, scheduleName, "s", "C", "s", "B")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	repo := neo4j.NewGraphRepository(client, logger)

	nodes, err := repo.GetTransitiveDownstream(context.Background(), scheduleName, "s", "A")
	require.NoError(t, err)

	tableNames := make([]string, len(nodes))
	for i, n := range nodes {
		tableNames[i] = n.TableName
	}
	assert.Contains(t, tableNames, "B", "FAILED node B should be included")
	assert.Contains(t, tableNames, "C", "PENDING node C should be included")
	assert.NotContains(t, tableNames, "A", "start node A should not be in results")
}

func TestSnapshotGraph_CreatesRunAndExecutesEdges(t *testing.T) {
	client := newTestNeo4jClient(t)
	repo := neo4j.NewGraphRepository(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	sn := "snap-test-1"
	runID := "run-abc-001"
	cleanupSchedule(t, client, sn)
	cleanupRun(t, client, runID)
	t.Cleanup(func() {
		cleanupSchedule(t, client, sn)
		cleanupRun(t, client, runID)
	})

	_, err := repo.CreateNode(ctx, &neo4j.CreateNodeRequest{
		TableName: "table_a", SchemaName: "public", ServiceName: "svc",
		Owner: "eng", ScheduleName: sn, Criticality: model.CriticalitySecondary,
		NodeType: "dbt-model", ManifestVersion: "v1",
	})
	require.NoError(t, err)
	_, err = repo.CreateNode(ctx, &neo4j.CreateNodeRequest{
		TableName: "seed_a", SchemaName: "seed_public", ServiceName: "svc",
		Owner: "eng", ScheduleName: "seed-schedule-" + sn, Criticality: model.CriticalitySecondary,
		NodeType: "dbt-seed", ManifestVersion: "v9",
	})
	require.NoError(t, err)
	_, err = repo.CreateNode(ctx, &neo4j.CreateNodeRequest{
		TableName: "table_b", SchemaName: "public", ServiceName: "svc",
		Owner: "eng", ScheduleName: sn, Criticality: model.CriticalitySecondary,
		NodeType: "dbt-model", ManifestVersion: "v1",
		UpstreamDependencies: []*model.UpstreamDependency{
			{TableName: "seed_a", SchemaName: "seed_public", ServiceName: "svc"},
		},
	})
	require.NoError(t, err)

	err = repo.SnapshotGraph(ctx, runID, sn)
	require.NoError(t, err)

	session := client.NewSession(ctx, neo4jdriver.AccessModeRead)
	defer session.Close(ctx)
	result, err := session.Run(ctx,
		`MATCH (:Run {run_id: $rid})-[e:EXECUTES]->(t:Table)
		 RETURN count(e) AS cnt, e.status AS status`,
		map[string]interface{}{"rid": runID})
	require.NoError(t, err)
	require.True(t, result.Next(ctx))
	cnt, _ := result.Record().Get("cnt")
	assert.Equal(t, int64(3), cnt)
	status, _ := result.Record().Get("status")
	assert.Equal(t, "PENDING", status)

	seedResult, err := session.Run(ctx,
		`MATCH (:Run {run_id: $rid})-[:EXECUTES]->(t:Table {table_name: "seed_a", node_type: "dbt-seed"})
		 RETURN count(t) AS cnt`,
		map[string]interface{}{"rid": runID})
	require.NoError(t, err)
	require.True(t, seedResult.Next(ctx))
	seedCount, _ := seedResult.Record().Get("cnt")
	assert.Equal(t, int64(1), seedCount)
}

func TestFinalizeRun(t *testing.T) {
	client := newTestNeo4jClient(t)
	repo := neo4j.NewGraphRepository(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	sn := "finalize-test-" + uuid.New().String()
	runID := "run-finalize-" + uuid.New().String()
	cleanupRun(t, client, runID)
	t.Cleanup(func() {
		cleanupRun(t, client, runID)
	})

	createTestRun(t, client, runID, sn)

	require.NoError(t, repo.FinalizeRun(ctx, runID, "SUCCEEDED"))

	run := getTestRun(t, client, runID)
	assert.Equal(t, "SUCCEEDED", run["terminal_status"])
	assert.NotEmpty(t, run["completed_at"])
}

func TestListRuns(t *testing.T) {
	client := newTestNeo4jClient(t)
	repo := neo4j.NewGraphRepository(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	sn := "list-runs-" + uuid.New().String()
	runID1 := "run-list-1-" + uuid.New().String()
	runID2 := "run-list-2-" + uuid.New().String()
	cleanupRun(t, client, runID1)
	cleanupRun(t, client, runID2)
	t.Cleanup(func() {
		cleanupRun(t, client, runID1)
		cleanupRun(t, client, runID2)
	})

	createTestRun(t, client, runID1, sn)
	createTestRun(t, client, runID2, sn)
	require.NoError(t, repo.FinalizeRun(ctx, runID1, "SUCCEEDED"))

	runs, err := repo.ListRuns(ctx, sn)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, runID1, runs[0].RunID)
	assert.Equal(t, "SUCCEEDED", runs[0].TerminalStatus)
}

func TestGetRunGraph(t *testing.T) {
	client := newTestNeo4jClient(t)
	repo := neo4j.NewGraphRepository(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	sn := "run-graph-" + uuid.New().String()
	runID := "run-graph-" + uuid.New().String()
	cleanupSchedule(t, client, sn)
	cleanupRun(t, client, runID)
	t.Cleanup(func() {
		cleanupSchedule(t, client, sn)
		cleanupRun(t, client, runID)
	})

	_, err := repo.CreateNode(ctx, &neo4j.CreateNodeRequest{
		TableName: "upstream_table", SchemaName: "dbt", ServiceName: "svc",
		Owner: "eng", ScheduleName: sn, Criticality: model.CriticalityCore,
		NodeType: "dbt-model", ManifestVersion: "v1",
	})
	require.NoError(t, err)
	_, err = repo.CreateNode(ctx, &neo4j.CreateNodeRequest{
		TableName: "downstream_table", SchemaName: "dbt", ServiceName: "svc",
		Owner: "eng", ScheduleName: sn, Criticality: model.CriticalityCore,
		NodeType: "dbt-model", ManifestVersion: "v1",
		UpstreamDependencies: []*model.UpstreamDependency{
			{TableName: "upstream_table", SchemaName: "dbt", ServiceName: "svc"},
		},
	})
	require.NoError(t, err)

	require.NoError(t, repo.SnapshotGraph(ctx, runID, sn))
	require.NoError(t, repo.UpdateNodeStatus(ctx, sn, "dbt", "upstream_table", "SUCCEEDED", runID))

	graph, err := repo.GetRunGraph(ctx, runID)
	require.NoError(t, err)
	require.Len(t, graph.Nodes, 2)
	require.Len(t, graph.Edges, 1)

	nodesByTable := map[string]*model.TableNode{}
	for _, node := range graph.Nodes {
		nodesByTable[node.TableName] = node
	}
	assert.Equal(t, "SUCCEEDED", nodesByTable["upstream_table"].Status)
}

func TestDeleteExpiredRuns(t *testing.T) {
	client := newTestNeo4jClient(t)
	repo := neo4j.NewGraphRepository(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	sn := "expire-runs-" + uuid.New().String()
	runID := "run-expire-" + uuid.New().String()
	cleanupRun(t, client, runID)
	t.Cleanup(func() {
		cleanupRun(t, client, runID)
	})

	createTestRun(t, client, runID, sn)
	require.NoError(t, repo.FinalizeRun(ctx, runID, "SUCCEEDED"))

	require.NoError(t, repo.DeleteExpiredRuns(ctx, 0))

	runs, err := repo.ListRuns(ctx, sn)
	require.NoError(t, err)
	assert.Empty(t, runs)
}

func TestCheckScheduleCompletion_FailedRunWithBlockedPendingDownstream_IsComplete(t *testing.T) {
	client := newTestNeo4jClient(t)
	repo := neo4j.NewGraphRepository(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	sn := "completion-failed-" + uuid.New().String()
	runID := "run-completion-" + uuid.New().String()
	cleanupSchedule(t, client, sn)
	cleanupRun(t, client, runID)
	t.Cleanup(func() {
		cleanupSchedule(t, client, sn)
		cleanupRun(t, client, runID)
	})

	createTopologyNode(t, client, sn, "public", "table_e")
	createTopologyNode(t, client, sn, "public", "table_g")
	createDependsOn(t, client, sn, "public", "table_g", "public", "table_e")

	setExecutesStatus(t, client, runID, sn, "public", "table_e", "FAILED")
	setExecutesStatus(t, client, runID, sn, "public", "table_g", "PENDING")

	isComplete, hasFailed, err := repo.CheckScheduleCompletion(ctx, sn, runID)
	require.NoError(t, err)
	assert.True(t, isComplete, "blocked pending downstream should not keep a failed run open")
	assert.True(t, hasFailed)
}

func TestGetTransitiveDownstream_ExcludesSUCCEEDEDNodes(t *testing.T) {
	client := newTestNeo4jClient(t)
	scheduleName := fmt.Sprintf("test-rerun-all-succeeded-%s", t.Name())
	defer cleanupSchedule(t, client, scheduleName)

	// A -> B -> C, all SUCCEEDED
	createTestNode(t, client, scheduleName, "s", "A", "SUCCEEDED")
	createTestNode(t, client, scheduleName, "s", "B", "SUCCEEDED")
	createTestNode(t, client, scheduleName, "s", "C", "SUCCEEDED")
	createDependsOn(t, client, scheduleName, "s", "B", "s", "A")
	createDependsOn(t, client, scheduleName, "s", "C", "s", "B")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	repo := neo4j.NewGraphRepository(client, logger)

	nodes, err := repo.GetTransitiveDownstream(context.Background(), scheduleName, "s", "A")
	require.NoError(t, err)
	assert.Empty(t, nodes, "all SUCCEEDED nodes should be excluded")
}
