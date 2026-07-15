package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingleNodeTestOperation verifies the `test` run operation end-to-end for a
// single node that has tests. seed_table_1 is seeded with test_count=2 and
// carries real dbt tests (not_null + unique on id), so a TriggerSingleNodeRun
// with operation="test" dispatches a `dbt test --select seed_table_1` Job that
// runs those tests, succeeds, and finalizes the run 'succeeded' with exactly one
// task. The :Run node records operation="test", proving the operation threaded
// from the trigger through to the run projection.
func TestSingleNodeTestOperation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	const (
		targetService = "service-1"
		targetSchema  = "e2e_schema"
		targetTable   = "seed_table_1" // seeded test_count=2, real dbt tests not_null+unique
	)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, "single-node-test-op")
	seedTopology(t, ctx, clients)

	t.Logf("TriggerSingleNodeRun operation=test: %s.%s.%s", targetService, targetSchema, targetTable)
	resp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    targetService,
		SchemaName:     targetSchema,
		TableName:      targetTable,
		MetadataSource: "latest",
		Operation:      "test",
	})
	require.NoError(t, err, "TriggerSingleNodeRun(operation=test) failed")
	require.NotEmpty(t, resp.RunId, "must return a non-empty run_id")

	runID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")
	defer cleanupSingleNodeRun(t, ctx, clients, runID, resp.ScheduleName)

	t.Log("Waiting for test-operation single-node run to reach 'succeeded'...")
	verifySchedulerSucceeded(t, ctx, clients, runID)

	// Exactly one task was dispatched (the tested node).
	var taskCount int
	err = clients.stateDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_tracker WHERE schedule_id = $1`, runID).Scan(&taskCount)
	require.NoError(t, err, "failed to count task_tracker rows")
	assert.Equal(t, 1, taskCount, "expected exactly 1 task for a single-node test run")

	// The run was created as a test run — proves operation threaded end-to-end.
	assert.Equal(t, "test", queryNeo4jRunOperation(t, clients, runID),
		":Run.operation must be 'test' for a test-operation run")

	t.Log("TestSingleNodeTestOperation passed")
}

// TestSingleNodeTestOperationNoTests verifies the no_tests gate: a single-node
// test on a node with no tests (seed_table_2, test_count=0) is rejected by the
// orchestrator (run.entries.dispatch_failed:v1 reason no_tests) rather than
// dispatching a pointless Job. The run finalizes 'failed' with zero tasks.
func TestSingleNodeTestOperationNoTests(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	const (
		targetService = "service-1"
		targetSchema  = "e2e_schema"
		targetTable   = "seed_table_2" // seeded with no tests (test_count=0)
	)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, "single-node-test-no-tests")
	seedTopology(t, ctx, clients)

	t.Logf("TriggerSingleNodeRun operation=test on a zero-test node: %s.%s.%s", targetService, targetSchema, targetTable)
	resp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    targetService,
		SchemaName:     targetSchema,
		TableName:      targetTable,
		MetadataSource: "latest",
		Operation:      "test",
	})
	require.NoError(t, err, "TriggerSingleNodeRun must succeed even when the node has no tests — the gate routes through events")
	require.NotEmpty(t, resp.RunId, "must return a non-empty run_id")

	runID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")
	defer cleanupSingleNodeRun(t, ctx, clients, runID, resp.ScheduleName)

	t.Log("Waiting for the no_tests gate to finalize the run 'failed'...")
	verifySchedulerFailed(t, ctx, clients, runID)

	// The gate must NOT create any task_tracker row — no Job was dispatched.
	var taskCount int
	err = clients.stateDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_tracker WHERE schedule_id = $1`, runID).Scan(&taskCount)
	require.NoError(t, err, "failed to count task_tracker rows")
	assert.Equal(t, 0, taskCount, "no task_tracker rows must exist for a no_tests-gated run")

	t.Log("TestSingleNodeTestOperationNoTests passed")
}

// TestSingleNodeTestOperationUnsetTestCountIsGated is the regression guard for the
// production incident where a single-node `test` on a node with no tests dispatched
// a `dbt test` Job that no-op'd ("Nothing to do") and failed-and-retried 3×.
//
// It reproduces the exact prod state: a node whose `test_count` is *unset* on the
// :Table (topology promoted before test_count capture existed, never re-released).
// An unset count is treated as "no known tests", so the orchestrator gates the run
// with reason no_tests — no Job is dispatched at all. This is symmetric with the
// known-zero gate: only a KNOWN, positive test_count is runnable. The run finalizes
// 'failed' with zero tasks, never dispatching a Job that could no-op and retry.
func TestSingleNodeTestOperationUnsetTestCountIsGated(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	const (
		targetService = "service-1"
		targetSchema  = "e2e_schema"
		targetTable   = "seed_table_2" // no dbt tests
	)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, "single-node-test-unset")
	seedTopology(t, ctx, clients)

	// Simulate a legacy node predating test_count capture: strip the property so
	// the orchestrator reads test_count as unknown (test_count_known=false).
	unsetNeo4jTestCount(t, ctx, clients, targetSchema+"."+targetTable)

	t.Logf("TriggerSingleNodeRun operation=test on an unset-test_count node: %s.%s.%s", targetService, targetSchema, targetTable)
	resp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    targetService,
		SchemaName:     targetSchema,
		TableName:      targetTable,
		MetadataSource: "latest",
		Operation:      "test",
	})
	require.NoError(t, err, "TriggerSingleNodeRun must succeed even when the node is gated — the gate routes through events")
	require.NotEmpty(t, resp.RunId, "must return a non-empty run_id")

	runID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")
	defer cleanupSingleNodeRun(t, ctx, clients, runID, resp.ScheduleName)

	t.Log("Waiting for the unset-test_count no_tests gate to finalize the run 'failed'...")
	verifySchedulerFailed(t, ctx, clients, runID)

	// The gate must NOT dispatch any Job — no task_tracker row, so no no-op run.
	var taskCount int
	err = clients.stateDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_tracker WHERE schedule_id = $1`, runID).Scan(&taskCount)
	require.NoError(t, err, "failed to count task_tracker rows")
	assert.Equal(t, 0, taskCount, "no task_tracker rows must exist for a gated unset-test_count run")

	t.Log("TestSingleNodeTestOperationUnsetTestCountIsGated passed")
}

// TestWholeDAGTestOperation verifies whole-DAG "flat fan-out" test runs. The
// "seed" schedule holds seed_table_1 (test_count=2), seed_table_2 (0), and
// seed_table_3 (1). A TriggerSchedule with operation="test" dispatches ONLY the
// nodes that have tests, each independently: seed_table_1 and seed_table_3 run
// their tests and the run finalizes 'succeeded', while seed_table_2 (no tests)
// is omitted from the projection entirely.
func TestWholeDAGTestOperation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	const seedSchedule = "seed"

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, seedSchedule)
	defer cleanupTestData(t, ctx, clients, seedSchedule)
	seedTopology(t, ctx, clients)

	t.Logf("TriggerSchedule operation=test on %q", seedSchedule)
	resp, err := clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
		ScheduleName: seedSchedule,
		Operation:    "test",
	})
	require.NoError(t, err, "TriggerSchedule(operation=test) failed")
	require.NotEmpty(t, resp.ScheduleId, "must return a non-empty schedule_id")

	scheduleID, err := uuid.Parse(resp.ScheduleId)
	require.NoError(t, err, "schedule_id must be a valid UUID")

	t.Log("Waiting for the whole-DAG test run to reach 'succeeded'...")
	verifySchedulerSucceeded(t, ctx, clients, scheduleID)

	// Flat fan-out: only the tested nodes get a task; seed_table_2 (no tests) is
	// excluded. Assert the dispatched set is exactly {seed_table_1, seed_table_3}.
	rows, err := clients.stateDB.QueryContext(ctx,
		`SELECT table_name FROM task_tracker WHERE schedule_id = $1 ORDER BY table_name`, scheduleID)
	require.NoError(t, err, "failed to query task_tracker rows")
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		require.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	require.NoError(t, rows.Err())

	assert.ElementsMatch(t, []string{"seed_table_1", "seed_table_3"}, tables,
		"whole-DAG test run must dispatch exactly the nodes with tests (test_count>0), omitting seed_table_2")

	// The run was created as a test run.
	assert.Equal(t, "test", queryNeo4jRunOperation(t, clients, scheduleID),
		":Run.operation must be 'test' for a whole-DAG test run")

	t.Log("TestWholeDAGTestOperation passed")
}

// TestNodeCatalogOperationScoping verifies the UI-facing node catalog
// (GET /api/nodes, ui-service's proxy of state.ListNodes) scopes stats per
// operation dimension end-to-end. A model run and a test run are triggered
// against the SAME node (seed_table_1, test_count=2), each populating a
// DISTINCT catalog slice under its own operation: operation=run reflects the
// model run's terminal status, operation=test reflects the test run's. A
// node with no tests (seed_table_2, test_count=0) never gets a task_tracker
// row under operation=test — the no_tests gate rejects it before dispatch
// (see TestSingleNodeTestOperationNoTests) — so it must be entirely ABSENT
// from the operation=test catalog rather than present with zero/null stats,
// since ListNodes inner-joins task_tracker filtered by operation and never
// left-joins against the topology.
func TestNodeCatalogOperationScoping(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	const (
		targetService = "service-1"
		targetSchema  = "e2e_schema"
		targetTable   = "seed_table_1" // test_count=2: runnable under both run and test
		noTestsTable  = "seed_table_2" // test_count=0: never dispatched under operation=test
	)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, "node-catalog-op-scoping")
	seedTopology(t, ctx, clients)

	t.Logf("TriggerSingleNodeRun operation=run: %s.%s.%s", targetService, targetSchema, targetTable)
	runResp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    targetService,
		SchemaName:     targetSchema,
		TableName:      targetTable,
		MetadataSource: "latest",
		Operation:      "run",
	})
	require.NoError(t, err, "TriggerSingleNodeRun(operation=run) failed")
	require.NotEmpty(t, runResp.RunId, "must return a non-empty run_id")

	modelRunID, err := uuid.Parse(runResp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")
	defer cleanupSingleNodeRun(t, ctx, clients, modelRunID, runResp.ScheduleName)

	t.Log("Waiting for the model run to reach 'succeeded'...")
	verifySchedulerSucceeded(t, ctx, clients, modelRunID)

	t.Logf("TriggerSingleNodeRun operation=test: %s.%s.%s", targetService, targetSchema, targetTable)
	testResp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    targetService,
		SchemaName:     targetSchema,
		TableName:      targetTable,
		MetadataSource: "latest",
		Operation:      "test",
	})
	require.NoError(t, err, "TriggerSingleNodeRun(operation=test) failed")
	require.NotEmpty(t, testResp.RunId, "must return a non-empty run_id")

	testRunID, err := uuid.Parse(testResp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")
	defer cleanupSingleNodeRun(t, ctx, clients, testRunID, testResp.ScheduleName)

	t.Log("Waiting for the test run to reach 'succeeded'...")
	verifySchedulerSucceeded(t, ctx, clients, testRunID)

	// GET /api/nodes?search=seed_table_1&operation=run — the model-run slice.
	runBody := mustGetJSON(t, fmt.Sprintf("%s/api/nodes?search=%s&operation=run", clients.uiBase, targetTable))
	runNodes, ok := runBody["nodes"].([]interface{})
	require.True(t, ok, "GET /api/nodes?operation=run: 'nodes' field missing or wrong type")
	require.Len(t, runNodes, 1, "operation=run catalog must contain exactly the model-run node")
	runNode, _ := runNodes[0].(map[string]interface{})
	assert.Equal(t, targetTable, runNode["table_name"])
	assert.Equal(t, "run", runNode["operation"], "catalog entry must be scoped operation=run")
	assert.Equal(t, "succeeded", runNode["last_status"],
		"operation=run catalog entry must reflect the model run's terminal status")

	// GET /api/nodes?search=seed_table_1&operation=test — the DISTINCT test-run slice.
	testBody := mustGetJSON(t, fmt.Sprintf("%s/api/nodes?search=%s&operation=test", clients.uiBase, targetTable))
	testNodes, ok := testBody["nodes"].([]interface{})
	require.True(t, ok, "GET /api/nodes?operation=test: 'nodes' field missing or wrong type")
	require.Len(t, testNodes, 1, "operation=test catalog must contain exactly the test-run node")
	testNode, _ := testNodes[0].(map[string]interface{})
	assert.Equal(t, targetTable, testNode["table_name"])
	assert.Equal(t, "test", testNode["operation"], "catalog entry must be scoped operation=test")
	assert.Equal(t, "succeeded", testNode["last_status"],
		"operation=test catalog entry must reflect the test run's terminal status")

	// GET /api/nodes?search=seed_table_2&operation=test — a node with no tests
	// must be absent entirely, not present with empty stats.
	noTestsBody := mustGetJSON(t, fmt.Sprintf("%s/api/nodes?search=%s&operation=test", clients.uiBase, noTestsTable))
	noTestsNodes, ok := noTestsBody["nodes"].([]interface{})
	require.True(t, ok, "GET /api/nodes?operation=test: 'nodes' field missing or wrong type")
	assert.Empty(t, noTestsNodes,
		"a node with no tests must be absent from the operation=test catalog, not present with empty stats")

	t.Log("TestNodeCatalogOperationScoping passed")
}

// unsetNeo4jTestCount strips the test_count property from an active :Table,
// simulating a topology node promoted before test_count capture existed. After
// this the orchestrator reads test_count as unknown (test_count_known=false).
func unsetNeo4jTestCount(t *testing.T, ctx context.Context, clients *testClients, uniqueID string) {
	t.Helper()
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeWrite,
	})
	defer func() { _ = session.Close(ctx) }()
	res, err := session.Run(ctx,
		`MATCH (t:Table {unique_id: $unique_id}) WHERE t.active REMOVE t.test_count RETURN count(t) AS n`,
		map[string]interface{}{"unique_id": uniqueID})
	require.NoError(t, err, "Neo4j REMOVE test_count for %s failed", uniqueID)
	require.True(t, res.Next(ctx), "no result row from REMOVE test_count for %s", uniqueID)
	n, _ := res.Record().Get("n")
	require.Equal(t, int64(1), n, "expected exactly one active :Table matched for %s", uniqueID)
}

// queryNeo4jRunOperation returns the :Run.operation property for the given
// run_id ("" if absent or the node is not found).
func queryNeo4jRunOperation(t *testing.T, clients *testClients, runID uuid.UUID) string {
	t.Helper()
	session := clients.neo4jDriver.NewSession(context.Background(), neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeRead,
	})
	defer func() { _ = session.Close(context.Background()) }()
	result, err := session.Run(context.Background(),
		`MATCH (r:Run {run_id: $run_id}) RETURN COALESCE(r.operation, "") AS operation`,
		map[string]interface{}{"run_id": runID.String()})
	require.NoError(t, err, "Neo4j query for :Run.operation failed")
	if !result.Next(context.Background()) {
		return ""
	}
	raw, _ := result.Record().Get("operation")
	s, _ := raw.(string)
	return s
}
