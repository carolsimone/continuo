package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingleNodeTestOperation verifies the `test` run operation end-to-end for a
// single node that has tests. seed_table_1 is seeded with test_count=2 and
// carries real dbt tests (not_null + unique on id), so a TriggerSingleNodeRun
// with operation="test" dispatches a `dbt test --select seed_table_1` Job that
// runs those tests, succeeds, and finalizes the run 'succeeded' with exactly one
// task. The run records operation="test", proving the operation threaded
// from the trigger through to the run projection.
func TestSingleNodeTestOperation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

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
	resp, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{RequestId: uuid.NewString(),
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
	err = clients.runControllerDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE run_id = $1`, runID).Scan(&taskCount)
	require.NoError(t, err, "failed to count tasks rows")
	assert.Equal(t, 1, taskCount, "expected exactly 1 task for a single-node test run")

	// The run was created as a test run — proves operation threaded end-to-end.
	assert.Equal(t, "test", queryRunOperation(t, clients, runID),
		"the run's operation must be 'test' for a test-operation run")

	t.Log("TestSingleNodeTestOperation passed")
}

// TestSingleNodeTestOperationNoTests verifies the no_tests gate: a single-node
// test on a node with no tests (seed_table_2, test_count=0) is refused with
// FailedPrecondition, so no run is created and no pointless Job is dispatched.
func TestSingleNodeTestOperationNoTests(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	const (
		targetService = "service-1"
		targetSchema  = "e2e_schema"
		targetTable   = "seed_table_2" // seeded with no tests (test_count=0)
	)

	verifyServicesHealthy(t)

	cleanupTestData(t, ctx, clients, "single-node-test-no-tests")
	seedTopology(t, ctx, clients)

	requireTestOfNodeRefused(t, ctx, clients, targetService, targetSchema, targetTable)

	t.Log("TestSingleNodeTestOperationNoTests passed")
}

// TestSingleNodeTestOperationUnsetTestCountIsGated is the regression guard for the
// production incident where a single-node `test` on a node with no tests dispatched
// a `dbt test` Job that no-op'd ("Nothing to do") and failed-and-retried 3×.
//
// It reproduces the exact prod state: a node whose `test_count` is unknown in the
// live release (a topology promoted before test_count capture existed). An unknown
// count is treated as "no known tests", so the trigger is refused like a known
// zero: only a KNOWN, positive test_count is runnable, and no run or Job exists
// that could no-op and retry.
func TestSingleNodeTestOperationUnsetTestCountIsGated(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	const (
		targetService = "service-1"
		targetSchema  = "e2e_schema"
		targetTable   = "seed_table_2" // no dbt tests
	)

	verifyServicesHealthy(t)

	cleanupTestData(t, ctx, clients, "single-node-test-unset")
	seedTopology(t, ctx, clients)

	// Simulate a legacy node predating test_count capture: clear its count in
	// the live release, so run-controller reads it as unknown.
	unsetTestCount(t, ctx, clients, targetSchema+"."+targetTable)

	requireTestOfNodeRefused(t, ctx, clients, targetService, targetSchema, targetTable)

	t.Log("TestSingleNodeTestOperationUnsetTestCountIsGated passed")
}

// requireTestOfNodeRefused triggers a single-node test of the node and asserts
// the trigger is refused with FailedPrecondition and creates no run.
func requireTestOfNodeRefused(t *testing.T, ctx context.Context, clients *testClients, service, schema, table string) {
	t.Helper()
	before := countRunsOfKind(t, clients, "single_node_run")
	t.Logf("TriggerSingleNodeRun operation=test on a node without known tests: %s.%s.%s", service, schema, table)
	_, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{RequestId: uuid.NewString(),
		ServiceName:    service,
		SchemaName:     schema,
		TableName:      table,
		MetadataSource: "latest",
		Operation:      "test",
	})
	require.Error(t, err, "a test of a node without known tests must be refused")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "the refusal is FailedPrecondition: %v", err)
	assert.Equal(t, before, countRunsOfKind(t, clients, "single_node_run"), "a refused trigger creates no run")
}

// TestWholeDAGTestOperation verifies whole-DAG "flat fan-out" test runs. The
// "seed" schedule holds seed_table_1 (test_count=2), seed_table_2 (0), and
// seed_table_3 (1). A StartRun with operation="test" dispatches ONLY the
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

	const seedSchedule = "seed"

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, seedSchedule)
	defer cleanupTestData(t, ctx, clients, seedSchedule)
	seedTopology(t, ctx, clients)

	t.Logf("StartRun operation=test on %q", seedSchedule)
	resp, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: uuid.NewString(),
		ScheduleName: seedSchedule,
		Operation:    "test",
	})
	require.NoError(t, err, "StartRun(operation=test) failed")
	require.NotEmpty(t, resp.RunId, "must return a non-empty run_id")

	scheduleID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")

	t.Log("Waiting for the whole-DAG test run to reach 'succeeded'...")
	verifySchedulerSucceeded(t, ctx, clients, scheduleID)

	// Flat fan-out: only the tested nodes get a task; seed_table_2 (no tests) is
	// excluded. Assert the dispatched set is exactly {seed_table_1, seed_table_3}.
	rows, err := clients.runControllerDB.QueryContext(ctx,
		`SELECT table_name FROM tasks WHERE run_id = $1 ORDER BY table_name`, scheduleID)
	require.NoError(t, err, "failed to query tasks rows")
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
	assert.Equal(t, "test", queryRunOperation(t, clients, scheduleID),
		"the run's operation must be 'test' for a whole-DAG test run")

	t.Log("TestWholeDAGTestOperation passed")
}

// TestWholeDAGTestOperationNoTestsFinalizesSkipped is the regression guard for a
// whole-DAG Test run over a schedule whose nodes all have no tests. Every node of
// e2e-schedule-failure defaults to test_count=0 (absent from e2eTestCounts), so a
// StartRun with operation="test" selects no node, and run-controller records the
// run 'skipped' — a benign non-failure — rather than 'failed'. No Job is
// dispatched, so no tasks row exists.
func TestWholeDAGTestOperationNoTestsFinalizesSkipped(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	const noTestsSchedule = failureTestScheduleName // e2e-schedule-failure: all nodes test_count=0

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, noTestsSchedule)
	defer cleanupTestData(t, ctx, clients, noTestsSchedule)
	seedTopology(t, ctx, clients)

	t.Logf("StartRun operation=test on %q (all nodes have no tests)", noTestsSchedule)
	resp, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: uuid.NewString(),
		ScheduleName: noTestsSchedule,
		Operation:    "test",
	})
	require.NoError(t, err, "StartRun(operation=test) failed")
	require.NotEmpty(t, resp.RunId, "must return a non-empty run_id")

	scheduleID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")

	t.Log("Waiting for the all-gated whole-DAG test run to finalize 'skipped'...")
	verifySchedulerSkipped(t, ctx, clients, scheduleID)

	// No node had tests, so no Job was dispatched — zero tasks rows.
	var taskCount int
	err = clients.runControllerDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE run_id = $1`, scheduleID).Scan(&taskCount)
	require.NoError(t, err, "failed to count tasks rows")
	assert.Equal(t, 0, taskCount, "no tasks rows must exist for an all-gated whole-DAG test run")

	t.Log("TestWholeDAGTestOperationNoTestsFinalizesSkipped passed")
}

// TestNodeCatalogOperationScoping verifies the UI-facing node list
// (GET /api/nodes, ui's proxy of run-controller.ListNodes) scopes stats per
// operation dimension end-to-end. A model run and a test run are triggered
// against the SAME node (seed_table_1, test_count=2), each populating a
// DISTINCT node-list slice under its own operation: operation=run reflects the
// model run's terminal status, operation=test reflects the test run's. A
// node with no tests (seed_table_2, test_count=0) never gets a tasks
// row under operation=test — the no_tests gate rejects it before dispatch
// (see TestSingleNodeTestOperationNoTests) — so it must be entirely ABSENT
// from the operation=test catalog rather than present with zero/null stats,
// since ListNodes inner-joins tasks filtered by operation and never
// left-joins against the topology.
func TestNodeCatalogOperationScoping(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

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
	runResp, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{RequestId: uuid.NewString(),
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
	testResp, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{RequestId: uuid.NewString(),
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

// unsetTestCount clears the test count of node key in the default tenant's live
// release, simulating a node promoted before test_count capture existed: run-controller
// then reads its count as unknown.
func unsetTestCount(t *testing.T, ctx context.Context, clients *testClients, key string) {
	t.Helper()
	res, err := clients.runControllerDB.ExecContext(ctx, `
		UPDATE topology_nodes n SET test_count = NULL
		FROM live_release l
		WHERE l.tenant_id = 'default' AND n.tenant_id = l.tenant_id AND n.release_id = l.release_id
		  AND n.node_key = $1`, key)
	require.NoError(t, err, "clear the test count of %s", key)
	n, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), n, "expected exactly one node %s in the live release", key)
}
