package e2e

import (
	"context"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingleNodeRunLatest verifies the full single-node-run flow in "latest"
// metadata mode:
//
//  1. Calls TriggerSingleNodeRun (gRPC) for a known seed node.
//  2. Waits for the synthesised run to reach 'succeeded' in scheduler_tracker.
//  3. Asserts exactly ONE task_tracker row exists with non-empty image_tag and
//     manifest_version.
//  4. Asserts the Neo4j :Run node has kind = "single_node_run" and no
//     source_run_id property.
func TestSingleNodeRunLatest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	// Use a well-known seed node that is always present after setup.sh.
	const (
		targetService = "service-1"
		targetSchema  = "e2e_schema"
		targetTable   = "seed_table_1"
	)

	// Verify infrastructure is up before proceeding.
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	// Ensure graph topology is loaded so that SnapshotSingleNodeRun can resolve
	// the target node in Neo4j.
	cleanupTestData(t, ctx, clients, "single-node-run")
	triggerGraphLoad(t, ctx, clients)

	// Trigger the single-node run via the state gRPC.
	t.Logf("Calling TriggerSingleNodeRun: %s.%s.%s (latest)", targetService, targetSchema, targetTable)
	resp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    targetService,
		SchemaName:     targetSchema,
		TableName:      targetTable,
		MetadataSource: "latest",
	})
	require.NoError(t, err, "TriggerSingleNodeRun gRPC call failed")
	require.NotEmpty(t, resp.RunId, "TriggerSingleNodeRun must return a non-empty run_id")
	require.NotEmpty(t, resp.ScheduleName, "TriggerSingleNodeRun must return a non-empty schedule_name")
	t.Logf("Single-node run created: run_id=%s schedule_name=%s", resp.RunId, resp.ScheduleName)

	runID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")

	// Register deferred cleanup now that we have a run ID.
	defer func() {
		cleanupSingleNodeRun(t, ctx, clients, runID, resp.ScheduleName)
	}()

	// Wait for the run to reach 'succeeded'.
	t.Log("Waiting for single-node run to reach 'succeeded'...")
	verifySchedulerSucceeded(t, ctx, clients, runID)
	t.Log("Single-node run reached 'succeeded'")

	// Assert exactly one task was dispatched.
	t.Log("Verifying task_tracker has exactly 1 row...")
	var taskCount int
	err = clients.stateDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_tracker WHERE schedule_id = $1`,
		runID,
	).Scan(&taskCount)
	require.NoError(t, err, "failed to count task_tracker rows")
	require.Equal(t, 1, taskCount, "expected exactly 1 task for a single-node run")

	// Assert image_tag and manifest_version are populated.
	manifestVersion, imageTag := queryFirstTaskTrackerMetadata(t, clients.stateDB, runID)
	assert.NotEmpty(t, manifestVersion, "task_tracker.manifest_version must be non-empty")
	assert.NotEmpty(t, imageTag, "task_tracker.image_tag must be non-empty")
	t.Logf("task_tracker metadata: manifest_version=%s image_tag=%s", manifestVersion, imageTag)

	// Assert Neo4j :Run.kind = "single_node_run".
	runKind := queryNeo4jRunKind(t, clients, runID)
	assert.Equal(t, "single_node_run", runKind, ":Run.kind must be 'single_node_run'")

	// Assert :Run.source_run_id is absent (latest mode has no source run).
	hasSourceRunID := queryNeo4jRunHasSourceRunID(t, clients, runID)
	assert.False(t, hasSourceRunID, ":Run.source_run_id must not be present for a latest-mode single-node run")

	t.Log("TestSingleNodeRunLatest passed")
}

// queryNeo4jRunHasSourceRunID returns true if the :Run node with the given
// run_id has a non-null source_run_id property set in Neo4j.
func queryNeo4jRunHasSourceRunID(t *testing.T, clients *testClients, runID uuid.UUID) bool {
	t.Helper()
	session := clients.neo4jDriver.NewSession(context.Background(), neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeRead,
	})
	defer session.Close(context.Background())

	result, err := session.Run(
		context.Background(),
		`MATCH (r:Run {run_id: $run_id})
		 RETURN r.source_run_id IS NOT NULL AS has_source`,
		map[string]interface{}{"run_id": runID.String()},
	)
	require.NoError(t, err, "Neo4j query for source_run_id failed")
	if !result.Next(context.Background()) {
		// Node not found — treat as absent.
		return false
	}
	raw, ok := result.Record().Get("has_source")
	if !ok {
		return false
	}
	v, _ := raw.(bool)
	return v
}

// cleanupSingleNodeRun removes test data created by TestSingleNodeRunLatest.
// Mirrors cleanupTestData but targets the synthesised schedule_name and
// run_id rather than a fixed schedule name.
func cleanupSingleNodeRun(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID, scheduleName string) {
	t.Helper()
	t.Logf("Cleaning up single-node run: run_id=%s schedule_name=%s", runID, scheduleName)

	// Neo4j: remove the synthesised Run node.
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeWrite,
	})
	defer session.Close(ctx)
	_, _ = session.Run(ctx,
		`MATCH (r:Run {run_id: $run_id}) DETACH DELETE r`,
		map[string]interface{}{"run_id": runID.String()},
	)

	// Postgres: remove task_execution → task_tracker → scheduler_tracker.
	_, _ = clients.stateDB.Exec(
		`DELETE FROM task_execution WHERE task_id IN (SELECT id FROM task_tracker WHERE schedule_id = $1)`,
		runID,
	)
	_, _ = clients.stateDB.Exec(`DELETE FROM task_tracker WHERE schedule_id = $1`, runID)
	_, _ = clients.stateDB.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, runID)

	// Outboxes.
	_, _ = clients.orchestratorDB.Exec(`DELETE FROM outbox WHERE aggregate_id = $1`, runID)
	_, _ = clients.stateDB.Exec(`DELETE FROM state_outbox WHERE aggregate_id = $1`, runID)

	// K8s jobs created for this run.
	cleanupK8s(t, ctx)

	t.Log("Single-node run cleanup complete")
}
