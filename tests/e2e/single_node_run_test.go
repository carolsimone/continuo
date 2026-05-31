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

	// Ensure graph topology is loaded so that Snapshot+SingleNode can resolve
	// the target node in Neo4j.
	cleanupTestData(t, ctx, clients, "single-node-run")
	seedTopology(t, ctx, clients)

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

	// Assert the new state.ListNodeRuns RPC returns this run as the most recent
	// row for the target node, with the right kind, image_tag, manifest_version,
	// and non-nil timings.
	//
	// task.status.updated:v1 and task.execution.recorded:v1 are delivered on
	// separate Redis streams, so the scheduler can flip to SUCCEEDED before the
	// execution consumer has inserted started_at/completed_at on task_execution.
	// Poll ListNodeRuns until the execution-level fields appear before asserting,
	// to avoid timestamp-vs-status race flakiness.
	t.Log("Polling state.ListNodeRuns until the new run appears with execution timestamps...")
	var top *statev1.NodeRun
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		listResp, err := clients.stateClient.ListNodeRuns(ctx, &statev1.ListNodeRunsRequest{
			ServiceName: targetService,
			SchemaName:  targetSchema,
			TableName:   targetTable,
			Limit:       50,
		})
		if err != nil {
			return false, err
		}
		if len(listResp.Runs) == 0 {
			return false, nil
		}
		row := listResp.Runs[0]
		if row.RunId != runID.String() {
			return false, nil
		}
		if row.StartedAt == "" || row.CompletedAt == "" {
			return false, nil
		}
		top = row
		return true, nil
	}, "ListNodeRuns never returned the new run with populated started_at/completed_at — task.execution.recorded:v1 consumer may have lagged")

	require.NotNil(t, top, "ListNodeRuns must return the new run before the timeout")
	assert.Equal(t, runID.String(), top.RunId, "ListNodeRuns[0].run_id must match the run we just triggered")
	assert.Equal(t, "single_node_run", top.Kind, "ListNodeRuns[0].kind must be 'single_node_run'")
	assert.NotEmpty(t, top.ImageTag, "ListNodeRuns[0].image_tag must be populated")
	assert.NotEmpty(t, top.ManifestVersion, "ListNodeRuns[0].manifest_version must be populated")
	assert.Equal(t, "succeeded", top.TaskStatus, "ListNodeRuns[0].task_status must be 'succeeded'")

	t.Log("TestSingleNodeRunLatest passed")
}

// TestSingleNodeRunStale verifies the single-node-run flow in "snapshot_of_run"
// (stale) metadata mode:
//
//  1. Triggers a normal cron-style run on the "seed" schedule so that a real
//     source run exists with stamped image_tag / manifest_version.
//  2. Waits for that source run to reach SUCCEEDED.
//  3. Reads the source run's task_tracker row for the target node to capture
//     the "old" image_tag and manifest_version pair.
//  4. Calls TriggerSingleNodeRun with metadata_source="snapshot_of_run" and
//     source_run_id = the source schedule_id.
//  5. Waits for the new single-node run to reach SUCCEEDED.
//  6. Asserts that the new run inherits the source's image_tag and
//     manifest_version, and that :Run.source_run_id in Neo4j equals the source
//     schedule_id.
func TestSingleNodeRunStale(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	const (
		targetService  = "service-1"
		targetSchema   = "e2e_schema"
		targetTable    = "seed_table_1"
		srcSchedule    = "seed"
	)

	// Verify infrastructure is up before proceeding.
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	// Ensure we have a clean slate for the source schedule and graph topology.
	cleanupTestData(t, ctx, clients, srcSchedule)
	defer cleanupTestData(t, ctx, clients, srcSchedule)
	seedTopology(t, ctx, clients)

	// ── Step 1: Trigger the source cron run ───────────────────────────────────
	t.Log("=== Step 1: Triggering source seed run ===")
	srcResp, err := clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
		ScheduleName: srcSchedule,
	})
	require.NoError(t, err, "TriggerSchedule(%q) failed", srcSchedule)
	require.NotEmpty(t, srcResp.ScheduleId, "TriggerSchedule must return a non-empty schedule_id")

	srcID, err := uuid.Parse(srcResp.ScheduleId)
	require.NoError(t, err, "source schedule_id must be a valid UUID")
	t.Logf("Source run created: schedule_id=%s", srcID)

	// ── Step 2: Wait for source run to succeed ────────────────────────────────
	t.Log("=== Step 2: Waiting for source run to reach 'succeeded' ===")
	verifySchedulerSucceeded(t, ctx, clients, srcID)
	t.Log("Source run reached 'succeeded'")

	// ── Step 3: Read source's image_tag + manifest_version for the target ─────
	t.Log("=== Step 3: Reading source task metadata ===")
	srcImage, srcManifest := readTaskMetadata(t, ctx, clients, srcID, targetService, targetSchema, targetTable)
	require.NotEmpty(t, srcImage, "source task_tracker.image_tag must be non-empty")
	require.NotEmpty(t, srcManifest, "source task_tracker.manifest_version must be non-empty")
	t.Logf("Source metadata: image_tag=%s manifest_version=%s", srcImage, srcManifest)

	// ── Step 4: Trigger single-node run in stale mode ─────────────────────────
	t.Log("=== Step 4: Triggering single-node run in stale (snapshot_of_run) mode ===")
	resp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    targetService,
		SchemaName:     targetSchema,
		TableName:      targetTable,
		MetadataSource: "snapshot_of_run",
		SourceRunId:    srcResp.ScheduleId,
	})
	require.NoError(t, err, "TriggerSingleNodeRun (stale) gRPC call failed")
	require.NotEmpty(t, resp.RunId, "TriggerSingleNodeRun must return a non-empty run_id")
	require.NotEmpty(t, resp.ScheduleName, "TriggerSingleNodeRun must return a non-empty schedule_name")
	t.Logf("Stale single-node run created: run_id=%s schedule_name=%s", resp.RunId, resp.ScheduleName)

	runID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")

	// Register cleanup for the synthesised single-node-run row.
	defer func() {
		cleanupSingleNodeRun(t, ctx, clients, runID, resp.ScheduleName)
	}()

	// ── Step 5: Wait for the new single-node run to succeed ───────────────────
	t.Log("=== Step 5: Waiting for stale single-node run to reach 'succeeded' ===")
	verifySchedulerSucceeded(t, ctx, clients, runID)
	t.Log("Stale single-node run reached 'succeeded'")

	// ── Step 6: Assert new run inherited source metadata ─────────────────────
	t.Log("=== Step 6: Asserting stale metadata inheritance ===")
	newImage, newManifest := readTaskMetadata(t, ctx, clients, runID, targetService, targetSchema, targetTable)
	require.Equal(t, srcImage, newImage,
		"stale mode must inherit source's image_tag: got %q, want %q", newImage, srcImage)
	require.Equal(t, srcManifest, newManifest,
		"stale mode must inherit source's manifest_version: got %q, want %q", newManifest, srcManifest)
	t.Logf("New run metadata matches source: image_tag=%s manifest_version=%s", newImage, newManifest)

	// Assert :Run.source_run_id in Neo4j equals the source schedule_id string.
	sourceRunID := readNeo4jRunSourceRunID(t, ctx, clients, runID)
	require.Equal(t, srcResp.ScheduleId, sourceRunID,
		":Run.source_run_id must equal source schedule_id: got %q, want %q", sourceRunID, srcResp.ScheduleId)
	t.Logf(":Run.source_run_id correctly set to %s", sourceRunID)

	t.Log("TestSingleNodeRunStale passed")
}

// TestSingleNodeRunTargetNotFound verifies that triggering a single-node run
// against a target that does not exist in the loaded topology terminates the
// run as 'failed' rather than getting stuck in 'pending'. This guards
// against the previous bug where orchestrator emitted run.failed:v1 with no
// consumer, leaving the synthesised scheduler_tracker row stuck.
func TestSingleNodeRunTargetNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)

	// Make sure the topology is loaded — the failure path runs through
	// Snapshot+SingleNode's "target not found" branch when the requested
	// node is absent from the loaded graph.
	cleanupTestData(t, ctx, clients, "single-node-run")
	seedTopology(t, ctx, clients)

	// Use a service/schema/table triple that is guaranteed to be absent.
	const (
		ghostService = "service-does-not-exist"
		ghostSchema  = "no_such_schema"
		ghostTable   = "no_such_table"
	)

	t.Logf("Calling TriggerSingleNodeRun against missing target: %s.%s.%s",
		ghostService, ghostSchema, ghostTable)
	resp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    ghostService,
		SchemaName:     ghostSchema,
		TableName:      ghostTable,
		MetadataSource: "latest",
	})
	require.NoError(t, err, "TriggerSingleNodeRun must succeed even for unknown targets — failure routes through events")
	require.NotEmpty(t, resp.RunId, "TriggerSingleNodeRun must return a non-empty run_id")

	runID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")

	defer func() {
		cleanupSingleNodeRun(t, ctx, clients, runID, resp.ScheduleName)
	}()

	t.Log("Waiting for scheduler_tracker to reach 'failed'...")
	verifySchedulerFailed(t, ctx, clients, runID)

	// The orchestrator must NOT create a task_tracker row when there is no
	// target to dispatch. A leftover row would mean the system actually
	// created phantom work.
	var taskCount int
	err = clients.stateDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_tracker WHERE schedule_id = $1`,
		runID,
	).Scan(&taskCount)
	require.NoError(t, err, "failed to count task_tracker rows")
	assert.Equal(t, 0, taskCount, "no task_tracker rows must exist for a target-not-found run")

	t.Log("TestSingleNodeRunTargetNotFound passed")
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

// readTaskMetadata returns the image_tag and manifest_version from the
// task_tracker row that matches the given schedule_id and target identity
// (service_name, schema_name, table_name). Fails the test if no row is found.
func readTaskMetadata(
	t *testing.T,
	_ context.Context,
	clients *testClients,
	scheduleID uuid.UUID,
	serviceName, schemaName, tableName string,
) (imageTag, manifestVersion string) {
	t.Helper()
	var row struct {
		ImageTag        string `db:"image_tag"`
		ManifestVersion string `db:"manifest_version"`
	}
	err := clients.stateDB.GetContext(context.Background(), &row,
		`SELECT image_tag, manifest_version
		   FROM task_tracker
		  WHERE schedule_id   = $1
		    AND service_name  = $2
		    AND schema_name   = $3
		    AND table_name    = $4
		  LIMIT 1`,
		scheduleID, serviceName, schemaName, tableName,
	)
	require.NoError(t, err,
		"readTaskMetadata: no task_tracker row for schedule_id=%s service=%s schema=%s table=%s",
		scheduleID, serviceName, schemaName, tableName,
	)
	return row.ImageTag, row.ManifestVersion
}

// readNeo4jRunSourceRunID returns the value of :Run.source_run_id for the
// given run_id, or "" if the property is absent or the node is not found.
func readNeo4jRunSourceRunID(t *testing.T, _ context.Context, clients *testClients, runID uuid.UUID) string {
	t.Helper()
	session := clients.neo4jDriver.NewSession(context.Background(), neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeRead,
	})
	defer session.Close(context.Background())

	result, err := session.Run(
		context.Background(),
		`MATCH (r:Run {run_id: $run_id})
		 RETURN COALESCE(r.source_run_id, "") AS source_run_id`,
		map[string]interface{}{"run_id": runID.String()},
	)
	require.NoError(t, err, "Neo4j query for source_run_id value failed")
	if !result.Next(context.Background()) {
		return ""
	}
	raw, ok := result.Record().Get("source_run_id")
	if !ok {
		return ""
	}
	v, _ := raw.(string)
	return v
}
