package e2e

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingleNodeRunLatest verifies the full single-node-run flow in "latest"
// metadata mode:
//
//  1. Calls TriggerSingleNodeRun (gRPC) for a known seed node.
//  2. Waits for the synthesised run to reach 'succeeded' in runs.
//  3. Asserts exactly ONE tasks row exists with non-empty image_tag.
//  4. Asserts the run is stored with kind = "single_node_run" and no
//     source_run_id.
func TestSingleNodeRunLatest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

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
	resp, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{RequestId: uuid.NewString(),
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
	t.Log("Verifying tasks has exactly 1 row...")
	var taskCount int
	err = clients.stateDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE run_id = $1`,
		runID,
	).Scan(&taskCount)
	require.NoError(t, err, "failed to count tasks rows")
	require.Equal(t, 1, taskCount, "expected exactly 1 task for a single-node run")

	// Assert image_tag is populated.
	imageTag := queryFirstTaskImageTag(t, clients.stateDB, runID)
	assert.NotEmpty(t, imageTag, "tasks.image_tag must be non-empty")
	t.Logf("tasks image_tag=%s", imageTag)

	// Assert the run's kind is "single_node_run".
	runKind := queryRunKind(t, clients.stateDB, runID)
	assert.Equal(t, "single_node_run", runKind, "runs.kind must be 'single_node_run'")

	// Assert the run has no source run (latest mode has none).
	assert.Empty(t, queryRunSourceRunID(t, clients, runID),
		"source_run_id must be empty for a latest-mode single-node run")

	// Assert state's ListNodeTasks RPC returns this run's task as the most
	// recent row for the target node, with the right kind, image_tag,
	// and non-nil timings.
	//
	// The attempt's final report carries its times, but the run can be read
	// before state has applied it: poll ListNodeTasks until the run appears with
	// its started_at and completed_at before asserting.
	t.Log("Polling state.ListNodeTasks until the new run appears with execution timestamps...")
	var top *runv1.NodeTask
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		listResp, err := clients.runClient.ListNodeTasks(ctx, &runv1.ListNodeTasksRequest{
			ServiceName: targetService,
			SchemaName:  targetSchema,
			TableName:   targetTable,
			Limit:       50,
		})
		if err != nil {
			return false, err
		}
		if len(listResp.GetTasks()) == 0 {
			return false, nil
		}
		row := listResp.GetTasks()[0]
		if row.RunId != runID.String() {
			return false, nil
		}
		if row.StartedAt == "" || row.CompletedAt == "" {
			return false, nil
		}
		top = row
		return true, nil
	}, "ListNodeTasks never returned the new run with populated started_at/completed_at — the task.status.updated:v2 consumer may have lagged")

	require.NotNil(t, top, "ListNodeTasks must return the new run before the timeout")
	assert.Equal(t, runID.String(), top.RunId, "ListNodeTasks[0].run_id must match the run we just triggered")
	assert.Equal(t, "single_node_run", top.Kind, "ListNodeTasks[0].kind must be 'single_node_run'")
	assert.NotEmpty(t, top.ImageTag, "ListNodeTasks[0].image_tag must be populated")
	assert.Equal(t, "succeeded", top.TaskStatus, "ListNodeTasks[0].task_status must be 'succeeded'")

	t.Log("TestSingleNodeRunLatest passed")
}

// TestSingleNodeRunStale verifies the single-node-run flow in "snapshot_of_run"
// (stale) metadata mode:
//
//  1. Starts a run of the "seed" schedule with StartRun so that a real
//     source run exists with stamped image_tag.
//  2. Waits for that source run to reach SUCCEEDED.
//  3. Reads the source run's tasks row for the target node to capture
//     the "old" image_tag.
//  4. Calls TriggerSingleNodeRun with metadata_source="snapshot_of_run" and
//     source_run_id = the source run_id.
//  5. Waits for the new single-node run to reach SUCCEEDED.
//  6. Asserts that the new run inherits the source's image_tag,
//     and that its source_run_id equals the source run_id.
func TestSingleNodeRunStale(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	const (
		targetService = "service-1"
		targetSchema  = "e2e_schema"
		targetTable   = "seed_table_1"
		srcSchedule   = "seed"
	)

	// Verify infrastructure is up before proceeding.
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	// Ensure we have a clean slate for the source schedule and graph topology.
	cleanupTestData(t, ctx, clients, srcSchedule)
	defer cleanupTestData(t, ctx, clients, srcSchedule)
	seedTopology(t, ctx, clients)

	// ── Step 1: Start the source run ──────────────────────────────────────────
	t.Log("=== Step 1: Triggering source seed run ===")
	srcResp, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: uuid.NewString(),
		ScheduleName: srcSchedule,
	})
	require.NoError(t, err, "StartRun(%q) failed", srcSchedule)
	require.NotEmpty(t, srcResp.RunId, "StartRun must return a non-empty run_id")

	srcID, err := uuid.Parse(srcResp.RunId)
	require.NoError(t, err, "source run_id must be a valid UUID")
	t.Logf("Source run created: run_id=%s", srcID)

	// ── Step 2: Wait for source run to succeed ────────────────────────────────
	t.Log("=== Step 2: Waiting for source run to reach 'succeeded' ===")
	verifySchedulerSucceeded(t, ctx, clients, srcID)
	t.Log("Source run reached 'succeeded'")

	// ── Step 3: Read source's image_tag for the target ────────────────────────
	t.Log("=== Step 3: Reading source task metadata ===")
	srcImage := readTaskImageTag(t, clients, srcID, targetService, targetSchema, targetTable)
	require.NotEmpty(t, srcImage, "source tasks.image_tag must be non-empty")
	t.Logf("Source metadata: image_tag=%s", srcImage)

	// ── Step 4: Trigger single-node run in stale mode ─────────────────────────
	t.Log("=== Step 4: Triggering single-node run in stale (snapshot_of_run) mode ===")
	resp, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{RequestId: uuid.NewString(),
		ServiceName:    targetService,
		SchemaName:     targetSchema,
		TableName:      targetTable,
		MetadataSource: "snapshot_of_run",
		SourceRunId:    srcResp.RunId,
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
	newImage := readTaskImageTag(t, clients, runID, targetService, targetSchema, targetTable)
	require.Equal(t, srcImage, newImage,
		"stale mode must inherit source's image_tag: got %q, want %q", newImage, srcImage)
	t.Logf("New run metadata matches source: image_tag=%s", newImage)

	// Assert the run's source_run_id equals the source run_id string.
	sourceRunID := queryRunSourceRunID(t, clients, runID)
	require.Equal(t, srcResp.RunId, sourceRunID,
		"source_run_id must equal source run_id: got %q, want %q", sourceRunID, srcResp.RunId)
	t.Logf("source_run_id correctly set to %s", sourceRunID)

	t.Log("TestSingleNodeRunStale passed")
}

// TestSingleNodeRunTargetNotFound verifies that triggering a single-node run
// against a target the live release does not hold is refused with NotFound and
// creates no run.
func TestSingleNodeRunTargetNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)

	// Make sure the topology is loaded, so the refusal comes from the target
	// being absent from the live release.
	cleanupTestData(t, ctx, clients, "single-node-run")
	seedTopology(t, ctx, clients)

	// Use a service/schema/table triple that is guaranteed to be absent.
	const (
		ghostService = "service-does-not-exist"
		ghostSchema  = "no_such_schema"
		ghostTable   = "no_such_table"
	)

	before := countRunsOfKind(t, clients, "single_node_run")
	t.Logf("Calling TriggerSingleNodeRun against missing target: %s.%s.%s",
		ghostService, ghostSchema, ghostTable)
	_, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{RequestId: uuid.NewString(),
		ServiceName:    ghostService,
		SchemaName:     ghostSchema,
		TableName:      ghostTable,
		MetadataSource: "latest",
	})
	require.Error(t, err, "TriggerSingleNodeRun must refuse a target the live release does not hold")
	assert.Equal(t, codes.NotFound, status.Code(err), "a missing target is NotFound: %v", err)
	assert.Equal(t, before, countRunsOfKind(t, clients, "single_node_run"), "a refused trigger creates no run")

	t.Log("TestSingleNodeRunTargetNotFound passed")
}

// cleanupSingleNodeRun removes test data created by TestSingleNodeRunLatest.
// Mirrors cleanupTestData but targets the synthesised schedule_name and
// run_id rather than a fixed schedule name.
func cleanupSingleNodeRun(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID, scheduleName string) {
	t.Helper()
	t.Logf("Cleaning up single-node run: run_id=%s schedule_name=%s", runID, scheduleName)

	// Postgres: remove tasks (its task_attempts go with it through the
	// foreign key's cascade) → runs.
	_, _ = clients.stateDB.Exec(`DELETE FROM tasks WHERE run_id = $1`, runID)
	_, _ = clients.stateDB.Exec(`DELETE FROM runs WHERE run_id = $1`, runID)

	// The run's outbox rows. A failed delete leaves rows for later tests to trip
	// over, so it is logged rather than discarded.
	if _, err := clients.stateDB.Exec(`DELETE FROM run_controller_outbox WHERE aggregate_id = $1`, runID); err != nil {
		t.Logf("Warning: failed to delete run_controller_outbox rows for run %s: %v", runID, err)
	}

	// K8s jobs created for this run.
	cleanupK8s(t, ctx)

	t.Log("Single-node run cleanup complete")
}

// readTaskImageTag returns the image_tag from the tasks row that matches
// the given run_id and target identity (service_name, schema_name,
// table_name). Fails the test if no row is found.
func readTaskImageTag(
	t *testing.T,
	clients *testClients,
	scheduleID uuid.UUID,
	serviceName, schemaName, tableName string,
) string {
	t.Helper()
	var imageTag string
	err := clients.stateDB.GetContext(context.Background(), &imageTag,
		`SELECT image_tag
		   FROM tasks
		  WHERE run_id   = $1
		    AND service_name  = $2
		    AND schema_name   = $3
		    AND table_name    = $4
		  LIMIT 1`,
		scheduleID, serviceName, schemaName, tableName,
	)
	require.NoError(t, err,
		"readTaskImageTag: no tasks row for run_id=%s service=%s schema=%s table=%s",
		scheduleID, serviceName, schemaName, tableName,
	)
	return imageTag
}
