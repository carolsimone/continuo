package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildOperationSingleNode verifies the `build` run operation end-to-end
// for a single node. Unlike `test`, `build` has no no_tests gate, so any node
// is a valid target — seed_table_1 is used purely because it's a known-good
// dbt-seed node already exercised by the test-operation e2e case. A
// TriggerSingleNodeRun with operation="build" dispatches a `dbt build
// --select seed_table_1` Job that succeeds, and the :Run node records
// operation="build", proving the operation threaded from the trigger through
// to the run projection.
func TestBuildOperationSingleNode(t *testing.T) {
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
		targetTable   = "seed_table_1"
	)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, "single-node-build-op")
	seedTopology(t, ctx, clients)

	t.Logf("TriggerSingleNodeRun operation=build: %s.%s.%s", targetService, targetSchema, targetTable)
	resp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    targetService,
		SchemaName:     targetSchema,
		TableName:      targetTable,
		MetadataSource: "latest",
		Operation:      "build",
	})
	require.NoError(t, err, "TriggerSingleNodeRun(operation=build) failed")
	require.NotEmpty(t, resp.RunId, "must return a non-empty run_id")

	runID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")
	defer cleanupSingleNodeRun(t, ctx, clients, runID, resp.ScheduleName)

	t.Log("Waiting for build-operation single-node run to reach 'succeeded'...")
	verifySchedulerSucceeded(t, ctx, clients, runID)

	// The run was created as a build run — proves operation threaded end-to-end.
	assert.Equal(t, "build", queryNeo4jRunOperation(t, clients, runID),
		":Run.operation must be 'build' for a build-operation run")

	t.Log("TestBuildOperationSingleNode passed")
}

// TestBuildOperationWholeDAGCascadeSkip verifies a whole-DAG `build` run keeps
// the normal blocking-dependency DAG semantics (NOT the `test` operation's
// edgeless flat fan-out): a failing node's descendants are cascade-skipped,
// never dispatched. It also carries the regression guard for Task 2's
// downstream-unblock fix — a non-root node dispatched via the unblock path
// must still run `dbt build`, not fall back to `dbt run`.
//
// DAG topology (see failure_test.go for the full diagram):
//
//	ftable_a ─┬─ ftable_g (FAILS) ─ ftable_h ← cascade-skipped
//	          └─┐
//	ftable_b ──┴─ ftable_c ─┬─ ftable_d ─┐
//	                        │             ├─ ftable_f ← cascade-skipped
//	                        └─ ftable_e (FAILS) ─┘
//
// ftable_c is dispatched via the unblock path (it has upstreams ftable_a and
// ftable_b), so inspecting its Job command proves the unblock dispatch
// carried operation=build through to job assembly.
func TestBuildOperationWholeDAGCascadeSkip(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	defer cleanupTestData(t, ctx, clients, failureTestScheduleName)
	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	t.Log("Seeding topology directly into Neo4j + schedule_catalog...")
	seedTopology(t, ctx, clients)

	t.Logf("TriggerSchedule operation=build on %q", failureTestScheduleName)
	resp, err := clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
		ScheduleName: failureTestScheduleName,
		Operation:    "build",
	})
	require.NoError(t, err, "TriggerSchedule(operation=build) failed")
	require.NotEmpty(t, resp.ScheduleId, "must return a non-empty schedule_id")

	runID, err := uuid.Parse(resp.ScheduleId)
	require.NoError(t, err, "schedule_id must be a valid UUID")

	t.Log("Waiting for ftable_e and ftable_g to exhaust retries...")
	verifyNodeExhaustedRetries(t, ctx, clients, runID, "ftable_e")
	verifyNodeExhaustedRetries(t, ctx, clients, runID, "ftable_g")

	t.Log("Verifying ftable_f and ftable_h were never deployed (cascade-skipped)...")
	verifyNoJobsDeployed(t, ctx, []string{"ftable_f", "ftable_h"})

	t.Log("Verifying scheduler reaches FAILED state...")
	verifySchedulerFailed(t, ctx, clients, runID)

	assert.Equal(t, "build", queryNeo4jRunOperation(t, clients, runID),
		":Run.operation must be 'build' for a whole-DAG build run")

	// Regression guard for Task 2's downstream/unblock dispatch fix: ftable_c
	// is a non-root node reached via the unblock path (upstreams ftable_a and
	// ftable_b both succeed before ftable_c is dispatched), so its Job command
	// must reflect operation=build, not silently fall back to `dbt run`.
	jobs, err := getK8sJobs(ctx, "table_name=ftable_c")
	require.NoError(t, err, "failed to query K8s jobs for ftable_c")
	require.NotEmpty(t, jobs.Items, "ftable_c must have been dispatched")
	cmd := strings.Join(jobs.Items[0].Spec.Template.Spec.Containers[0].Command, " ")
	assert.Contains(t, cmd, "build",
		"downstream ftable_c must run `dbt build`, not `dbt run` — regression guard for the unblock-dispatch operation drop")
	assert.NotContains(t, cmd, "dbt run",
		"downstream node must not fall back to dbt run under a build operation")

	t.Log("TestBuildOperationWholeDAGCascadeSkip passed")
}

// TestBuildOperationRerun verifies operation inheritance across a rerun: a
// rerun of a failed whole-DAG build run must itself be a build run (proving
// operation flows through DispatchDerivedRun's frontier), and a rebased node
// re-dispatched by the rerun must carry operation=build in its Job command.
func TestBuildOperationRerun(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	defer cleanupTestData(t, ctx, clients, failureTestScheduleName)
	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	t.Log("Seeding topology directly into Neo4j + schedule_catalog...")
	seedTopology(t, ctx, clients)

	// ── Drive a whole-DAG build run to FAILED ───────────────────────────────
	t.Logf("TriggerSchedule operation=build on %q", failureTestScheduleName)
	resp, err := clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
		ScheduleName: failureTestScheduleName,
		Operation:    "build",
	})
	require.NoError(t, err, "TriggerSchedule(operation=build) failed")
	require.NotEmpty(t, resp.ScheduleId, "must return a non-empty schedule_id")

	srcID, err := uuid.Parse(resp.ScheduleId)
	require.NoError(t, err, "schedule_id must be a valid UUID")

	t.Log("Waiting for ftable_e to exhaust retries...")
	verifyNodeExhaustedRetries(t, ctx, clients, srcID, "ftable_e")

	t.Log("Verifying scheduler reaches FAILED state...")
	verifySchedulerFailed(t, ctx, clients, srcID)

	require.Equal(t, "build", queryNeo4jRunOperation(t, clients, srcID),
		"precondition: source run must be a build run")

	// ── Rerun the failed build run ──────────────────────────────────────────
	t.Log("Triggering rerun via gRPC TriggerRerun with only source_run_id...")
	rerunResp, err := clients.stateClient.TriggerRerun(ctx, &statev1.TriggerRerunRequest{
		SourceRunId: srcID.String(),
	})
	require.NoError(t, err, "TriggerRerun must succeed against a FAILED build run")
	require.NotEmpty(t, rerunResp.RunId, "TriggerRerun must return the new run's id")
	require.NotEqual(t, srcID.String(), rerunResp.RunId, "rerun must mint a NEW run id")

	newRunID, err := uuid.Parse(rerunResp.RunId)
	require.NoError(t, err, "rerun run_id must be a valid UUID")

	newKind := queryPostgresTrackerKind(t, clients.stateDB, newRunID)
	assert.Equal(t, "rerun", newKind, "new run must have kind='rerun'")

	t.Log("Waiting for rerun task_tracker rows to materialise...")
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		var count int
		qErr := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM task_tracker WHERE schedule_id = $1`, newRunID,
		).Scan(&count)
		if qErr != nil {
			return false, qErr
		}
		return count >= 8, nil // 8 ftable_* nodes in the failure topology (a-h)
	}, "task_tracker rows for rerun run did not materialise (expected 8)")

	// Operation inheritance: the rerun run must itself be recorded as build.
	// Asserted only after the task-row poll above: TriggerRerun returns once the
	// Postgres tracker row exists, but the rerun's Neo4j :Run projection is
	// written asynchronously by the orchestrator consuming trigger.rerun:v1.
	// task_tracker rows are written downstream of that projection, so once they
	// exist the :Run node is guaranteed present (avoids reading "" in the timing
	// window between TriggerRerun returning and the projection materialising).
	assert.Equal(t, "build", queryNeo4jRunOperation(t, clients, newRunID),
		":Run.operation must be inherited as 'build' on the rerun run")

	t.Log("Waiting for ftable_e (rebased) to exhaust retries on the rerun...")
	verifyNodeExhaustedRetries(t, ctx, clients, newRunID, "ftable_e")

	// The derived-frontier discriminator: the rerun re-dispatches the rebased
	// node (ftable_e) as a build Job, proving DispatchDerivedRun threaded
	// operation through the rerun's frontier, not just the initial trigger.
	// The source run also dispatched a Job for ftable_e (it's the node that
	// failed there too), so the selector must scope to THIS run's schedule-id
	// label, not just table_name, or it could pick up the source run's Job.
	jobs, err := getK8sJobs(ctx, fmt.Sprintf("table_name=ftable_e,schedule-id=%s", newRunID.String()))
	require.NoError(t, err, "failed to query K8s jobs for ftable_e")
	require.NotEmpty(t, jobs.Items, "ftable_e must have been re-dispatched on the rerun")
	cmd := strings.Join(jobs.Items[0].Spec.Template.Spec.Containers[0].Command, " ")
	assert.Contains(t, cmd, "build",
		"rebased ftable_e on the rerun must run `dbt build` — regression guard for DispatchDerivedRun's frontier operation threading")

	t.Log("TestBuildOperationRerun passed")
}
