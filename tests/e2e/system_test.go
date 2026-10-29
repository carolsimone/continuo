package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_HappyPath_FullDAGExecution tests the complete pipeline
func TestE2E_HappyPath_FullDAGExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Setup clients
	clients := setupClients(t, ctx)

	// Cleanup at end (runs even on failure)
	defer func() {
		cleanupTestData(t, ctx, clients, testScheduleName)
	}()

	// Verify prerequisites
	t.Log("Verifying prerequisites...")
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	// Cleanup any existing data
	cleanupTestData(t, ctx, clients, testScheduleName)

	// Tap task.dispatch:v1 before anything is triggered: execution-controller consumes it
	// and the trim loop removes consumed entries, so the checks below read what
	// the tap recorded rather than the stream's history.
	dispatchTap := startStreamTap(t, ctx, clients.redisClient, streams.TaskDispatchV1)

	// Seed the full e2e topology by announcing it through announce-topology.
	t.Log("Seeding topology via announce-topology...")
	seedTopology(t, ctx, clients)

	// Start a run of the schedule
	t.Log("Starting a run of the schedule...")
	schedulerIDStr := startScheduleRun(t, ctx, clients)
	schedulerID, err := uuid.Parse(schedulerIDStr)
	require.NoError(t, err, "Invalid run_id returned from StartRun")

	// Verify run-controller dispatched the root nodes to task.dispatch:v1
	t.Log("Verifying run-controller dispatched the root nodes...")
	verifyRootNodesDispatched(t, ctx, dispatchTap, schedulerID, []string{"seed_table_1", "seed_table_2", "seed_table_3"})

	// Verify complete DAG execution
	t.Log("Verifying full DAG execution...")
	verifyFullDAGExecution(t, ctx, clients, dispatchTap, schedulerID)

	// Verify service-1 Jobs actually ran through the customname-dbt dialect and
	// service-2/3 Jobs through the built-in dialect (pins the dbt-commands
	// ConfigMap wiring so a regression to silent built-in fallback fails here
	// instead of passing unnoticed). Jobs are still present in the cluster:
	// execution-controller sets no TTLSecondsAfterFinished, and cleanupK8s only
	// runs in this test's deferred cleanup at function exit.
	t.Log("Verifying dbt command dialect routing...")
	verifyDialectRouting(t, ctx)

	// Verify scheduler reaches SUCCEEDED state
	t.Log("Verifying scheduler reaches SUCCEEDED state...")
	verifySchedulerSucceeded(t, ctx, clients, schedulerID)

	// Verify ui HTTP API returns the correct data
	t.Log("Verifying ui HTTP API...")
	verifyUIHTTPAPI(t, ctx, schedulerIDStr)

	// ── Audit fields: the run's kind and its tasks' images are stored ──
	t.Log("Verifying audit fields (kind, image_tag)...")

	trackerKind := queryRunKind(t, clients.runControllerDB, schedulerID)
	assert.Equal(t, "trigger", trackerKind, "runs.kind must be trigger after StartRun")

	imageTag := queryFirstTaskImageTag(t, clients.runControllerDB, schedulerID)
	assert.NotEmpty(t, imageTag, "tasks.image_tag must be populated")

	t.Log("✅ audit assertions passed")

	t.Log("🎉 E2E test completed successfully!")
}

// startScheduleRun starts a run of the schedule through RunService.StartRun.
// run-controller creates the run, chooses its tasks and dispatches the ready ones itself.
func startScheduleRun(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
) string {
	resp, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{
		RequestId:    uuid.NewString(),
		ScheduleName: testScheduleName,
	})
	require.NoError(t, err, "Failed to start a run of the schedule via run-controller service")

	t.Logf("Started a run of schedule %q: run_id=%s", testScheduleName, resp.GetRunId())
	return resp.GetRunId()
}
