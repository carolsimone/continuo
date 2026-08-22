package e2e

import (
	"context"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
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
	defer clients.close(ctx)

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

	// Seed the full e2e topology via a release.promoted:v1 event.
	t.Log("Seeding topology via release.promoted:v1...")
	seedTopology(t, ctx, clients)

	// Create and activate scheduler
	t.Log("Creating and activating scheduler...")
	schedulerIDStr := createAndActivateScheduler(t, ctx, clients)
	schedulerID, err := uuid.Parse(schedulerIDStr)
	require.NoError(t, err, "Invalid schedule_id returned from ActivateSchedule")

	// Verify orchestrator published root node messages to query.model:v1
	t.Log("Verifying orchestrator published root node messages...")
	verifyOrchestratorPublishedRootNodes(t, ctx, clients, schedulerID, []string{"seed_table_1", "seed_table_2", "seed_table_3"})

	// Verify complete DAG execution
	t.Log("Verifying full DAG execution...")
	verifyFullDAGExecution(t, ctx, clients, schedulerID)

	// Verify service-1 Jobs actually ran through the wise-dbt dialect and
	// service-2/3 Jobs through the built-in dialect (pins the dbt-commands
	// ConfigMap wiring so a regression to silent built-in fallback fails here
	// instead of passing unnoticed). Jobs are still present in the cluster:
	// executor-controller sets no TTLSecondsAfterFinished, and cleanupK8s only
	// runs in this test's deferred cleanup at function exit.
	t.Log("Verifying dbt command dialect routing...")
	verifyDialectRouting(t, ctx)

	// Verify scheduler reaches SUCCEEDED state
	t.Log("Verifying scheduler reaches SUCCEEDED state...")
	verifySchedulerSucceeded(t, ctx, clients, schedulerID)

	// Verify ui HTTP API returns the correct data
	t.Log("Verifying ui HTTP API...")
	verifyUIService(t, ctx, schedulerIDStr)

	// ── PR0 audit assertions: kind stamped on both stores; per-task metadata populated ──
	t.Log("Verifying PR0 audit fields (kind, image_tag)...")

	runKind := queryNeo4jRunKind(t, clients, schedulerID)
	assert.Equal(t, "cron", runKind, "fresh schedule trigger must stamp :Run.kind = cron")

	trackerKind := queryPostgresTrackerKind(t, clients.stateDB, schedulerID)
	assert.Equal(t, "cron", trackerKind, "scheduler_tracker.kind must be cron after fresh activation")

	// manifest_version is a legacy manifest-ingest field; release-sourced topology
	// (release.promoted) does not carry it — provenance is the release_id — so it
	// is empty here by design and is not asserted. image_tag still flows through.
	_, imageTag := queryFirstTaskTrackerMetadata(t, clients.stateDB, schedulerID)
	assert.NotEmpty(t, imageTag, "task_tracker.image_tag must be populated")

	t.Log("✅ PR0 audit assertions passed")

	t.Log("🎉 E2E test completed successfully!")
}

// createAndActivateScheduler triggers a schedule run via the state service gRPC.
// State creates the scheduler_tracker record and publishes scheduler.started:v1 itself.
func createAndActivateScheduler(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
) string {
	resp, err := clients.stateClient.ActivateSchedule(ctx, &statev1.ActivateScheduleRequest{
		ScheduleName: testScheduleName,
	})
	require.NoError(t, err, "Failed to activate schedule via state service")

	t.Logf("Activated schedule %q: schedule_id=%s", testScheduleName, resp.ScheduleId)
	return resp.ScheduleId
}
