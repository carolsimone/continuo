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

	// Trigger manifest-controller to load the graph
	t.Log("Triggering manifest-controller graph load...")
	triggerGraphLoad(t, ctx, clients)

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

	// Verify scheduler reaches SUCCEEDED state
	t.Log("Verifying scheduler reaches SUCCEEDED state...")
	verifySchedulerSucceeded(t, ctx, clients, schedulerID)

	// Verify ui-service HTTP API returns the correct data
	t.Log("Verifying ui-service HTTP API...")
	verifyUIService(t, ctx, schedulerIDStr)

	// ── PR0 audit assertions: kind stamped on both stores; per-task metadata populated ──
	t.Log("Verifying PR0 audit fields (kind, image_tag, manifest_version)...")

	runKind := queryNeo4jRunKind(t, clients, schedulerID)
	assert.Equal(t, "cron", runKind, "fresh schedule trigger must stamp :Run.kind = cron")

	trackerKind := queryPostgresTrackerKind(t, clients.stateDB, schedulerID)
	assert.Equal(t, "cron", trackerKind, "scheduler_tracker.kind must be cron after fresh activation")

	manifestVersion, imageTag := queryFirstTaskTrackerMetadata(t, clients.stateDB, schedulerID)
	assert.NotEmpty(t, manifestVersion, "task_tracker.manifest_version must be populated")
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
