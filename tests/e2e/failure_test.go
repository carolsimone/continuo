package e2e

import (
	"context"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestE2E_FailurePath_NodeFailureDrainsSchedule verifies that when a mid-DAG
// node (table_e) permanently fails after exhausting 2 retries (3 total attempts), all downstream
// nodes are never deployed and the scheduler is finalised as FAILED.
//
// DAG used (seeds as level 0, table_e uses service-3-broken):
//
//	Level 0: seed_table_1, seed_table_2, seed_table_3   (all succeed)
//	Level 1: table_a, table_b, table_c                  (all succeed)
//	Level 2: table_d (ok), table_e (FAILS), table_f (ok)
//	Level 3: table_g, table_h                           (never deployed)
//	Level 4: table_i, table_j                           (never deployed)
func TestE2E_FailurePath_NodeFailureDrainsSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	defer func() {
		cleanupTestData(t, ctx, clients, failureTestScheduleName)
	}()

	// Verify prerequisites
	t.Log("Verifying prerequisites...")
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	// Cleanup any existing data from a prior run
	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	// Seed failure DAG (table_e uses service-3-broken)
	t.Log("Seeding failure DAG...")
	seedFailureDAG(t, ctx, clients)

	// Activate schedule
	t.Log("Activating schedule...")
	schedulerIDStr := createAndActivateFailureScheduler(t, ctx, clients)
	schedulerID, err := uuid.Parse(schedulerIDStr)
	require.NoError(t, err, "Invalid schedule_id returned from ActivateSchedule")

	// Verify orchestrator published root node messages to query.model:v1
	t.Log("Verifying orchestrator published root node messages...")
	verifyOrchestratorPublishedRootNodes(t, ctx, clients, schedulerID, []string{"seed_table_1", "seed_table_2", "seed_table_3"})

	// Level 0: seeds run first
	t.Log("Verifying Level 0 (seeds) execute successfully...")
	level0Seeds := []string{"seed_table_1", "seed_table_2", "seed_table_3"}
	verifyExecutorDeployedJobs(t, ctx, clients, level0Seeds, failureTestScheduleName)
	verifyJobsCompleted(t, ctx, clients, level0Seeds, failureTestScheduleName)

	// Level 1: all root model nodes should succeed
	t.Log("Verifying Level 1 executes successfully...")
	level1Models := []string{"table_a", "table_b", "table_c"}
	verifyExecutorDeployedJobs(t, ctx, clients, level1Models, failureTestScheduleName)
	verifyJobsCompleted(t, ctx, clients, level1Models, failureTestScheduleName)

	// Level 2: all 3 deployed, but table_e will fail
	t.Log("Verifying Level 2 deployed...")
	level2 := []string{"table_d", "table_e", "table_f"}
	verifyExecutorDeployedJobs(t, ctx, clients, level2, failureTestScheduleName)

	// table_d and table_f should succeed
	t.Log("Verifying table_d and table_f succeed...")
	verifyJobsCompleted(t, ctx, clients, []string{"table_d", "table_f"}, failureTestScheduleName)

	// table_e should exhaust all 2 retries (3 total attempts) and be permanently failed
	t.Log("Waiting for table_e to exhaust retries...")
	verifyTableEExhaustedRetries(t, ctx, clients, schedulerID)

	// Level 3 and Level 4 must never be deployed
	t.Log("Verifying Level 3 and Level 4 are never deployed...")
	verifyNoJobsDeployed(t, ctx, []string{"table_g", "table_h", "table_i", "table_j"})

	// Scheduler must be finalised as FAILED
	t.Log("Verifying scheduler reaches FAILED state...")
	verifySchedulerFailed(t, ctx, clients, schedulerID)

	t.Log("✅ Failure path test completed successfully")
}

// createAndActivateFailureScheduler activates the failure schedule via state gRPC.
func createAndActivateFailureScheduler(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
) string {
	resp, err := clients.stateClient.ActivateSchedule(ctx, &statev1.ActivateScheduleRequest{
		ScheduleName: failureTestScheduleName,
	})
	require.NoError(t, err, "Failed to activate failure schedule via state service")
	t.Logf("Activated failure schedule: schedule_id=%s", resp.ScheduleId)
	return resp.ScheduleId
}
