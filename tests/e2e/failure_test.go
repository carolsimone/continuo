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

// TestE2E_FailurePath_NodeFailureDrainsSchedule verifies that when ftable_e
// permanently fails (exhausts 2 retries / 3 total attempts), its downstream
// node ftable_f is never deployed and the scheduler is finalised as FAILED.
//
// DAG topology loaded via manifest-controller from dbt/services/service-{1,2,3}/:
//
//	ftable_a (service-1)  ftable_b (service-1)
//	              \           /
//	           ftable_c (service-3)
//	          /                    \
//	ftable_d (service-2)   ftable_e (service-2, FAILS — JOINs public.wrong_name)
//	          \                    /
//	           ftable_f (service-3)  ← never deployed
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

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	t.Log("Loading graph via manifest-controller...")
	triggerGraphLoad(t, ctx, clients)

	t.Log("Activating schedule...")
	schedulerIDStr := createAndActivateFailureScheduler(t, ctx, clients)
	schedulerID, err := uuid.Parse(schedulerIDStr)
	require.NoError(t, err, "Invalid schedule_id returned from ActivateSchedule")

	t.Log("Waiting for ftable_e to exhaust retries...")
	verifyNodeExhaustedRetries(t, ctx, clients, schedulerID, "ftable_e")

	t.Log("Verifying ftable_f was never deployed...")
	verifyNoJobsDeployed(t, ctx, []string{"ftable_f"})

	t.Log("Verifying scheduler reaches FAILED state...")
	verifySchedulerFailed(t, ctx, clients, schedulerID)

	// ── PR0 rerun audit assertion: TriggerRerun must flip scheduler_tracker.kind to 'rerun' ──
	// ftable_e is permanently FAILED and the scheduler is FAILED, so there are no
	// running tasks — the preconditions for TriggerRerun are satisfied.
	t.Log("Triggering rerun on ftable_e via gRPC TriggerRerun...")
	_, err = clients.stateClient.TriggerRerun(ctx, &statev1.TriggerRerunRequest{
		ScheduleId:  schedulerIDStr,
		Schema:      testSchemaName,
		TableName:   "ftable_e",
		ServiceName: "service-2",
	})
	require.NoError(t, err, "TriggerRerun(ftable_e) must succeed on a permanently-failed task")

	// NOTE: :Run.kind stays 'cron' on rerun — SnapshotGraph's ON MATCH preserves
	// the original kind via COALESCE for replay-safety. The Postgres tracker
	// (mutated by the rerun handler) is the canonical kind=rerun signal for now.
	// A follow-up PR may revisit this if rerun semantics evolve.
	trackerKindAfterRerun := queryPostgresTrackerKind(t, clients.stateDB, schedulerID)
	assert.Equal(t, "rerun", trackerKindAfterRerun, "rerun must flip scheduler_tracker.kind to rerun")

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
