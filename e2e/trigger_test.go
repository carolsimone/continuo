package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestTriggerSchedule_SeedRunAndRerun verifies that the TriggerSchedule RPC
// (the flow the UI "Run" button uses) works end-to-end for the "seed" schedule.
// It triggers the schedule, waits for all 3 seed nodes to succeed, then
// re-triggers and verifies the second run also succeeds.
func TestTriggerSchedule_SeedRunAndRerun(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	const scheduleName = "seed"
	seedTables := []string{"seed_table_1", "seed_table_2", "seed_table_3"}

	defer func() {
		cleanupTestData(t, ctx, clients, scheduleName)
	}()

	// Prerequisites
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	cleanupTestData(t, ctx, clients, scheduleName)

	// Load graph — manifest-controller creates all nodes (seeds + models)
	triggerGraphLoad(t, ctx, clients)

	// Verify "seed" schedule is in the catalog (required by TriggerSchedule)
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		var count int
		err := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM schedule_catalog WHERE schedule_name = $1 AND removed_at IS NULL`,
			scheduleName,
		).Scan(&count)
		if err != nil {
			return false, err
		}
		return count > 0, nil
	}, "Timeout waiting for 'seed' to appear in schedule_catalog")

	// === Run 1 ===
	t.Log("=== Run 1: Triggering seed schedule via TriggerSchedule ===")
	resp1, err := clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
		ScheduleName: scheduleName,
	})
	require.NoError(t, err, "TriggerSchedule failed for run 1")
	require.NotEmpty(t, resp1.ScheduleId)
	scheduleID1, err := uuid.Parse(resp1.ScheduleId)
	require.NoError(t, err)
	t.Logf("Run 1 created: schedule_id=%s", scheduleID1)

	waitForAllTasksSucceeded(t, ctx, clients, scheduleID1, seedTables)
	verifySchedulerSucceeded(t, ctx, clients, scheduleID1)
	t.Log("✅ Run 1 completed successfully")

	// === Run 2: Re-trigger ===
	t.Log("=== Run 2: Re-triggering seed schedule ===")
	resp2, err := clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
		ScheduleName: scheduleName,
	})
	require.NoError(t, err, "TriggerSchedule failed for run 2 — schedule should be re-triggerable after completion")
	require.NotEmpty(t, resp2.ScheduleId)
	scheduleID2, err := uuid.Parse(resp2.ScheduleId)
	require.NoError(t, err)
	t.Logf("Run 2 created: schedule_id=%s", scheduleID2)

	require.NotEqual(t, scheduleID1, scheduleID2, "Run 2 must have a different schedule_id")

	waitForAllTasksSucceeded(t, ctx, clients, scheduleID2, seedTables)
	verifySchedulerSucceeded(t, ctx, clients, scheduleID2)
	t.Log("✅ Run 2 completed successfully")

	t.Log("🎉 TriggerSchedule run-and-rerun test passed")
}

// waitForAllTasksSucceeded polls task_tracker until all given tables reach
// "succeeded" status for the specified schedule_id. Unlike verifyJobsCompleted
// (which filters by schedule_name), this filters by schedule_id to support
// multiple runs of the same schedule in a single test.
func waitForAllTasksSucceeded(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	scheduleID uuid.UUID,
	tables []string,
) {
	t.Helper()
	pollUntil(t, ctx, 5*time.Minute, 2*time.Second, func() (bool, error) {
		for _, table := range tables {
			var status string
			err := clients.stateDB.QueryRowContext(ctx, `
				SELECT status FROM task_tracker
				WHERE schedule_id = $1 AND table_name = $2
			`, scheduleID, table).Scan(&status)
			if err != nil {
				return false, nil
			}
			if status != "succeeded" {
				return false, nil
			}
		}
		return true, nil
	}, fmt.Sprintf("Timeout waiting for %d tasks to succeed for schedule %s", len(tables), scheduleID))

	t.Logf("✅ All %d tasks succeeded for schedule %s", len(tables), scheduleID)
}
