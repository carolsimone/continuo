package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestTriggerSchedule_SeedRunAndRerun verifies the full trigger flow from the
// ui-service HTTP endpoint (the exact path the UI "Run" button uses) through
// to schedule completion. It triggers the 3-node seed schedule via
// POST /api/schedules/seed/trigger, waits for all tasks to succeed, then
// re-triggers and verifies the second run also completes.
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
	t.Log("=== Run 1: Triggering seed schedule via ui-service HTTP ===")
	scheduleID1 := triggerScheduleHTTP(t, clients.uiBase, scheduleName)
	t.Logf("Run 1 created: schedule_id=%s", scheduleID1)

	id1, err := uuid.Parse(scheduleID1)
	require.NoError(t, err)

	waitForAllTasksSucceeded(t, ctx, clients, id1, seedTables)
	verifySchedulerSucceeded(t, ctx, clients, id1)
	t.Log("✅ Run 1 completed successfully")

	// === Run 2: Re-trigger ===
	t.Log("=== Run 2: Re-triggering seed schedule ===")
	scheduleID2 := triggerScheduleHTTP(t, clients.uiBase, scheduleName)
	t.Logf("Run 2 created: schedule_id=%s", scheduleID2)

	require.NotEqual(t, scheduleID1, scheduleID2, "Run 2 must have a different schedule_id")

	id2, err := uuid.Parse(scheduleID2)
	require.NoError(t, err)

	waitForAllTasksSucceeded(t, ctx, clients, id2, seedTables)
	verifySchedulerSucceeded(t, ctx, clients, id2)
	t.Log("✅ Run 2 completed successfully")

	t.Log("🎉 TriggerSchedule run-and-rerun test passed")
}

// triggerScheduleHTTP triggers a schedule via the ui-service HTTP endpoint
// (POST /api/schedules/:name/trigger) and returns the schedule_id.
func triggerScheduleHTTP(t *testing.T, base, scheduleName string) string {
	t.Helper()

	resp, err := http.Post(
		fmt.Sprintf("%s/api/schedules/%s/trigger", base, scheduleName),
		"application/json", nil,
	)
	require.NoError(t, err, "POST /api/schedules/%s/trigger: request failed", scheduleName)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"POST /api/schedules/%s/trigger: expected 200, got %d: %s", scheduleName, resp.StatusCode, string(raw))

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &result))

	scheduleID, ok := result["schedule_id"].(string)
	require.True(t, ok && scheduleID != "", "schedule_id missing from trigger response")

	return scheduleID
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
