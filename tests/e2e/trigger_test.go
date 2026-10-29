package e2e

import (
	"bytes"
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
// ui HTTP endpoint (the exact path the UI "Run" button uses) through
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

	const scheduleName = "seed"
	seedTables := []string{"seed_table_1", "seed_table_2", "seed_table_3"}

	defer func() {
		cleanupTestData(t, ctx, clients, scheduleName)
	}()

	// Prerequisites
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	cleanupTestData(t, ctx, clients, scheduleName)

	// Announce the e2e DAG. seedTopology returns once run-controller's live release has
	// the seed schedule's models, which is what StartRun needs to know it.
	seedTopology(t, ctx, clients)

	// === Run 1 ===
	t.Log("=== Run 1: Triggering seed schedule via ui HTTP ===")
	scheduleID1 := triggerScheduleHTTP(t, clients.uiBase, scheduleName)
	t.Logf("Run 1 created: run_id=%s", scheduleID1)

	id1, err := uuid.Parse(scheduleID1)
	require.NoError(t, err)

	waitForAllTasksSucceeded(t, ctx, clients, id1, seedTables)
	verifySchedulerSucceeded(t, ctx, clients, id1)
	t.Log("✅ Run 1 completed successfully")

	// === Run 2: Re-trigger ===
	t.Log("=== Run 2: Re-triggering seed schedule ===")
	scheduleID2 := triggerScheduleHTTP(t, clients.uiBase, scheduleName)
	t.Logf("Run 2 created: run_id=%s", scheduleID2)

	require.NotEqual(t, scheduleID1, scheduleID2, "Run 2 must have a different run_id")

	id2, err := uuid.Parse(scheduleID2)
	require.NoError(t, err)

	waitForAllTasksSucceeded(t, ctx, clients, id2, seedTables)
	verifySchedulerSucceeded(t, ctx, clients, id2)
	t.Log("✅ Run 2 completed successfully")

	t.Log("🎉 StartRun run-and-rerun test passed")
}

// triggerScheduleHTTP triggers a schedule via the ui HTTP endpoint
// (POST /api/schedules/:name/trigger {"request_id": id}) and returns the
// run_id.
func triggerScheduleHTTP(t *testing.T, base, scheduleName string) string {
	t.Helper()

	body, err := json.Marshal(map[string]string{"request_id": uuid.NewString()})
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("%s/api/schedules/%s/trigger", base, scheduleName),
		"application/json", bytes.NewReader(body),
	)
	require.NoError(t, err, "POST /api/schedules/%s/trigger: request failed", scheduleName)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"POST /api/schedules/%s/trigger: expected 200, got %d: %s", scheduleName, resp.StatusCode, string(raw))

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &result))

	runID, ok := result["run_id"].(string)
	require.True(t, ok && runID != "", "run_id missing from trigger response")

	return runID
}

// waitForAllTasksSucceeded polls tasks until all given tables reach
// "succeeded" status for the specified run_id. Unlike verifyJobsCompleted
// (which filters by schedule_name), this filters by run_id to support
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
			err := clients.runControllerDB.QueryRowContext(ctx, `
				SELECT status FROM tasks
				WHERE run_id = $1 AND table_name = $2
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
