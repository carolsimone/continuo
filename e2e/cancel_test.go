package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCancelMidwayAndRetrigger verifies the cancel → retrigger recovery flow
// using the standard happy-path DAG (e2e-schedule, all working nodes).
//
// Phase 1: Load graph, activate schedule, let level 0 (seeds) complete.
// Phase 2: Cancel mid-way — before level 1 can finish.
//          Assert scheduler + tasks reach 'cancelled' and all three service
//          guards (cancelled_schedules tables) are armed via the Redis event.
// Phase 3: Clean up the cancelled run state, re-activate the same schedule.
// Phase 4: Assert the fresh run completes fully to 'succeeded'.
func TestCancelMidwayAndRetrigger(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	defer func() {
		cleanupTestData(t, ctx, clients, testScheduleName)
	}()

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, testScheduleName)

	// ── Phase 1: Load graph + run level 0 ────────────────────────────────────
	t.Log("Phase 1: loading graph and activating schedule...")
	triggerGraphLoad(t, ctx, clients)

	cancelledIDStr := createAndActivateScheduler(t, ctx, clients)
	cancelledID, err := uuid.Parse(cancelledIDStr)
	require.NoError(t, err)

	verifyOrchestratorPublishedRootNodes(t, ctx, clients, cancelledID, []string{"seed_table_1", "seed_table_2", "seed_table_3"})

	level0 := []string{"seed_table_1", "seed_table_2", "seed_table_3"}
	verifyExecutorDeployedJobs(t, ctx, clients, level0, testScheduleName)
	verifyJobsCompleted(t, ctx, clients, level0, testScheduleName)

	t.Log("Phase 1 complete: level 0 (seeds) succeeded")

	// ── Phase 2: Cancel mid-way ───────────────────────────────────────────────
	// Level 1 jobs (table_a/b/c) are now being dispatched. Cancel before they
	// can complete so we exercise the cancellation guards.
	t.Log("Phase 2: cancelling schedule mid-way...")
	callCancelEndpoint(t, ctx, testScheduleName, "e2e-test", "cancel-mid-run e2e test")

	verifySchedulerCancelled(t, ctx, clients, cancelledID)
	verifyTasksCancelled(t, ctx, clients, cancelledID)
	verifyCancelledSchedulesGuardArmed(t, ctx, clients, cancelledID)

	t.Log("Phase 2 complete: schedule cancelled, guards armed in all three services")

	// ── Phase 3: Clean up + re-activate ──────────────────────────────────────
	// The fresh run gets a new schedule_id → the guards for the old ID do not
	// affect it. Clean up Postgres state, Redis streams, and K8s jobs so the
	// fresh run starts from a clean slate.
	t.Log("Phase 3: cleaning up cancelled run and re-triggering...")
	cleanupPostgresByScheduleID(t, ctx, clients, cancelledIDStr, testScheduleName)
	cleanupNeo4j(t, ctx, clients, testScheduleName)
	cleanupRedis(t, ctx, clients)
	cleanupK8s(t, ctx)

	freshIDStr := createAndActivateScheduler(t, ctx, clients)
	freshID, err := uuid.Parse(freshIDStr)
	require.NoError(t, err)

	t.Logf("Phase 3 complete: fresh run activated (schedule_id=%s)", freshID)

	// ── Phase 4: Verify fresh run completes ───────────────────────────────────
	t.Log("Phase 4: verifying fresh run completes to SUCCEEDED...")
	verifyOrchestratorPublishedRootNodes(t, ctx, clients, freshID, []string{"seed_table_1", "seed_table_2", "seed_table_3"})
	verifyFullDAGExecution(t, ctx, clients, freshID)
	verifySchedulerSucceeded(t, ctx, clients, freshID)

	t.Log("TestCancelMidwayAndRetrigger completed successfully")
}

// ─── cancel helpers ──────────────────────────────────────────────────────────

// callCancelEndpoint calls POST /api/schedules/:name/cancel and asserts 200 OK.
func callCancelEndpoint(t *testing.T, ctx context.Context, scheduleName, cancelledBy, reason string) {
	t.Helper()
	uiBase := getEnv("UI_HTTP_BASE", "http://ui:8090")
	url := fmt.Sprintf("%s/api/schedules/%s/cancel", uiBase, scheduleName)

	body, err := json.Marshal(map[string]string{
		"cancelled_by":        cancelledBy,
		"cancellation_reason": reason,
	})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 15 * time.Second}
	resp, err := httpClient.Do(req)
	require.NoError(t, err, "POST /api/schedules/%s/cancel failed", scheduleName)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"expected 200 OK from cancel endpoint, got %d", resp.StatusCode)
	t.Logf("POST /api/schedules/%s/cancel → 200 OK", scheduleName)
}

// verifySchedulerCancelled polls scheduler_tracker until status = 'cancelled'.
func verifySchedulerCancelled(t *testing.T, ctx context.Context, clients *testClients, schedulerID uuid.UUID) {
	t.Helper()
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		var status string
		err := clients.stateDB.Get(&status, `
			SELECT status FROM scheduler_tracker WHERE schedule_id = $1
		`, schedulerID)
		if err != nil {
			return false, err
		}
		return status == "cancelled", nil
	}, "Timeout waiting for scheduler to reach 'cancelled' status")

	t.Log("✅ scheduler_tracker.status = 'cancelled'")
}

// verifyTasksCancelled asserts that at least some non-seed tasks were cancelled.
// CancelSchedule atomically sets all pending/running tasks to 'cancelled', so
// any task that had not yet succeeded before the cancel call must be cancelled.
func verifyTasksCancelled(t *testing.T, ctx context.Context, clients *testClients, schedulerID uuid.UUID) {
	t.Helper()
	var count int
	err := clients.stateDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM task_tracker
		WHERE schedule_id = $1 AND status = 'cancelled'
	`, schedulerID).Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0,
		"Expected at least one task to be 'cancelled' after CancelSchedule")
	t.Logf("✅ %d tasks have status='cancelled'", count)
}

// verifyCancelledSchedulesGuardArmed polls all three service databases until each
// has a row in cancelled_schedules for the given schedule_id, confirming that
// the schedule.cancelled:v1 Redis event was consumed by every consumer.
func verifyCancelledSchedulesGuardArmed(t *testing.T, ctx context.Context, clients *testClients, schedulerID uuid.UUID) {
	t.Helper()

	type dbCheck struct {
		name  string
		query func() (bool, error)
	}

	checks := []dbCheck{
		{
			name: "orchestrator",
			query: func() (bool, error) {
				var exists bool
				err := clients.orchestratorDB.QueryRowContext(ctx,
					"SELECT EXISTS(SELECT 1 FROM cancelled_schedules WHERE schedule_id = $1)",
					schedulerID,
				).Scan(&exists)
				return exists, err
			},
		},
		{
			name: "executor-controller",
			query: func() (bool, error) {
				var exists bool
				err := clients.executorDB.QueryRowContext(ctx,
					"SELECT EXISTS(SELECT 1 FROM cancelled_schedules WHERE schedule_id = $1)",
					schedulerID,
				).Scan(&exists)
				return exists, err
			},
		},
		{
			name: "k8s-controller",
			query: func() (bool, error) {
				var exists bool
				err := clients.k8sDB.QueryRowContext(ctx,
					"SELECT EXISTS(SELECT 1 FROM cancelled_schedules WHERE schedule_id = $1)",
					schedulerID,
				).Scan(&exists)
				return exists, err
			},
		},
	}

	for _, c := range checks {
		pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, c.query,
			fmt.Sprintf("Timeout waiting for cancelled_schedules guard in %s", c.name))
		t.Logf("✅ cancelled_schedules guard armed in %s", c.name)
	}
}

// assertTaskStatus asserts a task has the expected status.
func assertTaskStatus(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
	tableName, expectedStatus string,
) {
	t.Helper()
	var status string
	err := clients.stateDB.QueryRowContext(ctx, `
		SELECT status FROM task_tracker
		WHERE schedule_id = $1 AND table_name = $2
	`, schedulerID, tableName).Scan(&status)
	require.NoError(t, err, "Failed to query status for %s", tableName)
	require.Equal(t, expectedStatus, status,
		"Expected status=%s for %s, got %s", expectedStatus, tableName, status)
	t.Logf("Task %s has expected status=%s", tableName, expectedStatus)
}

// assertInitStatusCompleted asserts initialization_status = "completed".
func assertInitStatusCompleted(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
) {
	t.Helper()
	var initStatus string
	err := clients.stateDB.QueryRowContext(ctx, `
		SELECT initialization_status FROM scheduler_tracker WHERE schedule_id = $1
	`, schedulerID).Scan(&initStatus)
	require.NoError(t, err, "Failed to query initialization_status")
	require.Equal(t, "completed", initStatus,
		"Expected initialization_status=completed, got %s", initStatus)
	t.Log("initialization_status=completed confirmed")
}

// assertTaskExecutionAtLeast asserts a task has at least minCount task_execution records.
func assertTaskExecutionAtLeast(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
	tableName string,
	minCount int,
) {
	t.Helper()
	var count int
	err := clients.stateDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM task_execution te
		JOIN task_tracker tt ON te.task_id = tt.task_id
		WHERE tt.schedule_id = $1 AND tt.table_name = $2
	`, schedulerID, tableName).Scan(&count)
	require.NoError(t, err, "Failed to query task_execution count for %s", tableName)
	require.GreaterOrEqual(t, count, minCount,
		"Expected >= %d task_execution records for %s, got %d", minCount, tableName, count)
	t.Logf("task_execution count for %s: %d (>= %d required)", tableName, count, minCount)
}
