package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCancelMidwayAndRetrigger verifies the cancel → retrigger recovery flow
// using the standard happy-path DAG (e2e-schedule, all working nodes).
//
// Phase 1: Load graph, trigger schedule via UI HTTP endpoint, let level 0 (seeds) complete.
// Phase 2: Cancel mid-way — before level 1 can finish.
//
//	Assert scheduler + tasks reach 'cancelled' and all three service
//	guards (cancelled_schedules tables) are armed via the Redis event.
//
// Phase 3: Retrigger immediately via UI HTTP endpoint — no cleanup.
//
//	The system self-stabilises: guards reject late events for the old
//	schedule_id; TriggerSchedule creates a fresh schedule_id.
//
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

	// ── Phase 1: Load graph + trigger via UI endpoint ─────────────────────────
	t.Log("Phase 1: loading graph and triggering schedule via UI...")
	triggerGraphLoad(t, ctx, clients)

	cancelledIDStr := triggerScheduleHTTP(t, clients.uiBase, testScheduleName)
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
	verifyOrchestratorRunNotActive(t, ctx, clients, cancelledID)

	t.Log("Phase 2 complete: schedule cancelled, guards armed in all three services")

	// ── Phase 3: Retrigger — no cleanup ──────────────────────────────────────
	// Guards are armed; the system rejects any late events for cancelledID.
	// TriggerSchedule creates a brand-new schedule_id for the fresh run.
	t.Log("Phase 3: retriggering via UI endpoint (no cleanup)...")
	freshIDStr := triggerScheduleHTTP(t, clients.uiBase, testScheduleName)
	freshID, err := uuid.Parse(freshIDStr)
	require.NoError(t, err)
	require.NotEqual(t, cancelledID, freshID, "retrigger must produce a new schedule_id")

	t.Logf("Phase 3 complete: fresh run triggered (schedule_id=%s)", freshID)

	// ── Phase 4: Verify fresh run completes ───────────────────────────────────
	// Verify by schedule_id — two runs share the same schedule_name in the DB,
	// so schedule_name-scoped queries would be ambiguous.
	t.Log("Phase 4: verifying fresh run completes to SUCCEEDED...")
	verifyOrchestratorPublishedRootNodes(t, ctx, clients, freshID, []string{"seed_table_1", "seed_table_2", "seed_table_3"})

	allTables := []string{
		"seed_table_1", "seed_table_2", "seed_table_3",
		"table_a", "table_b", "table_c",
		"table_d", "table_e", "table_f",
		"table_g", "table_h",
		"table_i", "table_j",
	}
	waitForAllTasksSucceeded(t, ctx, clients, freshID, allTables)
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

// verifyOrchestratorRunNotActive polls OrchestratorQuery.ListActiveRunDrifts until
// the given run is no longer reported as in-flight. A cancelled run is finalized
// via run.finalized:v1 (the orchestrator stamps completed_at on its :Run node), so
// it must drop out of the active set rather than linger forever.
func verifyOrchestratorRunNotActive(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) {
	t.Helper()
	want := runID.String()
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		resp, err := clients.orchestratorClient.ListActiveRunDrifts(ctx, &orchestratorv1.ListActiveRunDriftsRequest{})
		if err != nil {
			return false, err
		}
		for _, ar := range resp.GetActiveRuns() {
			if ar.GetRunId() == want {
				return false, nil // still active — keep polling
			}
		}
		return true, nil // cancelled run no longer active
	}, "Timeout waiting for cancelled run to leave orchestrator active set")
	t.Logf("✅ orchestrator no longer reports run %s as active (finalized via run.finalized:v1)", want)
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

