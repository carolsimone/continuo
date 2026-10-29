package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
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
//	The run moves to 'cancelling'; execution-controller fences it (its
//	cancelled_runs table), deletes its Jobs and reports each attempt
//	'stopped' once the pods are gone; the run then ends 'cancelled'. Assert
//	the run reaches 'cancelled', its in-flight tasks are 'cancelled', the
//	fence is armed and no Job of the run, nor any pod of one, is still live.
//
// Phase 3: Retrigger once the run is 'cancelled' — no cleanup.
//
//	The fence rejects late dispatches for the old run_id; the trigger
//	creates a fresh run_id.
//
// Phase 4: Assert the fresh run completes fully to 'succeeded'.
func TestCancelMidwayAndRetrigger(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	defer func() {
		cleanupTestData(t, ctx, clients, testScheduleName)
	}()

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, testScheduleName)

	// Tap task.dispatch:v1 before anything is triggered: the trim loop removes
	// consumed entries, so the root-node checks read what the tap recorded.
	dispatchTap := startStreamTap(t, ctx, clients.redisClient, streams.TaskDispatchV1)

	// ── Phase 1: Seed topology + trigger via UI endpoint ─────────────────────────
	t.Log("Phase 1: seeding topology and triggering schedule via UI...")
	seedTopology(t, ctx, clients)

	cancelledIDStr := triggerScheduleHTTP(t, clients.uiBase, testScheduleName)
	cancelledID, err := uuid.Parse(cancelledIDStr)
	require.NoError(t, err)

	verifyRootNodesDispatched(t, ctx, dispatchTap, cancelledID, []string{"seed_table_1", "seed_table_2", "seed_table_3"})

	level0 := []string{"seed_table_1", "seed_table_2", "seed_table_3"}
	verifyExecutorDeployedJobs(t, ctx, clients, level0, testScheduleName)
	verifyJobsCompleted(t, ctx, clients, level0, testScheduleName)

	t.Log("Phase 1 complete: level 0 (seeds) succeeded")

	// ── Phase 2: Cancel mid-way ───────────────────────────────────────────────
	// Level 1 jobs (table_a/b/c) are now being dispatched. Cancel before they
	// can complete so we exercise the cancellation guards.
	t.Log("Phase 2: cancelling schedule mid-way...")
	callCancelEndpoint(t, ctx, cancelledIDStr, "cancel-mid-run e2e test")

	// The run ends 'cancelled' only after execution-controller reported every
	// attempt stopped, i.e. once its Jobs were deleted and their pods gone.
	verifySchedulerCancelled(t, ctx, clients, cancelledID)
	verifyTasksCancelled(t, ctx, clients, cancelledID)
	verifyRunFenced(t, ctx, clients, cancelledID)
	verifyNoLiveJobOfRun(t, ctx, clients, cancelledID)
	verifyRunNotActive(t, ctx, clients, cancelledID)

	t.Log("Phase 2 complete: run cancelled, fenced, no live Job")

	// ── Phase 3: Retrigger — no cleanup ──────────────────────────────────────
	// The run is 'cancelled' and fenced; the system rejects any late dispatch
	// for cancelledID. The trigger creates a brand-new run_id.
	t.Log("Phase 3: retriggering via UI endpoint (no cleanup)...")
	freshIDStr := triggerScheduleHTTP(t, clients.uiBase, testScheduleName)
	freshID, err := uuid.Parse(freshIDStr)
	require.NoError(t, err)
	require.NotEqual(t, cancelledID, freshID, "retrigger must produce a new run_id")

	t.Logf("Phase 3 complete: fresh run triggered (run_id=%s)", freshID)

	// ── Phase 4: Verify fresh run completes ───────────────────────────────────
	// Verify by run_id — two runs share the same schedule_name in the DB,
	// so schedule_name-scoped queries would be ambiguous.
	t.Log("Phase 4: verifying fresh run completes to SUCCEEDED...")
	verifyRootNodesDispatched(t, ctx, dispatchTap, freshID, []string{"seed_table_1", "seed_table_2", "seed_table_3"})

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

// callCancelEndpoint calls POST /api/runs/:id/cancel and asserts 200 OK.
func callCancelEndpoint(t *testing.T, ctx context.Context, runID, reason string) {
	t.Helper()
	uiBase := getEnv("UI_HTTP_BASE", "http://ui:8090")
	url := fmt.Sprintf("%s/api/runs/%s/cancel", uiBase, runID)

	body, err := json.Marshal(map[string]string{
		"cancellation_reason": reason,
	})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 15 * time.Second}
	resp, err := httpClient.Do(req)
	require.NoError(t, err, "POST /api/runs/%s/cancel failed", runID)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"expected 200 OK from cancel endpoint, got %d", resp.StatusCode)
	t.Logf("POST /api/runs/%s/cancel → 200 OK", runID)
}

// verifySchedulerCancelled polls runs until status = 'cancelled'.
// A cancel first moves the run to 'cancelling'; it ends 'cancelled' once
// execution-controller has deleted the run's Jobs and reported every attempt
// stopped, which takes up to 3 minutes.
func verifySchedulerCancelled(t *testing.T, ctx context.Context, clients *testClients, schedulerID uuid.UUID) {
	t.Helper()
	// lastStatus is what the poll saw last: 'cancelling' on a timeout means the
	// run is still waiting for its Jobs to stop, 'running' that the cancel
	// never took hold.
	lastStatus := "(not read)"
	pollUntilDescribed(t, ctx, 3*time.Minute, 500*time.Millisecond, func() (bool, error) {
		var status string
		err := clients.stateDB.Get(&status, `
			SELECT status FROM runs WHERE run_id = $1
		`, schedulerID)
		if err != nil {
			return false, err
		}
		lastStatus = status
		return status == "cancelled", nil
	}, func() string {
		return fmt.Sprintf("run %s did not reach 'cancelled' within 3m; last status %q", schedulerID, lastStatus)
	})

	t.Log("✅ runs.status = 'cancelled'")
}

// verifyTasksCancelled asserts that at least some non-seed tasks were cancelled.
// A cancel atomically sets every task that has not ended (pending,
// dispatched, queued or running) to 'cancelled', so any task still in flight
// before the cancel call must be cancelled.
func verifyTasksCancelled(t *testing.T, ctx context.Context, clients *testClients, schedulerID uuid.UUID) {
	t.Helper()
	var count int
	err := clients.stateDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM tasks
		WHERE run_id = $1 AND status = 'cancelled'
	`, schedulerID).Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0,
		"Expected at least one task to be 'cancelled' after the cancel")
	t.Logf("✅ %d tasks have status='cancelled'", count)
}

// verifyRunNotActive polls RunService.ListActiveRunDrifts until the given run
// is no longer reported as in-flight. A cancelled run's row is terminal, so it
// must drop out of the active set rather than linger forever.
func verifyRunNotActive(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) {
	t.Helper()
	want := runID.String()
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		resp, err := clients.runClient.ListActiveRunDrifts(ctx, &runv1.ListActiveRunDriftsRequest{})
		if err != nil {
			return false, err
		}
		for _, ar := range resp.GetActiveRuns() {
			if ar.GetRunId() == want {
				return false, nil // still active — keep polling
			}
		}
		return true, nil // cancelled run no longer active
	}, "Timeout waiting for cancelled run to leave state active set")
	t.Logf("✅ state no longer reports run %s as active", want)
}

// verifyRunFenced polls execution-controller's database until it has a row in
// cancelled_runs for the given run_id, confirming that the run.cancelled:v1
// Redis event was consumed and the run is fenced at every gate that starts work.
func verifyRunFenced(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) {
	t.Helper()
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		var exists bool
		err := clients.executionDB.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM cancelled_runs WHERE run_id = $1)",
			runID,
		).Scan(&exists)
		return exists, err
	}, "Timeout waiting for the cancelled_runs fence in execution-controller")
	t.Log("✅ cancelled_runs fence armed in execution-controller")
}

// verifyNoLiveJobOfRun asserts that a cancelled run holds no Job that can still
// run work: every Job labelled run-id=<run> is terminal (Complete or
// Failed) or being deleted, and no pod of the run has a phase other than
// Succeeded or Failed. A Job that finished before the cancel stays on the
// cluster until its TTL, so a terminal Job does not count as live.
func verifyNoLiveJobOfRun(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) {
	t.Helper()
	selector := "run-id=" + runID.String()

	jobs, err := getK8sJobs(ctx, selector)
	require.NoError(t, err)
	for _, job := range jobs.Items {
		if job.Metadata.DeletionTimestamp != nil || job.finished() {
			continue
		}
		assert.Failf(t, "live Job of a cancelled run",
			"Job %s of run %s has no deletion timestamp and has not finished", job.Metadata.Name, runID)
	}

	pods, err := getK8sPods(ctx, selector)
	require.NoError(t, err)
	for _, pod := range pods.Items {
		if pod.Status.Phase == "Succeeded" || pod.Status.Phase == "Failed" {
			continue
		}
		assert.Failf(t, "unfinished pod of a cancelled run",
			"pod %s of run %s is %s", pod.Metadata.Name, runID, pod.Status.Phase)
	}
	t.Logf("✅ no live Job or unfinished pod of run %s (%d Jobs, %d pods listed)", runID, len(jobs.Items), len(pods.Items))
}
