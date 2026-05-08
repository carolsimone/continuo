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

// TestRebaseFromFailedRun verifies the rebase flow against a permanently
// FAILED source run. Asserts that the new run is created on the source's
// schedule with kind='rebase', source_run_id pointing to source, and the
// expected task partition (failed-and-downstream → rebased PENDING with
// NULL inherited_from_task_id; SUCCEEDED-in-source → inherited with
// non-NULL inherited_from_task_id). We do NOT wait for the rebase run to
// reach a terminal state — ftable_e will fail again under the same dbt
// fixture, and this test's purpose is to validate the partition shape,
// not the dbt re-execution semantics (covered by unit tests).
func TestRebaseFromFailedRun(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	defer cleanupTestData(t, ctx, clients, failureTestScheduleName)
	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	triggerGraphLoad(t, ctx, clients)

	// ── Drive the source run to FAILED (mirrors failure_test.go) ────────────
	srcIDStr := createAndActivateFailureScheduler(t, ctx, clients)
	srcID, err := uuid.Parse(srcIDStr)
	require.NoError(t, err)
	t.Logf("Source schedule created: schedule_id=%s", srcIDStr)

	t.Log("Waiting for ftable_e to exhaust retries...")
	verifyNodeExhaustedRetries(t, ctx, clients, srcID, "ftable_e")

	t.Log("Verifying scheduler reaches FAILED state...")
	verifySchedulerFailed(t, ctx, clients, srcID)

	// ── Trigger rebase against the failed source ────────────────────────────
	t.Log("Triggering rebase against failed source run...")
	rebaseResp, err := clients.stateClient.TriggerRebase(ctx, &statev1.TriggerRebaseRequest{
		SourceRunId: srcIDStr,
	})
	require.NoError(t, err, "TriggerRebase must succeed against a FAILED source")
	require.NotEmpty(t, rebaseResp.RunId, "rebase must return a new run_id")
	require.NotEqual(t, srcIDStr, rebaseResp.RunId, "rebase must mint a NEW run id, not mutate the source")

	newRunID, err := uuid.Parse(rebaseResp.RunId)
	require.NoError(t, err, "rebase run_id must be a valid UUID")
	t.Logf("Rebase run created: run_id=%s schedule_name=%s", newRunID, rebaseResp.ScheduleName)

	// ── Source row stays unchanged ──────────────────────────────────────────
	srcKind := queryPostgresTrackerKind(t, clients.stateDB, srcID)
	assert.Equal(t, "cron", srcKind, "source row must NOT mutate — kind stays 'cron'")

	// ── New run row: kind='rebase', source_run_id=src ───────────────────────
	newKind := queryPostgresTrackerKind(t, clients.stateDB, newRunID)
	assert.Equal(t, "rebase", newKind, "new run must have kind='rebase'")

	var newSourceRunID *uuid.UUID
	err = clients.stateDB.QueryRowContext(ctx,
		`SELECT source_run_id FROM scheduler_tracker WHERE schedule_id = $1`, newRunID,
	).Scan(&newSourceRunID)
	require.NoError(t, err)
	require.NotNil(t, newSourceRunID, "rebase run must have source_run_id set")
	assert.Equal(t, srcID, *newSourceRunID, "source_run_id must point to the source")

	// ── Wait for the rebase run's task_tracker rows to materialise ──────────
	// The orchestrator's HandleRebase emits run.entries.dispatched:v1 which
	// state's run_entries_dispatched_handler ingests to create task_tracker.
	t.Log("Waiting for rebase task_tracker rows to land...")
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		var count int
		qErr := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM task_tracker WHERE schedule_id = $1`, newRunID,
		).Scan(&count)
		if qErr != nil {
			return false, qErr
		}
		return count >= 6, nil // 6 ftable_* nodes in the failure topology
	}, "task_tracker rows for rebase run did not materialise")

	// ── Assert partition shape ──────────────────────────────────────────────
	rows, err := clients.stateDB.QueryContext(ctx, `
		SELECT table_name, status, inherited_from_task_id IS NULL AS is_real
		FROM task_tracker
		WHERE schedule_id = $1
		ORDER BY table_name`, newRunID)
	require.NoError(t, err)
	defer rows.Close()

	type taskRow struct {
		tableName string
		status    string
		isReal    bool
	}
	var rebaseTasks []taskRow
	for rows.Next() {
		var r taskRow
		require.NoError(t, rows.Scan(&r.tableName, &r.status, &r.isReal))
		rebaseTasks = append(rebaseTasks, r)
	}
	require.NoError(t, rows.Err())

	// Categorize: ftable_a/b/c/d should be inherited (status=succeeded, is_real=false).
	// ftable_e/f should be rebased (is_real=true; status starts as 'pending').
	for _, r := range rebaseTasks {
		switch r.tableName {
		case "ftable_a", "ftable_b", "ftable_c", "ftable_d":
			assert.False(t, r.isReal, "%s should be inherited (inherited_from_task_id non-NULL)", r.tableName)
			assert.Equal(t, "succeeded", r.status, "%s should be inherited as succeeded", r.tableName)
		case "ftable_e", "ftable_f":
			assert.True(t, r.isReal, "%s should be rebased (inherited_from_task_id NULL)", r.tableName)
			// Status may be 'pending', 'running', or terminal — don't pin it here.
		default:
			t.Errorf("unexpected table_name in rebase task_tracker: %s", r.tableName)
		}
	}
	t.Log("Rebase partition shape verified")
}

// TestRebaseFromCancelledRun is intentionally skipped.
//
// Cancel-mid-flight timing is unreliable in e2e: by the time we observe a
// task running and issue CancelSchedule, the schedule may have already
// progressed past the cancellable window, leading to flaky test runs.
//
// Eligibility for CANCELLED source runs is covered deterministically by
// TestRebaseHandler_HappyPath_CancelledSource in the orchestrator unit
// tests, which exercises the same eligibility branch under controlled
// conditions.
func TestRebaseFromCancelledRun(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	t.Skip("TODO: requires reliable cancel-mid-flight timing; eligibility for CANCELLED source is covered by RebaseHandler unit tests (TestRebaseHandler_HappyPath_CancelledSource)")
}

// TestRebaseOfRebase is intentionally skipped.
//
// The test would need to wait for two chained terminal states (R1 FAILED,
// then R2 FAILED) before triggering R3 — but the rebase of R1 re-executes
// the same dbt fixture that fails ftable_e, so R2's failure path takes the
// full retry-exhaustion budget twice in sequence. That timing is too long
// and too noisy for a deterministic e2e signal.
//
// Root-forwarding semantics (R3.inherited_from_task_id points to R1's
// task_id, not R2's intermediate one) are covered by
// TestRebasePartition_RebaseOfRebase_RootForwarding in the snapshot
// selector unit tests.
func TestRebaseOfRebase(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	t.Skip("TODO: requires waiting for chained terminal states; root-forwarding semantics covered by TestRebasePartition_RebaseOfRebase_RootForwarding unit test")
}
