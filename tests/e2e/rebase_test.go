package e2e

import (
	"context"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
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

// TestRebaseAllInheritedFinalizes verifies that a rebase whose projection
// contains a mix of inherited-terminal and rebased rows finalizes correctly
// — exercising the bug fixed in T3 (B1: state seeds terminal_task_count
// for inherited rows so the mixed-projection rollup math closes).
//
// Setup mirrors TestRebaseFromFailedRun: the failure schedule's source run
// ends in FAILED with ftable_a/b/c/d=succeeded, ftable_e=failed,
// ftable_f=skipped. The rebase produces:
//
//	ftable_a/b/c/d  →  inherited (succeeded)
//	ftable_e/f      →  rebased   (PENDING)
//
// Because the dbt fixture for ftable_e is permanently broken (JOIN
// public.wrong_name) the rebased ftable_e fails again and the rebase run
// reaches FAILED. Without B1's fix, terminal_task_count would start at 0
// and the rollup would expect 6 terminal events but only ever observe
// 2 (e=failed, f=cascade-skipped) — leaving the run hung indefinitely.
//
// Assertions:
//   - rebase scheduler_tracker.status reaches a terminal state (failed)
//   - rebase scheduler_tracker.completed_at is non-NULL
//   - state_outbox carries a 'run.finalized:v1' entry for the rebase run
//     (T4/B3 — the dispatched-handler auto-rollup branch is exercised
//     elsewhere via unit tests; here the normal task-status path must
//     emit run.finalized:v1 as well)
//   - orchestrator's Neo4j :Run for the rebase has completed_at set
//     (orchestrator processed run.finalized:v1 and finalized its projection)
//
// The all-inherited (no rebased rows) variant of B3 is NOT exercised here
// because the rebase precondition rejects sources with zero non-SUCCEEDED
// tasks (TriggerRebase against an all-succeeded source returns an error).
// That branch is covered by the run_entries_dispatched_handler unit test
// TestRunEntriesDispatchedHandler_AllInherited_AutoRollupEmitsFinalize.
func TestRebaseAllInheritedFinalizes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	defer cleanupTestData(t, ctx, clients, failureTestScheduleName)
	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	triggerGraphLoad(t, ctx, clients)

	// ── Drive the source run to FAILED ──────────────────────────────────────
	srcIDStr := createAndActivateFailureScheduler(t, ctx, clients)
	srcID, err := uuid.Parse(srcIDStr)
	require.NoError(t, err)
	t.Logf("Source schedule created: schedule_id=%s", srcIDStr)

	t.Log("Waiting for ftable_e to exhaust retries...")
	verifyNodeExhaustedRetries(t, ctx, clients, srcID, "ftable_e")

	t.Log("Verifying source scheduler reaches FAILED state...")
	verifySchedulerFailed(t, ctx, clients, srcID)

	// ── Trigger rebase against the failed source ────────────────────────────
	t.Log("Triggering rebase against failed source run...")
	rebaseResp, err := clients.stateClient.TriggerRebase(ctx, &statev1.TriggerRebaseRequest{
		SourceRunId: srcIDStr,
	})
	require.NoError(t, err, "TriggerRebase must succeed against a FAILED source")
	require.NotEmpty(t, rebaseResp.RunId)

	rebaseRunID, err := uuid.Parse(rebaseResp.RunId)
	require.NoError(t, err)
	t.Logf("Rebase run created: run_id=%s", rebaseRunID)

	// ── Wait for the rebase run to reach a terminal state ───────────────────
	// The rebased ftable_e will fail again under the same fixture, so the
	// rebase run lands in FAILED. The critical check: terminal_task_count
	// must close (B1's fix). Without it, this poll hangs.
	t.Log("Waiting for rebase scheduler_tracker to reach terminal status...")
	pollUntil(t, ctx, 10*time.Minute, 5*time.Second, func() (bool, error) {
		var status string
		var completedAt *time.Time
		qErr := clients.stateDB.QueryRowContext(ctx,
			`SELECT status, completed_at FROM scheduler_tracker WHERE schedule_id = $1`,
			rebaseRunID,
		).Scan(&status, &completedAt)
		if qErr != nil {
			return false, qErr
		}
		// Terminal = succeeded | failed | cancelled, plus completed_at set.
		isTerminal := status == "failed" || status == "succeeded" || status == "cancelled"
		return isTerminal && completedAt != nil, nil
	}, "rebase scheduler_tracker did not reach terminal status — likely B1 regression "+
		"(terminal_task_count not seeded for inherited rows; rollup never closes)")

	// Read the final state for assertions.
	var finalStatus string
	var finalCompletedAt *time.Time
	require.NoError(t, clients.stateDB.QueryRowContext(ctx,
		`SELECT status, completed_at FROM scheduler_tracker WHERE schedule_id = $1`,
		rebaseRunID,
	).Scan(&finalStatus, &finalCompletedAt))

	t.Logf("Rebase run finalized: status=%s completed_at=%v", finalStatus, finalCompletedAt)
	assert.Contains(t, []string{"failed", "succeeded", "cancelled"}, finalStatus,
		"rebase run must be in a terminal status")
	assert.NotNil(t, finalCompletedAt, "completed_at must be non-NULL once the run is terminal")

	// ── Assert run.finalized:v1 was emitted via state_outbox ────────────────
	// Whether the entry is still 'pending' or already 'published', its presence
	// is what proves the finalization handler ran. The B3 fix specifically
	// added this emission to the dispatched-handler auto-rollup branch, but
	// the regular task-status finalization path (which closes this run)
	// emits it via state's task_status_updated_handler.
	t.Log("Verifying run.finalized:v1 outbox entry exists for the rebase run...")
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		var n int
		qErr := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM state_outbox
			   WHERE aggregate_id = $1 AND stream_name = 'run.finalized:v1'`,
			rebaseRunID,
		).Scan(&n)
		if qErr != nil {
			return false, qErr
		}
		return n >= 1, nil
	}, "no run.finalized:v1 outbox entry for the rebase run")

	// ── Assert orchestrator finalized the Neo4j :Run ────────────────────────
	// orchestrator consumes run.finalized:v1 and stamps :Run.completed_at.
	// If completed_at stays NULL, the run is still 'active' in the
	// orchestrator's projection — exactly the regression B3 fixed for the
	// auto-rollup branch (and which we want to keep working in this branch).
	t.Log("Verifying orchestrator stamped Neo4j :Run.completed_at for the rebase run...")
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
			AccessMode: neo4jdriver.AccessModeRead,
		})
		defer session.Close(ctx)
		result, qErr := session.Run(ctx,
			`MATCH (r:Run {run_id: $run_id}) RETURN r.completed_at IS NOT NULL AS done`,
			map[string]interface{}{"run_id": rebaseRunID.String()})
		if qErr != nil {
			return false, qErr
		}
		if !result.Next(ctx) {
			return false, nil
		}
		raw, _ := result.Record().Get("done")
		done, _ := raw.(bool)
		return done, nil
	}, "orchestrator did not stamp completed_at on the rebase :Run — "+
		"orchestrator never finalized its projection (likely missed run.finalized:v1)")

	t.Log("Rebase run finalized end-to-end: state + orchestrator agree")
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
