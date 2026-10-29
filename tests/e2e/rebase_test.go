package e2e

import (
	"context"
	"testing"
	"time"

	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
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

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	defer cleanupTestData(t, ctx, clients, failureTestScheduleName)
	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	seedTopology(t, ctx, clients)

	// ── Drive the source run to FAILED (mirrors failure_test.go) ────────────
	srcIDStr := startFailureScheduleRun(t, ctx, clients)
	srcID, err := uuid.Parse(srcIDStr)
	require.NoError(t, err)
	t.Logf("Source run created: run_id=%s", srcIDStr)

	t.Log("Waiting for ftable_e to exhaust retries...")
	verifyNodeExhaustedRetries(t, ctx, clients, srcID, "ftable_e")

	t.Log("Verifying scheduler reaches FAILED state...")
	verifySchedulerFailed(t, ctx, clients, srcID)

	// ── Trigger rebase against the failed source ────────────────────────────
	t.Log("Triggering rebase against failed source run...")
	rebaseResp, err := clients.runClient.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{RequestId: uuid.NewString(),
		SourceRunId: srcIDStr,
	})
	require.NoError(t, err, "TriggerRebase must succeed against a FAILED source")
	require.NotEmpty(t, rebaseResp.RunId, "rebase must return a new run_id")
	require.NotEqual(t, srcIDStr, rebaseResp.RunId, "rebase must mint a NEW run id, not mutate the source")

	newRunID, err := uuid.Parse(rebaseResp.RunId)
	require.NoError(t, err, "rebase run_id must be a valid UUID")
	t.Logf("Rebase run created: run_id=%s schedule_name=%s", newRunID, rebaseResp.ScheduleName)

	// ── Source row stays unchanged ──────────────────────────────────────────
	srcKind := queryRunKind(t, clients.stateDB, srcID)
	assert.Equal(t, "trigger", srcKind, "source row must NOT mutate — kind stays 'trigger'")

	// ── New run row: kind='rebase', source_run_id=src ───────────────────────
	newKind := queryRunKind(t, clients.stateDB, newRunID)
	assert.Equal(t, "rebase", newKind, "new run must have kind='rebase'")

	var newSourceRunID *uuid.UUID
	err = clients.stateDB.QueryRowContext(ctx,
		`SELECT source_run_id FROM runs WHERE run_id = $1`, newRunID,
	).Scan(&newSourceRunID)
	require.NoError(t, err)
	require.NotNil(t, newSourceRunID, "rebase run must have source_run_id set")
	assert.Equal(t, srcID, *newSourceRunID, "source_run_id must point to the source")

	// ── Wait for the rebase run's tasks rows to materialise ──────────
	// state stores the rebase run's tasks in the transaction that creates it.
	t.Log("Waiting for rebase tasks rows to land...")
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		var count int
		qErr := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM tasks WHERE run_id = $1`, newRunID,
		).Scan(&count)
		if qErr != nil {
			return false, qErr
		}
		return count >= 8, nil // 8 ftable_* nodes in the failure topology (a-h)
	}, "tasks rows for rebase run did not materialise")

	// ── Assert partition shape ──────────────────────────────────────────────
	rows, err := clients.stateDB.QueryContext(ctx, `
		SELECT table_name, status, inherited_from_task_id IS NULL AS is_real
		FROM tasks
		WHERE run_id = $1
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

	if len(rebaseTasks) != 8 {
		dumpRebaseRowsForDebug(t, ctx, clients, newRunID)
	}

	// Categorize: ftable_a/b/c/d should be inherited (status=succeeded, is_real=false).
	// ftable_e/f/g/h should be rebased (is_real=true; status starts as 'pending').
	for _, r := range rebaseTasks {
		switch r.tableName {
		case "ftable_a", "ftable_b", "ftable_c", "ftable_d":
			assert.False(t, r.isReal, "%s should be inherited (inherited_from_task_id non-NULL)", r.tableName)
			assert.Equal(t, "succeeded", r.status, "%s should be inherited as succeeded", r.tableName)
		case "ftable_e", "ftable_f", "ftable_g", "ftable_h":
			assert.True(t, r.isReal, "%s should be rebased (inherited_from_task_id NULL)", r.tableName)
			// Status may be 'pending', 'running', or terminal — don't pin it here.
		default:
			t.Errorf("unexpected table_name in rebase tasks: %s", r.tableName)
		}
	}
	t.Log("Rebase partition shape verified")
}

// TestRebaseAllInheritedFinalizes verifies that a rebase whose projection
// contains a mix of inherited-terminal and rebased rows finalizes correctly.
//
// Setup mirrors TestRebaseFromFailedRun: the failure schedule's source run
// ends in FAILED with ftable_a/b/c/d=succeeded, ftable_e and ftable_g=failed,
// ftable_f and ftable_h=skipped. Source and live release are the same, so no
// node changed and none is new. The rebase produces:
//
//	ftable_a/b/c/d  →  inherited (succeeded)
//	ftable_e/f/g/h  →  rebased   (PENDING)
//
// The rebased ftable_e fails again under the same broken dbt fixture
// (JOIN public.wrong_name), exhausts retries, and the rebase run reaches
// FAILED via the normal task-status finalization path.
//
// What this test bites on:
//   - B1 (terminal_task_count seeding for inherited rows). Without the
//     seeding fix, terminal_task_count starts at 0 and the rollup expects
//     6 terminal events while only ever observing 2 (e=failed,
//     f=cascade-skipped) — the run hangs indefinitely. The terminal-status
//     poll plus the explicit terminal_task_count==total_task_count
//     assertion below cover this.
//   - B4 (rebase-scoped 'latest' read). Implicit: the projection partition
//     materialises with the corrected scope; without it, the partition
//     shape would be wrong and the run would not finalize cleanly.
func TestRebaseAllInheritedFinalizes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	defer cleanupTestData(t, ctx, clients, failureTestScheduleName)
	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	seedTopology(t, ctx, clients)

	// ── Drive the source run to FAILED ──────────────────────────────────────
	srcIDStr := startFailureScheduleRun(t, ctx, clients)
	srcID, err := uuid.Parse(srcIDStr)
	require.NoError(t, err)
	t.Logf("Source run created: run_id=%s", srcIDStr)

	t.Log("Waiting for ftable_e to exhaust retries...")
	verifyNodeExhaustedRetries(t, ctx, clients, srcID, "ftable_e")
	verifyNodeExhaustedRetries(t, ctx, clients, srcID, "ftable_g")

	t.Log("Verifying source scheduler reaches FAILED state...")
	verifySchedulerFailed(t, ctx, clients, srcID)

	// ── Trigger rebase against the failed source ────────────────────────────
	t.Log("Triggering rebase against failed source run...")
	rebaseResp, err := clients.runClient.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{RequestId: uuid.NewString(),
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
	t.Log("Waiting for rebase runs to reach terminal status...")
	pollUntil(t, ctx, 10*time.Minute, 5*time.Second, func() (bool, error) {
		var status string
		var completedAt *time.Time
		qErr := clients.stateDB.QueryRowContext(ctx,
			`SELECT status, completed_at FROM runs WHERE run_id = $1`,
			rebaseRunID,
		).Scan(&status, &completedAt)
		if qErr != nil {
			return false, qErr
		}
		// Terminal = succeeded | failed | cancelled, plus completed_at set.
		isTerminal := status == "failed" || status == "succeeded" || status == "cancelled"
		return isTerminal && completedAt != nil, nil
	}, "rebase runs did not reach terminal status — likely B1 regression "+
		"(terminal_task_count not seeded for inherited rows; rollup never closes)")

	// Read the final state for assertions.
	var finalStatus string
	var finalCompletedAt *time.Time
	require.NoError(t, clients.stateDB.QueryRowContext(ctx,
		`SELECT status, completed_at FROM runs WHERE run_id = $1`,
		rebaseRunID,
	).Scan(&finalStatus, &finalCompletedAt))

	t.Logf("Rebase run finalized: status=%s completed_at=%v", finalStatus, finalCompletedAt)
	assert.Contains(t, []string{"failed", "succeeded", "cancelled"}, finalStatus,
		"rebase run must be in a terminal status")
	assert.NotNil(t, finalCompletedAt, "completed_at must be non-NULL once the run is terminal")

	// ── Assert B1 invariant directly: terminal_task_count == total_task_count ─
	// The terminal-status poll above already implies this (the run could not
	// have finalized otherwise), but checking the column directly documents
	// the seeding invariant: inherited rows must contribute to
	// terminal_task_count at projection time, not at task-status time.
	var terminalCount, totalCount int32
	require.NoError(t, clients.stateDB.QueryRowContext(ctx,
		`SELECT terminal_task_count, total_task_count
		   FROM runs WHERE run_id = $1`,
		rebaseRunID,
	).Scan(&terminalCount, &totalCount))
	if totalCount != 8 {
		dumpRebaseRowsForDebug(t, ctx, clients, rebaseRunID)
	}
	assert.Equal(t, totalCount, terminalCount,
		"terminal_task_count must equal total_task_count post-finalization (B1 invariant)")
	assert.Equal(t, int32(8), totalCount,
		"expected partition: 4 inherited + 4 rebased = 8 total")

	t.Log("Rebase run finalized end-to-end")
}

// dumpRebaseRowsForDebug logs every tasks row attached to a rebase
// run so an unexpected row count on CI surfaces the offending row's identity
// (table_name + service + schema + image_tag). Called only
// on assertion mismatch — no-op when counts match.
func dumpRebaseRowsForDebug(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) {
	t.Helper()
	rows, err := clients.stateDB.QueryContext(ctx, `
		SELECT table_name, service_name, schema_name, status,
		       inherited_from_task_id IS NULL AS is_real,
		       COALESCE(image_tag, '') AS image_tag
		  FROM tasks
		 WHERE run_id = $1
		 ORDER BY table_name, service_name`, runID)
	if err != nil {
		t.Logf("dumpRebaseRowsForDebug: query failed: %v", err)
		return
	}
	defer rows.Close()

	var n int
	for rows.Next() {
		var tn, svc, sch, status, img string
		var isReal bool
		if scanErr := rows.Scan(&tn, &svc, &sch, &status, &isReal, &img); scanErr != nil {
			t.Logf("dumpRebaseRowsForDebug: scan failed: %v", scanErr)
			return
		}
		n++
		t.Logf("rebase row %d: table=%s svc=%s sch=%s status=%s real=%v img=%q",
			n, tn, svc, sch, status, isReal, img)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		t.Logf("dumpRebaseRowsForDebug: rows.Err: %v", rowsErr)
	}
	t.Logf("dumpRebaseRowsForDebug: total rows seen = %d", n)
}
