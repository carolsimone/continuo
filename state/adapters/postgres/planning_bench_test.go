package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/attemptreconcile"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// The benchmarks below measure the run-planning hot paths on synthetic
// releases stored in the test database the state migrations built:
//
//	go test -tags integration -run '^$' -bench 'StartRun|TaskSucceeded|TaskFailedRetry|FinalFailure|ReconcileDue' -benchtime 20x ./adapters/postgres/

// The synthetic release shapes the benchmarks run on, besides cascade2000.
var (
	dag2000  = releaseShape{name: "dag-2000", nodes: 2000, levels: 20, fanIn: 2}
	chain500 = releaseShape{name: "chain-500", nodes: 500, levels: 500, fanIn: 1}
	test2000 = releaseShape{name: "test-2000", nodes: 2000, levels: 1, fanIn: 1, tests: true}
)

// BenchmarkStartRun measures a schedule run's creating transaction: read the
// schedule's graph, select its tasks, store the run row, start it (one COPY of
// every task and one INSERT of the first attempt of every ready task), save
// it, write one outbox row per ready task and commit.
func BenchmarkStartRun(b *testing.B) {
	for _, shape := range []releaseShape{dag2000, chain500, test2000, cascade2000} {
		b.Run(shape.name, func(b *testing.B) {
			db := newTestDB(b)
			tenant := storeShape(b, db, shape)
			op := model.OperationRun
			if shape.tests {
				op = model.OperationTest
			}
			b.ResetTimer()
			for range b.N {
				r := startScheduleRun(b, db, tenant, shape.schedule(), op)
				b.StopTimer()
				deleteRun(b, db, r.RunID())
				b.StartTimer()
			}
		})
	}
}

// BenchmarkTaskSucceeded measures the whole task.status.updated:v2
// transaction for a task's success, as AttemptReportHandler runs it: lock the
// run, the task and the attempt, apply the report, save the run, write its
// dispatches, commit. Tasks succeed in key order, which is level order; once
// every task has succeeded the run is restored to how it was created, outside
// the timer.
func BenchmarkTaskSucceeded(b *testing.B) {
	for _, shape := range []releaseShape{dag2000, chain500} {
		b.Run(shape.name, func(b *testing.B) {
			benchmarkReports(b, shape, func(keys []string, i int) (string, run.AttemptStatus, model.FailureReason) {
				return keys[i%len(keys)], run.AttemptSucceeded, ""
			})
		})
	}
}

// BenchmarkTaskFailedRetry measures the same transaction for a retryable
// failure with budget left: it stores the failed attempt, adds the next one
// and writes its dispatch. The failures cycle through the tasks dag-2000
// dispatched at creation, its first level.
func BenchmarkTaskFailedRetry(b *testing.B) {
	roots := dag2000.nodes / dag2000.levels
	b.Run(dag2000.name, func(b *testing.B) {
		benchmarkReports(b, dag2000, func(keys []string, i int) (string, run.AttemptStatus, model.FailureReason) {
			return keys[i%roots], run.AttemptFailed, model.FailureReasonExitCode
		})
	})
}

// BenchmarkFinalFailure measures the same transaction for the cascade root
// failing in a way that is not retried: every other task of the run descends
// from it, so the transaction skips them all and finalizes the run.
func BenchmarkFinalFailure(b *testing.B) {
	b.Run(cascade2000.name, func(b *testing.B) {
		benchmarkReports(b, cascade2000, func([]string, int) (string, run.AttemptStatus, model.FailureReason) {
			return "bench.fail_root", run.AttemptFailed, model.FailureReasonDeadlineExceeded
		})
	})
}

// benchmarkReports starts a run of shape and times one report of attempt 0
// per iteration; next names the task, status and failure reason of iteration
// i. The run is restored to how it was created before an iteration that would
// repeat a task, outside the timer.
func benchmarkReports(b *testing.B, shape releaseShape, next func(keys []string, i int) (string, run.AttemptStatus, model.FailureReason)) {
	ctx := context.Background()
	db := newTestDB(b)
	tenant := storeShape(b, db, shape)
	r := startScheduleRun(b, db, tenant, shape.schedule(), model.OperationRun)
	var keys []string
	require.NoError(b, db.Select(&keys,
		`SELECT node_key FROM tasks WHERE run_id = $1 ORDER BY node_key COLLATE "C"`, r.RunID()))
	handler := handlers.NewAttemptReportHandler(testTaskDefaults, noRunMetrics{}, discardLogger())
	u := newPlanningUoW(db)
	reported := map[string]bool{}

	b.ResetTimer()
	for i := range b.N {
		key, status, reason := next(keys, i)
		if reported[key] {
			b.StopTimer()
			restoreRun(b, db, r.RunID())
			clear(reported)
			b.StartTimer()
		}
		reported[key] = true
		require.NoError(b, u.Begin(ctx))
		require.NoError(b, handler.Handle(ctx, u, events.AttemptReported{TenantID: tenant, RunID: r.RunID(), Report: run.AttemptReport{
			TaskID:        run.TaskIDFor(tenant, r.RunID(), topology.NodeKey(key)),
			Attempt:       0,
			Status:        status,
			FailureReason: reason,
		}}, uuid.Nil))
		require.NoError(b, u.Commit())
	}
}

// BenchmarkReconcileDue measures one pass of the attempt reconcile with 2,000
// attempts in flight and every one of them due: it asks about the oldest
// DueLimit, each in a transaction of its own that locks the attempt, writes
// its dispatch and moves its next check. Before each pass every attempt is
// made due again, outside the timer.
func BenchmarkReconcileDue(b *testing.B) {
	ctx := context.Background()
	db := newTestDB(b)
	tenant := storeShape(b, db, dag2000)
	r := startScheduleRun(b, db, tenant, dag2000.schedule(), model.OperationRun)
	_, err := db.Exec(`
		INSERT INTO task_attempts (tenant_id, task_id, attempt, job_name, status, dispatched_at, next_check_at)
		SELECT tenant_id, task_id, 0, job_name, 'dispatched', now(), now() FROM tasks WHERE run_id = $1
		ON CONFLICT (tenant_id, task_id, attempt) DO NOTHING`, r.RunID())
	require.NoError(b, err)
	_, err = db.Exec(`UPDATE tasks SET status = 'dispatched', dispatched_at = coalesce(dispatched_at, now())
		WHERE run_id = $1`, r.RunID())
	require.NoError(b, err)
	loop := attemptreconcile.New(scopedUoW(db, tenant), testTaskDefaults, noRunMetrics{}, nil, discardLogger())

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		_, err := db.Exec(`
			UPDATE task_attempts a SET next_check_at = now() - interval '1 second'
			FROM tasks t WHERE a.task_id = t.task_id AND t.run_id = $1`, r.RunID())
		require.NoError(b, err)
		_, err = db.Exec(`DELETE FROM state_outbox WHERE aggregate_id = $1`, r.RunID())
		require.NoError(b, err)
		b.StartTimer()
		require.NoError(b, loop.Pass(ctx))
		b.StopTimer()
		var asked int
		require.NoError(b, db.Get(&asked, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, r.RunID()))
		require.Equalf(b, attemptreconcile.DueLimit, asked,
			"with %d attempts due, a pass must ask about exactly DueLimit (%d) of them, one dispatch each; it asked about %d",
			dag2000.nodes, attemptreconcile.DueLimit, asked)
	}
}

// noRunMetrics counts nothing.
type noRunMetrics struct{}

func (noRunMetrics) RunStalled()                   {}
func (noRunMetrics) RunsCancelling(_, _ int)       {}
func (noRunMetrics) RunCancelled(time.Duration)    {}
func (noRunMetrics) LeaseWaiters(int)              {}
func (noRunMetrics) LeaseGranted(time.Duration)    {}
func (noRunMetrics) LeaseRepaired()                {}
func (noRunMetrics) CommandRequest(string, string) {}
func (noRunMetrics) CronFire(string)               {}
func (noRunMetrics) CronFires(string, int)         {}
func (noRunMetrics) Schedules(int, int, int)       {}

// restoreRun returns a started run to how Start stored it: the tasks it
// dispatched at creation (their dispatched_at equals their created_at)
// dispatched at attempt 0 with only their attempt 0, dispatched; every other
// task pending with no attempt; and the run running with no terminal task.
// The run's outbox rows are deleted; no relay publishes them here.
func restoreRun(tb testing.TB, db *sqlx.DB, runID uuid.UUID) {
	tb.Helper()
	_, err := db.Exec(`
		DELETE FROM task_attempts a USING tasks t
		WHERE a.task_id = t.task_id AND t.run_id = $1
		  AND (a.attempt > 0 OR t.dispatched_at IS DISTINCT FROM t.created_at)`, runID)
	require.NoError(tb, err)
	_, err = db.Exec(`
		UPDATE task_attempts a SET status = 'dispatched', finished_at = NULL, failure_class = NULL, failure_reason = NULL,
		       next_check_at = now() + interval '5 minutes', last_report_at = NULL
		FROM tasks t WHERE a.task_id = t.task_id AND t.run_id = $1`, runID)
	require.NoError(tb, err)
	_, err = db.Exec(`
		UPDATE tasks
		SET status = CASE WHEN dispatched_at = created_at THEN 'dispatched' ELSE 'pending' END, attempt = 0,
		    dispatched_at = CASE WHEN dispatched_at = created_at THEN dispatched_at END
		WHERE run_id = $1`, runID)
	require.NoError(tb, err)
	_, err = db.Exec(`
		UPDATE runs SET status = 'running', terminal_task_count = 0, completed_at = NULL
		WHERE run_id = $1`, runID)
	require.NoError(tb, err)
	_, err = db.Exec(`DELETE FROM state_outbox WHERE aggregate_id = $1`, runID)
	require.NoError(tb, err)
}

// deleteRun deletes a run with its tasks and outbox rows.
func deleteRun(tb testing.TB, db *sqlx.DB, runID uuid.UUID) {
	tb.Helper()
	_, err := db.Exec(`DELETE FROM state_outbox WHERE aggregate_id = $1`, runID)
	require.NoError(tb, err)
	_, err = db.Exec(`DELETE FROM runs WHERE run_id = $1`, runID)
	require.NoError(tb, err)
}
