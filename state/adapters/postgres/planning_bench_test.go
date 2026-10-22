package postgres_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// The benchmarks below measure the run-planning hot paths on synthetic
// releases stored in the test database the state migrations built:
//
//	go test -tags integration -run '^$' -bench 'StartRun|TaskSucceeded|FinalFailure' -benchtime 20x ./adapters/postgres/

// The synthetic release shapes the benchmarks run on, besides cascade2000.
var (
	dag2000  = releaseShape{name: "dag-2000", nodes: 2000, levels: 20, fanIn: 2}
	chain500 = releaseShape{name: "chain-500", nodes: 500, levels: 500, fanIn: 1}
	test2000 = releaseShape{name: "test-2000", nodes: 2000, levels: 1, fanIn: 1, tests: true}
)

// BenchmarkStartRun measures a schedule run's creating transaction: read the
// schedule's graph, select its tasks, store the run row, start it (one COPY of
// every task), save it, write one outbox row per ready task and commit.
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
				deleteRun(b, db, r.ScheduleID())
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
		`SELECT node_key FROM task_tracker WHERE schedule_id = $1 ORDER BY node_key COLLATE "C"`, r.ScheduleID()))
	handler := handlers.NewAttemptReportHandler(tenant, testTaskDefaults, discardLogger())
	u := newPlanningUoW(db)
	reported := map[string]bool{}

	b.ResetTimer()
	for i := range b.N {
		key, status, reason := next(keys, i)
		if reported[key] {
			b.StopTimer()
			restoreRun(b, db, r.ScheduleID())
			clear(reported)
			b.StartTimer()
		}
		reported[key] = true
		require.NoError(b, u.Begin(ctx))
		require.NoError(b, handler.Handle(ctx, u, events.AttemptReported{RunID: r.ScheduleID(), Report: run.AttemptReport{
			TaskID:        run.TaskIDFor(tenant, r.ScheduleID(), topology.NodeKey(key)),
			Attempt:       0,
			Status:        status,
			FailureReason: reason,
		}}, uuid.Nil))
		require.NoError(b, u.Commit())
	}
}

// restoreRun returns a started run to how Start stored it: the tasks it
// dispatched at creation (their dispatched_at equals their created_at)
// dispatched at attempt 0 with only their attempt 0, dispatched; every other
// task pending with no attempt; and the run running with no terminal task.
// The run's outbox rows are deleted; no relay publishes them here.
func restoreRun(tb testing.TB, db *sqlx.DB, runID uuid.UUID) {
	tb.Helper()
	_, err := db.Exec(`
		DELETE FROM task_attempts a USING task_tracker t
		WHERE a.task_id = t.task_id AND t.schedule_id = $1
		  AND (a.attempt > 0 OR t.dispatched_at IS DISTINCT FROM t.created_at)`, runID)
	require.NoError(tb, err)
	_, err = db.Exec(`
		UPDATE task_attempts a SET status = 'dispatched', finished_at = NULL, failure_class = NULL, failure_reason = NULL,
		       next_check_at = now() + interval '5 minutes', last_report_at = NULL
		FROM task_tracker t WHERE a.task_id = t.task_id AND t.schedule_id = $1`, runID)
	require.NoError(tb, err)
	_, err = db.Exec(`
		UPDATE task_tracker
		SET status = CASE WHEN dispatched_at = created_at THEN 'dispatched' ELSE 'pending' END, attempt = 0,
		    dispatched_at = CASE WHEN dispatched_at = created_at THEN dispatched_at END
		WHERE schedule_id = $1`, runID)
	require.NoError(tb, err)
	_, err = db.Exec(`
		UPDATE scheduler_tracker SET status = 'running', terminal_task_count = 0, completed_at = NULL
		WHERE schedule_id = $1`, runID)
	require.NoError(tb, err)
	_, err = db.Exec(`DELETE FROM state_outbox WHERE aggregate_id = $1`, runID)
	require.NoError(tb, err)
}

// deleteRun deletes a run with its tasks and outbox rows.
func deleteRun(tb testing.TB, db *sqlx.DB, runID uuid.UUID) {
	tb.Helper()
	_, err := db.Exec(`DELETE FROM state_outbox WHERE aggregate_id = $1`, runID)
	require.NoError(tb, err)
	_, err = db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, runID)
	require.NoError(tb, err)
}
