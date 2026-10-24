//go:build integration

package postgres_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// attemptTx opens a transaction on db, hands fn the task collection bound to
// it, and rolls the transaction back: an attempt test leaves nothing behind
// but the task rows it created itself.
func attemptTx(t *testing.T, db *sqlx.DB, fn func(ctx context.Context, tx *sqlx.Tx, tasks run.TaskCollection)) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	fn(ctx, tx, postgres.NewTaskCollectionAdapter(postgres.NewTaskTrackerRepository(db, discardLogger()), tx))
}

// newRunWithTasks inserts one run and n tasks of it, and deletes the run (and
// through it the tasks and their attempts) when the test ends.
func newRunWithTasks(t *testing.T, db *sqlx.DB, n int) (uuid.UUID, []uuid.UUID) {
	t.Helper()
	scheduleID := uuid.New()
	_, err := db.Exec(`
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status)
		VALUES ($1, $2, 'running')`, scheduleID, "attempts-"+scheduleID.String())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
	})
	ids := make([]uuid.UUID, 0, n)
	for i := range n {
		id := uuid.New()
		_, err = db.Exec(`
			INSERT INTO task_tracker (task_id, schedule_id, service_name, schema_name, table_name, job_name, status, max_retries)
			VALUES ($1, $2, 'svc', 'sch', $3, 'job-0', 'running', 2)`, id, scheduleID, "tbl"+string(rune('a'+i)))
		require.NoError(t, err)
		ids = append(ids, id)
	}
	return scheduleID, ids
}

func ptr[T any](v T) *T { return &v }

// attemptStatuses maps "<task_id>/<attempt>" to the stored status of every
// attempt of the given tasks.
func attemptStatuses(t *testing.T, tx *sqlx.Tx, taskIDs ...uuid.UUID) map[string]string {
	t.Helper()
	var rows []struct {
		Key    string `db:"key"`
		Status string `db:"status"`
	}
	require.NoError(t, tx.Select(&rows,
		`SELECT task_id::text || '/' || attempt AS key, status FROM task_attempts WHERE task_id = ANY($1)`, pq.Array(taskIDs)))
	out := map[string]string{}
	for _, r := range rows {
		out[r.Key] = r.Status
	}
	return out
}

// Every field of an attempt survives a save and a load.
func TestTaskAttempts_SaveThenLoadRoundTripsEveryField(t *testing.T) {
	db := newTestDB(t)
	_, ids := newRunWithTasks(t, db, 1)
	at := func(s int) *time.Time { return ptr(time.Date(2026, 10, 21, 9, 0, s, 123456000, time.UTC)) }
	want := run.Attempt{
		TaskID: ids[0], Number: 1, JobName: "job-orders-r1", Status: run.AttemptFailed,
		FailureClass: ptr("transient"), FailureReason: ptr("oom_killed"),
		DispatchedAt: at(1), QueuedAt: at(2), StartedAt: at(3), FinishedAt: at(4),
		ExecutionSeconds: ptr(12.345), ExitCode: ptr(int32(137)),
		ErrorMessage: ptr("container killed"), LogS3Key: ptr("logs/job-orders-r1.log"),
		RunResultsURI: ptr("results/job-orders-r1.json"), ParseCache: ptr("hit"), ParseCacheReason: ptr("same hash"),
	}

	attemptTx(t, db, func(ctx context.Context, _ *sqlx.Tx, tasks run.TaskCollection) {
		require.NoError(t, tasks.SaveAttempt(ctx, want))

		got, found, err := tasks.LoadAttempt(ctx, ids[0], 1)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, want.TaskID, got.TaskID)
		assert.Equal(t, want.Number, got.Number)
		assert.Equal(t, want.JobName, got.JobName)
		assert.Equal(t, want.Status, got.Status)
		assert.Equal(t, want.FailureClass, got.FailureClass)
		assert.Equal(t, want.FailureReason, got.FailureReason)
		for name, pair := range map[string][2]*time.Time{
			"dispatched_at": {want.DispatchedAt, got.DispatchedAt}, "queued_at": {want.QueuedAt, got.QueuedAt},
			"started_at": {want.StartedAt, got.StartedAt}, "finished_at": {want.FinishedAt, got.FinishedAt},
		} {
			require.NotNil(t, pair[1], name)
			assert.True(t, pair[0].Equal(*pair[1]), "%s: want %v, got %v", name, pair[0], pair[1])
		}
		assert.Equal(t, want.ExecutionSeconds, got.ExecutionSeconds)
		assert.Equal(t, want.ExitCode, got.ExitCode)
		assert.Equal(t, want.ErrorMessage, got.ErrorMessage)
		assert.Equal(t, want.LogS3Key, got.LogS3Key)
		assert.Equal(t, want.RunResultsURI, got.RunResultsURI)
		assert.Equal(t, want.ParseCache, got.ParseCache)
		assert.Equal(t, want.ParseCacheReason, got.ParseCacheReason)
	})
}

// A second save of the same (task, attempt) replaces the facts, keeps the
// first dispatch time and never moves last_report_at backwards.
func TestTaskAttempts_SaveReplacesTheRowAndKeepsItsDispatchTime(t *testing.T) {
	db := newTestDB(t)
	_, ids := newRunWithTasks(t, db, 1)
	first := time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)
	later := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)

	attemptTx(t, db, func(ctx context.Context, tx *sqlx.Tx, tasks run.TaskCollection) {
		require.NoError(t, tasks.SaveAttempt(ctx, run.Attempt{
			TaskID: ids[0], Number: 0, JobName: "job-a", Status: run.AttemptRunning,
			DispatchedAt: &first, ErrorMessage: ptr("first"), LogS3Key: ptr("logs/first"),
		}))
		var reported time.Time
		require.NoError(t, tx.Get(&reported, `SELECT last_report_at FROM task_attempts WHERE task_id = $1`, ids[0]))
		var dbNow time.Time
		require.NoError(t, tx.Get(&dbNow, `SELECT now()`))
		assert.True(t, dbNow.Equal(reported), "last_report_at is the database clock")

		future := dbNow.Add(time.Hour)
		_, err := tx.Exec(`UPDATE task_attempts SET last_report_at = $2 WHERE task_id = $1`, ids[0], future)
		require.NoError(t, err)

		require.NoError(t, tasks.SaveAttempt(ctx, run.Attempt{
			TaskID: ids[0], Number: 0, JobName: "job-b", Status: run.AttemptFailed,
			DispatchedAt: &later, ErrorMessage: ptr("second"), ExitCode: ptr(int32(1)),
		}))

		got, found, err := tasks.LoadAttempt(ctx, ids[0], 0)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "job-b", got.JobName)
		assert.Equal(t, run.AttemptFailed, got.Status)
		assert.Equal(t, ptr("second"), got.ErrorMessage)
		assert.Equal(t, ptr(int32(1)), got.ExitCode)
		assert.Nil(t, got.LogS3Key, "a replace carries exactly the facts of the second save")
		require.NotNil(t, got.DispatchedAt)
		assert.True(t, first.Equal(*got.DispatchedAt), "dispatched_at stays at the first save: %v", got.DispatchedAt)

		require.NoError(t, tx.Get(&reported, `SELECT last_report_at FROM task_attempts WHERE task_id = $1`, ids[0]))
		assert.True(t, future.Equal(reported), "last_report_at never moves backwards: %v", reported)
	})
}

// A save that reaches a final status clears the next check; a save that does
// not leaves it alone.
func TestTaskAttempts_SaveClearsTheNextCheckOnlyOnceFinal(t *testing.T) {
	db := newTestDB(t)
	_, ids := newRunWithTasks(t, db, 1)

	attemptTx(t, db, func(ctx context.Context, tx *sqlx.Tx, tasks run.TaskCollection) {
		require.NoError(t, tasks.SaveAttempt(ctx, run.Attempt{TaskID: ids[0], JobName: "job-a", Status: run.AttemptDispatched}))
		_, err := tx.Exec(`UPDATE task_attempts SET next_check_at = now() + interval '1 minute' WHERE task_id = $1`, ids[0])
		require.NoError(t, err)

		require.NoError(t, tasks.SaveAttempt(ctx, run.Attempt{TaskID: ids[0], JobName: "job-a", Status: run.AttemptRunning}))
		var due *time.Time
		require.NoError(t, tx.Get(&due, `SELECT next_check_at FROM task_attempts WHERE task_id = $1`, ids[0]))
		assert.NotNil(t, due, "a running attempt keeps its next check")

		require.NoError(t, tasks.SaveAttempt(ctx, run.Attempt{TaskID: ids[0], JobName: "job-a", Status: run.AttemptSucceeded}))
		require.NoError(t, tx.Get(&due, `SELECT next_check_at FROM task_attempts WHERE task_id = $1`, ids[0]))
		assert.Nil(t, due, "a final attempt has no next check")
	})
}

// An attempt saved without a dispatch time is stamped by the database clock.
func TestTaskAttempts_DispatchedAtDefaultsToTheDatabaseClock(t *testing.T) {
	db := newTestDB(t)
	_, ids := newRunWithTasks(t, db, 1)

	attemptTx(t, db, func(ctx context.Context, tx *sqlx.Tx, tasks run.TaskCollection) {
		require.NoError(t, tasks.SaveAttempt(ctx, run.Attempt{TaskID: ids[0], Number: 0, JobName: "job-a", Status: run.AttemptDispatched}))

		got, found, err := tasks.LoadAttempt(ctx, ids[0], 0)
		require.NoError(t, err)
		require.True(t, found)
		var dbNow time.Time
		require.NoError(t, tx.Get(&dbNow, `SELECT now()`))
		require.NotNil(t, got.DispatchedAt)
		assert.True(t, dbNow.Equal(*got.DispatchedAt), "dispatched_at %v is not the database clock %v", got.DispatchedAt, dbNow)
	})
}

// An attempt that was never saved reads as absent, without an error.
func TestTaskAttempts_LoadOfAnAbsentAttemptReportsNone(t *testing.T) {
	db := newTestDB(t)
	_, ids := newRunWithTasks(t, db, 1)

	attemptTx(t, db, func(ctx context.Context, _ *sqlx.Tx, tasks run.TaskCollection) {
		got, found, err := tasks.LoadAttempt(ctx, ids[0], 3)
		require.NoError(t, err)
		assert.False(t, found)
		assert.Equal(t, run.Attempt{}, got)
	})
}

// A cancel asks the current attempt of each stopping task of its run to stop,
// when that attempt is open: it is stamped stop_requested_at, its re-send
// count restarts and its next check is five minutes after the database clock.
// The attempt keeps the status it last reported. An ended attempt, the attempt
// of a task that is not stopping and an attempt of another run are left as
// they are.
func TestTaskAttempts_RequestStopsAsksTheCurrentOpenAttemptOfEachStoppingTask(t *testing.T) {
	conn := newTestDB(t)
	runID, ids := newRunWithTasks(t, conn, 4)
	_, others := newRunWithTasks(t, conn, 1)
	_, err := conn.Exec(`UPDATE task_tracker SET status = 'stopping' WHERE task_id = ANY($1)`,
		pq.Array([]uuid.UUID{ids[0], ids[1], ids[3], others[0]}))
	require.NoError(t, err)
	_, err = conn.Exec(`UPDATE task_tracker SET attempt = 1 WHERE task_id = $1`, ids[0])
	require.NoError(t, err)

	attemptTx(t, conn, func(ctx context.Context, tx *sqlx.Tx, tasks run.TaskCollection) {
		for _, a := range []run.Attempt{
			{TaskID: ids[0], Number: 0, JobName: "j0", Status: run.AttemptFailed},
			{TaskID: ids[0], Number: 1, JobName: "j0-r1", Status: run.AttemptRunning},
			{TaskID: ids[1], Number: 0, JobName: "j1", Status: run.AttemptDispatched},
			{TaskID: ids[2], Number: 0, JobName: "j2", Status: run.AttemptRunning},
			{TaskID: ids[3], Number: 0, JobName: "j3", Status: run.AttemptSucceeded},
			{TaskID: others[0], Number: 0, JobName: "j4", Status: run.AttemptRunning},
		} {
			require.NoError(t, tasks.SaveAttempt(ctx, a))
		}
		all := []uuid.UUID{ids[0], ids[1], ids[2], ids[3], others[0]}
		_, err := tx.Exec(`UPDATE task_attempts SET next_check_at = now() - interval '1 hour', checks_sent = 3
			WHERE task_id = ANY($1) AND status NOT IN ('succeeded', 'failed', 'stopped')`, pq.Array(all))
		require.NoError(t, err)

		targets, err := tasks.RequestStops(ctx, runID)

		require.NoError(t, err)
		want := []run.StopTarget{
			{TaskID: ids[0], Attempt: 1, JobName: "j0-r1"},
			{TaskID: ids[1], Attempt: 0, JobName: "j1"},
		}
		slices.SortFunc(want, func(x, y run.StopTarget) int { return strings.Compare(x.TaskID.String(), y.TaskID.String()) })
		assert.Equal(t, want, targets)

		var rows []struct {
			Key       string   `db:"key"`
			Status    string   `db:"status"`
			Requested bool     `db:"requested"`
			DueIn     *float64 `db:"due_in"`
			Checks    int      `db:"checks_sent"`
		}
		require.NoError(t, tx.Select(&rows, `
			SELECT task_id::text || '/' || attempt AS key, status, stop_requested_at IS NOT NULL AS requested,
			       EXTRACT(EPOCH FROM next_check_at - now())::float8 AS due_in, checks_sent
			FROM task_attempts WHERE task_id = ANY($1)`, pq.Array(all)))
		require.Len(t, rows, 6)
		requested := map[string]bool{ids[0].String() + "/1": true, ids[1].String() + "/0": true}
		for _, r := range rows {
			if requested[r.Key] {
				assert.True(t, r.Requested, "%s is asked to stop", r.Key)
				require.NotNil(t, r.DueIn, r.Key)
				assert.InDelta(t, (5 * time.Minute).Seconds(), *r.DueIn, 1, "%s is next asked about five minutes after the database clock", r.Key)
				assert.Zero(t, r.Checks, "%s counts its re-sends from the stop", r.Key)
				continue
			}
			assert.False(t, r.Requested, "%s is not asked to stop", r.Key)
			if r.DueIn != nil {
				assert.Less(t, *r.DueIn, 0.0, "%s keeps its next check", r.Key)
				assert.Equal(t, 3, r.Checks, "%s keeps its question count", r.Key)
			}
		}
		statuses := attemptStatuses(t, tx, ids[0], ids[1])
		assert.Equal(t, "running", statuses[ids[0].String()+"/1"], "a stop is not a report")
		assert.Equal(t, "dispatched", statuses[ids[1].String()+"/0"])
	})
}

// The state an attempt report locks names the task: its job, node and first
// dispatch.
func TestTaskAttempts_LoadAttemptStateCarriesTheTasksIdentity(t *testing.T) {
	db := newTestDB(t)
	_, ids := newRunWithTasks(t, db, 1)
	dispatched := time.Date(2026, 10, 21, 7, 30, 0, 0, time.UTC)
	_, err := db.Exec(`UPDATE task_tracker SET dispatched_at = $2, attempt = 1 WHERE task_id = $1`, ids[0], dispatched)
	require.NoError(t, err)

	attemptTx(t, db, func(ctx context.Context, _ *sqlx.Tx, tasks run.TaskCollection) {
		got, found, err := tasks.LoadAttemptState(ctx, ids[0])

		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "job-0", got.JobName)
		assert.Equal(t, "svc", got.ServiceName)
		assert.Equal(t, "sch", got.SchemaName)
		assert.Equal(t, "tbla", got.TableName)
		require.NotNil(t, got.DispatchedAt)
		assert.True(t, dispatched.Equal(*got.DispatchedAt))
		assert.Equal(t, int32(1), got.Attempt)
		assert.Equal(t, int32(2), got.MaxRetries)
	})
}
