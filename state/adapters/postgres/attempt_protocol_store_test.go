//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dueIn reads how far after the database clock an attempt's next check is;
// nil when it has none.
func dueIn(t *testing.T, tx *sqlx.Tx, taskID any, attempt int) *time.Duration {
	t.Helper()
	var secs *float64
	require.NoError(t, tx.Get(&secs, `
		SELECT EXTRACT(EPOCH FROM next_check_at - now())::float8 FROM task_attempts WHERE task_id = $1 AND attempt = $2`,
		taskID, attempt))
	if secs == nil {
		return nil
	}
	d := time.Duration(*secs * float64(time.Second))
	return &d
}

// The attempts state dispatches are written in one statement, dispatched,
// each due for its first check on the database clock.
func TestTaskAttempts_AddAttemptsWritesDispatchedAttemptsDueOnTheDatabaseClock(t *testing.T) {
	db := newTestDB(t)
	_, ids := newRunWithTasks(t, db, 2)
	stamped := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)

	attemptTx(t, db, func(ctx context.Context, tx *sqlx.Tx, tasks run.TaskCollection) {
		require.NoError(t, tasks.AddAttempts(ctx, []run.Attempt{
			{TaskID: ids[0], Number: 0, JobName: "job-a", Status: run.AttemptDispatched, DispatchedAt: &stamped, NextCheckIn: 5 * time.Minute},
			{TaskID: ids[1], Number: 1, JobName: "job-b-r1", Status: run.AttemptDispatched, NextCheckIn: 5 * time.Minute},
		}))

		assert.Equal(t, map[string]string{ids[0].String() + "/0": "dispatched", ids[1].String() + "/1": "dispatched"},
			attemptStatuses(t, tx, ids...))
		for i, id := range ids {
			due := dueIn(t, tx, id, i)
			require.NotNil(t, due)
			assert.InDelta(t, (5 * time.Minute).Seconds(), due.Seconds(), 1, "attempt %d is due 5 minutes after the database clock", i)
		}
		a, _, err := tasks.LoadAttempt(ctx, ids[0], 0)
		require.NoError(t, err)
		assert.True(t, stamped.Equal(*a.DispatchedAt), "a given dispatch time is kept")
		var dbNow time.Time
		require.NoError(t, tx.Get(&dbNow, `SELECT now()`))
		b, _, err := tasks.LoadAttempt(ctx, ids[1], 1)
		require.NoError(t, err)
		assert.True(t, dbNow.Equal(*b.DispatchedAt), "an attempt without one takes the database clock")
	})
}

// A save moves the next check to the attempt's own window on the database
// clock, keeps it when the attempt names none, and clears it once final.
func TestTaskAttempts_SaveSetsTheNextCheckOnTheDatabaseClock(t *testing.T) {
	db := newTestDB(t)
	_, ids := newRunWithTasks(t, db, 1)

	attemptTx(t, db, func(ctx context.Context, tx *sqlx.Tx, tasks run.TaskCollection) {
		a := run.Attempt{TaskID: ids[0], JobName: "job-a", Status: run.AttemptDispatched, NextCheckIn: 5 * time.Minute}
		require.NoError(t, tasks.AddAttempts(ctx, []run.Attempt{a}))

		a.Status, a.NextCheckIn = run.AttemptRunning, 30*time.Minute
		require.NoError(t, tasks.SaveAttempt(ctx, a))
		require.NotNil(t, dueIn(t, tx, ids[0], 0))
		assert.InDelta(t, (30 * time.Minute).Seconds(), dueIn(t, tx, ids[0], 0).Seconds(), 1)

		a.NextCheckIn = 0
		require.NoError(t, tasks.SaveAttempt(ctx, a))
		assert.InDelta(t, (30 * time.Minute).Seconds(), dueIn(t, tx, ids[0], 0).Seconds(), 1, "no window keeps the stored next check")

		a.Status, a.NextCheckIn = run.AttemptFailed, 0
		require.NoError(t, tasks.SaveAttempt(ctx, a))
		assert.Nil(t, dueIn(t, tx, ids[0], 0), "a final attempt has no next check")
	})
}

// An attempt that is final has no next check, whichever way it is saved: over
// an open one that had a check, as a new row, and with or without a window.
func TestTaskAttempts_AFinalAttemptHasNoNextCheck(t *testing.T) {
	db := newTestDB(t)
	_, ids := newRunWithTasks(t, db, 3)

	attemptTx(t, db, func(ctx context.Context, tx *sqlx.Tx, tasks run.TaskCollection) {
		open := run.Attempt{TaskID: ids[0], JobName: "job-a", Status: run.AttemptRunning, NextCheckIn: 5 * time.Minute}
		require.NoError(t, tasks.SaveAttempt(ctx, open))
		require.NotNil(t, dueIn(t, tx, ids[0], 0), "the open attempt is due")
		open.Status, open.NextCheckIn = run.AttemptSucceeded, 0
		require.NoError(t, tasks.SaveAttempt(ctx, open))
		assert.Nil(t, dueIn(t, tx, ids[0], 0), "ending an open attempt without a window clears its check")

		open = run.Attempt{TaskID: ids[1], JobName: "job-b", Status: run.AttemptRunning, NextCheckIn: 5 * time.Minute}
		require.NoError(t, tasks.SaveAttempt(ctx, open))
		open.Status, open.NextCheckIn = run.AttemptFailed, 10*time.Minute
		require.NoError(t, tasks.SaveAttempt(ctx, open))
		assert.Nil(t, dueIn(t, tx, ids[1], 0), "a window on an ending attempt is ignored")

		for _, status := range []run.AttemptStatus{run.AttemptSucceeded, run.AttemptFailed, run.AttemptStopped} {
			for n, in := range []time.Duration{0, 10 * time.Minute} {
				fresh := run.Attempt{TaskID: ids[2], Number: int32(n), JobName: "job-c", Status: status, NextCheckIn: in}
				require.NoError(t, tasks.SaveAttempt(ctx, fresh))
				assert.Nil(t, dueIn(t, tx, ids[2], n), "a new %s attempt saved with a window of %s has no check", status, in)
				_, err := tx.Exec(`DELETE FROM task_attempts WHERE task_id = $1 AND attempt = $2`, ids[2], n)
				require.NoError(t, err)
			}
		}
	})
}

// A stalled run's pending tasks are skipped; its other tasks keep their status.
func TestTaskCollection_SkipPendingSkipsOnlyTheRunsPendingTasks(t *testing.T) {
	db := newTestDB(t)
	storeFailureSchedule(t, db)
	runID, _ := insertTasks(t, db, map[string]run.TaskStatus{
		"ftable_a": run.TaskStatusSucceeded, "ftable_c": run.TaskStatusPending, "ftable_d": run.TaskStatusPending,
	})
	otherRun, _ := insertTasks(t, db, map[string]run.TaskStatus{"ftable_c": run.TaskStatusPending})

	attemptTx(t, db, func(ctx context.Context, tx *sqlx.Tx, tasks run.TaskCollection) {
		n, err := tasks.SkipPending(ctx, runID)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		var skipped, untouched int
		require.NoError(t, tx.Get(&skipped, `SELECT count(*) FROM task_tracker WHERE schedule_id = $1 AND status = 'skipped'`, runID))
		require.NoError(t, tx.Get(&untouched, `SELECT count(*) FROM task_tracker WHERE schedule_id = $1 AND status = 'pending'`, otherRun))
		assert.Equal(t, 2, skipped)
		assert.Equal(t, 1, untouched)
	})
}

// A run has an attempt in flight while a task of it is dispatched, queued,
// running or stopping.
func TestTaskCollection_HasInFlight(t *testing.T) {
	db := newTestDB(t)
	storeFailureSchedule(t, db)
	for status, want := range map[run.TaskStatus]bool{
		run.TaskStatusDispatched: true, run.TaskStatusQueued: true, run.TaskStatusRunning: true,
		run.TaskStatusStopping: true,
		run.TaskStatusPending:  false, run.TaskStatusSucceeded: false, run.TaskStatusFailed: false,
		run.TaskStatusCancelled: false, run.TaskStatusSkipped: false,
	} {
		runID, _ := insertTasks(t, db, map[string]run.TaskStatus{"ftable_a": run.TaskStatusSucceeded, "ftable_c": status})
		attemptTx(t, db, func(ctx context.Context, tx *sqlx.Tx, tasks run.TaskCollection) {
			got, err := tasks.HasInFlight(ctx, runID)
			require.NoError(t, err)
			assert.Equal(t, want, got, "a task %s", status)
		})
	}
}

// A cancel stops a run's tasks that wait for or run an attempt: a pending task
// is cancelled, a dispatched, queued or running one stopping; an ended task
// and a task of another run are left as they are.
func TestTaskCollection_BeginStopCancelsPendingTasksAndStopsTasksInFlight(t *testing.T) {
	db := newTestDB(t)
	storeFailureSchedule(t, db)
	runID, ids := insertTasks(t, db, map[string]run.TaskStatus{
		"ftable_a": run.TaskStatusDispatched, "ftable_b": run.TaskStatusQueued, "ftable_c": run.TaskStatusPending,
		"ftable_d": run.TaskStatusSucceeded, "ftable_e": run.TaskStatusRunning,
	})
	_, others := insertTasks(t, db, map[string]run.TaskStatus{"ftable_a": run.TaskStatusRunning})

	attemptTx(t, db, func(ctx context.Context, tx *sqlx.Tx, tasks run.TaskCollection) {
		counts, err := tasks.BeginStop(ctx, runID, "user", time.Now())
		require.NoError(t, err)
		assert.Equal(t, run.StopCounts{Cancelled: 1, Stopping: 3}, counts)
		for table, want := range map[string]run.TaskStatus{
			"ftable_a": run.TaskStatusStopping, "ftable_b": run.TaskStatusStopping, "ftable_c": run.TaskStatusCancelled,
			"ftable_d": run.TaskStatusSucceeded, "ftable_e": run.TaskStatusStopping,
		} {
			var got run.TaskStatus
			require.NoError(t, tx.Get(&got, `SELECT status FROM task_tracker WHERE task_id = $1`, ids[table]))
			assert.Equal(t, want, got, table)
		}
		var other run.TaskStatus
		require.NoError(t, tx.Get(&other, `SELECT status FROM task_tracker WHERE task_id = $1`, others["ftable_a"]))
		assert.Equal(t, run.TaskStatusRunning, other, "another run's task")

		counts, err = tasks.BeginStop(ctx, runID, "user", time.Now())
		require.NoError(t, err)
		assert.Zero(t, counts, "a second stop finds nothing left to stop")
	})
}

// A retry is dispatched from the node its run's release holds.
func TestTaskGraph_NodeReadsOneNodeOfTheRelease(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)

	inGraphTx(t, db, tenant, func(g *postgres.TaskGraph) {
		n, found, err := g.Node(ctx, "rel-1", "e2e_schema.ftable_e")
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, topology.Node{
			Key: "e2e_schema.ftable_e", UniqueID: "e2e_schema.ftable_e", ServiceName: "service-1", SchemaName: "e2e_schema",
			TableName: "ftable_e", NodeType: "dbt-model", ContentHash: "h-ftable_e", ImageTag: "service-1:5",
			ScheduleName: "e2e-schedule-failure",
		}, n)
		_, found, err = g.Node(ctx, "rel-1", "e2e_schema.nope")
		require.NoError(t, err)
		assert.False(t, found)
	})
}

// A task a success makes ready is dispatched with its node's declared timeout.
func TestTaskGraph_ReadyCandidatesCarryTheNodesTimeout(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	_, err := db.Exec(`UPDATE topology_nodes SET timeout_seconds = 7200 WHERE tenant_id = $1 AND node_key = 'e2e_schema.ftable_g'`, tenant)
	require.NoError(t, err)
	r := startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)
	setTaskStatus(t, db, r, "e2e_schema.ftable_a", run.TaskStatusSucceeded)

	inGraphTx(t, db, tenant, func(g *postgres.TaskGraph) {
		got, err := g.ReadyCandidates(ctx, r.ScheduleID(), "rel-1", "e2e_schema.ftable_a")
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, topology.NodeKey("e2e_schema.ftable_g"), got[1].Node.Key)
		require.NotNil(t, got[1].Node.TimeoutSeconds)
		assert.Equal(t, 7200, *got[1].Node.TimeoutSeconds)
		assert.Nil(t, got[0].Node.TimeoutSeconds, "a node that declares none carries none")
	})
}
