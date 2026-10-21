//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordFlowKey is the task of e2e-schedule-failure these tests record.
const recordFlowKey = "e2e_schema.ftable_a"

// recordedFacts is one task_attempts row with every fact a record fills.
type recordedFacts struct {
	Attempt          int32      `db:"attempt"`
	JobName          string     `db:"job_name"`
	Status           string     `db:"status"`
	StartedAt        *time.Time `db:"started_at"`
	FinishedAt       *time.Time `db:"finished_at"`
	ExecutionSeconds *float64   `db:"execution_seconds"`
	ErrorMessage     *string    `db:"error_message"`
	LogS3Key         *string    `db:"log_s3_key"`
	RunResultsURI    *string    `db:"run_results_uri"`
	ParseCache       *string    `db:"parse_cache"`
	ParseCacheReason *string    `db:"parse_cache_reason"`
}

// recordedAttempts returns every attempt row of the task at key, in attempt
// order.
func (f statusFlow) recordedAttempts(t *testing.T, key string) []recordedFacts {
	t.Helper()
	var rows []recordedFacts
	require.NoError(t, f.db.Select(&rows, `
		SELECT attempt, job_name, status, started_at, finished_at, execution_seconds,
		       error_message, log_s3_key, run_results_uri, parse_cache, parse_cache_reason
		FROM task_attempts WHERE task_id = $1 ORDER BY attempt`, f.taskID(key)))
	return rows
}

func (f statusFlow) taskID(key string) uuid.UUID {
	return run.TaskIDFor(f.tenant, f.r.ScheduleID(), topology.NodeKey(key))
}

// executionRecordOf returns a record of the Job jobName of the task at key,
// carrying every fact; logKey tells records apart.
func (f statusFlow) executionRecordOf(key, jobName, logKey string) events.TaskExecutionRecorded {
	started := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	completed := started.Add(42 * time.Second)
	return events.TaskExecutionRecorded{
		ExecutionID: uuid.New(), TaskID: f.taskID(key), JobName: jobName,
		StartedAt: &started, CompletedAt: &completed, ExecutionTimeSeconds: ptr(42.0),
		ErrorMessage: ptr("model failed"), LogS3Key: ptr(logKey), RunResultsURI: ptr("run-results/" + jobName + ".json"),
		ParseCache: ptr("miss"), ParseCacheReason: ptr("manifest changed"),
	}
}

// record delivers evt through TaskExecutionRecordedHandler in a real unit of
// work, one transaction.
func (f statusFlow) record(t *testing.T, evt events.TaskExecutionRecorded) {
	t.Helper()
	ctx := context.Background()
	u := newPlanningUoW(f.db)
	require.NoError(t, u.Begin(ctx))
	defer u.Rollback()
	require.NoError(t, handlers.NewTaskExecutionRecordedHandler(discardLogger()).Handle(ctx, u, evt, uuid.Nil))
	require.NoError(t, u.Commit())
}

// assertRecorded checks row is attempt 1 of job, failed, with every fact of evt.
func assertRecorded(t *testing.T, evt events.TaskExecutionRecorded, row recordedFacts) {
	t.Helper()
	assert.Equal(t, int32(1), row.Attempt)
	assert.Equal(t, evt.JobName, row.JobName)
	assert.Equal(t, "failed", row.Status)
	require.NotNil(t, row.StartedAt)
	assert.True(t, row.StartedAt.Equal(*evt.StartedAt))
	require.NotNil(t, row.FinishedAt)
	assert.True(t, row.FinishedAt.Equal(*evt.CompletedAt))
	assert.Equal(t, evt.ExecutionTimeSeconds, row.ExecutionSeconds)
	assert.Equal(t, evt.ErrorMessage, row.ErrorMessage)
	assert.Equal(t, evt.LogS3Key, row.LogS3Key)
	assert.Equal(t, evt.RunResultsURI, row.RunResultsURI)
	assert.Equal(t, evt.ParseCache, row.ParseCache)
	assert.Equal(t, evt.ParseCacheReason, row.ParseCacheReason)
}

// A FAILED report of attempt 1 and the execution record of its Job leave one
// attempt row, failed and carrying every fact of the record, whichever arrives
// first.
func TestTaskExecutionRecorded_StatusAndRecordInEitherOrderLeaveOneAttempt(t *testing.T) {
	for _, order := range []string{"status first", "record first"} {
		t.Run(order, func(t *testing.T) {
			db := newTestDB(t)
			tenant := storeFailureSchedule(t, db)
			f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
			job, _, _ := f.task(t, recordFlowKey)
			evt := f.executionRecordOf(recordFlowKey, job+"-r1", "logs/"+job+"-r1.log")

			if order == "status first" {
				f.deliver(t, recordFlowKey, run.TaskStatusFailed, 1)
				f.record(t, evt)
			} else {
				f.record(t, evt)
				f.deliver(t, recordFlowKey, run.TaskStatusFailed, 1)
			}

			rows := f.recordedAttempts(t, recordFlowKey)
			require.Len(t, rows, 1)
			assertRecorded(t, evt, rows[0])
		})
	}
}

// Recording an execution writes no task_execution row.
func TestTaskExecutionRecorded_ARecordNoLongerWritesTaskExecution(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	job, _, _ := f.task(t, recordFlowKey)

	f.record(t, f.executionRecordOf(recordFlowKey, job, "logs/"+job+".log"))

	var n int
	require.NoError(t, db.Get(&n, `SELECT count(*) FROM task_execution WHERE task_id = $1`, f.taskID(recordFlowKey)))
	assert.Zero(t, n)
	require.Len(t, f.recordedAttempts(t, recordFlowKey), 1)
}

// singleConnDB opens a pool of exactly one connection, so every statement of
// it, transactions included, runs on the backend whose pid it returns.
func singleConnDB(t *testing.T) (*sqlx.DB, int) {
	t.Helper()
	db, err := pkgdb.Open(context.Background(), pkgconfig.LoadPostgres(&pkgconfig.Validator{}), pkgconfig.PoolConfig{MaxOpenConns: 1, MaxIdleConns: 1})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	var pid int
	require.NoError(t, db.Get(&pid, `SELECT pg_backend_pid()`))
	return db, pid
}

// A record that arrives while the status transaction holds the task lock waits
// for it, then fills the attempt that transaction saved: the record takes the
// task lock before the attempt lock, as the status transaction does, so the
// two never deadlock and the record lands on the committed row.
func TestTaskExecutionRecorded_ARecordWaitingOnTheTaskLockDoesNotDeadlockTheStatusTransaction(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	job, _, _ := f.task(t, recordFlowKey)
	id := f.taskID(recordFlowKey)
	evt := f.executionRecordOf(recordFlowKey, job+"-r1", "logs/b.log")

	a := newPlanningUoW(db)
	require.NoError(t, a.Begin(ctx))
	defer a.Rollback()
	_, held, err := a.TaskCollection().LoadAttemptState(ctx, id)
	require.NoError(t, err)
	require.True(t, held)

	dbB, pidB := singleConnDB(t)
	done := make(chan error, 1)
	go func() {
		b := newPlanningUoW(dbB)
		if err := b.Begin(ctx); err != nil {
			done <- err
			return
		}
		defer b.Rollback()
		if err := handlers.NewTaskExecutionRecordedHandler(discardLogger()).Handle(ctx, b, evt, uuid.Nil); err != nil {
			done <- err
			return
		}
		done <- b.Commit()
	}()

	require.Eventually(t, func() bool {
		var waiting string
		if err := db.Get(&waiting, `SELECT COALESCE(wait_event_type, '') FROM pg_stat_activity WHERE pid = $1`, pidB); err != nil {
			return false
		}
		return waiting == "Lock"
	}, 5*time.Second, 10*time.Millisecond, "the record never waited on the task lock")

	_, exists, err := a.TaskCollection().LoadAttempt(ctx, id, 1)
	require.NoError(t, err)
	require.False(t, exists)
	require.NoError(t, a.TaskCollection().SaveAttempt(ctx, run.Attempt{
		TaskID: id, Number: 1, JobName: job + "-r1", Status: run.AttemptFailed,
	}))
	require.NoError(t, a.Commit())

	select {
	case err := <-done:
		require.NoError(t, err, "the record's transaction commits")
	case <-time.After(10 * time.Second):
		t.Fatal("the record's transaction never finished")
	}
	rows := f.recordedAttempts(t, recordFlowKey)
	require.Len(t, rows, 1)
	assertRecorded(t, evt, rows[0])
}
