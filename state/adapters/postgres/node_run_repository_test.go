package postgres_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeRunRepository_List seeds two scheduler runs (cron + rerun) each with a
// task on (svc, sch, tbl), one extra task on (svc, sch, other) that must be
// excluded, and one execution per task.  It verifies ordering (rerun first),
// field projection, and that the unrelated task is filtered out.
func TestNodeRunRepository_List(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	nodeRunRepo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	svc := "svc-nr-" + uuid.New().String()[:8]
	sch := "sch-" + uuid.New().String()[:8]
	tbl := "tbl-" + uuid.New().String()[:8]
	other := "other-" + uuid.New().String()[:8]

	// --- scheduler 1: cron, succeeded ---
	sched1ID := uuid.New()
	sched1 := &postgres.SchedulerTracker{
		ScheduleID:           sched1ID,
		ScheduleName:         sch,
		Status:               run.SchedulerStatusSucceeded,
		Kind:                 "cron",
		CreatedAt:            time.Now().Add(-2 * time.Minute),
		InitializationStatus: "completed",
	}
	require.NoError(t, schedRepo.Create(ctx, sched1))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sched1ID)

	// task1: on the target node
	task1ID := uuid.New()
	task1 := &postgres.TaskTracker{
		TaskID:      task1ID,
		ScheduleID:  sched1ID,
		ServiceName: svc,
		SchemaName:  sch,
		TableName:   tbl,
		JobName:     "job-1",
		Status:      run.TaskStatusSucceeded,
		Attempt:     0,
		MaxRetries:  3,
		ImageTag:    "v1",
		CreatedAt:   time.Now().Add(-2 * time.Minute),
	}
	require.NoError(t, taskRepo.Create(ctx, task1))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", task1ID)

	// attempt 0 of task1
	insertAttemptRow(t, db, storedAttempt{
		TaskID:     task1ID,
		Status:     "succeeded",
		StartedAt:  ptrTime(time.Now().Add(-90 * time.Second)),
		FinishedAt: ptrTime(time.Now().Add(-60 * time.Second)),
	})

	// extra task on (svc, sch, other) — must NOT appear in results
	taskExtraID := uuid.New()
	taskExtra := &postgres.TaskTracker{
		TaskID:      taskExtraID,
		ScheduleID:  sched1ID,
		ServiceName: svc,
		SchemaName:  sch,
		TableName:   other,
		JobName:     "job-extra",
		Status:      run.TaskStatusSucceeded,
		Attempt:     0,
		MaxRetries:  3,
		ImageTag:    "vx",
		CreatedAt:   time.Now().Add(-2 * time.Minute),
	}
	require.NoError(t, taskRepo.Create(ctx, taskExtra))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", taskExtraID)

	// --- scheduler 2: rerun (source = sched1), failed — created more recently ---
	sched2ID := uuid.New()
	sched2 := &postgres.SchedulerTracker{
		ScheduleID:           sched2ID,
		ScheduleName:         sch,
		Status:               run.SchedulerStatusFailed,
		Kind:                 "rerun",
		SourceRunID:          &sched1ID,
		CreatedAt:            time.Now().Add(-1 * time.Minute),
		InitializationStatus: "completed",
	}
	require.NoError(t, schedRepo.Create(ctx, sched2))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sched2ID)

	// task2: on the target node, rerun
	task2ID := uuid.New()
	errMsg := "boom"
	task2 := &postgres.TaskTracker{
		TaskID:      task2ID,
		ScheduleID:  sched2ID,
		ServiceName: svc,
		SchemaName:  sch,
		TableName:   tbl,
		JobName:     "job-2",
		Status:      run.TaskStatusFailed,
		Attempt:     1,
		MaxRetries:  3,
		ImageTag:    "v2",
		CreatedAt:   time.Now().Add(-1 * time.Minute),
	}
	require.NoError(t, taskRepo.Create(ctx, task2))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", task2ID)

	// attempt 1 of task2 (with error_message)
	insertAttemptRow(t, db, storedAttempt{
		TaskID:       task2ID,
		Attempt:      1,
		Status:       "failed",
		StartedAt:    ptrTime(time.Now().Add(-50 * time.Second)),
		FinishedAt:   ptrTime(time.Now().Add(-30 * time.Second)),
		ErrorMessage: &errMsg,
	})

	// --- query ---
	rows, err := nodeRunRepo.List(ctx, svc, sch, tbl, "run", 50)
	require.NoError(t, err)
	require.Len(t, rows, 2, "expected exactly 2 rows for (svc, sch, tbl)")

	// ordered by scheduler_tracker.created_at DESC: sched2 (rerun) first
	assert.Equal(t, sched2ID, rows[0].ScheduleID)
	assert.Equal(t, "rerun", rows[0].Kind)
	assert.Equal(t, "v2", rows[0].ImageTag)
	assert.NotNil(t, rows[0].StartedAt)
	assert.NotNil(t, rows[0].CompletedAt)
	require.NotNil(t, rows[0].ErrorMessage)
	assert.Equal(t, "boom", *rows[0].ErrorMessage)
	assert.Equal(t, 1, rows[0].Attempt)

	assert.Equal(t, sched1ID, rows[1].ScheduleID)
	assert.Equal(t, "cron", rows[1].Kind)
}

// TestNodeRunRepository_List_TaskWithoutExecution verifies that a task with no
// corresponding task_attempts row produces a NodeRun with nil StartedAt and
// nil CompletedAt (LEFT JOIN semantics).
func TestNodeRunRepository_List_TaskWithoutExecution(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	nodeRunRepo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	svc := "svc-noe-" + uuid.New().String()[:8]
	sch := "sch-" + uuid.New().String()[:8]
	tbl := "tbl-" + uuid.New().String()[:8]

	schedID := uuid.New()
	sched := &postgres.SchedulerTracker{
		ScheduleID:           schedID,
		ScheduleName:         sch,
		Status:               run.SchedulerStatusRunning,
		Kind:                 "cron",
		CreatedAt:            time.Now(),
		InitializationStatus: "completed",
	}
	require.NoError(t, schedRepo.Create(ctx, sched))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", schedID)

	taskID := uuid.New()
	task := &postgres.TaskTracker{
		TaskID:      taskID,
		ScheduleID:  schedID,
		ServiceName: svc,
		SchemaName:  sch,
		TableName:   tbl,
		JobName:     "job-pending",
		Status:      run.TaskStatusPending,
		Attempt:     0,
		MaxRetries:  3,
		ImageTag:    "v1",
		CreatedAt:   time.Now(),
	}
	require.NoError(t, taskRepo.Create(ctx, task))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", taskID)

	// No task_attempts row inserted.

	rows, err := nodeRunRepo.List(ctx, svc, sch, tbl, "run", 50)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Nil(t, rows[0].StartedAt)
	assert.Nil(t, rows[0].CompletedAt)
}

// TestNodeRunRepository_List_NoMatch verifies that querying for a node identity
// with no data returns an empty slice and no error.
func TestNodeRunRepository_List_NoMatch(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	nodeRunRepo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	rows, err := nodeRunRepo.List(ctx, "nonexistent-svc", "nonexistent-sch", "nonexistent-tbl", "run", 50)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func ptrTime(t time.Time) *time.Time { return &t }

// storedAttempt is one task_attempts row a test stores. Attempt is the 0-based
// attempt number; unset facts stay NULL.
type storedAttempt struct {
	TaskID           uuid.UUID
	Attempt          int
	Status           string
	StartedAt        *time.Time
	FinishedAt       *time.Time
	ExecutionSeconds *float64
	ErrorMessage     *string
	LogS3Key         *string
	RunResultsURI    *string
	FailureClass     *string
	FailureReason    *string
}

// insertAttemptRow stores row, named after its task and attempt. It registers
// no cleanup of its own: the row is deleted with its task through the
// task_attempts foreign key's ON DELETE CASCADE.
func insertAttemptRow(t testing.TB, db *sqlx.DB, row storedAttempt) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO task_attempts (task_id, attempt, job_name, status, started_at, finished_at, execution_seconds,
		                           error_message, log_s3_key, run_results_uri, failure_class, failure_reason)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		row.TaskID, row.Attempt, fmt.Sprintf("job-%s-%d", row.TaskID.String()[:8], row.Attempt), row.Status,
		row.StartedAt, row.FinishedAt, row.ExecutionSeconds,
		row.ErrorMessage, row.LogS3Key, row.RunResultsURI, row.FailureClass, row.FailureReason)
	require.NoError(t, err)
}

// nodeRunTask stores a run created ageMin minutes ago holding one task of
// svc.sch.tbl in status at attempt, and returns the task's id.
func nodeRunTask(t *testing.T, db *sqlx.DB, svc, sch, tbl string, status run.TaskStatus, attempt, ageMin int) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	at := time.Now().Add(-time.Duration(ageMin) * time.Minute)
	sid, tid := uuid.New(), uuid.New()
	require.NoError(t, postgres.NewSchedulerTrackerRepository(db, discardLogger()).Create(ctx, &postgres.SchedulerTracker{
		ScheduleID: sid, ScheduleName: sch, Status: run.SchedulerStatusSucceeded, Kind: "cron",
		CreatedAt: at, InitializationStatus: "completed",
	}))
	t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
	require.NoError(t, postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger()).Create(ctx, &postgres.TaskTracker{
		TaskID: tid, ScheduleID: sid, ServiceName: svc, SchemaName: sch, TableName: tbl,
		JobName: "j", Status: status, Attempt: attempt, MaxRetries: 3, ImageTag: "v1", CreatedAt: at,
	}))
	return tid
}

// A task's run shows its latest attempt: the attempt number, that attempt's
// facts and, for a failed attempt, its failure class and reason; an earlier
// attempt's facts never show.
func TestNodeRunRepository_List_CarriesTheLatestAttemptsFacts(t *testing.T) {
	db := newTestDB(t)
	svc, sch := "svc-la-"+uuid.NewString()[:8], "sch"
	now := time.Now().UTC().Truncate(time.Millisecond)
	logA, logB, logFailed := "logs/a", "logs/b", "logs/failed"
	results, boom := "s3://results/b", "boom"
	class, reason := "task", "exit_code"

	// X: attempt 0 failed, attempt 1 succeeded.
	x := nodeRunTask(t, db, svc, sch, "x", run.TaskStatusSucceeded, 1, 10)
	insertAttemptRow(t, db, storedAttempt{TaskID: x, Attempt: 0, Status: "failed", LogS3Key: &logA,
		StartedAt: ptrTime(now.Add(-9 * time.Minute)), FinishedAt: ptrTime(now.Add(-8 * time.Minute)),
		ErrorMessage: &boom, FailureClass: &class, FailureReason: &reason})
	insertAttemptRow(t, db, storedAttempt{TaskID: x, Attempt: 1, Status: "succeeded", LogS3Key: &logB, RunResultsURI: &results,
		StartedAt: ptrTime(now.Add(-6 * time.Minute)), FinishedAt: ptrTime(now.Add(-5 * time.Minute))})
	// Y: its latest (only) attempt failed with a class and a reason.
	y := nodeRunTask(t, db, svc, sch, "y", run.TaskStatusFailed, 0, 5)
	insertAttemptRow(t, db, storedAttempt{TaskID: y, Attempt: 0, Status: "failed", LogS3Key: &logFailed,
		StartedAt: ptrTime(now.Add(-4 * time.Minute)), FinishedAt: ptrTime(now.Add(-3 * time.Minute)),
		ErrorMessage: &boom, FailureClass: &class, FailureReason: &reason})

	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	gotX, err := repo.List(context.Background(), svc, sch, "x", "run", 50)
	require.NoError(t, err)
	require.Len(t, gotX, 1)
	assert.Equal(t, 1, gotX[0].Attempt)
	require.NotNil(t, gotX[0].LogS3Key)
	assert.Equal(t, logB, *gotX[0].LogS3Key)
	require.NotNil(t, gotX[0].RunResultsURI)
	assert.Equal(t, results, *gotX[0].RunResultsURI)
	require.NotNil(t, gotX[0].StartedAt)
	assert.True(t, now.Add(-6*time.Minute).Equal(*gotX[0].StartedAt), "started at attempt 1's start: %v", gotX[0].StartedAt)
	require.NotNil(t, gotX[0].CompletedAt)
	assert.True(t, now.Add(-5*time.Minute).Equal(*gotX[0].CompletedAt), "completed at attempt 1's finish: %v", gotX[0].CompletedAt)
	assert.Nil(t, gotX[0].ErrorMessage)
	assert.Nil(t, gotX[0].FailureClass)
	assert.Nil(t, gotX[0].FailureReason)

	gotY, err := repo.List(context.Background(), svc, sch, "y", "run", 50)
	require.NoError(t, err)
	require.Len(t, gotY, 1)
	assert.Equal(t, 0, gotY[0].Attempt)
	require.NotNil(t, gotY[0].FailureClass)
	assert.Equal(t, class, *gotY[0].FailureClass)
	require.NotNil(t, gotY[0].FailureReason)
	assert.Equal(t, reason, *gotY[0].FailureReason)
	require.NotNil(t, gotY[0].ErrorMessage)
	assert.Equal(t, boom, *gotY[0].ErrorMessage)
}

// Node durations read each task's succeeded attempt only: a task that failed
// after 100 s adds no duration, a task whose 100 s attempt failed before a
// 10 s attempt succeeded adds 10, and a task with two succeeded attempts adds
// one duration.
func TestNodeRunRepository_ListNodes_DurationsCountOnlySucceededAttempts(t *testing.T) {
	start := time.Now().UTC().Add(-time.Hour)
	attempt := func(task uuid.UUID, n int, status string, sec int) storedAttempt {
		return storedAttempt{TaskID: task, Attempt: n, Status: status,
			StartedAt: ptrTime(start), FinishedAt: ptrTime(start.Add(time.Duration(sec) * time.Second))}
	}
	listOne := func(t *testing.T, db *sqlx.DB, svc string) *projection.NodeSummary {
		t.Helper()
		nodes, _, err := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger()).ListNodes(context.Background(), "", svc, "", 50, 0)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		return nodes[0]
	}

	t.Run("a failed task contributes nothing", func(t *testing.T) {
		db := newTestDB(t)
		svc, sch, tbl := "svc-dur-"+uuid.NewString()[:8], "sch", "orders"
		failed := nodeRunTask(t, db, svc, sch, tbl, run.TaskStatusFailed, 0, 20)
		insertAttemptRow(t, db, attempt(failed, 0, "failed", 100))
		succeeded := nodeRunTask(t, db, svc, sch, tbl, run.TaskStatusSucceeded, 0, 10)
		insertAttemptRow(t, db, attempt(succeeded, 0, "succeeded", 10))

		n := listOne(t, db, svc)
		assert.Equal(t, 10, n.AvgDurationSec)
		assert.Equal(t, 10, n.P95DurationSec)
	})

	t.Run("a task that failed then succeeded contributes its succeeded attempt", func(t *testing.T) {
		db := newTestDB(t)
		svc, sch, tbl := "svc-dur-"+uuid.NewString()[:8], "sch", "orders"
		task := nodeRunTask(t, db, svc, sch, tbl, run.TaskStatusSucceeded, 1, 10)
		insertAttemptRow(t, db, attempt(task, 0, "failed", 100))
		insertAttemptRow(t, db, attempt(task, 1, "succeeded", 10))

		n := listOne(t, db, svc)
		assert.Equal(t, 10, n.AvgDurationSec)
		assert.Equal(t, 10, n.P95DurationSec)
	})

	t.Run("a task with two succeeded attempts counts once", func(t *testing.T) {
		db := newTestDB(t)
		svc, sch, tbl := "svc-dur-"+uuid.NewString()[:8], "sch", "orders"
		once := nodeRunTask(t, db, svc, sch, tbl, run.TaskStatusSucceeded, 0, 20)
		insertAttemptRow(t, db, attempt(once, 0, "succeeded", 10))
		twice := nodeRunTask(t, db, svc, sch, tbl, run.TaskStatusSucceeded, 1, 10)
		insertAttemptRow(t, db, attempt(twice, 0, "succeeded", 30))
		insertAttemptRow(t, db, attempt(twice, 1, "succeeded", 30))

		// Two durations, 10 s and 30 s: counting the second task twice would
		// make the average 23 and the 95th percentile 30.
		n := listOne(t, db, svc)
		assert.Equal(t, 20, n.AvgDurationSec)
		assert.Equal(t, 29, n.P95DurationSec)
	})
}

// TestNodeRunRepository_ListNodes_AggregatesAndFilters seeds two nodes under the
// same service and asserts the catalog aggregates, ordering (newest last_run_at
// first), the service filter, and the exact table-name search filter.
func TestNodeRunRepository_ListNodes_AggregatesAndFilters(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	svc := "svc-ln-" + uuid.New().String()[:8]
	sch := "an"
	tblA := "fct_orders"
	tblB := "stg_customers"

	seed := func(table string, schedStatus run.SchedulerStatus, taskStatus run.TaskStatus, retry int, ageMin int, durSec int) {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: sch, Status: schedStatus, Kind: "cron",
			CreatedAt:            time.Now().Add(-time.Duration(ageMin) * time.Minute),
			InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		tid := uuid.New()
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: tid, ScheduleID: sid, ServiceName: svc, SchemaName: sch, TableName: table,
			JobName: "j", Status: taskStatus, Attempt: retry, MaxRetries: 3,
			ImageTag:  "v1",
			CreatedAt: time.Now().Add(-time.Duration(ageMin) * time.Minute),
		}))
		start := time.Now().Add(-time.Duration(ageMin) * time.Minute)
		end := start.Add(time.Duration(durSec) * time.Second)
		insertAttemptRow(t, db, storedAttempt{
			TaskID: tid, Attempt: retry, Status: string(taskStatus),
			StartedAt: ptrTime(start), FinishedAt: ptrTime(end),
		})
	}

	// node A: 2 runs, 1 succeeded + 1 failed (50% success), one flaky (attempt>0); durations count the succeeded attempt only (10s)
	seed(tblA, run.SchedulerStatusSucceeded, run.TaskStatusSucceeded, 0, 30, 10)
	seed(tblA, run.SchedulerStatusFailed, run.TaskStatusFailed, 1, 5, 30) // most recent for A
	// node B: 1 run, succeeded, duration 4s — most recent overall
	seed(tblB, run.SchedulerStatusSucceeded, run.TaskStatusSucceeded, 0, 2, 4)

	nodes, total, err := repo.ListNodes(ctx, "", svc, "", 50, 0)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, nodes, 2)

	assert.Equal(t, tblB, nodes[0].TableName) // B (2m) before A (5m)
	assert.Equal(t, tblA, nodes[1].TableName)

	a := nodes[1]
	assert.Equal(t, 2, a.RunCount)
	assert.Equal(t, 50, a.SuccessRatePct)
	assert.Equal(t, 10, a.AvgDurationSec) // only the succeeded task's 10s counts; the failed task's 30s does not
	assert.Equal(t, 10, a.P95DurationSec)
	assert.Equal(t, 50, a.FlakyRatePct) // 1 of 2 runs had attempt>0
	assert.Equal(t, "failed", a.LastStatus)

	none, total2, err := repo.ListNodes(ctx, "", "no-such-svc", "", 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, total2)
	assert.Empty(t, none)

	hit, total3, err := repo.ListNodes(ctx, "fct_orders", svc, "", 50, 0) // exact table-name match
	require.NoError(t, err)
	assert.Equal(t, 1, total3)
	require.Len(t, hit, 1)
	assert.Equal(t, tblA, hit[0].TableName)
}

// TestNodeRunRepository_ListNodes_PerNodeStatusIsolation verifies the aggregate
// keys off task_tracker.status, NOT scheduler_tracker.status.
func TestNodeRunRepository_ListNodes_PerNodeStatusIsolation(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	svc := "svc-iso-" + uuid.New().String()[:8]
	sid := uuid.New()
	require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusFailed, Kind: "cron",
		CreatedAt: time.Now().Add(-time.Minute), InitializationStatus: "completed",
	}))
	t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
	require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
		TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an", TableName: "ok_node",
		JobName: "j", Status: run.TaskStatusSucceeded, MaxRetries: 3, ImageTag: "v",
		CreatedAt: time.Now().Add(-time.Minute),
	}))

	nodes, _, err := repo.ListNodes(ctx, "", svc, "", 50, 0)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Equal(t, 100, nodes[0].SuccessRatePct, "node task succeeded => 100% despite failed schedule")
	assert.Equal(t, "succeeded", nodes[0].LastStatus)
}

// TestNodeRunRepository_ListNodes_Paging asserts limit/offset slice the result
// while total_count stays the full match count.
func TestNodeRunRepository_ListNodes_Paging(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	svc := "svc-pg-" + uuid.New().String()[:8]
	for i := 0; i < 3; i++ {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Duration(i) * time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an",
			TableName: "t" + uuid.New().String()[:4], JobName: "j", Status: run.TaskStatusSucceeded,
			MaxRetries: 3, ImageTag: "v",
			CreatedAt: time.Now().Add(-time.Duration(i) * time.Minute),
		}))
	}

	page1, total, err := repo.ListNodes(ctx, "", svc, "", 2, 0)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, page1, 2)

	page2, _, err := repo.ListNodes(ctx, "", svc, "", 2, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 1)

	seen := map[string]bool{}
	for _, n := range append(page1, page2...) {
		seen[n.TableName] = true
	}
	assert.Len(t, seen, 3, "page1 and page2 must be disjoint and cover all 3 nodes")
}

// TestNodeRunRepository_ListNodes_StablePagingOnTiedLastRun seeds many nodes
// under one scheduler so they share scheduler_tracker.created_at (identical
// last_run_at). Paging must stay disjoint and complete.
func TestNodeRunRepository_ListNodes_StablePagingOnTiedLastRun(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	svc := "svc-tie-" + uuid.New().String()[:8]
	sid := uuid.New()
	require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
		CreatedAt: time.Now().Add(-time.Minute), InitializationStatus: "completed",
	}))
	t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
	// 5 distinct nodes all under the SAME scheduler => identical last_run_at
	for i := 0; i < 5; i++ {
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an",
			TableName: "t" + uuid.New().String()[:6], JobName: "j", Status: run.TaskStatusSucceeded,
			MaxRetries: 3, ImageTag: "v", CreatedAt: time.Now().Add(-time.Minute),
		}))
	}
	seen := map[string]int{}
	for off := 0; off < 5; off += 2 {
		page, total, err := repo.ListNodes(ctx, "", svc, "", 2, off)
		require.NoError(t, err)
		assert.Equal(t, 5, total)
		for _, n := range page {
			seen[n.TableName]++
		}
	}
	assert.Len(t, seen, 5, "all 5 tied-last_run nodes appear exactly once across pages")
	for tbl, c := range seen {
		assert.Equal(t, 1, c, "node %s appeared %d times", tbl, c)
	}
}

// TestNodeRunRepository_ListNodes_SkippedCountsTerminal verifies skipped counts
// as terminal: succeeded + skipped => 50% (not 100%).
func TestNodeRunRepository_ListNodes_SkippedCountsTerminal(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())
	svc := "svc-skip-" + uuid.New().String()[:8]
	mk := func(taskStatus run.TaskStatus, ageMin int) {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Duration(ageMin) * time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an", TableName: "n",
			JobName: "j", Status: taskStatus, MaxRetries: 3, ImageTag: "v",
			CreatedAt: time.Now().Add(-time.Duration(ageMin) * time.Minute),
		}))
	}
	mk(run.TaskStatusSucceeded, 10)
	mk(run.TaskStatusSkipped, 5)
	nodes, _, err := repo.ListNodes(ctx, "", svc, "", 50, 0)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Equal(t, 50, nodes[0].SuccessRatePct, "succeeded+skipped => 1/2 = 50%")
}

// TestNodeRunRepository_ListNodes_ExactTableMatch verifies the search term is an
// EXACT (case-insensitive) match on the table name: "fct_orders" returns only
// "fct_orders", never a substring sibling ("fctxorders") or a suffixed variant
// ("fct_orders_v2").
func TestNodeRunRepository_ListNodes_ExactTableMatch(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())
	svc := "svc-exact-" + uuid.New().String()[:8]
	mk := func(table string) {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an", TableName: table,
			JobName: "j", Status: run.TaskStatusSucceeded, MaxRetries: 3, ImageTag: "v",
			CreatedAt: time.Now().Add(-time.Minute),
		}))
	}
	mk("fct_orders")
	mk("fctxorders")    // substring sibling — must NOT match
	mk("fct_orders_v2") // suffixed variant — must NOT match

	hit, total, err := repo.ListNodes(ctx, "fct_orders", svc, "", 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, total, "exact match -> only fct_orders, not fctxorders or fct_orders_v2")
	require.Len(t, hit, 1)
	assert.Equal(t, "fct_orders", hit[0].TableName)

	// match is case-insensitive
	hitCI, totalCI, err := repo.ListNodes(ctx, "FCT_ORDERS", svc, "", 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, totalCI)
	require.Len(t, hitCI, 1)
	assert.Equal(t, "fct_orders", hitCI[0].TableName)
}

// TestNodeRunRepository_ListNodeNames returns distinct table names (deduped
// across schemas), service-filtered, sorted ascending.
func TestNodeRunRepository_ListNodeNames(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	svcA := "svc-names-a-" + uuid.New().String()[:8]
	svcB := "svc-names-b-" + uuid.New().String()[:8]
	mk := func(svc, schema, table string) {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: schema, TableName: table,
			JobName: "j", Status: run.TaskStatusSucceeded, MaxRetries: 3, ImageTag: "v",
			CreatedAt: time.Now().Add(-time.Minute),
		}))
	}
	// svcA: orders + customers (customers in two schemas -> dedup to one)
	mk(svcA, "an", "orders")
	mk(svcA, "an", "customers")
	mk(svcA, "marts", "customers")
	// svcB: revenue
	mk(svcB, "an", "revenue")

	// service filter -> only svcA names, deduped + sorted
	namesA, err := repo.ListNodeNames(ctx, svcA)
	require.NoError(t, err)
	assert.Equal(t, []string{"customers", "orders"}, namesA)

	// filtering by svcB keeps the assertion deterministic in a shared DB
	namesB, err := repo.ListNodeNames(ctx, svcB)
	require.NoError(t, err)
	assert.Equal(t, []string{"revenue"}, namesB)
}

// TestNodeRunRepository_ListNodeServices returns the distinct service names of
// nodes that have run, deduped and sorted ascending. The query has no service
// filter, so on a shared DB the seeded services must appear as a deduped,
// sorted subset rather than the whole result.
func TestNodeRunRepository_ListNodeServices(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	svcA := "svc-list-a-" + uuid.New().String()[:8]
	svcB := "svc-list-b-" + uuid.New().String()[:8]
	mk := func(svc, schema, table string) {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: schema, TableName: table,
			JobName: "j", Status: run.TaskStatusSucceeded, MaxRetries: 3, ImageTag: "v",
			CreatedAt: time.Now().Add(-time.Minute),
		}))
	}
	// svcA seeded by two nodes (must dedupe to one entry); svcB by one.
	mk(svcA, "an", "orders")
	mk(svcA, "an", "customers")
	mk(svcB, "an", "revenue")

	services, err := repo.ListNodeServices(ctx)
	require.NoError(t, err)

	// Deduped and sorted ascending across the whole result.
	assert.IsIncreasing(t, services)
	seen := map[string]int{}
	for _, s := range services {
		seen[s]++
	}
	for s, n := range seen {
		assert.Equalf(t, 1, n, "service %s appears more than once", s)
	}
	// The seeded services are present, each exactly once.
	assert.Equal(t, 1, seen[svcA])
	assert.Equal(t, 1, seen[svcB])
}

// TestNodeRunRepository_ListNodeServices_ByteOrderRegardlessOfLocale verifies
// the service list is sorted by byte value, not by the database's collation.
// A locale collation such as en_US ignores '-', so it would put "svca…" before
// "svc-z…"; byte order puts '-' (0x2d) before 'a' (0x61).
func TestNodeRunRepository_ListNodeServices_ByteOrderRegardlessOfLocale(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	suffix := uuid.New().String()[:8]
	hyphenated := "svc-z" + suffix
	plain := "svca" + suffix
	for _, svc := range []string{plain, hyphenated} {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an", TableName: "t",
			JobName: "j", Status: run.TaskStatusSucceeded, MaxRetries: 3, ImageTag: "v",
			CreatedAt: time.Now().Add(-time.Minute),
		}))
	}

	services, err := repo.ListNodeServices(ctx)
	require.NoError(t, err)

	idx := map[string]int{}
	for i, s := range services {
		idx[s] = i
	}
	require.Contains(t, idx, hyphenated)
	require.Contains(t, idx, plain)
	assert.Less(t, idx[hyphenated], idx[plain], "byte order puts %q before %q", hyphenated, plain)
	assert.IsIncreasing(t, services)
}

// TestNodeRunRepository_ListNodes_EmptyPageKeepsTotal verifies an empty page
// (offset beyond the end) still returns the true total_count.
func TestNodeRunRepository_ListNodes_EmptyPageKeepsTotal(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())
	svc := "svc-emp-" + uuid.New().String()[:8]
	for i := 0; i < 3; i++ {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Duration(i) * time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an",
			TableName: "t" + uuid.New().String()[:6], JobName: "j", Status: run.TaskStatusSucceeded,
			MaxRetries: 3, ImageTag: "v", CreatedAt: time.Now().Add(-time.Duration(i) * time.Minute),
		}))
	}
	page, total, err := repo.ListNodes(ctx, "", svc, "", 2, 5) // offset past the 3 rows
	require.NoError(t, err)
	assert.Empty(t, page)
	assert.Equal(t, 3, total, "total_count must survive an empty page")
}

// A node run reports the node's content hash in the run's own release, not in
// the live release, and none when the run names no release.
func TestNodeRunRepository_ContentHashComesFromTheRunsRelease(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := "default"
	oldRelease := "rel-" + uuid.NewString()
	liveRelease := "rel-" + uuid.NewString()
	table := "hash_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	key := "raw." + table
	base := time.Now().UnixMicro()

	// insertRelease stores a release holding the node under a given content
	// hash and removes it (with its nodes) when the test ends.
	insertRelease := func(release string, promotionSeq int64, contentHash string) {
		_, err := db.Exec(`INSERT INTO topology_releases (tenant_id, release_id, promotion_seq, topology_uri, topology_sha256) VALUES ($1, $2, $3, 's3://b/k', 'x')`, tenant, release, promotionSeq)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = db.Exec(`DELETE FROM topology_releases WHERE tenant_id = $1 AND release_id = $2`, tenant, release)
		})
		_, err = db.Exec(`INSERT INTO topology_nodes (tenant_id, release_id, node_key, unique_id, service_name, schema_name, table_name, node_type, content_hash)
			VALUES ($1, $2, $3, $3, 'svc', 'raw', $4, 'dbt-model', $5)`, tenant, release, key, table, contentHash)
		require.NoError(t, err)
	}
	insertRelease(oldRelease, base, "sha256:h1")
	insertRelease(liveRelease, base+1, "sha256:h2")

	// The newer release is the live one; whatever live row the tenant had is
	// put back when the test ends.
	var prior struct {
		ReleaseID    string `db:"release_id"`
		PromotionSeq int64  `db:"promotion_seq"`
	}
	hadPrior := db.GetContext(ctx, &prior, `SELECT release_id, promotion_seq FROM live_release WHERE tenant_id = $1`, tenant) == nil
	_, err := db.Exec(`
		INSERT INTO live_release (tenant_id, release_id, promotion_seq, updated_at) VALUES ($1, $2, $3, now())
		ON CONFLICT (tenant_id) DO UPDATE SET release_id = EXCLUDED.release_id, promotion_seq = EXCLUDED.promotion_seq`,
		tenant, liveRelease, base+1)
	require.NoError(t, err)
	t.Cleanup(func() {
		if hadPrior {
			_, _ = db.Exec(`UPDATE live_release SET release_id = $2, promotion_seq = $3 WHERE tenant_id = $1`, tenant, prior.ReleaseID, prior.PromotionSeq)
			return
		}
		_, _ = db.Exec(`DELETE FROM live_release WHERE tenant_id = $1`, tenant)
	})

	stamped := readRun(t, db, "hash-"+uuid.NewString()[:8], &oldRelease, nil, "succeeded", true)
	legacy := readRun(t, db, "hash-"+uuid.NewString()[:8], nil, nil, "succeeded", true)
	readTask(t, db, stamped, table, "succeeded")
	readTask(t, db, legacy, table, "succeeded")

	rows, err := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger()).List(ctx, "svc", "raw", table, "run", 50)
	require.NoError(t, err)
	hashes := map[uuid.UUID]string{}
	for _, r := range rows {
		hashes[r.ScheduleID] = r.ContentHash
	}
	assert.Equal(t, map[uuid.UUID]string{stamped: "sha256:h1", legacy: ""}, hashes)
}

// declaredNode is one node a stored release declares: its service, schema and
// table, and the task timeout in seconds it declares (nil when it declares
// none).
type declaredNode struct {
	service, schema, table string
	timeoutSeconds         *int
}

// storeRelease stores a release holding nodes and removes it (with its nodes)
// when the test ends.
func storeRelease(t *testing.T, db *sqlx.DB, release string, promotionSeq int64, nodes ...declaredNode) {
	t.Helper()
	const tenant = "default"
	_, err := db.Exec(`INSERT INTO topology_releases (tenant_id, release_id, promotion_seq, topology_uri, topology_sha256) VALUES ($1, $2, $3, 's3://b/k', 'x')`, tenant, release, promotionSeq)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM topology_releases WHERE tenant_id = $1 AND release_id = $2`, tenant, release)
	})
	for _, n := range nodes {
		key := n.schema + "." + n.table
		_, err = db.Exec(`INSERT INTO topology_nodes (tenant_id, release_id, node_key, unique_id, service_name, schema_name, table_name, node_type, timeout_seconds)
			VALUES ($1, $2, $3, $3, $4, $5, $6, 'dbt-model', $7)`, tenant, release, key, n.service, n.schema, n.table, n.timeoutSeconds)
		require.NoError(t, err)
	}
}

// makeLive points the default tenant's live_release at release and puts back
// whatever it pointed at when the test ends.
func makeLive(t *testing.T, db *sqlx.DB, release string, promotionSeq int64) {
	t.Helper()
	const tenant = "default"
	var prior struct {
		ReleaseID    string `db:"release_id"`
		PromotionSeq int64  `db:"promotion_seq"`
	}
	hadPrior := db.GetContext(context.Background(), &prior, `SELECT release_id, promotion_seq FROM live_release WHERE tenant_id = $1`, tenant) == nil
	_, err := db.Exec(`
		INSERT INTO live_release (tenant_id, release_id, promotion_seq, updated_at) VALUES ($1, $2, $3, now())
		ON CONFLICT (tenant_id) DO UPDATE SET release_id = EXCLUDED.release_id, promotion_seq = EXCLUDED.promotion_seq`,
		tenant, release, promotionSeq)
	require.NoError(t, err)
	t.Cleanup(func() {
		if hadPrior {
			_, _ = db.Exec(`UPDATE live_release SET release_id = $2, promotion_seq = $3 WHERE tenant_id = $1`, tenant, prior.ReleaseID, prior.PromotionSeq)
			return
		}
		_, _ = db.Exec(`DELETE FROM live_release WHERE tenant_id = $1`, tenant)
	})
}

func timeoutOf(rows []*projection.NodeSummary) map[string]*int {
	out := map[string]*int{}
	for _, r := range rows {
		out[r.TableName] = r.TimeoutSeconds
	}
	return out
}

// A node's catalog row carries the timeout the live release declares for it:
// an older release's value never shows, and a node declaring none reports
// none.
func TestNodeRunRepository_ListNodes_CarriesTheLiveReleasesTimeout(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	svc := "svc-to-" + uuid.NewString()[:8]
	sch := "an"
	tblA, tblB := "tmo_a", "tmo_b"
	base := time.Now().UnixMicro()
	oldRelease, liveRelease := "rel-"+uuid.NewString(), "rel-"+uuid.NewString()

	ptr := func(v int) *int { return &v }
	storeRelease(t, db, oldRelease, base,
		declaredNode{svc, sch, tblA, ptr(600)}, declaredNode{svc, sch, tblB, ptr(900)})
	storeRelease(t, db, liveRelease, base+1,
		declaredNode{svc, sch, tblA, ptr(7200)}, declaredNode{svc, sch, tblB, nil})
	makeLive(t, db, liveRelease, base+1)

	nodeRunTask(t, db, svc, sch, tblA, run.TaskStatusSucceeded, 0, 5)
	nodeRunTask(t, db, svc, sch, tblB, run.TaskStatusSucceeded, 0, 6)

	rows, total, err := repo.ListNodes(ctx, "", svc, "", 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, total, "joining the live release must not change the catalog count")
	assert.Equal(t, map[string]*int{tblA: ptr(7200), tblB: nil}, timeoutOf(rows))
}

// A node the live release does not hold reports no timeout.
func TestNodeRunRepository_ListNodes_ANodeOutsideTheLiveReleaseHasNoTimeout(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	svc := "svc-to-" + uuid.NewString()[:8]
	sch := "an"
	held, outside := "tmo_held", "tmo_outside"
	base := time.Now().UnixMicro()
	oldRelease, liveRelease := "rel-"+uuid.NewString(), "rel-"+uuid.NewString()

	ptr := func(v int) *int { return &v }
	storeRelease(t, db, oldRelease, base, declaredNode{svc, sch, outside, ptr(1800)})
	storeRelease(t, db, liveRelease, base+1, declaredNode{svc, sch, held, ptr(300)})
	makeLive(t, db, liveRelease, base+1)

	nodeRunTask(t, db, svc, sch, held, run.TaskStatusSucceeded, 0, 5)
	nodeRunTask(t, db, svc, sch, outside, run.TaskStatusSucceeded, 0, 6)

	rows, total, err := repo.ListNodes(ctx, "", svc, "", 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, map[string]*int{held: ptr(300), outside: nil}, timeoutOf(rows))
}

// Two nodes of the live release that share a service, schema and table (they
// differ only in node key) still make one catalog row.
func TestNodeRunRepository_ListNodes_NodesSharingATableStayOneRow(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())

	svc := "svc-to-" + uuid.NewString()[:8]
	sch, tbl := "an", "tmo_shared"
	base := time.Now().UnixMicro()
	liveRelease := "rel-" + uuid.NewString()

	storeRelease(t, db, liveRelease, base, declaredNode{svc, sch, tbl, nil})
	_, err := db.Exec(`INSERT INTO topology_nodes (tenant_id, release_id, node_key, unique_id, service_name, schema_name, table_name, node_type, timeout_seconds)
		VALUES ('default', $1, 'seed.shared', 'seed.shared', $2, $3, $4, 'dbt-seed', 1200)`, liveRelease, svc, sch, tbl)
	require.NoError(t, err)
	makeLive(t, db, liveRelease, base)

	nodeRunTask(t, db, svc, sch, tbl, run.TaskStatusSucceeded, 0, 5)

	rows, total, err := repo.ListNodes(ctx, "", svc, "", 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, rows, 1)
	require.NotNil(t, rows[0].TimeoutSeconds)
	assert.Equal(t, 1200, *rows[0].TimeoutSeconds)
}

// A node's history shows, on a run whose task waits for the node's lease, the
// reason, when it began waiting and the run whose task holds the node in the
// tenant the repository reads; the holding run's own row waits on no run. A
// lease on the same node key in another tenant is not read.
func TestNodeRunRepository_List_ShowsALeaseWaitAndItsHolder(t *testing.T) {
	db := newTestDB(t)
	tenant, otherTenant := leaseTenant(t, db), leaseTenant(t, db)
	tbl := "w" + uuid.NewString()[:8]
	holderRun := readRun(t, db, "cron-"+uuid.NewString()[:8], nil, nil, "running", false)
	holder := readTask(t, db, holderRun, tbl, "running")
	require.NoError(t, insertLease(db, tenant, "raw."+tbl, holder, holderRun))
	waitingRun := readRun(t, db, "single-"+uuid.NewString()[:8], nil, nil, "running", false)
	waiter := readTask(t, db, waitingRun, tbl, "pending")
	since := time.Date(2026, 10, 24, 10, 5, 0, 0, time.UTC)
	waitForLease(t, db, waiter, since)

	rows, err := postgres.NewNodeRunRepository(db, tenant, discardLogger()).List(context.Background(), "svc", "raw", tbl, "run", 50)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	other, err := postgres.NewNodeRunRepository(db, otherTenant, discardLogger()).List(context.Background(), "svc", "raw", tbl, "run", 50)
	require.NoError(t, err)
	for _, r := range other {
		assert.Nil(t, r.WaitingOnRunID, "another tenant's repository reads none of this tenant's leases")
	}
	byTask := map[uuid.UUID]*projection.NodeRun{}
	for _, r := range rows {
		byTask[r.TaskID] = r
	}

	waiting := byTask[waiter]
	assert.Equal(t, run.WaitReasonNodeLease, waiting.WaitReason)
	require.NotNil(t, waiting.WaitingOnRunID)
	assert.Equal(t, holderRun, *waiting.WaitingOnRunID)
	require.NotNil(t, waiting.WaitingSince)
	assert.True(t, since.Equal(*waiting.WaitingSince))

	holding := byTask[holder]
	assert.Empty(t, holding.WaitReason)
	assert.Nil(t, holding.WaitingOnRunID, "the holder waits on no run")
	assert.Nil(t, holding.WaitingSince)
}
