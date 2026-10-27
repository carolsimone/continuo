package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTask(t *testing.T, repo postgres.TaskRepository, scheduleID uuid.UUID) *postgres.TaskRow {
	t.Helper()
	task := &postgres.TaskRow{
		TenantID:    pkgevents.DefaultTenantID,
		TaskID:      uuid.New(),
		ScheduleID:  scheduleID,
		ServiceName: "svc",
		SchemaName:  "schema",
		TableName:   "tbl_" + uuid.New().String()[:8],
		JobName:     "job",
		Status:      run.TaskStatusFailed,
		Attempt:     3,
		MaxRetries:  5,
		CreatedAt:   time.Now(),
	}
	err := repo.Create(context.Background(), task)
	require.NoError(t, err)
	return task
}

// TestTaskRepository_HasFailedTaskTx_CountsCancelledTasks pins the finalize
// predicate: a run with a failed or a cancelled task did not succeed, while
// succeeded and skipped tasks never make it fail.
func TestTaskRepository_HasFailedTaskTx_CountsCancelledTasks(t *testing.T) {
	db := newTestDB(t)
	schedulerRepo := postgres.NewRunTableRepository(db, discardLogger())
	taskRepo := postgres.NewTaskRepository(db, discardLogger())
	ctx := context.Background()

	cases := []struct {
		name     string
		statuses []run.TaskStatus
		want     bool
	}{
		{"failed task", []run.TaskStatus{run.TaskStatusSucceeded, run.TaskStatusFailed}, true},
		{"cancelled task", []run.TaskStatus{run.TaskStatusSucceeded, run.TaskStatusCancelled}, true},
		{"succeeded and skipped", []run.TaskStatus{run.TaskStatusSucceeded, run.TaskStatusSkipped}, false},
		{"no tasks", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := createScheduler(t, schedulerRepo, "test-schedule-"+uuid.New().String())
			t.Cleanup(func() {
				db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", s.ScheduleID)
			})
			for _, status := range c.statuses {
				require.NoError(t, taskRepo.Create(ctx, &postgres.TaskRow{
					TenantID:    pkgevents.DefaultTenantID,
					TaskID:      uuid.New(),
					ScheduleID:  s.ScheduleID,
					ServiceName: "svc",
					SchemaName:  "schema",
					TableName:   "tbl_" + uuid.New().String()[:8],
					JobName:     "job",
					Status:      status,
					MaxRetries:  3,
					CreatedAt:   time.Now(),
				}))
			}

			tx, err := db.BeginTxx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			got, err := taskRepo.HasFailedTaskTx(ctx, tx, pkgevents.DefaultTenantID, s.ScheduleID)
			require.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestTaskRepository_BeginStopTx(t *testing.T) {
	db := newTestDB(t)
	schedRepo := postgres.NewRunTableRepository(db, discardLogger())
	taskRepo := postgres.NewTaskRepository(db, discardLogger())
	ctx := context.Background()

	schedID := uuid.New()
	require.NoError(t, schedRepo.Create(ctx, &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           schedID,
		ScheduleName:         "begin-stop-" + schedID.String(),
		Status:               run.RunStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "completed",
	}))
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", schedID)

	// One task per status; a failed task is final whether or not its budget
	// is spent.
	pendingID := uuid.New()
	dispatchedID := uuid.New()
	queuedID := uuid.New()
	runningID := uuid.New()
	budgetLeftID := uuid.New()
	exhaustedID := uuid.New()
	succeededID := uuid.New()
	skippedID := uuid.New()
	// A running task with no attempt row, and a dispatched task at attempt 1
	// whose open attempt is attempt 0: neither has an open current attempt.
	noAttemptID := uuid.New()
	staleAttemptID := uuid.New()
	for _, tt := range []struct {
		id         uuid.UUID
		status     run.TaskStatus
		attempt    int
		maxRetries int
	}{
		{pendingID, run.TaskStatusPending, 0, 2},
		{dispatchedID, run.TaskStatusDispatched, 0, 2},
		{queuedID, run.TaskStatusQueued, 1, 2},
		{runningID, run.TaskStatusRunning, 0, 2},
		{budgetLeftID, run.TaskStatusFailed, 1, 2},
		{exhaustedID, run.TaskStatusFailed, 2, 2},
		{succeededID, run.TaskStatusSucceeded, 0, 2},
		{skippedID, run.TaskStatusSkipped, 0, 2},
		{noAttemptID, run.TaskStatusRunning, 0, 2},
		{staleAttemptID, run.TaskStatusDispatched, 1, 2},
	} {
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskRow{
			TenantID:    pkgevents.DefaultTenantID,
			TaskID:      tt.id,
			ScheduleID:  schedID,
			ServiceName: "svc",
			SchemaName:  "sch",
			TableName:   "t-" + tt.id.String()[:8],
			JobName:     "j-" + tt.id.String()[:8],
			Status:      tt.status,
			Attempt:     tt.attempt,
			MaxRetries:  tt.maxRetries,
			CreatedAt:   time.Now(),
		}))
	}
	defer db.ExecContext(ctx, "DELETE FROM tasks WHERE run_id = $1", schedID)
	// The dispatched, queued and running tasks' current attempts are open.
	require.NoError(t, insertAttempt(db, dispatchedID, 0, "dispatched"))
	require.NoError(t, insertAttempt(db, queuedID, 0, "failed"))
	require.NoError(t, insertAttempt(db, queuedID, 1, "queued"))
	require.NoError(t, insertAttempt(db, runningID, 0, "running"))
	require.NoError(t, insertAttempt(db, staleAttemptID, 0, "running"))

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	counts, err := taskRepo.BeginStopTx(ctx, tx, pkgevents.DefaultTenantID, schedID, "user1", time.Now().UTC())
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	assert.Equal(t, run.StopCounts{Cancelled: 3, Stopping: 3}, counts)

	// The dispatched, queued and running tasks with an open current attempt
	// are stopping until their attempts answer; the pending task and the
	// tasks with no open current attempt are cancelled, as nothing of them
	// can answer a stop.
	for id, want := range map[uuid.UUID]run.TaskStatus{
		pendingID:      run.TaskStatusCancelled,
		dispatchedID:   run.TaskStatusStopping,
		queuedID:       run.TaskStatusStopping,
		runningID:      run.TaskStatusStopping,
		noAttemptID:    run.TaskStatusCancelled,
		staleAttemptID: run.TaskStatusCancelled,
	} {
		got, err := taskRepo.GetByID(ctx, pkgevents.DefaultTenantID, id)
		require.NoError(t, err)
		assert.Equal(t, want, got.Status)
		assert.NotNil(t, got.CancelledAt)
		require.NotNil(t, got.CancelledBy)
		assert.Equal(t, "user1", *got.CancelledBy)
	}

	// A task that ended — a failure, with or without budget left, a success, a
	// skip — is untouched.
	for id, want := range map[uuid.UUID]run.TaskStatus{
		budgetLeftID: run.TaskStatusFailed,
		exhaustedID:  run.TaskStatusFailed,
		succeededID:  run.TaskStatusSucceeded,
		skippedID:    run.TaskStatusSkipped,
	} {
		got, err := taskRepo.GetByID(ctx, pkgevents.DefaultTenantID, id)
		require.NoError(t, err)
		assert.Equal(t, want, got.Status)
		assert.Nil(t, got.CancelledAt)
		assert.Nil(t, got.CancelledBy)
	}
}

func TestTaskRepository_CreateAndGet_RoundTripsImageTag(t *testing.T) {
	db := newTestDB(t)
	schedulerRepo := postgres.NewRunTableRepository(db, discardLogger())
	repo := postgres.NewTaskRepository(db, discardLogger())

	ctx := context.Background()
	parent := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           uuid.New(),
		ScheduleName:         "tt-" + uuid.New().String()[:8],
		Status:               run.RunStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
		Kind:                 "cron",
	}
	require.NoError(t, schedulerRepo.Create(ctx, parent))
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", parent.ScheduleID)

	task := &postgres.TaskRow{
		TenantID:    pkgevents.DefaultTenantID,
		TaskID:      uuid.New(),
		ScheduleID:  parent.ScheduleID,
		CreatedAt:   time.Now(),
		ServiceName: "service-1",
		SchemaName:  "analytics",
		TableName:   "daily_metrics",
		JobName:     "job-x",
		Status:      run.TaskStatusPending,
		Attempt:     0,
		MaxRetries:  3,
		ImageTag:    "registry/img:abcdef",
	}
	require.NoError(t, repo.Create(ctx, task))
	defer db.ExecContext(ctx, "DELETE FROM tasks WHERE task_id = $1", task.TaskID)

	got, err := repo.GetByID(ctx, pkgevents.DefaultTenantID, task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, "registry/img:abcdef", got.ImageTag)
}

// tasks.manifest_version is gone: nothing writes or reads it, and the
// schema no longer carries it.
func TestTaskRow_ManifestVersionColumnIsDropped(t *testing.T) {
	db := newTestDB(t)

	var count int
	require.NoError(t, db.GetContext(context.Background(), &count, `
		SELECT COUNT(*) FROM information_schema.columns
		 WHERE table_name = 'tasks' AND column_name = 'manifest_version'`))
	assert.Equal(t, 0, count, "tasks.manifest_version must not exist")
}

func TestTaskRepository_CreateAndGet_RoundTripsInheritedFromTaskID(t *testing.T) {
	db := newTestDB(t)
	schedulerRepo := postgres.NewRunTableRepository(db, discardLogger())
	repo := postgres.NewTaskRepository(db, discardLogger())

	ctx := context.Background()
	parent := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           uuid.New(),
		ScheduleName:         "tt-inh-" + uuid.New().String()[:8],
		Status:               run.RunStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
		Kind:                 "rebase",
	}
	require.NoError(t, schedulerRepo.Create(ctx, parent))
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", parent.ScheduleID)

	// Inherited row: non-NULL pointer.
	rootTaskID := uuid.New()
	inherited := &postgres.TaskRow{
		TenantID:            pkgevents.DefaultTenantID,
		TaskID:              uuid.New(),
		ScheduleID:          parent.ScheduleID,
		CreatedAt:           time.Now(),
		ServiceName:         "svc",
		SchemaName:          "s",
		TableName:           "inherited_table",
		JobName:             "job-i",
		Status:              run.TaskStatusSucceeded,
		Attempt:             0,
		MaxRetries:          0,
		ImageTag:            "img:OLD",
		InheritedFromTaskID: &rootTaskID,
	}
	require.NoError(t, repo.Create(ctx, inherited))
	defer db.ExecContext(ctx, "DELETE FROM tasks WHERE task_id = $1", inherited.TaskID)

	gotInherited, err := repo.GetByID(ctx, pkgevents.DefaultTenantID, inherited.TaskID)
	require.NoError(t, err)
	require.NotNil(t, gotInherited.InheritedFromTaskID, "InheritedFromTaskID round-trip lost the pointer")
	assert.Equal(t, rootTaskID, *gotInherited.InheritedFromTaskID)

	// Real-execution row: NULL pointer should round-trip as nil.
	real := &postgres.TaskRow{
		TenantID:    pkgevents.DefaultTenantID,
		TaskID:      uuid.New(),
		ScheduleID:  parent.ScheduleID,
		CreatedAt:   time.Now(),
		ServiceName: "svc",
		SchemaName:  "s",
		TableName:   "real_table",
		JobName:     "job-r",
		Status:      run.TaskStatusPending,
		Attempt:     0,
		MaxRetries:  3,
		ImageTag:    "img:NEW",
		// InheritedFromTaskID intentionally nil
	}
	require.NoError(t, repo.Create(ctx, real))
	defer db.ExecContext(ctx, "DELETE FROM tasks WHERE task_id = $1", real.TaskID)

	gotReal, err := repo.GetByID(ctx, pkgevents.DefaultTenantID, real.TaskID)
	require.NoError(t, err)
	assert.Nil(t, gotReal.InheritedFromTaskID, "real-execution row should round-trip InheritedFromTaskID as nil")
}

func TestTaskRepository_SetStatusAndAttemptTx_DoesNotReviveCancelledTask(t *testing.T) {
	db := newTestDB(t)
	schedulerRepo := postgres.NewRunTableRepository(db, discardLogger())
	scheduler := createScheduler(t, schedulerRepo, "test-schedule-"+uuid.New().String())
	defer db.ExecContext(context.Background(), "DELETE FROM runs WHERE run_id = $1", scheduler.ScheduleID)

	taskRepo := postgres.NewTaskRepository(db, discardLogger())
	task := createTask(t, taskRepo, scheduler.ScheduleID)
	defer db.ExecContext(context.Background(), "DELETE FROM tasks WHERE task_id = $1", task.TaskID)

	// Mark the task cancelled, as BeginStopTx cancels a pending task.
	_, err := db.ExecContext(context.Background(),
		`UPDATE tasks SET status = 'cancelled', cancelled_at = NOW(), cancelled_by = 'test' WHERE task_id = $1`,
		task.TaskID,
	)
	require.NoError(t, err)

	// A late report tries to flip it to 'succeeded'.
	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	affected, err := taskRepo.SetStatusAndAttemptTx(context.Background(), tx, pkgevents.DefaultTenantID, task.TaskID, "succeeded", 0)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	assert.Equal(t, int32(0), affected, "cancelled task must not be revived")

	got, err := taskRepo.GetByID(context.Background(), pkgevents.DefaultTenantID, task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, run.TaskStatusCancelled, got.Status, "status must remain cancelled")
}

// TestTaskRepository_SetStatusAndAttemptTx_AdvancesAttemptOnSameStatus asserts
// that a newer attempt's terminal carrying the same status (failed→failed at a
// higher attempt) still writes the higher attempt, so the stored attempt the
// Run aggregate compares a report's attempt with stays current.
func TestTaskRepository_SetStatusAndAttemptTx_AdvancesAttemptOnSameStatus(t *testing.T) {
	db := newTestDB(t)
	schedulerRepo := postgres.NewRunTableRepository(db, discardLogger())
	scheduler := createScheduler(t, schedulerRepo, "test-schedule-"+uuid.New().String())
	defer db.ExecContext(context.Background(), "DELETE FROM runs WHERE run_id = $1", scheduler.ScheduleID)

	taskRepo := postgres.NewTaskRepository(db, discardLogger())
	task := createTask(t, taskRepo, scheduler.ScheduleID)
	defer db.ExecContext(context.Background(), "DELETE FROM tasks WHERE task_id = $1", task.TaskID)

	ctx := context.Background()
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = taskRepo.SetStatusAndAttemptTx(ctx, tx, pkgevents.DefaultTenantID, task.TaskID, "failed", 1)
	require.NoError(t, err)
	affected, err := taskRepo.SetStatusAndAttemptTx(ctx, tx, pkgevents.DefaultTenantID, task.TaskID, "failed", 2)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	assert.Equal(t, int32(1), affected, "same-status write must still affect the row")

	row, found, err := func() (postgres.TaskAttemptRow, bool, error) {
		tx2, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return postgres.TaskAttemptRow{}, false, err
		}
		defer tx2.Rollback()
		return taskRepo.LoadAttemptStateTx(ctx, tx2, pkgevents.DefaultTenantID, task.TaskID)
	}()
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "failed", row.Status)
	assert.Equal(t, int32(2), row.Attempt, "attempt must advance to the newer attempt")
}

// TestTaskRepository_BulkCreateTx_CopiesTenThousandTasks covers a run
// larger than any multi-row INSERT can bind: every task is written by one COPY
// in one transaction. Copying the same tasks again fails on the first stored
// task id and adds none.
func TestTaskRepository_BulkCreateTx_CopiesTenThousandTasks(t *testing.T) {
	db := newTestDB(t)
	schedRepo := postgres.NewRunTableRepository(db, discardLogger())
	taskRepo := postgres.NewTaskRepository(db, discardLogger())
	ctx := context.Background()

	sched := createScheduler(t, schedRepo, "bulk-create-"+uuid.New().String())
	// Deleting the run row cascades to its task rows.
	t.Cleanup(func() {
		db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", sched.ScheduleID)
	})

	const n = 10000
	tasks := make([]*postgres.TaskRow, n)
	for i := range tasks {
		key := fmt.Sprintf("sch.t_%05d", i)
		tasks[i] = &postgres.TaskRow{
			TenantID:    pkgevents.DefaultTenantID,
			TaskID:      uuid.New(),
			ScheduleID:  sched.ScheduleID,
			CreatedAt:   time.Now(),
			ServiceName: "svc",
			SchemaName:  "sch",
			TableName:   fmt.Sprintf("t_%05d", i),
			JobName:     fmt.Sprintf("j-%05d", i),
			Status:      run.TaskStatusPending,
			MaxRetries:  2,
			NodeKey:     &key,
		}
	}

	copyAll := func() error {
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		if err := taskRepo.BulkCreateTx(ctx, tx, tasks); err != nil {
			return err
		}
		return tx.Commit()
	}
	countRows := func() int {
		var count int
		require.NoError(t, db.GetContext(ctx, &count,
			"SELECT COUNT(*) FROM tasks WHERE run_id = $1", sched.ScheduleID))
		return count
	}

	require.NoError(t, copyAll())
	assert.Equal(t, n, countRows(), "every task of the run is inserted")

	err := copyAll()
	require.Error(t, err)
	assert.Equal(t, "23505", pqCode(t, err), "a stored task id fails the copy")
	assert.Equal(t, n, countRows(), "the failed copy adds no rows")
}

// BulkCreateTx stores when a task was dispatched and whom an inherited task
// inherits from, and GetByID reads both back; a task not dispatched reads nil.
func TestTaskRepository_DispatchedAtRoundTrips(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	logger := discardLogger()
	runs := postgres.NewRunTableRepository(db, logger)
	tasks := postgres.NewTaskRepository(db, logger)
	runRow := createScheduler(t, runs, "dispatched-"+uuid.NewString()[:8])
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, runRow.ScheduleID) })

	at := time.Date(2026, 10, 14, 23, 0, 41, 123456000, time.UTC)
	root := uuid.New()
	dispatchedKey, inheritedKey := "raw.orders", "raw.customers"
	dispatched := &postgres.TaskRow{
		TenantID: pkgevents.DefaultTenantID,
		TaskID:   uuid.New(), ScheduleID: runRow.ScheduleID, CreatedAt: time.Now(),
		ServiceName: "svc", SchemaName: "raw", TableName: "orders", JobName: "job-orders",
		Status: run.TaskStatusPending, MaxRetries: 2, NodeKey: &dispatchedKey, DispatchedAt: &at,
	}
	inherited := &postgres.TaskRow{
		TenantID: pkgevents.DefaultTenantID,
		TaskID:   uuid.New(), ScheduleID: runRow.ScheduleID, CreatedAt: time.Now(),
		ServiceName: "svc", SchemaName: "raw", TableName: "customers", JobName: "job-customers",
		Status: run.TaskStatusSucceeded, NodeKey: &inheritedKey, InheritedFromTaskID: &root,
	}

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tasks.BulkCreateTx(ctx, tx, []*postgres.TaskRow{dispatched, inherited}))
	require.NoError(t, tx.Commit())

	read, err := tasks.GetByID(ctx, pkgevents.DefaultTenantID, dispatched.TaskID)
	require.NoError(t, err)
	require.NotNil(t, read.DispatchedAt)
	assert.True(t, at.Equal(*read.DispatchedAt))
	assert.Nil(t, read.InheritedFromTaskID)
	assert.Equal(t, "run", read.Operation, "an empty operation is stored as run")

	read, err = tasks.GetByID(ctx, pkgevents.DefaultTenantID, inherited.TaskID)
	require.NoError(t, err)
	assert.Nil(t, read.DispatchedAt)
	require.NotNil(t, read.InheritedFromTaskID)
	assert.Equal(t, root, *read.InheritedFromTaskID)
	assert.Equal(t, run.TaskStatusSucceeded, read.Status)
}

// BulkCreateTx stores each task's node key and GetByID reads it back.
func TestTaskRepository_NodeKeyRoundTrips(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	logger := discardLogger()
	runs := postgres.NewRunTableRepository(db, logger)
	tasks := postgres.NewTaskRepository(db, logger)
	runRow := createScheduler(t, runs, "nodekey-"+uuid.NewString()[:8])
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, runRow.ScheduleID) })
	key := "raw.orders"
	row := &postgres.TaskRow{
		TenantID: pkgevents.DefaultTenantID,
		TaskID:   uuid.New(), ScheduleID: runRow.ScheduleID, CreatedAt: time.Now(),
		ServiceName: "svc", SchemaName: "raw", TableName: "orders", JobName: "job",
		Status: run.TaskStatusPending, MaxRetries: 2, NodeKey: &key,
	}

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tasks.BulkCreateTx(ctx, tx, []*postgres.TaskRow{row}))
	require.NoError(t, tx.Commit())

	read, err := tasks.GetByID(ctx, pkgevents.DefaultTenantID, row.TaskID)
	require.NoError(t, err)
	require.NotNil(t, read.NodeKey)
	assert.Equal(t, key, *read.NodeKey)
}

// The task reads (ListByRunID, GetByID, GetByRunAndNode) carry a
// pending task's wait for its node's lease: the reason, when it began waiting
// and the run whose task holds the node in the tenant the repository reads. A
// task that waits for nothing, or whose node no task of that tenant holds,
// names no holder run, whatever another tenant holds on the same node key.
func TestListByRunID_ShowsALeaseWaitAndItsHolder(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant, otherTenant := leaseTenant(t, db), leaseTenant(t, db)
	tasks := postgres.NewTaskRepository(db, discardLogger())
	held, free := "w"+uuid.NewString()[:8], "f"+uuid.NewString()[:8]
	holderRun := readRun(t, db, tenant, "cron-"+uuid.NewString()[:8], nil, nil, "running", false)
	holder := readTask(t, db, holderRun, held, "running")
	require.NoError(t, insertLease(db, tenant, "raw."+held, holder, holderRun))
	otherRun := readRun(t, db, otherTenant, "other-"+uuid.NewString()[:8], nil, nil, "running", false)
	otherHolder := readTask(t, db, otherRun, free, "running")
	require.NoError(t, insertLease(db, otherTenant, "raw."+free, otherHolder, otherRun))

	waitingRun := readRun(t, db, tenant, "single-"+uuid.NewString()[:8], nil, nil, "running", false)
	waiter := readTask(t, db, waitingRun, held, "pending")
	since := time.Date(2026, 10, 24, 10, 5, 0, 0, time.UTC)
	waitForLease(t, db, waiter, since)
	freed := readTask(t, db, waitingRun, free, "pending")
	waitForLease(t, db, freed, since)

	list, total, err := tasks.ListByRunID(ctx, tenant, waitingRun, nil, 10, 0)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	byID := map[uuid.UUID]*postgres.TaskRow{}
	for _, task := range list {
		byID[task.TaskID] = task
	}
	assertWaitsOn := func(t *testing.T, task *postgres.TaskRow, holderRun *uuid.UUID) {
		t.Helper()
		require.NotNil(t, task.WaitReason)
		assert.Equal(t, string(run.WaitReasonNodeLease), *task.WaitReason)
		require.NotNil(t, task.LeaseWaitSince)
		assert.True(t, since.Equal(*task.LeaseWaitSince))
		assert.Equal(t, holderRun, task.WaitingOnRunID)
	}
	assertWaitsOn(t, byID[waiter], &holderRun)
	assertWaitsOn(t, byID[freed], nil)

	got, err := tasks.GetByID(ctx, tenant, waiter)
	require.NoError(t, err)
	assertWaitsOn(t, got, &holderRun)
	got, err = tasks.GetByRunAndNode(ctx, tenant, waitingRun, "svc", "raw", held)
	require.NoError(t, err)
	assertWaitsOn(t, got, &holderRun)

	got, err = tasks.GetByID(ctx, tenant, holder)
	require.NoError(t, err)
	assert.Nil(t, got.WaitReason)
	assert.Nil(t, got.LeaseWaitSince)
	assert.Nil(t, got.WaitingOnRunID, "the holder waits on no run")
}

// ListByRunID pages a schedule's tasks newest first and counts the tasks
// its status filter matches; no filter lists every task.
func TestListByRunID_FiltersByStatusAndPages(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tasks := postgres.NewTaskRepository(db, discardLogger())
	runID := readRun(t, db, pkgevents.DefaultTenantID, "list-"+uuid.NewString()[:8], nil, nil, "running", false)
	oldest := readTask(t, db, runID, "a", "pending")
	middle := readTask(t, db, runID, "b", "running")
	newest := readTask(t, db, runID, "c", "pending")
	for i, id := range []uuid.UUID{oldest, middle, newest} {
		_, err := db.Exec(`UPDATE tasks SET created_at = now() - make_interval(mins => $2) WHERE task_id = $1`, id, 10-i)
		require.NoError(t, err)
	}

	all, total, err := tasks.ListByRunID(ctx, pkgevents.DefaultTenantID, runID, nil, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	require.Len(t, all, 3)
	assert.Equal(t, []uuid.UUID{newest, middle, oldest}, []uuid.UUID{all[0].TaskID, all[1].TaskID, all[2].TaskID})

	pending := run.TaskStatusPending
	page, total, err := tasks.ListByRunID(ctx, pkgevents.DefaultTenantID, runID, &pending, 1, 1)
	require.NoError(t, err)
	assert.Equal(t, 2, total, "the count is of the filter's matches, not of the page")
	require.Len(t, page, 1)
	assert.Equal(t, oldest, page[0].TaskID)
}
