package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDispatchedSchedulerRepo records every *Tx method the handler calls and
// stubs the rest with a panic so accidental use surfaces immediately.
type fakeDispatchedSchedulerRepo struct {
	scheduler *model.SchedulerTracker
	getErr    error

	setTotalCalls       []int32
	setTotalErr         error
	initStatusCalls     []string
	initStatusErr       error
	finalizedTo         string
	finalizeErr         error
	setTerminalCalls    []int32
	setTerminalErr      error
	updateStatusCalls   []string
	updateStatusErr     error
}

func (f *fakeDispatchedSchedulerRepo) GetByIDForUpdateTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return f.scheduler, f.getErr
}
func (f *fakeDispatchedSchedulerRepo) SetTotalTaskCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, n int32) error {
	if f.setTotalErr != nil {
		return f.setTotalErr
	}
	f.setTotalCalls = append(f.setTotalCalls, n)
	return nil
}
func (f *fakeDispatchedSchedulerRepo) UpdateInitializationStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, status string) error {
	if f.initStatusErr != nil {
		return f.initStatusErr
	}
	f.initStatusCalls = append(f.initStatusCalls, status)
	return nil
}
func (f *fakeDispatchedSchedulerRepo) FinalizeRunTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, status string) error {
	if f.finalizeErr != nil {
		return f.finalizeErr
	}
	f.finalizedTo = status
	return nil
}
func (f *fakeDispatchedSchedulerRepo) SetTerminalTaskCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, n int32) error {
	if f.setTerminalErr != nil {
		return f.setTerminalErr
	}
	f.setTerminalCalls = append(f.setTerminalCalls, n)
	return nil
}
func (f *fakeDispatchedSchedulerRepo) UpdateStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, status string) error {
	if f.updateStatusErr != nil {
		return f.updateStatusErr
	}
	f.updateStatusCalls = append(f.updateStatusCalls, status)
	return nil
}

// Unused methods — panic on accidental use.
func (f *fakeDispatchedSchedulerRepo) Create(context.Context, *model.SchedulerTracker) error {
	panic("Create not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) GetByID(context.Context, uuid.UUID) (*model.SchedulerTracker, error) {
	panic("GetByID not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) Update(context.Context, *model.SchedulerTracker) error {
	panic("Update not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) Cancel(context.Context, uuid.UUID, string, string) error {
	panic("Cancel not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) CancelTx(context.Context, *sqlx.Tx, uuid.UUID, string, string) error {
	panic("CancelTx not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) List(context.Context, postgres.SchedulerFilters) ([]*model.SchedulerTracker, int, error) {
	panic("List not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) HasActiveSchedule(context.Context, string) (bool, error) {
	panic("HasActiveSchedule not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) UpdateInitializationStatus(context.Context, uuid.UUID, string) error {
	panic("UpdateInitializationStatus not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) ResetInProgressInitializations(context.Context) (int, error) {
	panic("ResetInProgressInitializations not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) GetLastRunPerSchedule(context.Context) (map[string]postgres.LastRunData, error) {
	panic("GetLastRunPerSchedule not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) GetActiveScheduler(context.Context, string) (*model.SchedulerTracker, error) {
	panic("GetActiveScheduler not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) UpdateTx(context.Context, *sqlx.Tx, *model.SchedulerTracker) error {
	panic("UpdateTx not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) CreateTx(context.Context, *sqlx.Tx, *model.SchedulerTracker) error {
	panic("CreateTx not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) IncrementTerminalCountTx(context.Context, *sqlx.Tx, uuid.UUID) (int32, int32, error) {
	panic("IncrementTerminalCountTx not implemented in fake")
}
func (f *fakeDispatchedSchedulerRepo) DecrementTerminalCountTx(context.Context, *sqlx.Tx, uuid.UUID, int32) error {
	panic("DecrementTerminalCountTx not implemented in fake")
}

// fakeDispatchedTaskRepo captures the bulk-create call.
type fakeDispatchedTaskRepo struct {
	bulkCreated []*model.TaskTracker
	bulkErr     error
}

func (f *fakeDispatchedTaskRepo) BulkCreateTx(_ context.Context, _ *sqlx.Tx, tasks []*model.TaskTracker) error {
	if f.bulkErr != nil {
		return f.bulkErr
	}
	f.bulkCreated = append(f.bulkCreated, tasks...)
	return nil
}

// Unused methods — panic on accidental use.
func (f *fakeDispatchedTaskRepo) Create(context.Context, *model.TaskTracker) error {
	panic("Create not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) GetByID(context.Context, uuid.UUID) (*model.TaskTracker, error) {
	panic("GetByID not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) GetByScheduleAndNode(context.Context, uuid.UUID, string, string, string) (*model.TaskTracker, error) {
	panic("GetByScheduleAndNode not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) Update(context.Context, *model.TaskTracker) error {
	panic("Update not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) Delete(context.Context, uuid.UUID) error {
	panic("Delete not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) ListByScheduleID(context.Context, uuid.UUID, *model.TaskStatus, int, int) ([]*model.TaskTracker, int, error) {
	panic("ListByScheduleID not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) List(context.Context, postgres.TaskFilters) ([]*model.TaskTracker, int, error) {
	panic("List not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) UpdateTx(context.Context, *sqlx.Tx, *model.TaskTracker) error {
	panic("UpdateTx not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) ListAllByScheduleID(context.Context, uuid.UUID) ([]*model.TaskTracker, error) {
	panic("ListAllByScheduleID not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) ResetTasksTx(context.Context, *sqlx.Tx, []uuid.UUID) (int32, error) {
	panic("ResetTasksTx not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) UpdateStatusIfChangedTx(context.Context, *sqlx.Tx, uuid.UUID, string, int32) (int32, error) {
	panic("UpdateStatusIfChangedTx not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) ExistsTx(context.Context, *sqlx.Tx, uuid.UUID) (bool, error) {
	panic("ExistsTx not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) GetStatusTx(context.Context, *sqlx.Tx, uuid.UUID) (string, error) {
	panic("GetStatusTx not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) HasFailedTaskTx(context.Context, *sqlx.Tx, uuid.UUID) (bool, error) {
	panic("HasFailedTaskTx not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) HasRetryableFailedTaskTx(context.Context, *sqlx.Tx, uuid.UUID) (bool, error) {
	panic("HasRetryableFailedTaskTx not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) HasNonSucceededTask(context.Context, uuid.UUID) (bool, error) {
	panic("HasNonSucceededTask not implemented in fake")
}
func (f *fakeDispatchedTaskRepo) BulkCancelByScheduleIDTx(context.Context, *sqlx.Tx, uuid.UUID, string) (int64, error) {
	panic("BulkCancelByScheduleIDTx not implemented in fake")
}

// fakeDispatchedOutboxRepo captures outbox writes.
type fakeDispatchedOutboxRepo struct {
	created []*postgres.OutboxEntry
	err     error
}

func (f *fakeDispatchedOutboxRepo) Create(_ context.Context, _ *sqlx.Tx, e *postgres.OutboxEntry) error {
	if f.err != nil {
		return f.err
	}
	f.created = append(f.created, e)
	return nil
}
func (f *fakeDispatchedOutboxRepo) ListPending(context.Context, int) ([]*postgres.OutboxEntry, error) {
	panic("ListPending not implemented in fake")
}
func (f *fakeDispatchedOutboxRepo) MarkPublished(context.Context, uuid.UUID) error {
	panic("MarkPublished not implemented in fake")
}
func (f *fakeDispatchedOutboxRepo) IncrementRetry(context.Context, uuid.UUID) error {
	panic("IncrementRetry not implemented in fake")
}

func dispatchedTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// newDispatchedUoW assembles a FakeUnitOfWork already in a transaction
// (Begin called) wired to fresh fakes.
func newDispatchedUoW(
	sched *fakeDispatchedSchedulerRepo,
	task *fakeDispatchedTaskRepo,
	outbox *fakeDispatchedOutboxRepo,
) *uow.FakeUnitOfWork {
	u := &uow.FakeUnitOfWork{Scheduler: sched, Task: task, Outbox: outbox}
	_ = u.Begin(context.Background())
	return u
}

// TestRunEntriesDispatched_PendingTasksTransitionToRunning is the canonical
// happy path: a projection of all-PENDING tasks bulk-creates rows, sets
// total_task_count, transitions initialization_status to "completed" and
// scheduler status to "running". No outbox emission, no terminal-count seeding.
func TestRunEntriesDispatched_PendingTasksTransitionToRunning(t *testing.T) {
	scheduleID := uuid.New()
	sched := &fakeDispatchedSchedulerRepo{
		scheduler: &model.SchedulerTracker{
			ScheduleID:   scheduleID,
			ScheduleName: "happy-path",
			Status:       model.SchedulerStatusPending,
		},
	}
	taskRepo := &fakeDispatchedTaskRepo{}
	outboxRepo := &fakeDispatchedOutboxRepo{}
	u := newDispatchedUoW(sched, taskRepo, outboxRepo)

	t1, t2 := uuid.New(), uuid.New()
	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 2,
		AllTasks: []events.RunEntriesDispatchedTask{
			{TaskID: t1, ServiceName: "svc-a", SchemaName: "public", TableName: "orders", Status: model.TaskStatusPending, MaxRetries: 3},
			{TaskID: t2, ServiceName: "svc-b", SchemaName: "raw", TableName: "events", Status: model.TaskStatusPending, MaxRetries: 2},
		},
	}

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, taskRepo.bulkCreated, 2, "BulkCreateTx must receive both tasks")
	for _, tt := range taskRepo.bulkCreated {
		assert.Equal(t, scheduleID, tt.ScheduleID)
		assert.NotEmpty(t, tt.JobName, "job_name must be computed")
	}
	require.Equal(t, []int32{2}, sched.setTotalCalls)
	require.Equal(t, []string{"completed"}, sched.initStatusCalls)
	require.Equal(t, []string{string(model.SchedulerStatusRunning)}, sched.updateStatusCalls)
	assert.Empty(t, sched.setTerminalCalls, "no terminal tasks → terminal_task_count not seeded")
	assert.Empty(t, sched.finalizedTo, "pending tasks → no auto-rollup")
	assert.Empty(t, outboxRepo.created, "pending tasks → no run.finalized outbox")
}

// TestRunEntriesDispatched_PreservesPerTaskFields verifies that every wire
// field (Status, ManifestVersion, ImageTag, MaxRetries, InheritedFromTaskID)
// flows through to the TaskTracker row passed to BulkCreateTx.
func TestRunEntriesDispatched_PreservesPerTaskFields(t *testing.T) {
	scheduleID := uuid.New()
	sched := &fakeDispatchedSchedulerRepo{
		scheduler: &model.SchedulerTracker{
			ScheduleID: scheduleID, ScheduleName: "fields", Status: model.SchedulerStatusPending,
		},
	}
	taskRepo := &fakeDispatchedTaskRepo{}
	outboxRepo := &fakeDispatchedOutboxRepo{}
	u := newDispatchedUoW(sched, taskRepo, outboxRepo)

	rebasedID := uuid.New()
	inheritedID := uuid.New()
	rootID := uuid.New()
	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 2,
		AllTasks: []events.RunEntriesDispatchedTask{
			{
				TaskID: rebasedID, ServiceName: "svc", SchemaName: "s", TableName: "rebased",
				Status: model.TaskStatusPending, MaxRetries: 2,
				ManifestVersion: "v2", ImageTag: "img:2",
			},
			{
				TaskID: inheritedID, ServiceName: "svc", SchemaName: "s", TableName: "inherited",
				Status: model.TaskStatusSucceeded, MaxRetries: 0,
				ManifestVersion: "v1", ImageTag: "img:1",
				InheritedFromTaskID: &rootID,
			},
		},
	}

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, taskRepo.bulkCreated, 2)
	byID := map[uuid.UUID]*model.TaskTracker{}
	for _, tt := range taskRepo.bulkCreated {
		byID[tt.TaskID] = tt
	}
	rebased := byID[rebasedID]
	require.NotNil(t, rebased)
	assert.Equal(t, model.TaskStatusPending, rebased.Status)
	assert.Equal(t, "v2", rebased.ManifestVersion)
	assert.Equal(t, "img:2", rebased.ImageTag)
	assert.Equal(t, 2, rebased.MaxRetries)
	assert.Nil(t, rebased.InheritedFromTaskID)

	inherited := byID[inheritedID]
	require.NotNil(t, inherited)
	assert.Equal(t, model.TaskStatusSucceeded, inherited.Status)
	assert.Equal(t, "v1", inherited.ManifestVersion)
	assert.Equal(t, "img:1", inherited.ImageTag)
	require.NotNil(t, inherited.InheritedFromTaskID)
	assert.Equal(t, rootID, *inherited.InheritedFromTaskID)
}

// TestRunEntriesDispatched_NoopWhenSchedulerTerminal verifies that a duplicate
// delivery (or a late dispatch after cancel/finalize) against a scheduler
// already in any terminal state is a complete no-op: no bulk-create, no
// terminal-count overwrite, no finalize, no outbox.
func TestRunEntriesDispatched_NoopWhenSchedulerTerminal(t *testing.T) {
	for _, status := range []model.SchedulerStatus{
		model.SchedulerStatusSucceeded,
		model.SchedulerStatusFailed,
		model.SchedulerStatusCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			scheduleID := uuid.New()
			sched := &fakeDispatchedSchedulerRepo{
				scheduler: &model.SchedulerTracker{
					ScheduleID: scheduleID, ScheduleName: "terminal-guard", Status: status,
				},
			}
			taskRepo := &fakeDispatchedTaskRepo{}
			outboxRepo := &fakeDispatchedOutboxRepo{}
			u := newDispatchedUoW(sched, taskRepo, outboxRepo)

			evt := events.RunEntriesDispatched{
				ScheduleID:     scheduleID,
				TotalTaskCount: 1,
				AllTasks: []events.RunEntriesDispatchedTask{
					{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "t", Status: model.TaskStatusSucceeded},
				},
			}

			h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
			require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

			assert.Empty(t, taskRepo.bulkCreated, "terminal scheduler → no task rows created")
			assert.Empty(t, sched.setTotalCalls, "terminal scheduler → no total_task_count update")
			assert.Empty(t, sched.initStatusCalls)
			assert.Empty(t, sched.setTerminalCalls, "terminal scheduler → terminal_task_count must NOT be overwritten")
			assert.Empty(t, sched.finalizedTo)
			assert.Empty(t, sched.updateStatusCalls)
			assert.Empty(t, outboxRepo.created)
		})
	}
}

// TestRunEntriesDispatched_AllSucceededAutoRollup verifies that when every
// projected task arrives in terminal-succeeded state, the scheduler is
// auto-rolled-up to SUCCEEDED, terminal_task_count is seeded, and a
// run.finalized:v1 outbox entry is written with status="succeeded" and
// MessageProcessingID set.
func TestRunEntriesDispatched_AllSucceededAutoRollup(t *testing.T) {
	scheduleID := uuid.New()
	msgProcID := uuid.New()
	sched := &fakeDispatchedSchedulerRepo{
		scheduler: &model.SchedulerTracker{
			ScheduleID: scheduleID, ScheduleName: "rebase-rollup",
			Status: model.SchedulerStatusPending,
		},
	}
	taskRepo := &fakeDispatchedTaskRepo{}
	outboxRepo := &fakeDispatchedOutboxRepo{}
	u := newDispatchedUoW(sched, taskRepo, outboxRepo)

	root1, root2 := uuid.New(), uuid.New()
	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 2,
		AllTasks: []events.RunEntriesDispatchedTask{
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "a",
				Status: model.TaskStatusSucceeded, InheritedFromTaskID: &root1},
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "b",
				Status: model.TaskStatusSucceeded, InheritedFromTaskID: &root2},
		},
	}

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, msgProcID))

	assert.Equal(t, string(model.SchedulerStatusSucceeded), sched.finalizedTo,
		"all-succeeded projection must auto-rollup to SUCCEEDED")
	require.Equal(t, []int32{2}, sched.setTerminalCalls,
		"auto-rollup must seed terminal_task_count with the terminal row count")
	assert.Empty(t, sched.updateStatusCalls,
		"auto-rollup must NOT route through UpdateStatusTx(running)")

	require.Len(t, outboxRepo.created, 1, "auto-rollup must emit exactly one run.finalized:v1")
	entry := outboxRepo.created[0]
	assert.Equal(t, "run.finalized:v1", entry.EventType)
	assert.Equal(t, "run.finalized:v1", entry.StreamName)
	assert.Equal(t, "scheduler_tracker", entry.AggregateType)
	assert.Equal(t, scheduleID, entry.AggregateID)
	assert.Equal(t, "pending", entry.Status)
	require.NotNil(t, entry.MessageProcessingID)
	assert.Equal(t, msgProcID, *entry.MessageProcessingID)

	var fin pkgevents.RunFinalized
	require.NoError(t, json.Unmarshal(entry.Payload, &fin))
	assert.Equal(t, scheduleID.String(), fin.ScheduleID)
	assert.Equal(t, "rebase-rollup", fin.ScheduleName)
	assert.Equal(t, "succeeded", fin.Status)
}

// TestRunEntriesDispatched_MixedTerminalAutoRollupAsFailed verifies that when
// every task is terminal but at least one is not "succeeded", the run rolls up
// to FAILED (conservative classification) and the outbox payload reflects it.
func TestRunEntriesDispatched_MixedTerminalAutoRollupAsFailed(t *testing.T) {
	scheduleID := uuid.New()
	sched := &fakeDispatchedSchedulerRepo{
		scheduler: &model.SchedulerTracker{
			ScheduleID: scheduleID, ScheduleName: "mixed-terminal",
			Status: model.SchedulerStatusPending,
		},
	}
	taskRepo := &fakeDispatchedTaskRepo{}
	outboxRepo := &fakeDispatchedOutboxRepo{}
	u := newDispatchedUoW(sched, taskRepo, outboxRepo)

	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 2,
		AllTasks: []events.RunEntriesDispatchedTask{
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "a", Status: model.TaskStatusSucceeded},
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "b", Status: model.TaskStatusFailed},
		},
	}

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	assert.Equal(t, string(model.SchedulerStatusFailed), sched.finalizedTo,
		"mixed terminal (one failed) must roll up to FAILED")
	require.Equal(t, []int32{2}, sched.setTerminalCalls)
	assert.Empty(t, sched.updateStatusCalls)

	require.Len(t, outboxRepo.created, 1)
	var fin pkgevents.RunFinalized
	require.NoError(t, json.Unmarshal(outboxRepo.created[0].Payload, &fin))
	assert.Equal(t, "failed", fin.Status)
}

// TestRunEntriesDispatched_MixedPendingAndTerminalSeedsCount verifies that a
// projection with at least one PENDING task does NOT auto-rollup, but the
// inherited terminal rows are accounted for via SetTerminalTaskCountTx so the
// remaining pending tasks can finalize the run later. Status still moves to
// RUNNING; no outbox emission.
func TestRunEntriesDispatched_MixedPendingAndTerminalSeedsCount(t *testing.T) {
	scheduleID := uuid.New()
	sched := &fakeDispatchedSchedulerRepo{
		scheduler: &model.SchedulerTracker{
			ScheduleID: scheduleID, ScheduleName: "rebase-mixed",
			Status: model.SchedulerStatusPending,
		},
	}
	taskRepo := &fakeDispatchedTaskRepo{}
	outboxRepo := &fakeDispatchedOutboxRepo{}
	u := newDispatchedUoW(sched, taskRepo, outboxRepo)

	root1, root2 := uuid.New(), uuid.New()
	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 3,
		AllTasks: []events.RunEntriesDispatchedTask{
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "rebased",
				Status: model.TaskStatusPending, MaxRetries: 2},
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "ok1",
				Status: model.TaskStatusSucceeded, InheritedFromTaskID: &root1},
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "ok2",
				Status: model.TaskStatusSucceeded, InheritedFromTaskID: &root2},
		},
	}

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Equal(t, []int32{3}, sched.setTotalCalls)
	require.Equal(t, []int32{2}, sched.setTerminalCalls,
		"mixed pending+terminal must seed terminal_task_count with the count of terminal rows")
	require.Equal(t, []string{string(model.SchedulerStatusRunning)}, sched.updateStatusCalls)
	assert.Empty(t, sched.finalizedTo, "any PENDING must keep the run in RUNNING")
	assert.Empty(t, outboxRepo.created, "no rollup → no run.finalized outbox")
}

// TestRunEntriesDispatched_GetByIDErrorPropagates verifies that a scheduler
// lookup failure short-circuits the handler before any mutation.
func TestRunEntriesDispatched_GetByIDErrorPropagates(t *testing.T) {
	sched := &fakeDispatchedSchedulerRepo{getErr: errors.New("row lock failed")}
	taskRepo := &fakeDispatchedTaskRepo{}
	outboxRepo := &fakeDispatchedOutboxRepo{}
	u := newDispatchedUoW(sched, taskRepo, outboxRepo)

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	err := h.Handle(context.Background(), u, events.RunEntriesDispatched{
		ScheduleID:     uuid.New(),
		TotalTaskCount: 0,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "lock scheduler row")
	assert.Empty(t, taskRepo.bulkCreated)
	assert.Empty(t, sched.setTotalCalls)
	assert.Empty(t, outboxRepo.created)
}

// TestRunEntriesDispatched_BulkCreateErrorPropagates verifies that a
// task-repo failure surfaces an error after the lock but before any
// scheduler mutation.
func TestRunEntriesDispatched_BulkCreateErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()
	sched := &fakeDispatchedSchedulerRepo{
		scheduler: &model.SchedulerTracker{
			ScheduleID: scheduleID, ScheduleName: "bulk-error",
			Status: model.SchedulerStatusPending,
		},
	}
	taskRepo := &fakeDispatchedTaskRepo{bulkErr: errors.New("db down")}
	outboxRepo := &fakeDispatchedOutboxRepo{}
	u := newDispatchedUoW(sched, taskRepo, outboxRepo)

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	err := h.Handle(context.Background(), u, events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 1,
		AllTasks: []events.RunEntriesDispatchedTask{
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "t", Status: model.TaskStatusPending},
		},
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "bulk create tasks")
	assert.Empty(t, sched.setTotalCalls)
	assert.Empty(t, outboxRepo.created)
}

// TestRunEntriesDispatched_OutboxErrorPropagates verifies that an outbox
// write failure on the auto-rollup branch bubbles up so the binding rolls
// back the entire tx (no half-committed finalize).
func TestRunEntriesDispatched_OutboxErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()
	sched := &fakeDispatchedSchedulerRepo{
		scheduler: &model.SchedulerTracker{
			ScheduleID: scheduleID, ScheduleName: "outbox-error",
			Status: model.SchedulerStatusPending,
		},
	}
	taskRepo := &fakeDispatchedTaskRepo{}
	outboxRepo := &fakeDispatchedOutboxRepo{err: errors.New("outbox down")}
	u := newDispatchedUoW(sched, taskRepo, outboxRepo)

	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 1,
		AllTasks: []events.RunEntriesDispatchedTask{
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "a", Status: model.TaskStatusSucceeded},
		},
	}

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	err := h.Handle(context.Background(), u, evt, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create outbox entry for run.finalized")
}
