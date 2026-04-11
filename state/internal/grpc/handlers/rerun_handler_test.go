package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ─── stubs ───────────────────────────────────────────────────────────────────

type rerunSchedStub struct {
	scheduler   *model.SchedulerTracker
	getErr      error
	updateTxErr error
	initTxErr   error
	updated     *model.SchedulerTracker
}

func (s *rerunSchedStub) Create(_ context.Context, _ *model.SchedulerTracker) error { return nil }
func (s *rerunSchedStub) GetByID(_ context.Context, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return s.scheduler, s.getErr
}
func (s *rerunSchedStub) Update(_ context.Context, _ *model.SchedulerTracker) error { return nil }
func (s *rerunSchedStub) Cancel(_ context.Context, _ uuid.UUID, _, _ string) error  { return nil }
func (s *rerunSchedStub) List(_ context.Context, _ postgres.SchedulerFilters) ([]*model.SchedulerTracker, int, error) {
	return nil, 0, nil
}
func (s *rerunSchedStub) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (s *rerunSchedStub) UpdateInitializationStatus(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (s *rerunSchedStub) ResetInProgressInitializations(_ context.Context) (int, error) {
	return 0, nil
}
func (s *rerunSchedStub) GetLastRunPerSchedule(_ context.Context) (map[string]postgres.LastRunData, error) {
	return map[string]postgres.LastRunData{}, nil
}
func (s *rerunSchedStub) GetActiveScheduler(_ context.Context, _ string) (*model.SchedulerTracker, error) {
	return nil, nil
}
func (s *rerunSchedStub) UpdateTx(_ context.Context, _ *sqlx.Tx, t *model.SchedulerTracker) error {
	s.updated = t
	return s.updateTxErr
}
func (s *rerunSchedStub) UpdateInitializationStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return s.initTxErr
}
func (s *rerunSchedStub) CreateTx(_ context.Context, _ *sqlx.Tx, _ *model.SchedulerTracker) error {
	return nil
}

type rerunTaskStub struct {
	task         *model.TaskTracker
	getNodeErr   error
	runningTasks []*model.TaskTracker
	updateTxErr  error
	updated      *model.TaskTracker
}

func (s *rerunTaskStub) Create(_ context.Context, _ *model.TaskTracker) error { return nil }
func (s *rerunTaskStub) GetByID(_ context.Context, _ uuid.UUID) (*model.TaskTracker, error) {
	return nil, postgres.ErrNotFound
}
func (s *rerunTaskStub) GetByScheduleAndNode(_ context.Context, _ uuid.UUID, _, _, _ string) (*model.TaskTracker, error) {
	return s.task, s.getNodeErr
}
func (s *rerunTaskStub) Update(_ context.Context, _ *model.TaskTracker) error { return nil }
func (s *rerunTaskStub) Delete(_ context.Context, _ uuid.UUID) error           { return nil }
func (s *rerunTaskStub) ListByScheduleID(_ context.Context, _ uuid.UUID, _ *model.TaskStatus, _, _ int) ([]*model.TaskTracker, int, error) {
	return nil, 0, nil
}
func (s *rerunTaskStub) List(_ context.Context, _ postgres.TaskFilters) ([]*model.TaskTracker, int, error) {
	return s.runningTasks, len(s.runningTasks), nil
}
func (s *rerunTaskStub) UpdateTx(_ context.Context, _ *sqlx.Tx, t *model.TaskTracker) error {
	s.updated = t
	return s.updateTxErr
}

type rerunOutboxStub struct {
	created   *postgres.OutboxEntry
	createErr error
}

func (s *rerunOutboxStub) Create(_ context.Context, _ *sqlx.Tx, e *postgres.OutboxEntry) error {
	s.created = e
	return s.createErr
}
func (s *rerunOutboxStub) ListPending(_ context.Context, _ int) ([]*postgres.OutboxEntry, error) {
	return nil, nil
}
func (s *rerunOutboxStub) MarkPublished(_ context.Context, _ uuid.UUID) error  { return nil }
func (s *rerunOutboxStub) IncrementRetry(_ context.Context, _ uuid.UUID) error { return nil }

// ─── helpers ─────────────────────────────────────────────────────────────────

// buildRerunHandler builds a handler with a nil DB — safe for tests that never
// reach db.BeginTxx (all pre-flight error cases).
func buildRerunHandler(sched *rerunSchedStub, task *rerunTaskStub, outbox *rerunOutboxStub) *RerunHandler {
	return NewRerunHandler(nil, sched, task, outbox, newTestLogger())
}

// buildRerunHandlerWithDB builds a handler with a real DB connection.
// The test is skipped if the DB is not available.
func buildRerunHandlerWithDB(t *testing.T, sched *rerunSchedStub, task *rerunTaskStub, outbox *rerunOutboxStub) *RerunHandler {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	t.Cleanup(func() { db.Close() })
	return NewRerunHandler(db, sched, task, outbox, newTestLogger())
}

// ─── tests ───────────────────────────────────────────────────────────────────

func TestTriggerRerun_EmptyScheduleID(t *testing.T) {
	h := buildRerunHandler(&rerunSchedStub{}, &rerunTaskStub{}, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestTriggerRerun_InvalidScheduleIDFormat(t *testing.T) {
	h := buildRerunHandler(&rerunSchedStub{}, &rerunTaskStub{}, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId: "not-a-uuid",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestTriggerRerun_ScheduleNotFound(t *testing.T) {
	sched := &rerunSchedStub{getErr: postgres.ErrNotFound}
	h := buildRerunHandler(sched, &rerunTaskStub{}, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId: uuid.New().String(),
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestTriggerRerun_TaskNotFound(t *testing.T) {
	sched := &rerunSchedStub{scheduler: &model.SchedulerTracker{ScheduleID: uuid.New(), ScheduleName: "s"}}
	task := &rerunTaskStub{getNodeErr: postgres.ErrNotFound}
	h := buildRerunHandler(sched, task, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId: sched.scheduler.ScheduleID.String(),
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestTriggerRerun_RunningTasksExist(t *testing.T) {
	sched := &rerunSchedStub{scheduler: &model.SchedulerTracker{ScheduleID: uuid.New(), ScheduleName: "s"}}
	task := &rerunTaskStub{
		task:         &model.TaskTracker{TaskID: uuid.New(), Status: model.TaskStatusFailed},
		runningTasks: []*model.TaskTracker{{Status: model.TaskStatusRunning}},
	}
	h := buildRerunHandler(sched, task, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId: sched.scheduler.ScheduleID.String(),
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestTriggerRerun_TargetNotFailed(t *testing.T) {
	sched := &rerunSchedStub{scheduler: &model.SchedulerTracker{ScheduleID: uuid.New(), ScheduleName: "s"}}
	task := &rerunTaskStub{
		task: &model.TaskTracker{TaskID: uuid.New(), Status: model.TaskStatusSucceeded},
	}
	h := buildRerunHandler(sched, task, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId: sched.scheduler.ScheduleID.String(),
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestTriggerRerun_Success(t *testing.T) {
	scheduleID := uuid.New()
	sched := &rerunSchedStub{scheduler: &model.SchedulerTracker{
		ScheduleID:   scheduleID,
		ScheduleName: "my-schedule",
		Status:       model.SchedulerStatusFailed,
		CreatedAt:    time.Now(),
	}}
	task := &rerunTaskStub{
		task: &model.TaskTracker{
			TaskID:     uuid.New(),
			ScheduleID: scheduleID,
			Status:     model.TaskStatusFailed,
			RetryCount: 3,
		},
	}
	outbox := &rerunOutboxStub{}
	h := buildRerunHandlerWithDB(t, sched, task, outbox)

	resp, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  scheduleID.String(),
		Schema:      "public",
		TableName:   "orders",
		ServiceName: "svc-1",
	})

	require.NoError(t, err)
	assert.NotNil(t, resp)

	// Scheduler was reset to RUNNING
	require.NotNil(t, sched.updated)
	assert.Equal(t, model.SchedulerStatusRunning, sched.updated.Status)
	assert.Nil(t, sched.updated.CompletedAt)

	// Task was reset to PENDING with retry_count=0
	require.NotNil(t, task.updated)
	assert.Equal(t, model.TaskStatusPending, task.updated.Status)
	assert.Equal(t, 0, task.updated.RetryCount)

	// Outbox entry written to command.rerun:v1
	require.NotNil(t, outbox.created)
	assert.Equal(t, "command.rerun:v1", outbox.created.StreamName)
	assert.Equal(t, "rerun_node", outbox.created.EventType)
	assert.Equal(t, scheduleID, outbox.created.AggregateID)
}
