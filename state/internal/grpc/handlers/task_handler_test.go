package handlers

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/ports"
	"github.com/carolsimone/continuo/state/service/uow"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// stubTaskRepo is a minimal in-memory fake for TaskTrackerRepository.
type stubTaskRepo struct {
	tasks     map[uuid.UUID]*postgres.TaskTracker
	updateErr error
}

func newStubTaskRepo() *stubTaskRepo {
	return &stubTaskRepo{tasks: make(map[uuid.UUID]*postgres.TaskTracker)}
}

func (s *stubTaskRepo) Create(_ context.Context, task *postgres.TaskTracker) error {
	s.tasks[task.TaskID] = task
	return nil
}

func (s *stubTaskRepo) GetByID(_ context.Context, id uuid.UUID) (*postgres.TaskTracker, error) {
	t, ok := s.tasks[id]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	return t, nil
}

func (s *stubTaskRepo) GetByScheduleAndNode(_ context.Context, _ uuid.UUID, _, _, _ string) (*postgres.TaskTracker, error) {
	return nil, postgres.ErrNotFound
}

func (s *stubTaskRepo) Update(_ context.Context, task *postgres.TaskTracker) error {
	if s.updateErr != nil {
		return s.updateErr
	}
	if _, ok := s.tasks[task.TaskID]; !ok {
		return postgres.ErrNotFound
	}
	s.tasks[task.TaskID] = task
	return nil
}

func (s *stubTaskRepo) Delete(_ context.Context, id uuid.UUID) error {
	if _, ok := s.tasks[id]; !ok {
		return postgres.ErrNotFound
	}
	delete(s.tasks, id)
	return nil
}

func (s *stubTaskRepo) ListByScheduleID(_ context.Context, _ uuid.UUID, _ *run.TaskStatus, _, _ int) ([]*postgres.TaskTracker, int, error) {
	return nil, 0, nil
}

func (s *stubTaskRepo) List(_ context.Context, _ postgres.TaskFilters) ([]*postgres.TaskTracker, int, error) {
	return nil, 0, nil
}

func (s *stubTaskRepo) UpdateTx(_ context.Context, _ *sqlx.Tx, task *postgres.TaskTracker) error {
	if _, ok := s.tasks[task.TaskID]; !ok {
		return postgres.ErrNotFound
	}
	s.tasks[task.TaskID] = task
	return nil
}
func (s *stubTaskRepo) BulkCreateTx(_ context.Context, _ *sqlx.Tx, _ []*postgres.TaskTracker) error {
	return nil
}
func (s *stubTaskRepo) ListAllByScheduleID(_ context.Context, _ uuid.UUID) ([]*postgres.TaskTracker, error) {
	return nil, nil
}
func (s *stubTaskRepo) ResetTasksTx(_ context.Context, _ *sqlx.Tx, _ []uuid.UUID) (int32, error) {
	return 0, nil
}
func (s *stubTaskRepo) SetStatusAndAttemptTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string, _ int32) (int32, error) {
	return 0, nil
}
func (s *stubTaskRepo) ExistsTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (s *stubTaskRepo) GetStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (string, error) {
	return "", nil
}
func (s *stubTaskRepo) LoadStatusAndAttemptTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (string, int32, error) {
	return "", 0, nil
}
func (s *stubTaskRepo) HasFailedTaskTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (s *stubTaskRepo) HasRetryableFailedTaskTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (s *stubTaskRepo) HasNonSucceededTask(_ context.Context, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (s *stubTaskRepo) BulkCancelByScheduleIDTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) (int64, error) {
	return 0, nil
}

func ctxWithCaller(caller run.CallerID) context.Context {
	md := metadata.Pairs("x-caller-id", string(caller))
	return metadata.NewIncomingContext(context.Background(), md)
}

// helper to create a task in the stub repo
func makeTask(repo *stubTaskRepo, taskStatus run.TaskStatus, retryCount int) *postgres.TaskTracker {
	task := &postgres.TaskTracker{
		TaskID:      uuid.New(),
		ScheduleID:  uuid.New(),
		CreatedAt:   time.Now(),
		ServiceName: "svc",
		SchemaName:  "schema",
		TableName:   "table",
		JobName:     "svc-schema-table",
		Status:      taskStatus,
		RetryCount:  retryCount,
		MaxRetries:  3,
	}
	repo.tasks[task.TaskID] = task
	return task
}

// ---- fakes for ResetTask aggregate path ----

// resetFakeRunRepo satisfies ports.RunRepository for ResetTask tests.
// LoadRunForUpdate returns the pre-built run stored in the fake; other
// methods panic because ResetTask does not call them.
type resetFakeRunRepo struct {
	stored  *run.Run
	loadErr error
}

func (f *resetFakeRunRepo) GetRun(_ context.Context, _ uuid.UUID) (*run.Run, error) {
	panic("GetRun not used in reset tests")
}
func (f *resetFakeRunRepo) LoadRunForUpdate(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*run.Run, error) {
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	return f.stored, nil
}
func (f *resetFakeRunRepo) SaveRun(_ context.Context, _ *sqlx.Tx, _ *run.Run) error {
	panic("SaveRun not used in reset tests")
}
func (f *resetFakeRunRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	panic("HasActiveSchedule not used in reset tests")
}
func (f *resetFakeRunRepo) GetActiveScheduler(_ context.Context, _ string) (*run.Run, error) {
	panic("GetActiveScheduler not used in reset tests")
}
func (f *resetFakeRunRepo) GetLastRunPerSchedule(_ context.Context) (map[string]ports.LastRunSummary, error) {
	panic("GetLastRunPerSchedule not used in reset tests")
}

// resetFakeTaskCollection satisfies run.TaskCollection for ResetTask tests.
// GetStatus and Update are the only methods called by Run.ResetTaskToPending.
type resetFakeTaskCollection struct {
	statuses  map[uuid.UUID]run.TaskStatus
	updateErr error
	updated   []run.Task
}

func newResetFakeTaskCollection() *resetFakeTaskCollection {
	return &resetFakeTaskCollection{statuses: make(map[uuid.UUID]run.TaskStatus)}
}

func (f *resetFakeTaskCollection) GetStatus(_ context.Context, taskID uuid.UUID) (run.TaskStatus, bool, error) {
	s, ok := f.statuses[taskID]
	return s, ok, nil
}
func (f *resetFakeTaskCollection) LoadStatusAndAttempt(_ context.Context, taskID uuid.UUID) (run.TaskStatus, int32, bool, error) {
	s, ok := f.statuses[taskID]
	return s, 0, ok, nil
}
func (f *resetFakeTaskCollection) Update(_ context.Context, t run.Task) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	f.updated = append(f.updated, t)
	return nil
}
func (f *resetFakeTaskCollection) BulkCancel(_ context.Context, _ uuid.UUID, _ string) (int, error) {
	panic("BulkCancel not used in reset tests")
}
func (f *resetFakeTaskCollection) BulkCreate(_ context.Context, _ []run.Task) error {
	panic("BulkCreate not used in reset tests")
}
func (f *resetFakeTaskCollection) GetByNode(_ context.Context, _ uuid.UUID, _ run.NodeID) (run.Task, error) {
	panic("GetByNode not used in reset tests")
}
func (f *resetFakeTaskCollection) Exists(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("Exists not used in reset tests")
}
func (f *resetFakeTaskCollection) SetStatusAndAttempt(_ context.Context, _ uuid.UUID, _ run.TaskStatus, _ int32) (int, error) {
	panic("SetStatusAndAttempt not used in reset tests")
}
func (f *resetFakeTaskCollection) HasRetryableFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("HasRetryableFailed not used in reset tests")
}
func (f *resetFakeTaskCollection) HasFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("HasFailed not used in reset tests")
}
func (f *resetFakeTaskCollection) HasNonSucceeded(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("HasNonSucceeded not used in reset tests")
}

// newResetUoWFactory wires a FakeUnitOfWork for ResetTask unit tests.
func newResetUoWFactory(runRepo *resetFakeRunRepo, tasks *resetFakeTaskCollection) (*uow.FakeUnitOfWork, func() uow.UnitOfWork) {
	fu := &uow.FakeUnitOfWork{}
	fu.SetRunRepo(runRepo)
	fu.SetTaskCollection(tasks)
	return fu, func() uow.UnitOfWork { return fu }
}

// newPendingRunForReset builds a minimal PENDING Run aggregate for ResetTask tests.
func newPendingRunForReset(scheduleID uuid.UUID) *run.Run {
	return run.HydrateRun(
		scheduleID, "schedule-name",
		run.SchedulerStatusRunning,
		run.InitStatusCompleted,
		run.Kind("cron"),
		nil,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		sql.NullInt32{Valid: false},
		0,
		nil,
	)
}

// ---- ResetTask tests ----

func TestResetTask_EmptyTaskID(t *testing.T) {
	repo := newStubTaskRepo()
	h := NewTaskHandler(repo, noopUoWFactory(), newTestLogger())

	_, err := h.ResetTask(context.Background(), &statev1.ResetTaskRequest{})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestResetTask_InvalidTaskIDFormat(t *testing.T) {
	repo := newStubTaskRepo()
	h := NewTaskHandler(repo, noopUoWFactory(), newTestLogger())

	_, err := h.ResetTask(context.Background(), &statev1.ResetTaskRequest{
		TaskId: "not-a-uuid",
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestResetTask_RequiresCallerIdentity(t *testing.T) {
	repo := newStubTaskRepo()
	task := makeTask(repo, run.TaskStatusFailed, 3)
	h := NewTaskHandler(repo, noopUoWFactory(), newTestLogger())

	_, err := h.ResetTask(context.Background(), &statev1.ResetTaskRequest{
		TaskId: task.TaskID.String(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestResetTask_NotFound(t *testing.T) {
	repo := newStubTaskRepo()
	h := NewTaskHandler(repo, noopUoWFactory(), newTestLogger())

	_, err := h.ResetTask(ctxWithCaller(run.CallerStartupController), &statev1.ResetTaskRequest{
		TaskId: uuid.NewString(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestResetTask_HappyPath_FailedToStartupController(t *testing.T) {
	repo := newStubTaskRepo()
	task := makeTask(repo, run.TaskStatusFailed, 3)

	tasks := newResetFakeTaskCollection()
	tasks.statuses[task.TaskID] = run.TaskStatusFailed
	runRepo := &resetFakeRunRepo{stored: newPendingRunForReset(task.ScheduleID)}
	_, factory := newResetUoWFactory(runRepo, tasks)

	h := NewTaskHandler(repo, factory, newTestLogger())

	resp, err := h.ResetTask(ctxWithCaller(run.CallerStartupController), &statev1.ResetTaskRequest{
		TaskId: task.TaskID.String(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp.Task)
	assert.Equal(t, statev1.TaskStatus_TASK_STATUS_PENDING, resp.Task.Status)
	assert.Equal(t, int32(0), resp.Task.RetryCount)
	// The pre-reset task fields must be preserved in the response.
	assert.Equal(t, task.ServiceName, resp.Task.ServiceName)
	assert.Equal(t, task.SchemaName, resp.Task.SchemaName)
	assert.Equal(t, task.TableName, resp.Task.TableName)
	assert.Equal(t, int32(task.MaxRetries), resp.Task.MaxRetries)
	// Update must have been called on the task collection.
	require.Len(t, tasks.updated, 1)
	assert.Equal(t, task.TaskID, tasks.updated[0].TaskID)
	assert.Equal(t, run.TaskStatusPending, tasks.updated[0].Status)
}

func TestResetTask_RunIsTerminal_ReturnsFailedPrecondition(t *testing.T) {
	repo := newStubTaskRepo()
	task := makeTask(repo, run.TaskStatusFailed, 0)

	terminalRun := run.HydrateRun(
		task.ScheduleID, "sched",
		run.SchedulerStatusCancelled, // terminal
		run.InitStatusCompleted,
		run.Kind("cron"),
		nil, time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		sql.NullInt32{Valid: false},
		0, nil,
	)
	tasks := newResetFakeTaskCollection()
	tasks.statuses[task.TaskID] = run.TaskStatusFailed
	runRepo := &resetFakeRunRepo{stored: terminalRun}
	_, factory := newResetUoWFactory(runRepo, tasks)

	h := NewTaskHandler(repo, factory, newTestLogger())

	_, err := h.ResetTask(ctxWithCaller(run.CallerStartupController), &statev1.ResetTaskRequest{
		TaskId: task.TaskID.String(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestResetTask_InvalidTransition_ReturnsFailedPrecondition(t *testing.T) {
	// Task is PENDING → transition to PENDING is not in the table, yields ErrInvalidTransition.
	repo := newStubTaskRepo()
	task := makeTask(repo, run.TaskStatusPending, 0)

	tasks := newResetFakeTaskCollection()
	tasks.statuses[task.TaskID] = run.TaskStatusPending // already pending
	runRepo := &resetFakeRunRepo{stored: newPendingRunForReset(task.ScheduleID)}
	_, factory := newResetUoWFactory(runRepo, tasks)

	h := NewTaskHandler(repo, factory, newTestLogger())

	_, err := h.ResetTask(ctxWithCaller(run.CallerStartupController), &statev1.ResetTaskRequest{
		TaskId: task.TaskID.String(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestResetTask_UnauthorizedCaller_ReturnsPermissionDenied(t *testing.T) {
	// executor-controller is not authorized to reset tasks (failed→pending is
	// owned by startup-controller only).
	repo := newStubTaskRepo()
	task := makeTask(repo, run.TaskStatusFailed, 2)

	tasks := newResetFakeTaskCollection()
	tasks.statuses[task.TaskID] = run.TaskStatusFailed
	runRepo := &resetFakeRunRepo{stored: newPendingRunForReset(task.ScheduleID)}
	_, factory := newResetUoWFactory(runRepo, tasks)

	h := NewTaskHandler(repo, factory, newTestLogger())

	_, err := h.ResetTask(ctxWithCaller(run.CallerExecutorController), &statev1.ResetTaskRequest{
		TaskId: task.TaskID.String(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.PermissionDenied, st.Code())
}

func TestResetTask_TaskNotFoundInAggregate_ReturnsNotFound(t *testing.T) {
	// Task exists in the pre-load repo but not in the task collection seen by
	// the aggregate (e.g. race condition). Aggregate returns ErrTaskNotFound.
	repo := newStubTaskRepo()
	task := makeTask(repo, run.TaskStatusFailed, 0)

	tasks := newResetFakeTaskCollection()
	// No entry for task.TaskID in tasks.statuses → GetStatus returns exists=false.
	runRepo := &resetFakeRunRepo{stored: newPendingRunForReset(task.ScheduleID)}
	_, factory := newResetUoWFactory(runRepo, tasks)

	h := NewTaskHandler(repo, factory, newTestLogger())

	_, err := h.ResetTask(ctxWithCaller(run.CallerStartupController), &statev1.ResetTaskRequest{
		TaskId: task.TaskID.String(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

