package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
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
	tasks     map[uuid.UUID]*model.TaskTracker
	updateErr error
}

func newStubTaskRepo() *stubTaskRepo {
	return &stubTaskRepo{tasks: make(map[uuid.UUID]*model.TaskTracker)}
}

func (s *stubTaskRepo) Create(_ context.Context, task *model.TaskTracker) error {
	s.tasks[task.TaskID] = task
	return nil
}

func (s *stubTaskRepo) GetByID(_ context.Context, id uuid.UUID) (*model.TaskTracker, error) {
	t, ok := s.tasks[id]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	return t, nil
}

func (s *stubTaskRepo) GetByScheduleAndNode(_ context.Context, _ uuid.UUID, _, _, _ string) (*model.TaskTracker, error) {
	return nil, postgres.ErrNotFound
}

func (s *stubTaskRepo) Update(_ context.Context, task *model.TaskTracker) error {
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

func (s *stubTaskRepo) ListByScheduleID(_ context.Context, _ uuid.UUID, _ *model.TaskStatus, _, _ int) ([]*model.TaskTracker, int, error) {
	return nil, 0, nil
}

func (s *stubTaskRepo) List(_ context.Context, _ postgres.TaskFilters) ([]*model.TaskTracker, int, error) {
	return nil, 0, nil
}

func (s *stubTaskRepo) UpdateTx(_ context.Context, _ *sqlx.Tx, task *model.TaskTracker) error {
	if _, ok := s.tasks[task.TaskID]; !ok {
		return postgres.ErrNotFound
	}
	s.tasks[task.TaskID] = task
	return nil
}
func (s *stubTaskRepo) BulkCreateTx(_ context.Context, _ *sqlx.Tx, _ []*model.TaskTracker) error {
	return nil
}
func (s *stubTaskRepo) ListAllByScheduleID(_ context.Context, _ uuid.UUID) ([]*model.TaskTracker, error) {
	return nil, nil
}
func (s *stubTaskRepo) ResetTasksTx(_ context.Context, _ *sqlx.Tx, _ []uuid.UUID) (int32, error) {
	return 0, nil
}
func (s *stubTaskRepo) UpdateStatusIfChangedTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string, _ int32) (int32, error) {
	return 0, nil
}
func (s *stubTaskRepo) ExistsTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (s *stubTaskRepo) HasFailedTaskTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (s *stubTaskRepo) HasRetryableFailedTaskTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (s *stubTaskRepo) BulkCancelByScheduleIDTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) (int64, error) {
	return 0, nil
}

func ctxWithCaller(caller model.CallerID) context.Context {
	md := metadata.Pairs("x-caller-id", string(caller))
	return metadata.NewIncomingContext(context.Background(), md)
}

// helper to create a task in the stub repo
func makeTask(repo *stubTaskRepo, taskStatus model.TaskStatus, retryCount int) *model.TaskTracker {
	task := &model.TaskTracker{
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

// ---- ResetTask tests ----

func TestResetTask_SetsStatusPendingAndRetryCountZero(t *testing.T) {
	repo := newStubTaskRepo()
	task := makeTask(repo, model.TaskStatusFailed, 3)
	h := NewTaskHandler(repo, newTestLogger())

	resp, err := h.ResetTask(ctxWithCaller(model.CallerStartupController), &statev1.ResetTaskRequest{
		TaskId: task.TaskID.String(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp.Task)
	assert.Equal(t, statev1.TaskStatus_TASK_STATUS_PENDING, resp.Task.Status)
	assert.Equal(t, int32(0), resp.Task.RetryCount)

	// verify persisted state
	updated := repo.tasks[task.TaskID]
	assert.Equal(t, model.TaskStatusPending, updated.Status)
	assert.Equal(t, 0, updated.RetryCount)
}

func TestResetTask_NotFound(t *testing.T) {
	repo := newStubTaskRepo()
	h := NewTaskHandler(repo, newTestLogger())

	_, err := h.ResetTask(ctxWithCaller(model.CallerStartupController), &statev1.ResetTaskRequest{
		TaskId: uuid.NewString(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestResetTask_EmptyTaskID(t *testing.T) {
	repo := newStubTaskRepo()
	h := NewTaskHandler(repo, newTestLogger())

	_, err := h.ResetTask(context.Background(), &statev1.ResetTaskRequest{})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestResetTask_InvalidTaskIDFormat(t *testing.T) {
	repo := newStubTaskRepo()
	h := NewTaskHandler(repo, newTestLogger())

	_, err := h.ResetTask(context.Background(), &statev1.ResetTaskRequest{
		TaskId: "not-a-uuid",
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

// ---- ResetTask tests (caller identity enforcement) --

func TestResetTask_RequiresCallerIdentity(t *testing.T) {
	repo := newStubTaskRepo()
	task := makeTask(repo, model.TaskStatusFailed, 3)
	h := NewTaskHandler(repo, newTestLogger())

	_, err := h.ResetTask(context.Background(), &statev1.ResetTaskRequest{
		TaskId: task.TaskID.String(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestResetTask_OnlyStartupControllerCanReset(t *testing.T) {
	repo := newStubTaskRepo()
	task := makeTask(repo, model.TaskStatusFailed, 3)
	h := NewTaskHandler(repo, newTestLogger())

	// k8s-controller should not be allowed to reset tasks
	_, err := h.ResetTask(ctxWithCaller(model.CallerK8sController), &statev1.ResetTaskRequest{
		TaskId: task.TaskID.String(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.PermissionDenied, st.Code())
}

func TestResetTask_StartupControllerCanReset(t *testing.T) {
	repo := newStubTaskRepo()
	task := makeTask(repo, model.TaskStatusFailed, 3)
	h := NewTaskHandler(repo, newTestLogger())

	resp, err := h.ResetTask(ctxWithCaller(model.CallerStartupController), &statev1.ResetTaskRequest{
		TaskId: task.TaskID.String(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp.Task)
	assert.Equal(t, statev1.TaskStatus_TASK_STATUS_PENDING, resp.Task.Status)
	assert.Equal(t, int32(0), resp.Task.RetryCount)

	// verify persisted state
	updated := repo.tasks[task.TaskID]
	assert.Equal(t, model.TaskStatusPending, updated.Status)
	assert.Equal(t, 0, updated.RetryCount)
}

func TestResetTask_OnlyFromFailedState(t *testing.T) {
	repo := newStubTaskRepo()
	task := makeTask(repo, model.TaskStatusRunning, 0)
	h := NewTaskHandler(repo, newTestLogger())

	_, err := h.ResetTask(ctxWithCaller(model.CallerStartupController), &statev1.ResetTaskRequest{
		TaskId: task.TaskID.String(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

