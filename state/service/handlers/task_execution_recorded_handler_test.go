package handlers_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeTaskExecutionRepo satisfies postgres.TaskExecutionRepository for handler
// unit tests. Only CreateTx is exercised by the handler; the other methods
// are stubbed to return zero values.
type fakeTaskExecutionRepo struct {
	created   []*postgres.TaskExecution
	createErr error
}

func (f *fakeTaskExecutionRepo) Create(_ context.Context, _ *postgres.TaskExecution) error {
	return nil
}

func (f *fakeTaskExecutionRepo) CreateTx(_ context.Context, _ *sqlx.Tx, e *postgres.TaskExecution) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.created = append(f.created, e)
	return nil
}

func (f *fakeTaskExecutionRepo) GetByID(_ context.Context, _ uuid.UUID) (*postgres.TaskExecution, error) {
	return nil, nil
}

func (f *fakeTaskExecutionRepo) ListByScheduleID(_ context.Context, _ string, _, _ int) ([]*postgres.TaskExecution, int, error) {
	return nil, 0, nil
}

func testLoggerExec() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestTaskExecutionRecordedHandler_HappyPath(t *testing.T) {
	repo := &fakeTaskExecutionRepo{}
	u := &uow.FakeUnitOfWork{TaskExecution: repo}
	require.NoError(t, u.Begin(context.Background()))

	h := handlers.NewTaskExecutionRecordedHandler(testLoggerExec())

	execID, taskID := uuid.New(), uuid.New()
	startedAt := time.Now().UTC().Truncate(time.Second)
	completedAt := startedAt.Add(time.Minute)
	secs := 60.0
	jobName, logKey := "job-1", "s3://bucket/key"

	err := h.Handle(context.Background(), u, events.TaskExecutionRecorded{
		ExecutionID:          execID,
		TaskID:               taskID,
		JobName:              &jobName,
		StartedAt:            &startedAt,
		CompletedAt:          &completedAt,
		ExecutionTimeSeconds: &secs,
		LogS3Key:             &logKey,
	}, uuid.New())
	require.NoError(t, err)

	require.Len(t, repo.created, 1)
	got := repo.created[0]
	assert.Equal(t, execID, got.ID)
	assert.Equal(t, taskID, got.TaskID)
	require.NotNil(t, got.StartedAt)
	assert.True(t, got.StartedAt.Equal(startedAt))
	require.NotNil(t, got.CompletedAt)
	assert.True(t, got.CompletedAt.Equal(completedAt))
	require.NotNil(t, got.ExecutionTimeSeconds)
	assert.InDelta(t, secs, *got.ExecutionTimeSeconds, 0.001)
	require.NotNil(t, got.K8sJobName)
	assert.Equal(t, jobName, *got.K8sJobName)
	require.NotNil(t, got.LogS3Key)
	assert.Equal(t, logKey, *got.LogS3Key)
	assert.Nil(t, got.ErrorMessage)
	assert.Nil(t, got.ExecutorID)
}

// TestTaskExecutionRecordedHandler_OptionalFieldsNilWhenAbsent verifies the
// handler maps absent (nil) event pointers straight through to nil model
// fields without materializing empty strings.
func TestTaskExecutionRecordedHandler_OptionalFieldsNilWhenAbsent(t *testing.T) {
	repo := &fakeTaskExecutionRepo{}
	u := &uow.FakeUnitOfWork{TaskExecution: repo}
	require.NoError(t, u.Begin(context.Background()))

	h := handlers.NewTaskExecutionRecordedHandler(testLoggerExec())

	execID, taskID := uuid.New(), uuid.New()
	err := h.Handle(context.Background(), u, events.TaskExecutionRecorded{
		ExecutionID: execID,
		TaskID:      taskID,
	}, uuid.New())
	require.NoError(t, err)

	require.Len(t, repo.created, 1)
	got := repo.created[0]
	assert.Equal(t, execID, got.ID)
	assert.Equal(t, taskID, got.TaskID)
	assert.Nil(t, got.StartedAt)
	assert.Nil(t, got.CompletedAt)
	assert.Nil(t, got.ExecutionTimeSeconds)
	assert.Nil(t, got.K8sJobName)
	assert.Nil(t, got.ErrorMessage)
	assert.Nil(t, got.LogS3Key)
	assert.Nil(t, got.ExecutorID)
}

func TestTaskExecutionRecordedHandler_RepoErrorPropagates(t *testing.T) {
	repo := &fakeTaskExecutionRepo{createErr: errors.New("db down")}
	u := &uow.FakeUnitOfWork{TaskExecution: repo}
	require.NoError(t, u.Begin(context.Background()))

	h := handlers.NewTaskExecutionRecordedHandler(testLoggerExec())
	err := h.Handle(context.Background(), u, events.TaskExecutionRecorded{
		ExecutionID: uuid.New(),
		TaskID:      uuid.New(),
	}, uuid.New())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create task_execution")
}
