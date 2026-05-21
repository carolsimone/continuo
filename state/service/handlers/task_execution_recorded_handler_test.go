package handlers_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeTaskExecutionWriter satisfies repository.TaskExecutionWriter for handler
// unit tests. It captures the recorded-execution events the handler forwards
// so tests can assert the execution was persisted. The event-to-row mapping
// itself lives in the postgres adapter and is exercised there.
type fakeTaskExecutionWriter struct {
	recorded  []events.TaskExecutionRecorded
	createErr error
}

func (f *fakeTaskExecutionWriter) CreateRecord(_ context.Context, evt events.TaskExecutionRecorded) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.recorded = append(f.recorded, evt)
	return nil
}

func testLoggerExec() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestTaskExecutionRecordedHandler_HappyPath(t *testing.T) {
	writer := &fakeTaskExecutionWriter{}
	u := &uow.FakeUnitOfWork{TaskExecutionWriter: writer}
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

	require.Len(t, writer.recorded, 1)
	got := writer.recorded[0]
	assert.Equal(t, execID, got.ExecutionID)
	assert.Equal(t, taskID, got.TaskID)
	require.NotNil(t, got.StartedAt)
	assert.True(t, got.StartedAt.Equal(startedAt))
	require.NotNil(t, got.CompletedAt)
	assert.True(t, got.CompletedAt.Equal(completedAt))
	require.NotNil(t, got.ExecutionTimeSeconds)
	assert.InDelta(t, secs, *got.ExecutionTimeSeconds, 0.001)
	require.NotNil(t, got.JobName)
	assert.Equal(t, jobName, *got.JobName)
	require.NotNil(t, got.LogS3Key)
	assert.Equal(t, logKey, *got.LogS3Key)
	assert.Nil(t, got.ErrorMessage)
}

// TestTaskExecutionRecordedHandler_OptionalFieldsNilWhenAbsent verifies the
// handler forwards absent (nil) event pointers straight through to the writer
// without materializing empty values.
func TestTaskExecutionRecordedHandler_OptionalFieldsNilWhenAbsent(t *testing.T) {
	writer := &fakeTaskExecutionWriter{}
	u := &uow.FakeUnitOfWork{TaskExecutionWriter: writer}
	require.NoError(t, u.Begin(context.Background()))

	h := handlers.NewTaskExecutionRecordedHandler(testLoggerExec())

	execID, taskID := uuid.New(), uuid.New()
	err := h.Handle(context.Background(), u, events.TaskExecutionRecorded{
		ExecutionID: execID,
		TaskID:      taskID,
	}, uuid.New())
	require.NoError(t, err)

	require.Len(t, writer.recorded, 1)
	got := writer.recorded[0]
	assert.Equal(t, execID, got.ExecutionID)
	assert.Equal(t, taskID, got.TaskID)
	assert.Nil(t, got.StartedAt)
	assert.Nil(t, got.CompletedAt)
	assert.Nil(t, got.ExecutionTimeSeconds)
	assert.Nil(t, got.JobName)
	assert.Nil(t, got.ErrorMessage)
	assert.Nil(t, got.LogS3Key)
}

func TestTaskExecutionRecordedHandler_RepoErrorPropagates(t *testing.T) {
	writer := &fakeTaskExecutionWriter{createErr: errors.New("db down")}
	u := &uow.FakeUnitOfWork{TaskExecutionWriter: writer}
	require.NoError(t, u.Begin(context.Background()))

	h := handlers.NewTaskExecutionRecordedHandler(testLoggerExec())
	err := h.Handle(context.Background(), u, events.TaskExecutionRecorded{
		ExecutionID: uuid.New(),
		TaskID:      uuid.New(),
	}, uuid.New())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create task_execution")
}
