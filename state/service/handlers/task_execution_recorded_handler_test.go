package handlers_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLoggerExec() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// The handler applies the record to the attempt its Job name names through the
// unit of work's task collection, and returns what the collection fails with.
func TestTaskExecutionRecordedHandler_RecordsThroughTheRunsTasks(t *testing.T) {
	taskID := uuid.New()
	started := time.Now().UTC().Truncate(time.Second)
	completed := started.Add(time.Minute)
	secs := 60.0
	logKey := "logs/job-1-r1.log"
	evt := events.TaskExecutionRecorded{
		ExecutionID: uuid.New(), TaskID: taskID, JobName: "job-1-r1",
		StartedAt: &started, CompletedAt: &completed, ExecutionTimeSeconds: &secs, LogS3Key: &logKey,
	}
	h := handlers.NewTaskExecutionRecordedHandler(testLoggerExec())

	t.Run("records the attempt", func(t *testing.T) {
		tc := &fakeTaskStatusTaskCollection{prevStatus: run.TaskStatusRunning, prevExists: true, prevAttempt: 1}
		u := &uow.FakeUnitOfWork{}
		u.SetTaskCollection(tc)
		require.NoError(t, u.Begin(context.Background()))

		require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

		a, ok := tc.attemptRows[statusAttemptKey{taskID, 1}]
		require.True(t, ok)
		assert.Equal(t, "job-1-r1", a.JobName)
		assert.Equal(t, run.AttemptRunning, a.Status)
		assert.Equal(t, &logKey, a.LogS3Key)
		assert.Equal(t, &secs, a.ExecutionSeconds)
		assert.Equal(t, 0, u.CommitCalled, "the binding owns the transaction")
	})

	t.Run("a failing collection fails the record", func(t *testing.T) {
		tc := &fakeTaskStatusTaskCollection{getStatusErr: errors.New("db down")}
		u := &uow.FakeUnitOfWork{}
		u.SetTaskCollection(tc)
		require.NoError(t, u.Begin(context.Background()))

		err := h.Handle(context.Background(), u, evt, uuid.New())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "db down")
		assert.Empty(t, tc.attemptRows)
	})
}
