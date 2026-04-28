package redis_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/adapters/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRunFinalizer struct {
	calledRunID  string
	calledStatus string
	err          error
}

func (f *fakeRunFinalizer) FinalizeRun(_ context.Context, runID, terminalStatus string) error {
	f.calledRunID = runID
	f.calledStatus = terminalStatus
	return f.err
}

func TestRunFinalizedHandler_CallsFinalizeRun(t *testing.T) {
	finalizer := &fakeRunFinalizer{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := redis.NewRunFinalizedHandler(finalizer, logger)

	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"schedule_id":   "550e8400-e29b-41d4-a716-446655440000",
			"schedule_name": "daily-run",
			"status":        "succeeded",
		},
	}

	err := handler(context.Background(), msg)
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", finalizer.calledRunID)
	assert.Equal(t, "succeeded", finalizer.calledStatus)
}

func TestRunFinalizedHandler_DiscardsEmptyScheduleID(t *testing.T) {
	finalizer := &fakeRunFinalizer{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := redis.NewRunFinalizedHandler(finalizer, logger)

	msg := goredis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{"schedule_id": "", "status": "succeeded"},
	}

	err := handler(context.Background(), msg)
	require.NoError(t, err)
	assert.Empty(t, finalizer.calledRunID, "FinalizeRun must not be called for empty schedule_id")
}

func TestRunFinalizedHandler_DiscardsEmptyStatus(t *testing.T) {
	finalizer := &fakeRunFinalizer{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := redis.NewRunFinalizedHandler(finalizer, logger)

	msg := goredis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{"schedule_id": "550e8400-e29b-41d4-a716-446655440000", "status": ""},
	}

	err := handler(context.Background(), msg)
	require.NoError(t, err)
	assert.Empty(t, finalizer.calledRunID, "FinalizeRun must not be called for empty status")
}
