package redis_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	"github.com/carolsimone/continuo/orchestrator/adapters/redis"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeCancelledRepo struct {
	inserted []uuid.UUID
}

func (f *fakeCancelledRepo) Insert(_ context.Context, id uuid.UUID) error {
	f.inserted = append(f.inserted, id)
	return nil
}
func (f *fakeCancelledRepo) Exists(_ context.Context, _ uuid.UUID) (bool, error) { return false, nil }
func (f *fakeCancelledRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

var _ postgres.CancelledSchedulesRepository = (*fakeCancelledRepo)(nil)

func TestScheduleCancelledHandler_InsertsScheduleID(t *testing.T) {
	repo := &fakeCancelledRepo{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := redis.NewScheduleCancelledHandler(repo, logger)

	id := uuid.New()
	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"schedule_id":   id.String(),
			"schedule_name": "test-schedule",
		},
	}

	err := handler(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, repo.inserted, 1)
	assert.Equal(t, id, repo.inserted[0])
}

func TestScheduleCancelledHandler_DiscardsBadUUID(t *testing.T) {
	repo := &fakeCancelledRepo{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := redis.NewScheduleCancelledHandler(repo, logger)

	msg := goredis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{"schedule_id": "not-a-uuid"},
	}

	err := handler(context.Background(), msg)
	require.NoError(t, err)
	assert.Empty(t, repo.inserted)
}
