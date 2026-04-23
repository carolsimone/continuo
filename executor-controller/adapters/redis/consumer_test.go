package redis

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type fakeCancelledRepo struct {
	ids map[uuid.UUID]bool
}

func (f *fakeCancelledRepo) Insert(_ context.Context, id uuid.UUID) error { return nil }
func (f *fakeCancelledRepo) Exists(_ context.Context, id uuid.UUID) (bool, error) {
	return f.ids[id], nil
}
func (f *fakeCancelledRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

var _ postgres.CancelledSchedulesRepository = (*fakeCancelledRepo)(nil)

func TestConsumer_DropsQueryModelWhenScheduleCancelled(t *testing.T) {
	scheduleID := uuid.New()
	taskID := uuid.New()

	cancelledRepo := &fakeCancelledRepo{ids: map[uuid.UUID]bool{scheduleID: true}}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Consumer with minimal setup (no real Redis needed)
	c := &Consumer{
		cancelledSchedules: cancelledRepo,
		consumerGroup:      "test-group",
		logger:             logger,
	}

	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"schedule_id":   scheduleID.String(),
			"task_id":       taskID.String(),
			"schedule_name": "s",
			"service_name":  "svc",
			"schema_name":   "sc",
			"table_name":    "t",
			"job_name":      "j",
			"node_type":     "dbt_model",
		},
	}

	// The guard should short-circuit before messageBus is called.
	// Since messageBus is nil, if the guard doesn't fire, it will panic.
	// A panic means the guard is missing; returning nil means it worked.
	err := c.processMessage(context.Background(), msg, "query.model:v1")
	require.NoError(t, err, "should no-op when schedule is cancelled, not panic")
}
