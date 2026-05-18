package handlers_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubCancelledRepo returns Exists()=true for any ID in ids.
type stubCancelledRepo struct {
	ids map[uuid.UUID]bool
}

func (r *stubCancelledRepo) Insert(_ context.Context, _ uuid.UUID) error { return nil }
func (r *stubCancelledRepo) Exists(_ context.Context, id uuid.UUID) (bool, error) {
	return r.ids[id], nil
}
func (r *stubCancelledRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

func newFakeUoW(outbox postgres.OutboxRepository, cancelled postgres.CancelledSchedulesRepository) *uow.FakeUnitOfWork {
	return &uow.FakeUnitOfWork{Outbox: outbox, Cancelled: cancelled}
}

func TestQueryModelHandler_WritesOutboxRow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	outbox := &stubOutboxRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(outbox, cancelled)

	taskID := uuid.New()
	scheduleID := uuid.New()
	evt := events.QueryModel{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "orders",
		JobName:      "dbt-public-orders",
		NodeType:     pkg_model.NodeTypeDbtModel,
		ImageTag:     "sha-abc",
	}

	h := handlers.NewQueryModelHandler(logger)
	err := h.Handle(context.Background(), u, evt, uuid.New())
	require.NoError(t, err)
	require.Len(t, outbox.entries, 1)
	assert.Equal(t, taskID, outbox.entries[0].TaskID)
	assert.Equal(t, "dbt-public-orders", outbox.entries[0].JobName)
	assert.Equal(t, 0, outbox.entries[0].TaskRetryCount)
	assert.Equal(t, 2, outbox.entries[0].TaskMaxRetries, "default max retries when not on the retry stream")
}

func TestQueryModelHandler_DropsWhenScheduleCancelled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	outbox := &stubOutboxRepo{}
	scheduleID := uuid.New()
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{scheduleID: true}}
	u := newFakeUoW(outbox, cancelled)

	evt := events.QueryModel{
		TaskID:     uuid.New(),
		ScheduleID: scheduleID,
		NodeType:   pkg_model.NodeTypeDbtModel,
	}

	h := handlers.NewQueryModelHandler(logger)
	err := h.Handle(context.Background(), u, evt, uuid.New())
	require.NoError(t, err, "cancelled-schedule path returns nil so the binding commits and ACKs")
	assert.Empty(t, outbox.entries, "no outbox row when schedule is cancelled")
}
