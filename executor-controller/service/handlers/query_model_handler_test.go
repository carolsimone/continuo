package handlers_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/event"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
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

func newFakeUoW(outbox pkgoutbox.Repository, cancelled postgres.CancelledSchedulesRepository) *uow.FakeUnitOfWork {
	return &uow.FakeUnitOfWork{Outbox: outbox, Cancelled: cancelled}
}

func TestQueryModelHandler_WritesOutboxRow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	stub := &stubOutboxRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(stub, cancelled)

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
	require.Len(t, stub.entries, 1)

	entry := stub.entries[0]
	assert.Equal(t, "task", entry.AggregateType)
	assert.Equal(t, taskID, entry.AggregateID)
	assert.Equal(t, "deploy_task", entry.EventType)

	var d event.DeployTask
	require.NoError(t, json.Unmarshal(entry.Payload, &d))
	assert.Equal(t, taskID.String(), d.TaskID)
	assert.Equal(t, "dbt-public-orders", d.JobName)
	assert.Equal(t, 0, d.TaskRetryCount)
	assert.Equal(t, 2, d.TaskMaxRetries, "default max retries when not on the retry stream")
}

func TestQueryModelHandler_DropsWhenScheduleCancelled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	stub := &stubOutboxRepo{}
	scheduleID := uuid.New()
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{scheduleID: true}}
	u := newFakeUoW(stub, cancelled)

	evt := events.QueryModel{
		TaskID:     uuid.New(),
		ScheduleID: scheduleID,
		NodeType:   pkg_model.NodeTypeDbtModel,
	}

	h := handlers.NewQueryModelHandler(logger)
	err := h.Handle(context.Background(), u, evt, uuid.New())
	require.NoError(t, err, "cancelled-schedule path returns nil so the binding commits and ACKs")
	assert.Empty(t, stub.entries, "no outbox row when schedule is cancelled")
}
