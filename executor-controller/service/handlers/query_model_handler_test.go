package handlers_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
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

func newFakeUoW(depl repository.DeploymentRepository, cancelled postgres.CancelledSchedulesRepository) *uow.FakeUnitOfWork {
	return &uow.FakeUnitOfWork{Deployments: depl, Cancelled: cancelled}
}

func TestQueryModelHandler_EnqueuesDeployment(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(depl, cancelled)

	taskID := uuid.New()
	scheduleID := uuid.New()
	evt := events.QueryModel{
		TaskID: taskID, ScheduleID: scheduleID, ScheduleName: "daily",
		ServiceName: "dbt", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "sha-abc",
	}

	h := handlers.NewQueryModelHandler(logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.Len(t, depl.added, 1)

	dep := depl.added[0]
	assert.Equal(t, model.StatusPending, dep.Status())

	cmd := dep.Command()
	assert.Equal(t, taskID.String(), cmd.TaskID)
	assert.Equal(t, scheduleID.String(), cmd.ScheduleID)
	assert.Equal(t, "dbt-public-orders", cmd.JobName)
	assert.Equal(t, 0, cmd.TaskRetryCount)
	assert.Equal(t, 2, cmd.TaskMaxRetries, "default task max retries off the retry stream")
	assert.True(t, dep.IsDeployable())
}

func TestQueryModelHandler_DropsWhenScheduleCancelled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	scheduleID := uuid.New()
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{scheduleID: true}}
	u := newFakeUoW(depl, cancelled)

	evt := events.QueryModel{TaskID: uuid.New(), ScheduleID: scheduleID, NodeType: pkg_model.NodeTypeDbtModel}

	h := handlers.NewQueryModelHandler(logger)
	// Cancelled-schedule path returns nil so the binding commits and ACKs the
	// message rather than leaving it pending for endless redelivery.
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	assert.Empty(t, depl.added, "no deployment enqueued when schedule is cancelled")
}

func TestQueryModelHandler_PropagatesMsgProcIDToDeployment(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(depl, cancelled)

	msgProcID := uuid.New()
	evt := events.QueryModel{
		TaskID: uuid.New(), ScheduleID: uuid.New(),
		OutboxEntryID: uuid.New(), NodeType: pkg_model.NodeTypeDbtModel, JobName: "j",
	}

	h := handlers.NewQueryModelHandler(logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, msgProcID))
	require.Len(t, depl.added, 1)

	dep := depl.added[0]
	require.NotNil(t, dep.MessageProcessingID())
	assert.Equal(t, msgProcID, *dep.MessageProcessingID())
	assert.NotEqual(t, evt.OutboxEntryID, *dep.MessageProcessingID(),
		"orchestrator's OutboxEntryID must never be used as the executor's message_processing FK")
}
