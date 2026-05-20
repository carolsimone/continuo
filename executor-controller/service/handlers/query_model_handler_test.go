package handlers_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/deployer"
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

func newFakeUoW(depl deployer.Repository, cancelled postgres.CancelledSchedulesRepository) *uow.FakeUnitOfWork {
	return &uow.FakeUnitOfWork{Deployments: depl, Cancelled: cancelled}
}

func TestQueryModelHandler_WritesDeploymentRow(t *testing.T) {
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
	require.Len(t, depl.rows, 1)

	row := depl.rows[0]
	assert.Equal(t, taskID, row.TaskID)
	assert.Equal(t, scheduleID, row.ScheduleID)
	assert.Equal(t, "pending", row.Status)

	var job deployer.DeployJob
	require.NoError(t, json.Unmarshal(row.JobParams, &job))
	assert.Equal(t, taskID.String(), job.TaskID)
	assert.Equal(t, "dbt-public-orders", job.JobName)
	assert.Equal(t, 0, job.TaskRetryCount)
	assert.Equal(t, 2, job.TaskMaxRetries, "default task max retries off the retry stream")
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
	assert.Empty(t, depl.rows, "no deployment row when schedule is cancelled")
}

func TestQueryModelHandler_PropagatesMsgProcIDToDeploymentRow(t *testing.T) {
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
	require.Len(t, depl.rows, 1)

	row := depl.rows[0]
	require.NotNil(t, row.MessageProcessingID)
	assert.Equal(t, msgProcID, *row.MessageProcessingID)
	assert.NotEqual(t, evt.OutboxEntryID, *row.MessageProcessingID,
		"orchestrator's OutboxEntryID must never be used as the executor's message_processing FK")
}
