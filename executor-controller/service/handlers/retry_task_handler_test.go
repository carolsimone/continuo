// executor-controller/service/handlers/retry_task_handler_test.go
package handlers_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/deployer"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryTaskHandler_PropagatesRetryFields(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(depl, cancelled)

	evt := events.RetryTask{
		QueryModel: events.QueryModel{
			TaskID:     uuid.New(),
			ScheduleID: uuid.New(),
			NodeType:   pkg_model.NodeTypeDbtModel,
			JobName:    "j",
		},
		TaskRetryCount: 3,
		MaxRetries:     7,
	}

	h := handlers.NewRetryTaskHandler(logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.Len(t, depl.rows, 1)

	var job deployer.DeployJob
	require.NoError(t, json.Unmarshal(depl.rows[0].JobParams, &job))
	assert.Equal(t, 3, job.TaskRetryCount)
	assert.Equal(t, 7, job.TaskMaxRetries)
}

func TestRetryTaskHandler_DefaultsMaxRetriesWhenZero(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(depl, cancelled)

	evt := events.RetryTask{
		QueryModel: events.QueryModel{
			TaskID:     uuid.New(),
			ScheduleID: uuid.New(),
			NodeType:   pkg_model.NodeTypeDbtModel,
		},
		TaskRetryCount: 1,
		MaxRetries:     0,
	}

	h := handlers.NewRetryTaskHandler(logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.Len(t, depl.rows, 1)

	var job deployer.DeployJob
	require.NoError(t, json.Unmarshal(depl.rows[0].JobParams, &job))
	assert.Equal(t, 2, job.TaskMaxRetries, "max_retries=0 on the wire falls back to service default 2")
}

func TestRetryTaskHandler_DropsWhenScheduleCancelled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	scheduleID := uuid.New()
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{scheduleID: true}}
	u := newFakeUoW(depl, cancelled)

	evt := events.RetryTask{
		QueryModel: events.QueryModel{
			TaskID:     uuid.New(),
			ScheduleID: scheduleID,
			NodeType:   pkg_model.NodeTypeDbtModel,
		},
		TaskRetryCount: 2,
		MaxRetries:     5,
	}

	h := handlers.NewRetryTaskHandler(logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	assert.Empty(t, depl.rows)
}
