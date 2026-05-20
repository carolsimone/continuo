// executor-controller/service/handlers/retry_task_handler_test.go
package handlers_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/event"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryTaskHandler_PropagatesRetryFields(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	stub := &stubOutboxRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(stub, cancelled)

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
	err := h.Handle(context.Background(), u, evt, uuid.New())
	require.NoError(t, err)
	require.Len(t, stub.entries, 1)

	var d event.DeployTask
	require.NoError(t, json.Unmarshal(stub.entries[0].Payload, &d))
	assert.Equal(t, 3, d.TaskRetryCount)
	assert.Equal(t, 7, d.TaskMaxRetries)
}

func TestRetryTaskHandler_DefaultsMaxRetriesWhenZero(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	stub := &stubOutboxRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(stub, cancelled)

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
	err := h.Handle(context.Background(), u, evt, uuid.New())
	require.NoError(t, err)
	require.Len(t, stub.entries, 1)

	var d event.DeployTask
	require.NoError(t, json.Unmarshal(stub.entries[0].Payload, &d))
	assert.Equal(t, 2, d.TaskMaxRetries, "max_retries=0 on the wire falls back to service default 2")
}

func TestRetryTaskHandler_DropsWhenScheduleCancelled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	stub := &stubOutboxRepo{}
	scheduleID := uuid.New()
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{scheduleID: true}}
	u := newFakeUoW(stub, cancelled)

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
	err := h.Handle(context.Background(), u, evt, uuid.New())
	require.NoError(t, err)
	assert.Empty(t, stub.entries)
}
