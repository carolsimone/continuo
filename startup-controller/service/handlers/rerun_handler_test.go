package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	grpcadapter "github.com/carolsimone/continuo/startup-controller/adapters/grpc"
	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/service/handlers"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes ─────────────────────────────────────────────────────────────────────

type fakeRerunStateClient struct {
	updateInitStatusFn    func(ctx context.Context, scheduleID uuid.UUID, initStatus string) error
	updateInitStatusCalls []string
}

func (f *fakeRerunStateClient) UpdateSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID, initStatus string) error {
	f.updateInitStatusCalls = append(f.updateInitStatusCalls, initStatus)
	if f.updateInitStatusFn != nil {
		return f.updateInitStatusFn(ctx, scheduleID, initStatus)
	}
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newRerunCmd() command.RerunNode {
	return command.RerunNode{
		ScheduleID:   uuid.New(),
		ScheduleName: "test-schedule",
		SchemaName:   "s",
		TableName:    "A",
		ServiceName:  "svc",
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestRerunHandler_SkipsWhenAlreadyCompleted(t *testing.T) {
	stateClient := &fakeRerunStateClient{
		updateInitStatusFn: func(_ context.Context, _ uuid.UUID, _ string) error {
			return grpcadapter.ErrAlreadyCompleted
		},
	}
	uow := newMockStartupUoW()
	h := handlers.NewRerunHandler(uow, stateClient, "initialize.run:v1", newStartupLogger())

	err := h.Handle(context.Background(), newRerunCmd())
	require.NoError(t, err)
	assert.Empty(t, uow.outboxRepo.CreatedEntries, "no outbox entry when already completed")
}

func TestRerunHandler_SkipsWhenErrAlreadyCompletedWrapped(t *testing.T) {
	stateClient := &fakeRerunStateClient{
		updateInitStatusFn: func(_ context.Context, _ uuid.UUID, _ string) error {
			return fmt.Errorf("wrapper: %w", grpcadapter.ErrAlreadyCompleted)
		},
	}
	uow := newMockStartupUoW()
	h := handlers.NewRerunHandler(uow, stateClient, "initialize.run:v1", newStartupLogger())

	err := h.Handle(context.Background(), newRerunCmd())
	require.NoError(t, err)
	assert.Empty(t, uow.outboxRepo.CreatedEntries, "no outbox entry when already completed (wrapped)")
}

func TestRerunHandler_EmitsInitializeRunEventWithRerunTarget(t *testing.T) {
	stateClient := &fakeRerunStateClient{}
	uow := newMockStartupUoW()
	h := handlers.NewRerunHandler(uow, stateClient, "initialize.run:v1", newStartupLogger())

	cmd := newRerunCmd()
	err := h.Handle(context.Background(), cmd)
	require.NoError(t, err)

	// Verify init_status was set to "in_progress"
	require.NotEmpty(t, stateClient.updateInitStatusCalls)
	assert.Equal(t, "in_progress", stateClient.updateInitStatusCalls[0])

	// Verify exactly one outbox entry was written
	require.Len(t, uow.outboxRepo.CreatedEntries, 1)
	entry := uow.outboxRepo.CreatedEntries[0]
	assert.Equal(t, "initialize.run:v1", entry.StreamName)
	assert.Equal(t, "rerun_requested", entry.EventType)

	// Verify payload contents include flat rerun_* fields
	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, cmd.ScheduleName, payload["schedule_name"])
	assert.Equal(t, cmd.ScheduleID.String(), payload["run_id"])
	assert.Equal(t, cmd.ServiceName, payload["rerun_service_name"])
	assert.Equal(t, cmd.SchemaName, payload["rerun_schema_name"])
	assert.Equal(t, cmd.TableName, payload["rerun_table_name"])
}
