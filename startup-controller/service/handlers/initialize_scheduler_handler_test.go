package handlers_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	grpcadapter "github.com/carolsimone/continuo/startup-controller/adapters/grpc"
	"github.com/carolsimone/continuo/startup-controller/adapters/postgres"
	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/domain/model"
	"github.com/carolsimone/continuo/startup-controller/service/handlers"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── mocks ────────────────────────────────────────────────────────────────────

type mockInitStateClient struct {
	updateSchedulerInitStatusFn func(ctx context.Context, scheduleID uuid.UUID, initStatus string) error

	// call-order tracking
	callLog []string
}

func (m *mockInitStateClient) UpdateSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID, initStatus string) error {
	m.callLog = append(m.callLog, "init:"+initStatus)
	if m.updateSchedulerInitStatusFn != nil {
		return m.updateSchedulerInitStatusFn(ctx, scheduleID, initStatus)
	}
	return nil
}

type mockStartupOutboxRepo struct {
	CreatedEntries []*model.OutboxEntry
}

func (m *mockStartupOutboxRepo) Create(ctx context.Context, entry *model.OutboxEntry) error {
	m.CreatedEntries = append(m.CreatedEntries, entry)
	return nil
}
func (m *mockStartupOutboxRepo) GetPendingBatch(ctx context.Context, limit int) ([]*model.OutboxEntry, error) {
	return nil, nil
}
func (m *mockStartupOutboxRepo) MarkProcessed(ctx context.Context, id uuid.UUID) error { return nil }
func (m *mockStartupOutboxRepo) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	return nil
}
func (m *mockStartupOutboxRepo) IncrementRetry(ctx context.Context, id uuid.UUID) error { return nil }

type mockStartupUoW struct {
	outboxRepo *mockStartupOutboxRepo
}

func newMockStartupUoW() *mockStartupUoW {
	return &mockStartupUoW{outboxRepo: &mockStartupOutboxRepo{}}
}

func (m *mockStartupUoW) OutboxRepo() postgres.OutboxRepository { return m.outboxRepo }
func (m *mockStartupUoW) Begin(ctx context.Context) error       { return nil }
func (m *mockStartupUoW) Commit() error                         { return nil }
func (m *mockStartupUoW) Rollback() error                       { return nil }

func newStartupLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// ── tests ─────────────────────────────────────────────────────────────────────

// Handler must emit an initialize.run:v1 event to the outbox and set
// init_status to "in_progress" (no "completed" — that moves to HandleRunInitialized).
func TestHandle_EmitsInitializeRunEvent(t *testing.T) {
	stateClient := &mockInitStateClient{}
	uow := newMockStartupUoW()

	h := handlers.NewInitializeSchedulerHandler(uow, stateClient, "initialize.run:v1", newStartupLogger())

	cmd := command.InitializeScheduler{RunnerID: uuid.New(), ScheduleName: "daily"}
	err := h.Handle(context.Background(), cmd)

	require.NoError(t, err)

	// Verify init_status was set to "in_progress"
	require.NotEmpty(t, stateClient.callLog)
	assert.Equal(t, "init:in_progress", stateClient.callLog[0])

	// Verify exactly one outbox entry was written
	require.Len(t, uow.outboxRepo.CreatedEntries, 1)
	entry := uow.outboxRepo.CreatedEntries[0]
	assert.Equal(t, "initialize.run:v1", entry.StreamName)
	assert.Equal(t, "run_initialization_requested", entry.EventType)
	assert.Equal(t, "scheduler", entry.AggregateType)

	// Verify payload contents
	var payload map[string]string
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, "daily", payload["schedule_name"])
	assert.Equal(t, cmd.RunnerID.String(), payload["run_id"])
}

// When already completed, handler should skip without writing to outbox.
func TestHandle_SkipsWhenAlreadyCompleted(t *testing.T) {
	stateClient := &mockInitStateClient{
		updateSchedulerInitStatusFn: func(_ context.Context, _ uuid.UUID, _ string) error {
			return grpcadapter.ErrAlreadyCompleted
		},
	}
	uow := newMockStartupUoW()

	h := handlers.NewInitializeSchedulerHandler(uow, stateClient, "initialize.run:v1", newStartupLogger())

	cmd := command.InitializeScheduler{RunnerID: uuid.New(), ScheduleName: "daily"}
	err := h.Handle(context.Background(), cmd)

	require.NoError(t, err)
	assert.Empty(t, uow.outboxRepo.CreatedEntries, "no outbox entry when already completed")
}
