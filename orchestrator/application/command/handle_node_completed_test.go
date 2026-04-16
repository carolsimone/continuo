package command_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/application/command"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/infrastructure/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes: run.Repository ─────────────────────────────────────────────────────

type fakeRunRepository struct {
	// Configurable behaviour
	updateNodeStatusFn        func(ctx context.Context, runID, scheduleName, schema, tableName, status string) error
	getReadyDownstreamFn      func(ctx context.Context, runID, scheduleName, schema, tableName string) ([]*run.DownstreamNode, error)
	checkScheduleCompletionFn func(ctx context.Context, runID, scheduleName string) (bool, bool, error)
	finalizeRunFn             func(ctx context.Context, runID, terminalStatus string) error

	// Call-capture
	updateNodeStatusCalls   int
	getReadyDownstreamCalls int
}

func (f *fakeRunRepository) UpdateNodeStatus(ctx context.Context, runID, scheduleName, schema, tableName, status string) error {
	f.updateNodeStatusCalls++
	if f.updateNodeStatusFn != nil {
		return f.updateNodeStatusFn(ctx, runID, scheduleName, schema, tableName, status)
	}
	return nil
}

func (f *fakeRunRepository) GetReadyDownstream(ctx context.Context, runID, scheduleName, schema, tableName string) ([]*run.DownstreamNode, error) {
	f.getReadyDownstreamCalls++
	if f.getReadyDownstreamFn != nil {
		return f.getReadyDownstreamFn(ctx, runID, scheduleName, schema, tableName)
	}
	return nil, nil
}

func (f *fakeRunRepository) CheckScheduleCompletion(ctx context.Context, runID, scheduleName string) (bool, bool, error) {
	if f.checkScheduleCompletionFn != nil {
		return f.checkScheduleCompletionFn(ctx, runID, scheduleName)
	}
	return false, false, nil
}

func (f *fakeRunRepository) FinalizeRun(ctx context.Context, runID, terminalStatus string) error {
	if f.finalizeRunFn != nil {
		return f.finalizeRunFn(ctx, runID, terminalStatus)
	}
	return nil
}

// Stubs for read-side methods (unused by the handler)
func (f *fakeRunRepository) SnapshotGraph(ctx context.Context, runID, scheduleName string) error {
	return nil
}
func (f *fakeRunRepository) GetScheduleInitNodes(ctx context.Context, scheduleName, runID string) (*run.ScheduleInitNodes, error) {
	return nil, nil
}
func (f *fakeRunRepository) GetTransitiveDownstream(ctx context.Context, scheduleName, schema, tableName string) ([]*domain.TableNode, error) {
	return nil, nil
}
func (f *fakeRunRepository) GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error) {
	return nil, nil
}
func (f *fakeRunRepository) ListRuns(ctx context.Context, scheduleName string) ([]*domain.RunSummary, error) {
	return nil, nil
}
func (f *fakeRunRepository) GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error) {
	return nil, nil, nil
}
func (f *fakeRunRepository) GetNodeType(ctx context.Context, schema, tableName string) (string, error) {
	return "dbt-model", nil
}
func (f *fakeRunRepository) GetNodeServiceName(ctx context.Context, schema, tableName string) (string, error) {
	return "test-service", nil
}

// ── fakes: StateTaskClient ────────────────────────────────────────────────────

type fakeStateClient struct {
	getTaskFn                func(ctx context.Context, scheduleID uuid.UUID, serviceName, schema, tableName string) (string, error)
	getSchedulerInitStatusFn func(ctx context.Context, scheduleID uuid.UUID) (string, error)
	updateSchedulerStatusFn  func(ctx context.Context, scheduleID uuid.UUID, status string) error

	updateSchedulerCalled  bool
	capturedSchedulerStatus string
}

func (f *fakeStateClient) GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schema, tableName string) (string, error) {
	if f.getTaskFn != nil {
		return f.getTaskFn(ctx, scheduleID, serviceName, schema, tableName)
	}
	return uuid.New().String(), nil
}

func (f *fakeStateClient) GetSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID) (string, error) {
	if f.getSchedulerInitStatusFn != nil {
		return f.getSchedulerInitStatusFn(ctx, scheduleID)
	}
	return "completed", nil
}

func (f *fakeStateClient) UpdateSchedulerStatus(ctx context.Context, scheduleID uuid.UUID, status string) error {
	f.updateSchedulerCalled = true
	f.capturedSchedulerStatus = status
	if f.updateSchedulerStatusFn != nil {
		return f.updateSchedulerStatusFn(ctx, scheduleID, status)
	}
	return nil
}

// ── fakes: outbox and message processing repos ────────────────────────────────

type fakeOutboxRepository struct {
	CreatedEntries []*domain.OutboxEntry
}

func (f *fakeOutboxRepository) Create(ctx context.Context, entry *domain.OutboxEntry) error {
	f.CreatedEntries = append(f.CreatedEntries, entry)
	return nil
}
func (f *fakeOutboxRepository) GetPendingBatch(ctx context.Context, limit int) ([]*domain.OutboxEntry, error) {
	return nil, nil
}
func (f *fakeOutboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error { return nil }
func (f *fakeOutboxRepository) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	return nil
}
func (f *fakeOutboxRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error { return nil }
func (f *fakeOutboxRepository) UpdateStatus(ctx context.Context, id uuid.UUID, newStatus, expectedStatus string) error {
	return nil
}

type fakeMessageProcessingRepository struct {
	// In-memory store keyed by messageID
	messages map[string]*domain.MessageProcessing
}

func newFakeMessageProcessingRepository() *fakeMessageProcessingRepository {
	return &fakeMessageProcessingRepository{
		messages: make(map[string]*domain.MessageProcessing),
	}
}

func (f *fakeMessageProcessingRepository) InsertIfNotExists(ctx context.Context, msgProc *domain.MessageProcessing) (uuid.UUID, bool, error) {
	if existing, ok := f.messages[msgProc.MessageID]; ok {
		return existing.ID, false, nil
	}
	msgProc.ID = uuid.New()
	f.messages[msgProc.MessageID] = msgProc
	return msgProc.ID, true, nil
}

func (f *fakeMessageProcessingRepository) GetByMessageID(ctx context.Context, messageID string) (*domain.MessageProcessing, error) {
	msg, ok := f.messages[messageID]
	if !ok {
		return nil, nil
	}
	return msg, nil
}

func (f *fakeMessageProcessingRepository) UpdateState(ctx context.Context, id uuid.UUID, state string) error {
	for _, msg := range f.messages {
		if msg.ID == id {
			msg.State = state
			return nil
		}
	}
	return nil
}

// ── fakes: UnitOfWork ─────────────────────────────────────────────────────────

type fakeUnitOfWork struct {
	outboxRepo    *fakeOutboxRepository
	msgProcRepo   *fakeMessageProcessingRepository
	BegunTx       bool
	CommittedTx   bool
	RolledBackTx  bool
}

func newFakeUnitOfWork() *fakeUnitOfWork {
	return &fakeUnitOfWork{
		outboxRepo:  &fakeOutboxRepository{},
		msgProcRepo: newFakeMessageProcessingRepository(),
	}
}

func (f *fakeUnitOfWork) OutboxRepo() postgres.OutboxRepository     { return f.outboxRepo }
func (f *fakeUnitOfWork) MessageProcessingRepo() postgres.MessageProcessingRepository {
	return f.msgProcRepo
}
func (f *fakeUnitOfWork) Begin(ctx context.Context) error { f.BegunTx = true; return nil }
func (f *fakeUnitOfWork) Commit() error                   { f.CommittedTx = true; return nil }
func (f *fakeUnitOfWork) Rollback() error                 { f.RolledBackTx = true; return nil }

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func baseCmd() command.HandleNodeCompletedCmd {
	return command.HandleNodeCompletedCmd{
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "daily",
		ServiceName:  "warehouse",
		Schema:       "public",
		TableName:    "orders",
		Status:       "SUCCEEDED",
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. SUCCEEDED node with no downstream → UpdateNodeStatus called, no outbox entries.
func TestHandleNodeCompleted_SucceededNoDownstream(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	runRepo := &fakeRunRepository{}
	stateClient := &fakeStateClient{}

	h := command.NewHandleNodeCompletedHandler(uow, runRepo, stateClient, newTestLogger())
	cmd := baseCmd()

	err := h.Handle(ctx, cmd, "msg-1")
	require.NoError(t, err)

	assert.Equal(t, 1, runRepo.updateNodeStatusCalls, "UpdateNodeStatus should be called once")
	assert.Equal(t, 1, runRepo.getReadyDownstreamCalls, "GetReadyDownstream should be called for SUCCEEDED")
	assert.Len(t, uow.outboxRepo.CreatedEntries, 0, "no outbox entries when no downstream")
	assert.True(t, uow.CommittedTx, "transaction should be committed")
}

// 2. SUCCEEDED node with downstream → UpdateNodeStatus + GetReadyDownstream + outbox entries.
func TestHandleNodeCompleted_SucceededWithDownstream(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	existingTaskID := uuid.New().String()
	scheduleID := uuid.New()

	runRepo := &fakeRunRepository{
		getReadyDownstreamFn: func(_ context.Context, runID, scheduleName, schema, tableName string) ([]*run.DownstreamNode, error) {
			return []*run.DownstreamNode{
				{ServiceName: "warehouse", SchemaName: "analytics", TableName: "daily_summary", NodeType: "dbt-model", ScheduleName: "daily"},
				{ServiceName: "warehouse", SchemaName: "analytics", TableName: "daily_metrics", NodeType: "dbt-model", ScheduleName: "daily"},
			}, nil
		},
	}

	stateClient := &fakeStateClient{
		getTaskFn: func(_ context.Context, sid uuid.UUID, svc, schema, table string) (string, error) {
			return existingTaskID, nil
		},
	}

	h := command.NewHandleNodeCompletedHandler(uow, runRepo, stateClient, newTestLogger())
	cmd := baseCmd()
	cmd.ScheduleID = scheduleID

	err := h.Handle(ctx, cmd, "msg-2")
	require.NoError(t, err)

	assert.Equal(t, 1, runRepo.updateNodeStatusCalls)
	assert.Equal(t, 1, runRepo.getReadyDownstreamCalls)
	require.Len(t, uow.outboxRepo.CreatedEntries, 2, "one outbox entry per downstream node")

	for _, entry := range uow.outboxRepo.CreatedEntries {
		assert.Equal(t, "node_ready_for_execution", entry.EventType)
		assert.Equal(t, "query.model:v1", entry.StreamName)
		assert.Equal(t, "orchestrator", entry.AggregateType)
		assert.Equal(t, "pending", entry.Status)
		assert.Equal(t, scheduleID, entry.AggregateID)

		var evt domain.NodeReadyForExecution
		require.NoError(t, json.Unmarshal(entry.Payload, &evt))
		assert.Equal(t, scheduleID.String(), evt.ScheduleID)
		assert.Equal(t, "daily", evt.ScheduleName)
		assert.Equal(t, existingTaskID, evt.TaskID)
		assert.Equal(t, "dbt-model", evt.NodeType)
	}
	assert.True(t, uow.CommittedTx)
}

// 3. FAILED node → UpdateNodeStatus called, NO GetReadyDownstream call, no outbox entries.
func TestHandleNodeCompleted_FailedNoDownstream(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	runRepo := &fakeRunRepository{}
	stateClient := &fakeStateClient{}

	h := command.NewHandleNodeCompletedHandler(uow, runRepo, stateClient, newTestLogger())
	cmd := baseCmd()
	cmd.Status = "FAILED"

	err := h.Handle(ctx, cmd, "msg-3")
	require.NoError(t, err)

	assert.Equal(t, 1, runRepo.updateNodeStatusCalls, "UpdateNodeStatus should be called")
	assert.Equal(t, 0, runRepo.getReadyDownstreamCalls, "GetReadyDownstream must NOT be called for FAILED")
	assert.Len(t, uow.outboxRepo.CreatedEntries, 0)
	assert.True(t, uow.CommittedTx)
}

// 4. Duplicate message → should skip processing (no UpdateNodeStatus, no outbox).
func TestHandleNodeCompleted_DuplicateMessage(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	runRepo := &fakeRunRepository{}
	stateClient := &fakeStateClient{}

	h := command.NewHandleNodeCompletedHandler(uow, runRepo, stateClient, newTestLogger())
	cmd := baseCmd()

	// First call: should process normally
	err := h.Handle(ctx, cmd, "dup-msg-1")
	require.NoError(t, err)
	assert.Equal(t, 1, runRepo.updateNodeStatusCalls)

	// Simulate the message state being set to "completed" by the first processing
	// (The fake repo already has it in state "completed" after Commit updated it)

	// Reset call counts to track only what the second call does
	runRepo.updateNodeStatusCalls = 0
	runRepo.getReadyDownstreamCalls = 0
	uow.outboxRepo.CreatedEntries = nil
	uow.CommittedTx = false

	// Second call with the same message ID: should be skipped due to dedup
	err = h.Handle(ctx, cmd, "dup-msg-1")
	require.NoError(t, err)

	assert.Equal(t, 0, runRepo.updateNodeStatusCalls, "UpdateNodeStatus must NOT be called for duplicate")
	assert.Equal(t, 0, runRepo.getReadyDownstreamCalls, "GetReadyDownstream must NOT be called for duplicate")
	assert.Len(t, uow.outboxRepo.CreatedEntries, 0, "no outbox entries for duplicate")
}

// 5. SUCCEEDED node, schedule fully drained, no failures → scheduler status updated to SUCCEEDED.
func TestHandleNodeCompleted_SucceededScheduleComplete(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	finalizedRunID := ""
	finalizedStatus := ""
	cmd := baseCmd()

	runRepo := &fakeRunRepository{
		checkScheduleCompletionFn: func(_ context.Context, runID, _ string) (bool, bool, error) {
			return true, false, nil
		},
		finalizeRunFn: func(_ context.Context, runID, status string) error {
			finalizedRunID = runID
			finalizedStatus = status
			return nil
		},
	}
	stateClient := &fakeStateClient{}

	h := command.NewHandleNodeCompletedHandler(uow, runRepo, stateClient, newTestLogger())

	require.NoError(t, h.Handle(ctx, cmd, "msg-5"))
	assert.True(t, stateClient.updateSchedulerCalled)
	assert.Equal(t, "SUCCEEDED", stateClient.capturedSchedulerStatus)
	assert.Equal(t, cmd.ScheduleID.String(), finalizedRunID)
	assert.Equal(t, "SUCCEEDED", finalizedStatus)
}

// 6. SUCCEEDED node, schedule drained with failures → scheduler status updated to FAILED.
func TestHandleNodeCompleted_SucceededScheduleCompleteWithFailures(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	runRepo := &fakeRunRepository{
		checkScheduleCompletionFn: func(_ context.Context, _, _ string) (bool, bool, error) {
			return true, true, nil
		},
	}
	stateClient := &fakeStateClient{}

	h := command.NewHandleNodeCompletedHandler(uow, runRepo, stateClient, newTestLogger())

	require.NoError(t, h.Handle(ctx, baseCmd(), "msg-6"))
	assert.True(t, stateClient.updateSchedulerCalled)
	assert.Equal(t, "FAILED", stateClient.capturedSchedulerStatus)
}

// 7. FAILED node, schedule fully drained → scheduler status updated to FAILED.
func TestHandleNodeCompleted_FailedScheduleComplete(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	runRepo := &fakeRunRepository{
		checkScheduleCompletionFn: func(_ context.Context, _, _ string) (bool, bool, error) {
			return true, true, nil
		},
	}
	stateClient := &fakeStateClient{}

	h := command.NewHandleNodeCompletedHandler(uow, runRepo, stateClient, newTestLogger())
	cmd := baseCmd()
	cmd.Status = "FAILED"

	require.NoError(t, h.Handle(ctx, cmd, "msg-7"))
	assert.True(t, stateClient.updateSchedulerCalled)
	assert.Equal(t, "FAILED", stateClient.capturedSchedulerStatus)
}

// 8. Init status not "completed" → finalization skipped.
func TestHandleNodeCompleted_InitStatusNotCompleted_SkipsFinalization(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	checkCalled := false
	runRepo := &fakeRunRepository{
		checkScheduleCompletionFn: func(_ context.Context, _, _ string) (bool, bool, error) {
			checkCalled = true
			return true, false, nil
		},
	}
	stateClient := &fakeStateClient{
		getSchedulerInitStatusFn: func(_ context.Context, _ uuid.UUID) (string, error) {
			return "initializing", nil
		},
	}

	h := command.NewHandleNodeCompletedHandler(uow, runRepo, stateClient, newTestLogger())

	require.NoError(t, h.Handle(ctx, baseCmd(), "msg-8"))
	assert.False(t, checkCalled, "CheckScheduleCompletion must NOT be called when init_status != completed")
	assert.False(t, stateClient.updateSchedulerCalled)
}
