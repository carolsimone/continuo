package handlers_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/orchestrator/domain"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/adapters/postgres"
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
	getTaskIDForNodeFn        func(ctx context.Context, runID, serviceName, schemaName, tableName string) (string, error)

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
func (f *fakeRunRepository) GetTaskIDForNode(ctx context.Context, runID, serviceName, schemaName, tableName string) (string, error) {
	if f.getTaskIDForNodeFn != nil {
		return f.getTaskIDForNodeFn(ctx, runID, serviceName, schemaName, tableName)
	}
	return "task-id-stub", nil
}
func (f *fakeRunRepository) GetSkippedDownstreamTaskIDs(ctx context.Context, runID, schemaName, tableName string) ([]string, error) {
	return nil, nil
}
func (f *fakeRunRepository) MarkPendingDownstreamSkipped(ctx context.Context, runID, scheduleName, schemaName, tableName string) ([]*run.CascadedFailureNode, error) {
	return nil, nil
}
func (f *fakeRunRepository) ResetSkippedDownstreamToPending(ctx context.Context, runID, schemaName, tableName string) error {
	return nil
}
func (f *fakeRunRepository) GetNodeEdgeData(ctx context.Context, runID, schemaName, tableName string) (string, string, error) {
	return "v1-stub", "tag-stub", nil
}
func (f *fakeRunRepository) SnapshotSingleNodeRun(ctx context.Context, runID, scheduleName string, sourceRunID *uuid.UUID, serviceName, schemaName, tableName string, metadataSource string) (taskID, imageTag, manifestVersion, nodeType string, err error) {
	return "", "", "", "", nil
}

func (f *fakeRunRepository) Snapshot(ctx context.Context, params snapshot.Params) ([]snapshot.TaskProjection, error) {
	return nil, nil
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

// ── fakes: CancelledSchedulesRepository ──────────────────────────────────────

type fakeCancelledSchedulesRepo struct {
	ids map[uuid.UUID]bool
}

func newFakeCancelledSchedulesRepo() *fakeCancelledSchedulesRepo {
	return &fakeCancelledSchedulesRepo{ids: make(map[uuid.UUID]bool)}
}

func (f *fakeCancelledSchedulesRepo) Insert(_ context.Context, id uuid.UUID) error {
	f.ids[id] = true
	return nil
}
func (f *fakeCancelledSchedulesRepo) Exists(_ context.Context, id uuid.UUID) (bool, error) {
	return f.ids[id], nil
}
func (f *fakeCancelledSchedulesRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

var _ postgres.CancelledSchedulesRepository = (*fakeCancelledSchedulesRepo)(nil)

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

func baseCmd() domainCmd.HandleNodeCompletedCmd {
	return domainCmd.HandleNodeCompletedCmd{
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "daily",
		ServiceName:  "warehouse",
		SchemaName:   "public",
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

	h := handlers.NewHandleNodeCompletedHandler(uow, runRepo, newFakeCancelledSchedulesRepo(), newTestLogger())
	cmd := baseCmd()

	err := h.Handle(ctx, cmd, "msg-1")
	require.NoError(t, err)

	assert.Equal(t, 1, runRepo.updateNodeStatusCalls, "UpdateNodeStatus should be called once")
	assert.Equal(t, 1, runRepo.getReadyDownstreamCalls, "GetReadyDownstream should be called for SUCCEEDED")
	assert.Len(t, uow.outboxRepo.CreatedEntries, 0, "no outbox entries when no downstream")
	assert.True(t, uow.CommittedTx, "transaction should be committed")
}

// 2. SUCCEEDED node with downstream → UpdateNodeStatus + GetReadyDownstream + outbox entries.
// Task IDs come from runRepo.GetTaskIDForNode (not state gRPC).
func TestHandleNodeCompleted_SucceededWithDownstream(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	repoTaskID := uuid.New().String()
	scheduleID := uuid.New()

	runRepo := &fakeRunRepository{
		getReadyDownstreamFn: func(_ context.Context, runID, scheduleName, schema, tableName string) ([]*run.DownstreamNode, error) {
			return []*run.DownstreamNode{
				{ServiceName: "warehouse", SchemaName: "analytics", TableName: "daily_summary", NodeType: "dbt-model", ScheduleName: "daily"},
				{ServiceName: "warehouse", SchemaName: "analytics", TableName: "daily_metrics", NodeType: "dbt-model", ScheduleName: "daily"},
			}, nil
		},
		getTaskIDForNodeFn: func(_ context.Context, _, _, _, _ string) (string, error) {
			return repoTaskID, nil
		},
	}

	h := handlers.NewHandleNodeCompletedHandler(uow, runRepo, newFakeCancelledSchedulesRepo(), newTestLogger())
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
		assert.Equal(t, repoTaskID, evt.TaskID)
		assert.Equal(t, "dbt-model", evt.NodeType)
	}
	assert.True(t, uow.CommittedTx)
}

// 3. FAILED node → UpdateNodeStatus called, NO GetReadyDownstream call, no outbox entries.
func TestHandleNodeCompleted_FailedNoDownstream(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	runRepo := &fakeRunRepository{}

	h := handlers.NewHandleNodeCompletedHandler(uow, runRepo, newFakeCancelledSchedulesRepo(), newTestLogger())
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

	h := handlers.NewHandleNodeCompletedHandler(uow, runRepo, newFakeCancelledSchedulesRepo(), newTestLogger())
	cmd := baseCmd()

	// First call: should process normally
	err := h.Handle(ctx, cmd, "dup-msg-1")
	require.NoError(t, err)
	assert.Equal(t, 1, runRepo.updateNodeStatusCalls)

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

// 5. Handler must not call any state gRPC methods — task IDs come from the run repo.
func TestHandleNodeCompleted_DoesNotCallStateGRPC(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	taskIDFromRepo := uuid.New().String()
	getTaskIDCalls := 0

	runRepo := &fakeRunRepository{
		getReadyDownstreamFn: func(_ context.Context, _, _, _, _ string) ([]*run.DownstreamNode, error) {
			return []*run.DownstreamNode{
				{ServiceName: "warehouse", SchemaName: "analytics", TableName: "daily_summary", NodeType: "dbt-model", ScheduleName: "daily"},
			}, nil
		},
		getTaskIDForNodeFn: func(_ context.Context, _, _, _, _ string) (string, error) {
			getTaskIDCalls++
			return taskIDFromRepo, nil
		},
	}

	h := handlers.NewHandleNodeCompletedHandler(uow, runRepo, newFakeCancelledSchedulesRepo(), newTestLogger())
	cmd := baseCmd()
	cmd.Status = "SUCCEEDED"

	require.NoError(t, h.Handle(ctx, cmd, "msg-5"))

	// Task ID must come from the run repo.
	assert.Equal(t, 1, getTaskIDCalls, "GetTaskIDForNode should be called once for the downstream node")

	// The downstream event must carry the task ID from the repo.
	require.Len(t, uow.outboxRepo.CreatedEntries, 1)
	var evt domain.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &evt))
	assert.Equal(t, taskIDFromRepo, evt.TaskID)
}

// 6. Cancelled schedule → UpdateNodeStatus called, no outbox entries produced.
func TestHandleNodeCompleted_DropsOutboxWhenScheduleCancelled(t *testing.T) {
	ctx := context.Background()
	scheduleID := uuid.New()

	cancelledRepo := newFakeCancelledSchedulesRepo()
	cancelledRepo.ids[scheduleID] = true

	uow := newFakeUnitOfWork()
	runRepo := &fakeRunRepository{
		getReadyDownstreamFn: func(_ context.Context, _, _, _, _ string) ([]*run.DownstreamNode, error) {
			return []*run.DownstreamNode{
				{ServiceName: "warehouse", SchemaName: "analytics", TableName: "daily_summary", NodeType: "dbt-model", ScheduleName: "daily"},
			}, nil
		},
	}

	h := handlers.NewHandleNodeCompletedHandler(uow, runRepo, cancelledRepo, newTestLogger())
	cmd := baseCmd()
	cmd.ScheduleID = scheduleID
	cmd.Status = "SUCCEEDED"

	err := h.Handle(ctx, cmd, "msg-cancelled-1")
	require.NoError(t, err)

	assert.Equal(t, 1, runRepo.updateNodeStatusCalls, "UpdateNodeStatus must have been called")
	assert.Equal(t, 0, runRepo.getReadyDownstreamCalls, "GetReadyDownstream must not be called for a cancelled schedule")
	assert.Empty(t, uow.outboxRepo.CreatedEntries, "no outbox entries for cancelled schedule")
	assert.True(t, uow.CommittedTx, "transaction should be committed")
}
