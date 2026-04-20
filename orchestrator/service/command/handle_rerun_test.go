package command_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/service/command"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── extended fakeRunRepository for HandleRerun ────────────────────────────────

type rerunFakeRunRepository struct {
	fakeRunRepository

	getTaskIDForNodeFn            func(ctx context.Context, runID, serviceName, schemaName, tableName string) (string, error)
	getSkippedDownstreamTaskIDsFn func(ctx context.Context, runID, schemaName, tableName string) ([]string, error)

	getTaskIDForNodeCalls            int
	getSkippedDownstreamTaskIDsCalls int
}

func (f *rerunFakeRunRepository) GetTaskIDForNode(ctx context.Context, runID, serviceName, schemaName, tableName string) (string, error) {
	f.getTaskIDForNodeCalls++
	if f.getTaskIDForNodeFn != nil {
		return f.getTaskIDForNodeFn(ctx, runID, serviceName, schemaName, tableName)
	}
	return "task-id-target", nil
}

func (f *rerunFakeRunRepository) GetSkippedDownstreamTaskIDs(ctx context.Context, runID, schemaName, tableName string) ([]string, error) {
	f.getSkippedDownstreamTaskIDsCalls++
	if f.getSkippedDownstreamTaskIDsFn != nil {
		return f.getSkippedDownstreamTaskIDsFn(ctx, runID, schemaName, tableName)
	}
	return nil, nil
}
func (f *rerunFakeRunRepository) MarkPendingDownstreamSkipped(ctx context.Context, runID, scheduleName, schemaName, tableName string) ([]*run.CascadedFailureNode, error) {
	return nil, nil
}
func (f *rerunFakeRunRepository) ResetSkippedDownstreamToPending(ctx context.Context, runID, schemaName, tableName string) error {
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func makeRerunCmd() domainCmd.HandleRerunCmd {
	return domainCmd.HandleRerunCmd{
		ScheduleName: "daily",
		RunID:        "00000000-0000-0000-0000-000000000001",
		ServiceName:  "svc1",
		SchemaName:   "public",
		TableName:    "orders",
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. Happy path: target + 2 failed downstream → 2 outbox entries, NO UpdateNodeStatus calls.
func TestHandleRerun_WritesRunRerunDispatchedAndQueryModel(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	const targetTaskID = "task-orders"
	const ds1TaskID = "task-downstream1"
	const ds2TaskID = "task-downstream2"

	runRepo := &rerunFakeRunRepository{
		getTaskIDForNodeFn: func(_ context.Context, runID, _, schema, table string) (string, error) {
			assert.Equal(t, "00000000-0000-0000-0000-000000000001", runID)
			assert.Equal(t, "public", schema)
			assert.Equal(t, "orders", table)
			return targetTaskID, nil
		},
		getSkippedDownstreamTaskIDsFn: func(_ context.Context, runID, schema, table string) ([]string, error) {
			return []string{ds1TaskID, ds2TaskID}, nil
		},
	}

	h := command.NewHandleRerunHandler(uow, runRepo, newTestLogger())
	cmd := makeRerunCmd()

	err := h.Handle(ctx, cmd, "msg-rerun-1")
	require.NoError(t, err)

	// No direct node status mutations — those are handled by state via the event.
	assert.Equal(t, 0, runRepo.updateNodeStatusCalls, "UpdateNodeStatus must NOT be called")

	assert.True(t, uow.CommittedTx, "transaction should be committed")

	// Exactly 2 outbox entries: run.rerun.dispatched + query.model
	require.Len(t, uow.outboxRepo.CreatedEntries, 2, "expected exactly 2 outbox entries")

	// ── Entry 0: run.rerun.dispatched:v1 ─────────────────────────────────────
	dispatchedEntry := uow.outboxRepo.CreatedEntries[0]
	assert.Equal(t, "run_rerun_dispatched", dispatchedEntry.EventType)
	assert.Equal(t, "run.rerun.dispatched:v1", dispatchedEntry.StreamName)
	assert.Equal(t, "orchestrator", dispatchedEntry.AggregateType)
	assert.Equal(t, "pending", dispatchedEntry.Status)

	var dispatched pkgEvents.RunRerunDispatched
	require.NoError(t, json.Unmarshal(dispatchedEntry.Payload, &dispatched))
	assert.Equal(t, "00000000-0000-0000-0000-000000000001", dispatched.ScheduleID)
	assert.Equal(t, "daily", dispatched.ScheduleName)
	assert.Equal(t, targetTaskID, dispatched.EntryTaskID)
	require.Len(t, dispatched.TasksToReset, 3, "target + 2 downstream")
	assert.Equal(t, targetTaskID, dispatched.TasksToReset[0], "target task must be first")
	assert.Contains(t, dispatched.TasksToReset, ds1TaskID)
	assert.Contains(t, dispatched.TasksToReset, ds2TaskID)

	// ── Entry 1: query.model:v1 ───────────────────────────────────────────────
	queryModelEntry := uow.outboxRepo.CreatedEntries[1]
	assert.Equal(t, "node_ready_for_execution", queryModelEntry.EventType)
	assert.Equal(t, "query.model:v1", queryModelEntry.StreamName)
	assert.Equal(t, "orchestrator", queryModelEntry.AggregateType)
	assert.Equal(t, "pending", queryModelEntry.Status)

	var queryModel domain.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(queryModelEntry.Payload, &queryModel))
	assert.Equal(t, "00000000-0000-0000-0000-000000000001", queryModel.ScheduleID)
	assert.Equal(t, "daily", queryModel.ScheduleName)
	assert.Equal(t, targetTaskID, queryModel.TaskID)
	assert.Equal(t, "public", queryModel.SchemaName)
	assert.Equal(t, "orders", queryModel.TableName)
	// service_name comes from the graph mock (fakeRunRepository.GetNodeServiceName returns "test-service")
	assert.Equal(t, "test-service", queryModel.ServiceName)
}

// 2. Target only, no failed downstream → 2 outbox entries, TasksToReset has 1 entry.
func TestHandleRerun_NoFailedDownstream(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	const targetTaskID = "task-orders"

	runRepo := &rerunFakeRunRepository{
		getTaskIDForNodeFn: func(_ context.Context, _, _, _, _ string) (string, error) {
			return targetTaskID, nil
		},
		getSkippedDownstreamTaskIDsFn: func(_ context.Context, _, _, _ string) ([]string, error) {
			return nil, nil // no skipped downstream
		},
	}

	h := command.NewHandleRerunHandler(uow, runRepo, newTestLogger())

	err := h.Handle(ctx, makeRerunCmd(), "msg-rerun-2")
	require.NoError(t, err)

	require.Len(t, uow.outboxRepo.CreatedEntries, 2)

	var dispatched pkgEvents.RunRerunDispatched
	require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &dispatched))
	require.Len(t, dispatched.TasksToReset, 1, "only the target task should be reset")
	assert.Equal(t, targetTaskID, dispatched.TasksToReset[0])
	assert.Equal(t, targetTaskID, dispatched.EntryTaskID)
}

// 3. Duplicate message → skip processing entirely.
func TestHandleRerun_DuplicateMessage(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	runRepo := &rerunFakeRunRepository{}

	h := command.NewHandleRerunHandler(uow, runRepo, newTestLogger())
	cmd := makeRerunCmd()

	// First call: processes normally.
	err := h.Handle(ctx, cmd, "dup-rerun-1")
	require.NoError(t, err)
	assert.Equal(t, 1, runRepo.getTaskIDForNodeCalls)

	// Reset tracking.
	runRepo.getTaskIDForNodeCalls = 0
	runRepo.getSkippedDownstreamTaskIDsCalls = 0
	uow.outboxRepo.CreatedEntries = nil
	uow.CommittedTx = false

	// Second call with same message ID: should be skipped.
	err = h.Handle(ctx, cmd, "dup-rerun-1")
	require.NoError(t, err)

	assert.Equal(t, 0, runRepo.getTaskIDForNodeCalls, "GetTaskIDForNode must NOT be called for duplicate")
	assert.Equal(t, 0, runRepo.getSkippedDownstreamTaskIDsCalls, "GetSkippedDownstreamTaskIDs must NOT be called for duplicate")
	assert.Equal(t, 0, runRepo.updateNodeStatusCalls, "UpdateNodeStatus must NOT be called for duplicate")
	assert.Len(t, uow.outboxRepo.CreatedEntries, 0, "no outbox entries for duplicate")
}
