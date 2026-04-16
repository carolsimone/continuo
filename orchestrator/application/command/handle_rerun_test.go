package command_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/application/command"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── extended fakeRunRepository for HandleRerun ────────────────────────────────
// We need configurable GetTransitiveDownstream and UpdateNodeStatus for rerun.

type rerunFakeRunRepository struct {
	fakeRunRepository
	getTransitiveDownstreamFn func(ctx context.Context, scheduleName, schema, tableName string) ([]*domain.TableNode, error)

	getTransitiveDownstreamCalls int
	updateNodeStatusArgs         [][]string // captures [runID, scheduleName, schema, tableName, status]
}

func (f *rerunFakeRunRepository) UpdateNodeStatus(ctx context.Context, runID, scheduleName, schema, tableName, status string) error {
	f.updateNodeStatusCalls++
	f.updateNodeStatusArgs = append(f.updateNodeStatusArgs, []string{runID, scheduleName, schema, tableName, status})
	if f.updateNodeStatusFn != nil {
		return f.updateNodeStatusFn(ctx, runID, scheduleName, schema, tableName, status)
	}
	return nil
}

func (f *rerunFakeRunRepository) GetTransitiveDownstream(ctx context.Context, scheduleName, schema, tableName string) ([]*domain.TableNode, error) {
	f.getTransitiveDownstreamCalls++
	if f.getTransitiveDownstreamFn != nil {
		return f.getTransitiveDownstreamFn(ctx, scheduleName, schema, tableName)
	}
	return nil, nil
}

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. Rerun target with 2 failed downstream → UpdateNodeStatus called 3 times, outbox entry created.
func TestHandleRerun_WithFailedDownstream(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	runRepo := &rerunFakeRunRepository{
		getTransitiveDownstreamFn: func(_ context.Context, scheduleName, schema, tableName string) ([]*domain.TableNode, error) {
			return []*domain.TableNode{
				{
					TableName:    "downstream1",
					SchemaName:   "analytics",
					ServiceName:  "svc1",
					Owner:        "team-a",
					ScheduleName: "daily",
					NodeType:     "dbt-model",
					Status:       string(domain.NodeStatusFailed),
				},
				{
					TableName:    "downstream2",
					SchemaName:   "analytics",
					ServiceName:  "svc1",
					Owner:        "team-a",
					ScheduleName: "daily",
					NodeType:     "dbt-model",
					Status:       string(domain.NodeStatusFailed),
				},
			}, nil
		},
	}

	h := command.NewHandleRerunHandler(uow, runRepo, newTestLogger())
	cmd := command.HandleRerunCmd{
		ScheduleName: "daily",
		RunID:        "run-rerun-1",
		ServiceName:  "svc1",
		SchemaName:   "public",
		TableName:    "orders",
	}

	err := h.Handle(ctx, cmd, "msg-rerun-1")
	require.NoError(t, err)

	// UpdateNodeStatus: 1 for target + 2 for failed downstream = 3
	assert.Equal(t, 3, runRepo.updateNodeStatusCalls,
		"UpdateNodeStatus should be called 3 times (target + 2 failed downstream)")

	// Verify the target was reset
	assert.Equal(t, []string{"run-rerun-1", "daily", "public", "orders", "PENDING"}, runRepo.updateNodeStatusArgs[0])

	// Verify downstream nodes were reset
	assert.Equal(t, "PENDING", runRepo.updateNodeStatusArgs[1][4])
	assert.Equal(t, "PENDING", runRepo.updateNodeStatusArgs[2][4])

	assert.True(t, uow.CommittedTx, "transaction should be committed")

	// One outbox entry should be created
	require.Len(t, uow.outboxRepo.CreatedEntries, 1, "one outbox entry should be created")

	entry := uow.outboxRepo.CreatedEntries[0]
	assert.Equal(t, "rerun_ready", entry.EventType)
	assert.Equal(t, "rerun.ready:v1", entry.StreamName)
	assert.Equal(t, "orchestrator", entry.AggregateType)
	assert.Equal(t, "pending", entry.Status)

	// Verify payload
	var payload struct {
		RunID       string               `json:"run_id"`
		TargetNodes []command.NodePayload `json:"target_nodes"`
	}
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, "run-rerun-1", payload.RunID)
	// target_nodes includes the rerun target + 2 FAILED downstream = 3
	assert.Len(t, payload.TargetNodes, 3, "target_nodes should include rerun target + 2 failed downstream")
	// Verify the rerun target has schedule_name populated
	assert.Equal(t, "daily", payload.TargetNodes[0].ScheduleName, "rerun target must carry schedule_name")
	// Verify service_name comes from the graph (mock returns "test-service"), not from cmd
	assert.Equal(t, "test-service", payload.TargetNodes[0].ServiceName, "rerun target service_name should come from graph")
	// Verify original_service_name carries the command's value for task lookup
	assert.Equal(t, "svc1", payload.TargetNodes[0].OriginalServiceName, "rerun target original_service_name should come from cmd")
}

// 2. Duplicate message → skip processing.
func TestHandleRerun_DuplicateMessage(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	runRepo := &rerunFakeRunRepository{}

	h := command.NewHandleRerunHandler(uow, runRepo, newTestLogger())
	cmd := command.HandleRerunCmd{
		ScheduleName: "daily",
		RunID:        "run-rerun-2",
		ServiceName:  "svc1",
		SchemaName:   "public",
		TableName:    "orders",
	}

	// First call: processes normally
	err := h.Handle(ctx, cmd, "dup-rerun-1")
	require.NoError(t, err)
	assert.Equal(t, 1, runRepo.updateNodeStatusCalls)

	// Reset tracking
	runRepo.updateNodeStatusCalls = 0
	runRepo.updateNodeStatusArgs = nil
	runRepo.getTransitiveDownstreamCalls = 0
	uow.outboxRepo.CreatedEntries = nil
	uow.CommittedTx = false

	// Second call with same message ID: should be skipped
	err = h.Handle(ctx, cmd, "dup-rerun-1")
	require.NoError(t, err)

	assert.Equal(t, 0, runRepo.updateNodeStatusCalls, "UpdateNodeStatus must NOT be called for duplicate")
	assert.Equal(t, 0, runRepo.getTransitiveDownstreamCalls, "GetTransitiveDownstream must NOT be called for duplicate")
	assert.Len(t, uow.outboxRepo.CreatedEntries, 0, "no outbox entries for duplicate")
}
