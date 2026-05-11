package handlers_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/orchestrator/domain"
	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakeSnapshotService for InitializeRun tests ───────────────────────────────

// initRunFakeSnapshotService wraps the init-nodes fixture to satisfy
// handlers.SnapshotService. The snapshotGraphCalls counter is kept for
// backward-compat with existing assertions.
type initRunFakeSnapshotService struct {
	getScheduleInitNodesFn func(ctx context.Context, scheduleName, runID string) (*run.ScheduleInitNodes, error)

	snapshotGraphCalls int
	snapshotCalls      []snapshotCall
}

func (f *initRunFakeSnapshotService) Snapshot(ctx context.Context, params snapshot.Params) ([]snapshot.TaskProjection, error) {
	f.snapshotGraphCalls++
	f.snapshotCalls = append(f.snapshotCalls, snapshotCall{
		RunID: params.RunID, ScheduleName: params.ScheduleName, Kind: params.Kind, SourceRunID: params.SourceRunID,
	})
	if f.getScheduleInitNodesFn == nil {
		return nil, snapshot.ErrEmptyProjection
	}
	initNodes, err := f.getScheduleInitNodesFn(ctx, params.ScheduleName, params.RunID)
	if err != nil {
		return nil, err
	}
	if initNodes == nil || len(initNodes.AllNodes) == 0 {
		return nil, snapshot.ErrEmptyProjection
	}
	projection := make([]snapshot.TaskProjection, 0, len(initNodes.AllNodes))
	for _, n := range initNodes.AllNodes {
		taskID, _ := uuid.Parse(n.TaskID)
		if taskID == uuid.Nil {
			taskID = uuid.New()
		}
		projection = append(projection, snapshot.TaskProjection{
			TaskID:          taskID,
			ServiceName:     n.ServiceName,
			SchemaName:      n.SchemaName,
			TableName:       n.TableName,
			ScheduleName:    n.ScheduleName,
			NodeType:        n.NodeType,
			InitialStatus:   "PENDING",
			ImageTag:        n.ImageTag,
			ManifestVersion: n.ManifestVersion,
			MaxRetries:      2, // pkgevents.DefaultTaskMaxRetries
		})
	}
	return projection, nil
}

// ── extended fakeRunRepository for InitializeRun ─────────────────────────────
// We reuse fakeRunRepository from handle_node_completed_test.go but need to
// extend it with configurable behaviour for GetScheduleInitNodes.

// snapshotCall records the arguments passed to Snapshot for assertion.
type snapshotCall struct {
	RunID        string
	ScheduleName string
	Kind         string
	SourceRunID  *uuid.UUID
}

// extendedFakeRunRepository wraps fakeRunRepository and adds configurable fns.
type extendedFakeRunRepository struct {
	fakeRunRepository
	getScheduleInitNodesFn func(ctx context.Context, scheduleName, runID string) (*run.ScheduleInitNodes, error)

	getScheduleInitNodesCalls int
}

func (f *extendedFakeRunRepository) GetScheduleInitNodes(ctx context.Context, scheduleName, runID string) (*run.ScheduleInitNodes, error) {
	f.getScheduleInitNodesCalls++
	if f.getScheduleInitNodesFn != nil {
		return f.getScheduleInitNodesFn(ctx, scheduleName, runID)
	}
	return &run.ScheduleInitNodes{
		AllNodes:  []*domain.TableNode{},
		RootNodes: []*domain.TableNode{},
		SeedNodes: []*domain.TableNode{},
	}, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func makeInitNodes() *run.ScheduleInitNodes {
	return &run.ScheduleInitNodes{
		AllNodes: []*domain.TableNode{
			{TableName: "orders", SchemaName: "public", ServiceName: "svc1", Owner: "team-a", ScheduleName: "daily", NodeType: "dbt-model", Status: "PENDING"},
			{TableName: "customers", SchemaName: "public", ServiceName: "svc1", Owner: "team-a", ScheduleName: "daily", NodeType: "dbt-model", Status: "PENDING"},
			{TableName: "seeds_data", SchemaName: "raw", ServiceName: "svc1", Owner: "team-a", ScheduleName: "daily", NodeType: "dbt-seed", Status: "PENDING"},
		},
		RootNodes: []*domain.TableNode{
			{TableName: "orders", SchemaName: "public", ServiceName: "svc1", Owner: "team-a", ScheduleName: "daily", NodeType: "dbt-model", Status: "PENDING"},
		},
		SeedNodes: []*domain.TableNode{
			{TableName: "seeds_data", SchemaName: "raw", ServiceName: "svc1", Owner: "team-a", ScheduleName: "daily", NodeType: "dbt-seed", Status: "PENDING"},
		},
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. Fresh init → SnapshotGraph called, GetScheduleInitNodes called, outbox entry with all/root/seed nodes.
func TestInitializeRun_FreshInit(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	initNodes := makeInitNodes()

	runRepo := &extendedFakeRunRepository{
		getScheduleInitNodesFn: func(_ context.Context, scheduleName, runID string) (*run.ScheduleInitNodes, error) {
			assert.Equal(t, "daily", scheduleName)
			assert.Equal(t, "run-123", runID)
			return initNodes, nil
		},
	}
	snapSvc := &initRunFakeSnapshotService{
		getScheduleInitNodesFn: func(_ context.Context, scheduleName, runID string) (*run.ScheduleInitNodes, error) {
			return initNodes, nil
		},
	}

	h := handlers.NewInitializeRunHandler(uow, runRepo, snapSvc, newTestLogger())
	cmd := domainModel.InitializeRunInput{
		ScheduleName: "daily",
		RunID:        "run-123",
	}

	err := h.Handle(ctx, cmd, "msg-init-1")
	require.NoError(t, err)

	assert.Equal(t, 1, snapSvc.snapshotGraphCalls, "SnapshotGraph should be called once")
	assert.Equal(t, 1, runRepo.getScheduleInitNodesCalls, "GetScheduleInitNodes should be called once")
	assert.True(t, uow.CommittedTx, "transaction should be committed")

	require.Len(t, uow.outboxRepo.CreatedEntries, 1, "one outbox entry should be created")

	entry := uow.outboxRepo.CreatedEntries[0]
	assert.Equal(t, "run_initialized", entry.EventType)
	assert.Equal(t, "run.initialized:v1", entry.StreamName)
	assert.Equal(t, "orchestrator", entry.AggregateType)
	assert.Equal(t, "pending", entry.Status)

	// Parse and verify payload
	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))

	var runID string
	require.NoError(t, json.Unmarshal(payload["run_id"], &runID))
	assert.Equal(t, "run-123", runID)

	var allNodes []domainEvent.RunInitializedNode
	require.NoError(t, json.Unmarshal(payload["all_nodes"], &allNodes))
	assert.Len(t, allNodes, 3, "all_nodes should have 3 entries")

	var rootNodes []domainEvent.RunInitializedNode
	require.NoError(t, json.Unmarshal(payload["root_nodes"], &rootNodes))
	assert.Len(t, rootNodes, 1, "root_nodes should have 1 entry")
	assert.Equal(t, "orders", rootNodes[0].TableName)

	var seedNodes []domainEvent.RunInitializedNode
	require.NoError(t, json.Unmarshal(payload["seed_nodes"], &seedNodes))
	assert.Len(t, seedNodes, 1, "seed_nodes should have 1 entry")
	assert.Equal(t, "seeds_data", seedNodes[0].TableName)
}

// 2. Duplicate message → skip processing.
func TestInitializeRun_DuplicateMessage(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	runRepo := &extendedFakeRunRepository{}
	snapSvc := &initRunFakeSnapshotService{}

	h := handlers.NewInitializeRunHandler(uow, runRepo, snapSvc, newTestLogger())
	cmd := domainModel.InitializeRunInput{
		ScheduleName: "daily",
		RunID:        "run-456",
	}

	// First call: processes normally
	err := h.Handle(ctx, cmd, "dup-init-1")
	require.NoError(t, err)
	assert.Equal(t, 1, snapSvc.snapshotGraphCalls)

	// Reset tracking
	snapSvc.snapshotGraphCalls = 0
	runRepo.getScheduleInitNodesCalls = 0
	uow.outboxRepo.CreatedEntries = nil
	uow.CommittedTx = false

	// Second call with same message ID: should be skipped
	err = h.Handle(ctx, cmd, "dup-init-1")
	require.NoError(t, err)

	assert.Equal(t, 0, snapSvc.snapshotGraphCalls, "SnapshotGraph must NOT be called for duplicate")
	assert.Equal(t, 0, runRepo.getScheduleInitNodesCalls, "GetScheduleInitNodes must NOT be called for duplicate")
	assert.Len(t, uow.outboxRepo.CreatedEntries, 0, "no outbox entries for duplicate")
}
