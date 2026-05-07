package handlers_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeSchedulerStartedNodes returns a 3-node topology:
//   a (root/seed) ← no deps
//   b depends on a
//   c depends on a
func makeSchedulerStartedNodes() *run.ScheduleInitNodes {
	taskA := uuid.New().String()
	taskB := uuid.New().String()
	taskC := uuid.New().String()

	nodeA := &domain.TableNode{
		TableName:       "a",
		SchemaName:      "public",
		ServiceName:     "svc1",
		Owner:           "team",
		ScheduleName:    "daily",
		NodeType:        "dbt-model",
		Status:          "PENDING",
		TaskID:          taskA,
		ManifestVersion: "v3",
		ImageTag:        "abc123-1714300000",
	}
	nodeB := &domain.TableNode{
		TableName:       "b",
		SchemaName:      "public",
		ServiceName:     "svc1",
		Owner:           "team",
		ScheduleName:    "daily",
		NodeType:        "dbt-model",
		Status:          "PENDING",
		TaskID:          taskB,
		ManifestVersion: "v3",
		ImageTag:        "abc123-1714300000",
	}
	nodeC := &domain.TableNode{
		TableName:       "c",
		SchemaName:      "public",
		ServiceName:     "svc1",
		Owner:           "team",
		ScheduleName:    "daily",
		NodeType:        "dbt-model",
		Status:          "PENDING",
		TaskID:          taskC,
		ManifestVersion: "v3",
		ImageTag:        "abc123-1714300000",
	}

	return &run.ScheduleInitNodes{
		AllNodes:  []*domain.TableNode{nodeA, nodeB, nodeC},
		RootNodes: []*domain.TableNode{nodeA},
		SeedNodes: []*domain.TableNode{},
	}
}

// TestHandleSchedulerStarted_WritesEntriesDispatchedAndDispatches verifies that:
//  1. run.entries.dispatched:v1 is written with all 3 tasks
//  2. 1 query.model:v1 entry is written (root node a only)
func TestHandleSchedulerStarted_WritesEntriesDispatchedAndDispatches(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()
	scheduleID := uuid.New()
	nodes := makeSchedulerStartedNodes()

	runRepo := &extendedFakeRunRepository{
		getScheduleInitNodesFn: func(_ context.Context, scheduleName, runID string) (*run.ScheduleInitNodes, error) {
			assert.Equal(t, "daily", scheduleName)
			assert.Equal(t, scheduleID.String(), runID)
			return nodes, nil
		},
	}

	h := handlers.NewHandleSchedulerStartedHandler(u, runRepo, newTestLogger())
	evt := domain.SchedulerStarted{
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
	}

	err := h.Handle(ctx, evt, "msg-start-1")
	require.NoError(t, err)

	assert.Equal(t, 1, runRepo.snapshotGraphCalls, "SnapshotGraph must be called once")
	assert.Equal(t, 1, runRepo.getScheduleInitNodesCalls, "GetScheduleInitNodes must be called once")
	assert.True(t, u.CommittedTx, "transaction must be committed")

	require.Len(t, u.outboxRepo.CreatedEntries, 2, "expected 2 outbox entries: entries_dispatched + 1 query.model")

	// Find entries by stream
	var entriesDispatchedEntry *domain.OutboxEntry
	var queryModelEntries []*domain.OutboxEntry
	for _, e := range u.outboxRepo.CreatedEntries {
		switch e.StreamName {
		case "run.entries.dispatched:v1":
			entriesDispatchedEntry = e
		case "query.model:v1":
			queryModelEntries = append(queryModelEntries, e)
		}
	}

	// Assert run.entries.dispatched:v1 entry
	require.NotNil(t, entriesDispatchedEntry, "run.entries.dispatched:v1 entry must exist")
	assert.Equal(t, "run_entries_dispatched", entriesDispatchedEntry.EventType)
	assert.Equal(t, "orchestrator", entriesDispatchedEntry.AggregateType)
	assert.Equal(t, "pending", entriesDispatchedEntry.Status)
	assert.Equal(t, scheduleID, entriesDispatchedEntry.AggregateID)

	var dispatched pkgevents.RunEntriesDispatched
	require.NoError(t, json.Unmarshal(entriesDispatchedEntry.Payload, &dispatched))
	assert.Equal(t, scheduleID.String(), dispatched.ScheduleID)
	assert.Equal(t, "daily", dispatched.ScheduleName)
	assert.Len(t, dispatched.AllTasks, 3, "AllTasks must have all 3 nodes")
	assert.Equal(t, int32(3), dispatched.TotalTaskCount)

	// Assert query.model:v1 entries — only root node (nodeA), no seed nodes so fall back to roots
	require.Len(t, queryModelEntries, 1, "exactly 1 query.model:v1 entry for the single root node")
	var nodeEvt domain.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(queryModelEntries[0].Payload, &nodeEvt))
	assert.Equal(t, scheduleID.String(), nodeEvt.ScheduleID)
	assert.Equal(t, "daily", nodeEvt.ScheduleName)
	assert.Equal(t, "a", nodeEvt.TableName)
	assert.Equal(t, "public", nodeEvt.SchemaName)
	assert.Equal(t, "svc1", nodeEvt.ServiceName)
	assert.Equal(t, nodes.RootNodes[0].TaskID, nodeEvt.TaskID)

	for _, task := range dispatched.AllTasks {
		assert.NotEmpty(t, task.ManifestVersion, "every dispatched task must carry manifest_version from EXECUTES edge")
		assert.NotEmpty(t, task.ImageTag, "every dispatched task must carry image_tag from EXECUTES edge")
	}

	var nodeReady domain.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(queryModelEntries[0].Payload, &nodeReady))
	assert.NotEmpty(t, nodeReady.ManifestVersion, "query.model payload must carry manifest_version")
	assert.NotEmpty(t, nodeReady.ImageTag, "query.model payload must carry image_tag")
}

// TestHandleSchedulerStarted_DispatchesSeedsWhenPresent verifies that when
// seed nodes exist, they are dispatched instead of root nodes.
func TestHandleSchedulerStarted_DispatchesSeedsWhenPresent(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()
	scheduleID := uuid.New()

	seedNode := &domain.TableNode{
		TableName:    "seed_data",
		SchemaName:   "raw",
		ServiceName:  "svc1",
		Owner:        "team",
		ScheduleName: "daily",
		NodeType:     "dbt-seed",
		Status:       "PENDING",
		TaskID:       uuid.New().String(),
	}
	rootNode := &domain.TableNode{
		TableName:    "orders",
		SchemaName:   "public",
		ServiceName:  "svc1",
		Owner:        "team",
		ScheduleName: "daily",
		NodeType:     "dbt-model",
		Status:       "PENDING",
		TaskID:       uuid.New().String(),
	}

	nodes := &run.ScheduleInitNodes{
		AllNodes:  []*domain.TableNode{seedNode, rootNode},
		RootNodes: []*domain.TableNode{rootNode},
		SeedNodes: []*domain.TableNode{seedNode},
	}

	runRepo := &extendedFakeRunRepository{
		getScheduleInitNodesFn: func(_ context.Context, _, _ string) (*run.ScheduleInitNodes, error) {
			return nodes, nil
		},
	}

	h := handlers.NewHandleSchedulerStartedHandler(u, runRepo, newTestLogger())
	evt := domain.SchedulerStarted{ScheduleID: scheduleID, ScheduleName: "daily"}

	require.NoError(t, h.Handle(ctx, evt, "msg-seeds-1"))

	var queryModelEntries []*domain.OutboxEntry
	for _, e := range u.outboxRepo.CreatedEntries {
		if e.StreamName == "query.model:v1" {
			queryModelEntries = append(queryModelEntries, e)
		}
	}

	// Seeds present → dispatch only seeds (1 seed node)
	require.Len(t, queryModelEntries, 1, "must dispatch seed node only")
	var nodeEvt domain.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(queryModelEntries[0].Payload, &nodeEvt))
	assert.Equal(t, "seed_data", nodeEvt.TableName)
	assert.Equal(t, "raw", nodeEvt.SchemaName)
}

// TestHandleSchedulerStarted_Idempotent ensures calling Handle twice with the
// same message ID produces no duplicate outbox entries on the second call.
func TestHandleSchedulerStarted_Idempotent(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()
	scheduleID := uuid.New()

	runRepo := &extendedFakeRunRepository{
		getScheduleInitNodesFn: func(_ context.Context, _, _ string) (*run.ScheduleInitNodes, error) {
			return makeSchedulerStartedNodes(), nil
		},
	}

	h := handlers.NewHandleSchedulerStartedHandler(u, runRepo, newTestLogger())
	evt := domain.SchedulerStarted{ScheduleID: scheduleID, ScheduleName: "daily"}

	// First call: processes normally
	require.NoError(t, h.Handle(ctx, evt, "msg-start-dup"))
	firstCount := len(u.outboxRepo.CreatedEntries)
	assert.Greater(t, firstCount, 0, "first call should produce outbox entries")

	// Reset tracking between calls
	runRepo.snapshotGraphCalls = 0
	runRepo.getScheduleInitNodesCalls = 0
	u.outboxRepo.CreatedEntries = nil
	u.CommittedTx = false

	// Second call with same message ID: must be skipped due to dedup
	require.NoError(t, h.Handle(ctx, evt, "msg-start-dup"))

	assert.Equal(t, 0, runRepo.snapshotGraphCalls, "SnapshotGraph must NOT be called for duplicate")
	assert.Equal(t, 0, runRepo.getScheduleInitNodesCalls, "GetScheduleInitNodes must NOT be called for duplicate")
	assert.Len(t, u.outboxRepo.CreatedEntries, 0, "no outbox entries for duplicate")
	assert.False(t, u.CommittedTx, "no commit for duplicate")
}

// TestHandleSchedulerStarted_PropagatesKindAndSourceRunID verifies that kind and
// sourceRunID from domain.SchedulerStarted are threaded through to SnapshotGraph.
func TestHandleSchedulerStarted_PropagatesKindAndSourceRunID(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()
	sourceRunID := uuid.New()

	runRepo := &extendedFakeRunRepository{
		getScheduleInitNodesFn: func(_ context.Context, _, _ string) (*run.ScheduleInitNodes, error) {
			return makeSchedulerStartedNodes(), nil
		},
	}

	h := handlers.NewHandleSchedulerStartedHandler(u, runRepo, newTestLogger())
	evt := domain.SchedulerStarted{
		ScheduleID:   uuid.New(),
		ScheduleName: "propagate-test",
		Kind:         "rerun",
		SourceRunID:  &sourceRunID,
	}

	require.NoError(t, h.Handle(ctx, evt, "msg-propagate-1"))
	require.Len(t, runRepo.snapshotCalls, 1)
	assert.Equal(t, "rerun", runRepo.snapshotCalls[0].Kind)
	require.NotNil(t, runRepo.snapshotCalls[0].SourceRunID)
	assert.Equal(t, sourceRunID, *runRepo.snapshotCalls[0].SourceRunID)
}
