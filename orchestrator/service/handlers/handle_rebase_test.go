package handlers_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakeSnapshotService for HandleRebase tests ────────────────────────────────
//
// PR2 Feature 2: rebase handler drives snapshotSvc.Snapshot via the
// RebasePartition selector. The fake returns a configurable projection (or error).

type rebaseFakeSnapshotService struct {
	snapshotFn    func(ctx context.Context, params snapshot.Params) ([]snapshot.TaskProjection, error)
	snapshotCalls int
}

func (f *rebaseFakeSnapshotService) Snapshot(
	ctx context.Context,
	params snapshot.Params,
) ([]snapshot.TaskProjection, error) {
	f.snapshotCalls++
	if f.snapshotFn != nil {
		return f.snapshotFn(ctx, params)
	}
	return nil, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func makeRebaseCmd() domainModel.RebaseInput {
	return domainModel.RebaseInput{
		ScheduleName: "daily",
		RunID:        "00000000-0000-0000-0000-000000000001",
		SourceRunID:  "00000000-0000-0000-0000-000000000999",
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. Happy path. Snapshot returns 3 rows: rebased target (PENDING) +
//    rebased descendant (PENDING) + inherited sibling (SUCCEEDED with
//    InheritedFromTaskID).
//    Expect: ONE run.entries.dispatched:v1 with all 3 rows + TWO query.model:v1
//    entries (only PENDING rows).
func TestHandleRebase_HappyPath_ProjectsAndDispatches(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	rebasedTaskID := uuid.New()
	rebasedDescTaskID := uuid.New()
	inheritedTaskID := uuid.New()
	inheritedRoot := uuid.New()

	runRepo := &fakeRunRepository{}
	snapSvc := &rebaseFakeSnapshotService{
		snapshotFn: func(_ context.Context, p snapshot.Params) ([]snapshot.TaskProjection, error) {
			// Sanity-check the params the handler hands us.
			assert.Equal(t, "rebase", p.Kind)
			assert.Equal(t, "00000000-0000-0000-0000-000000000001", p.RunID)
			require.NotNil(t, p.SourceRunID)
			assert.Equal(t, "00000000-0000-0000-0000-000000000999", p.SourceRunID.String())
			_, ok := p.Selector.(snapshot.RebasePartition)
			require.True(t, ok, "selector must be RebasePartition")

			return []snapshot.TaskProjection{
				{
					TaskID:          rebasedTaskID,
					ServiceName:     "svc1",
					SchemaName:      "public",
					TableName:       "orders",
					ScheduleName:    "daily",
					NodeType:        "dbt-model",
					InitialStatus:   "PENDING",
					ImageTag:        "v4-latest",
					ManifestVersion: "m4-latest",
					MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
				},
				{
					TaskID:          rebasedDescTaskID,
					ServiceName:     "svc1",
					SchemaName:      "public",
					TableName:       "order_lines",
					ScheduleName:    "daily",
					NodeType:        "dbt-model",
					InitialStatus:   "PENDING",
					ImageTag:        "v4-latest",
					ManifestVersion: "m4-latest",
					MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
				},
				{
					TaskID:              inheritedTaskID,
					ServiceName:         "svc1",
					SchemaName:          "public",
					TableName:           "users",
					ScheduleName:        "daily",
					NodeType:            "dbt-model",
					InitialStatus:       "SUCCEEDED",
					ImageTag:            "v3-source-pinned",
					ManifestVersion:     "m3-source-pinned",
					InheritedFromTaskID: &inheritedRoot,
					MaxRetries:          0,
				},
			}, nil
		},
	}

	h := handlers.NewHandleRebaseHandler(uow, runRepo, snapSvc, newTestLogger())
	require.NoError(t, h.Handle(ctx, makeRebaseCmd(), "msg-rebase-happy"))
	require.True(t, uow.CommittedTx, "transaction must be committed")
	require.Equal(t, 1, snapSvc.snapshotCalls)

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 3, "expect 1 run.entries.dispatched + 2 query.model entries")

	// ── Entry 0: run.entries.dispatched:v1 ─────────────────────────────────────
	require.Equal(t, "run.entries.dispatched:v1", entries[0].StreamName)
	require.Equal(t, "run_entries_dispatched", entries[0].EventType)
	require.Equal(t, "orchestrator", entries[0].AggregateType)
	require.Equal(t, "pending", entries[0].Status)

	var dispatched pkgEvents.RunEntriesDispatched
	require.NoError(t, json.Unmarshal(entries[0].Payload, &dispatched))
	assert.Equal(t, "00000000-0000-0000-0000-000000000001", dispatched.ScheduleID)
	assert.Equal(t, "daily", dispatched.ScheduleName)
	assert.Equal(t, int32(3), dispatched.TotalTaskCount)
	require.Len(t, dispatched.AllTasks, 3)

	// Per-task assertions: rebased rows are "pending" with empty inherited;
	// inherited rows are "succeeded" with InheritedFromTaskID == root.
	byTable := make(map[string]pkgEvents.DispatchedTask, 3)
	for _, dt := range dispatched.AllTasks {
		byTable[dt.TableName] = dt
	}
	require.Contains(t, byTable, "orders")
	require.Contains(t, byTable, "order_lines")
	require.Contains(t, byTable, "users")

	tgt := byTable["orders"]
	assert.Equal(t, rebasedTaskID.String(), tgt.TaskID)
	assert.Equal(t, "pending", tgt.Status)
	assert.Empty(t, tgt.InheritedFromTaskID, "rebased rows must NOT carry InheritedFromTaskID")
	assert.Equal(t, pkgEvents.DefaultTaskMaxRetries, tgt.MaxRetries)
	assert.Equal(t, "v4-latest", tgt.ImageTag)
	assert.Equal(t, "m4-latest", tgt.ManifestVersion)

	desc := byTable["order_lines"]
	assert.Equal(t, rebasedDescTaskID.String(), desc.TaskID)
	assert.Equal(t, "pending", desc.Status)
	assert.Empty(t, desc.InheritedFromTaskID)

	inh := byTable["users"]
	assert.Equal(t, inheritedTaskID.String(), inh.TaskID)
	assert.Equal(t, "succeeded", inh.Status)
	assert.Equal(t, inheritedRoot.String(), inh.InheritedFromTaskID,
		"inherited rows must carry the root task_id")
	assert.Equal(t, int32(0), inh.MaxRetries, "inherited rows do not retry")
	assert.Equal(t, "v3-source-pinned", inh.ImageTag,
		"inherited rows pin to source's image_tag")

	// ── Entries 1 & 2: query.model:v1 — only for PENDING (rebased) rows ────────
	queryEntries := entries[1:]
	require.Len(t, queryEntries, 2)
	dispatchedTables := map[string]bool{}
	for _, qe := range queryEntries {
		require.Equal(t, "query.model:v1", qe.StreamName)
		require.Equal(t, "node_ready_for_execution", qe.EventType)
		require.Equal(t, "pending", qe.Status)
		var qevt domain.NodeReadyForExecution
		require.NoError(t, json.Unmarshal(qe.Payload, &qevt))
		assert.Equal(t, "00000000-0000-0000-0000-000000000001", qevt.ScheduleID)
		assert.Equal(t, "daily", qevt.ScheduleName)
		assert.Equal(t, "v4-latest", qevt.ImageTag)
		assert.Equal(t, "m4-latest", qevt.ManifestVersion)
		assert.NotEmpty(t, qevt.JobName, "computed job name must be present")
		dispatchedTables[qevt.TableName] = true
	}
	assert.True(t, dispatchedTables["orders"], "rebased target must be dispatched")
	assert.True(t, dispatchedTables["order_lines"], "rebased descendant must be dispatched")
	assert.False(t, dispatchedTables["users"], "inherited row must NOT be dispatched")
}

// 2. ErrEmptyProjection from Snapshot → ONE run.entries.dispatch_failed:v1
//    outbox entry with reason="rebase_yielded_empty_projection"; tx committed.
func TestHandleRebase_EmptyProjection_EmitsDispatchFailed(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	runRepo := &fakeRunRepository{}
	snapSvc := &rebaseFakeSnapshotService{
		snapshotFn: func(_ context.Context, _ snapshot.Params) ([]snapshot.TaskProjection, error) {
			return nil, snapshot.ErrEmptyProjection
		},
	}

	h := handlers.NewHandleRebaseHandler(uow, runRepo, snapSvc, newTestLogger())
	require.NoError(t, h.Handle(ctx, makeRebaseCmd(), "msg-rebase-empty"),
		"ErrEmptyProjection must not surface as a handler error")
	require.True(t, uow.CommittedTx, "transaction must still be committed")

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 1, "expect exactly 1 outbox entry: run.entries.dispatch_failed:v1")
	require.Equal(t, "run.entries.dispatch_failed:v1", entries[0].StreamName)
	require.Equal(t, "run_entries_dispatch_failed", entries[0].EventType)
	require.Equal(t, "pending", entries[0].Status)

	var failed pkgEvents.RunEntriesDispatchFailed
	require.NoError(t, json.Unmarshal(entries[0].Payload, &failed))
	assert.Equal(t, "00000000-0000-0000-0000-000000000001", failed.ScheduleID)
	assert.Equal(t, "daily", failed.ScheduleName)
	assert.Equal(t, "rebase_yielded_empty_projection", failed.Reason)
}

// 3. Second delivery of the same messageID is a no-op (dedup).
func TestHandleRebase_DedupSecondDelivery(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	taskID := uuid.New()
	runRepo := &fakeRunRepository{}
	snapSvc := &rebaseFakeSnapshotService{
		snapshotFn: func(_ context.Context, _ snapshot.Params) ([]snapshot.TaskProjection, error) {
			return []snapshot.TaskProjection{
				{
					TaskID:          taskID,
					ServiceName:     "svc1",
					SchemaName:      "public",
					TableName:       "orders",
					ScheduleName:    "daily",
					NodeType:        "dbt-model",
					InitialStatus:   "PENDING",
					ImageTag:        "v4",
					ManifestVersion: "m4",
					MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
				},
			}, nil
		},
	}

	h := handlers.NewHandleRebaseHandler(uow, runRepo, snapSvc, newTestLogger())
	cmd := makeRebaseCmd()

	// First delivery: processes normally.
	require.NoError(t, h.Handle(ctx, cmd, "msg-rebase-dup"))
	require.Equal(t, 1, snapSvc.snapshotCalls)
	firstCount := len(uow.outboxRepo.CreatedEntries)
	require.Equal(t, 2, firstCount, "1 dispatched + 1 query.model")

	// Second delivery with the same message ID: must be skipped.
	require.NoError(t, h.Handle(ctx, cmd, "msg-rebase-dup"))
	assert.Equal(t, 1, snapSvc.snapshotCalls, "Snapshot must NOT be called again")
	assert.Len(t, uow.outboxRepo.CreatedEntries, firstCount,
		"outbox count must not grow on duplicate delivery")
}
