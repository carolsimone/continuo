package handlers_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── extended fakeRunRepository for HandleRerun ────────────────────────────────
//
// PR2 unification: rerun handler now drives runRepo.Snapshot via the
// SourcePinnedDAG selector. The fake overrides Snapshot to return a
// configurable projection (or error). All other read methods are stubbed by
// the embedded fakeRunRepository.

type rerunFakeRunRepository struct {
	fakeRunRepository

	snapshotFn    func(ctx context.Context, params snapshot.Params) ([]snapshot.TaskProjection, error)
	snapshotCalls int
}

func (f *rerunFakeRunRepository) Snapshot(
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

func makeRerunCmd() domainCmd.HandleRerunCmd {
	return domainCmd.HandleRerunCmd{
		ScheduleName: "daily",
		RunID:        "00000000-0000-0000-0000-000000000001",
		SourceRunID:  "00000000-0000-0000-0000-000000000999",
		ServiceName:  "svc1",
		SchemaName:   "public",
		TableName:    "orders",
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. Happy path. Snapshot returns 3 rows: target (PENDING/rebased) +
//    descendant (PENDING/rebased) + sibling (SUCCEEDED/inherited).
//    Expect: ONE run.entries.dispatched:v1 with all 3 rows + TWO query.model:v1
//    entries (only PENDING rows).
func TestHandleRerun_HappyPath_ProjectsAndDispatches(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	targetTaskID := uuid.New()
	descTaskID := uuid.New()
	siblingTaskID := uuid.New()
	inheritedRoot := uuid.New()

	runRepo := &rerunFakeRunRepository{
		snapshotFn: func(_ context.Context, p snapshot.Params) ([]snapshot.TaskProjection, error) {
			// Sanity-check the params the handler hands us.
			assert.Equal(t, "rerun", p.Kind)
			assert.Equal(t, "00000000-0000-0000-0000-000000000001", p.RunID)
			require.NotNil(t, p.SourceRunID)
			assert.Equal(t, "00000000-0000-0000-0000-000000000999", p.SourceRunID.String())
			sel, ok := p.Selector.(snapshot.SourcePinnedDAG)
			require.True(t, ok, "selector must be SourcePinnedDAG")
			assert.Equal(t, "svc1", sel.TargetService)
			assert.Equal(t, "public", sel.TargetSchema)
			assert.Equal(t, "orders", sel.TargetTable)

			return []snapshot.TaskProjection{
				{
					TaskID:          targetTaskID,
					ServiceName:     "svc1",
					SchemaName:      "public",
					TableName:       "orders",
					ScheduleName:    "daily",
					NodeType:        "dbt-model",
					InitialStatus:   "PENDING",
					ImageTag:        "v3-pinned",
					ManifestVersion: "m3-pinned",
					MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
				},
				{
					TaskID:          descTaskID,
					ServiceName:     "svc1",
					SchemaName:      "public",
					TableName:       "order_lines",
					ScheduleName:    "daily",
					NodeType:        "dbt-model",
					InitialStatus:   "PENDING",
					ImageTag:        "v3-pinned",
					ManifestVersion: "m3-pinned",
					MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
				},
				{
					TaskID:              siblingTaskID,
					ServiceName:         "svc1",
					SchemaName:          "public",
					TableName:           "users",
					ScheduleName:        "daily",
					NodeType:            "dbt-model",
					InitialStatus:       "SUCCEEDED",
					ImageTag:            "v3-pinned",
					ManifestVersion:     "m3-pinned",
					InheritedFromTaskID: &inheritedRoot,
					MaxRetries:          0,
				},
			}, nil
		},
	}

	h := handlers.NewHandleRerunHandler(uow, runRepo, newTestLogger())
	require.NoError(t, h.Handle(ctx, makeRerunCmd(), "msg-rerun-happy"))
	require.True(t, uow.CommittedTx, "transaction must be committed")
	require.Equal(t, 1, runRepo.snapshotCalls)

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
	assert.Equal(t, targetTaskID.String(), tgt.TaskID)
	assert.Equal(t, "pending", tgt.Status)
	assert.Empty(t, tgt.InheritedFromTaskID, "rebased rows must NOT carry InheritedFromTaskID")
	assert.Equal(t, pkgEvents.DefaultTaskMaxRetries, tgt.MaxRetries)
	assert.Equal(t, "v3-pinned", tgt.ImageTag)
	assert.Equal(t, "m3-pinned", tgt.ManifestVersion)

	desc := byTable["order_lines"]
	assert.Equal(t, descTaskID.String(), desc.TaskID)
	assert.Equal(t, "pending", desc.Status)
	assert.Empty(t, desc.InheritedFromTaskID)

	inh := byTable["users"]
	assert.Equal(t, siblingTaskID.String(), inh.TaskID)
	assert.Equal(t, "succeeded", inh.Status)
	assert.Equal(t, inheritedRoot.String(), inh.InheritedFromTaskID,
		"inherited rows must carry the root task_id")
	assert.Equal(t, int32(0), inh.MaxRetries, "inherited rows do not retry")

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
		assert.Equal(t, "v3-pinned", qevt.ImageTag)
		assert.Equal(t, "m3-pinned", qevt.ManifestVersion)
		assert.NotEmpty(t, qevt.JobName, "computed job name must be present")
		dispatchedTables[qevt.TableName] = true
	}
	assert.True(t, dispatchedTables["orders"], "target must be dispatched")
	assert.True(t, dispatchedTables["order_lines"], "rebased descendant must be dispatched")
	assert.False(t, dispatchedTables["users"], "inherited row must NOT be dispatched")
}

// 2. ErrTargetNotFound from Snapshot → ONE run.entries.dispatch_failed:v1
//    outbox entry; tx committed; no query.model.
func TestHandleRerun_TargetNotFound_EmitsDispatchFailed(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	runRepo := &rerunFakeRunRepository{
		snapshotFn: func(_ context.Context, _ snapshot.Params) ([]snapshot.TaskProjection, error) {
			return nil, snapshot.ErrTargetNotFound
		},
	}

	h := handlers.NewHandleRerunHandler(uow, runRepo, newTestLogger())
	require.NoError(t, h.Handle(ctx, makeRerunCmd(), "msg-rerun-tnf"),
		"ErrTargetNotFound must not surface as a handler error")
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
	assert.Equal(t, "target_not_found", failed.Reason)
}

// 3. ErrEmptyProjection from Snapshot → run.entries.dispatch_failed with
//    reason="rerun_yielded_empty_projection".
func TestHandleRerun_EmptyProjection_EmitsDispatchFailed(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	runRepo := &rerunFakeRunRepository{
		snapshotFn: func(_ context.Context, _ snapshot.Params) ([]snapshot.TaskProjection, error) {
			return nil, snapshot.ErrEmptyProjection
		},
	}

	h := handlers.NewHandleRerunHandler(uow, runRepo, newTestLogger())
	require.NoError(t, h.Handle(ctx, makeRerunCmd(), "msg-rerun-empty"))
	require.True(t, uow.CommittedTx)

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 1)
	require.Equal(t, "run.entries.dispatch_failed:v1", entries[0].StreamName)

	var failed pkgEvents.RunEntriesDispatchFailed
	require.NoError(t, json.Unmarshal(entries[0].Payload, &failed))
	assert.Equal(t, "rerun_yielded_empty_projection", failed.Reason)
}

// 4. Second delivery of the same messageID is a no-op (dedup).
func TestHandleRerun_DedupSecondDelivery(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	taskID := uuid.New()
	runRepo := &rerunFakeRunRepository{
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
					ImageTag:        "v3",
					ManifestVersion: "m3",
					MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
				},
			}, nil
		},
	}

	h := handlers.NewHandleRerunHandler(uow, runRepo, newTestLogger())
	cmd := makeRerunCmd()

	// First delivery: processes normally.
	require.NoError(t, h.Handle(ctx, cmd, "msg-rerun-dup"))
	require.Equal(t, 1, runRepo.snapshotCalls)
	firstCount := len(uow.outboxRepo.CreatedEntries)
	require.Equal(t, 2, firstCount, "1 dispatched + 1 query.model")

	// Second delivery with the same message ID: must be skipped.
	require.NoError(t, h.Handle(ctx, cmd, "msg-rerun-dup"))
	assert.Equal(t, 1, runRepo.snapshotCalls, "Snapshot must NOT be called again")
	assert.Len(t, uow.outboxRepo.CreatedEntries, firstCount,
		"outbox count must not grow on duplicate delivery")
}
