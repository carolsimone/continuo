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

// ── fakeSnapshotService for HandleRerun tests ─────────────────────────────────
//
// rerunFakeSnapshotService stubs SnapshotService for HandleRerun unit tests.
// It lets each test inject a custom Snapshot result (projection + error) and
// exposes the recorded call count for assertions.

type rerunFakeSnapshotService struct {
	snapshotFn    func(ctx context.Context, params snapshot.Params) ([]snapshot.TaskProjection, error)
	snapshotCalls int
}

func (f *rerunFakeSnapshotService) Snapshot(
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

func makeRerunCmd() domainModel.RerunInput {
	return domainModel.RerunInput{
		ScheduleName: "daily",
		RunID:        "00000000-0000-0000-0000-000000000001",
		SourceRunID:  "00000000-0000-0000-0000-000000000999",
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

	runRepo := &fakeRunRepository{}
	snapSvc := &rerunFakeSnapshotService{
		snapshotFn: func(_ context.Context, p snapshot.Params) ([]snapshot.TaskProjection, error) {
			// Sanity-check the params the handler hands us.
			assert.Equal(t, "rerun", p.Kind)
			assert.Equal(t, "00000000-0000-0000-0000-000000000001", p.RunID)
			require.NotNil(t, p.SourceRunID)
			assert.Equal(t, "00000000-0000-0000-0000-000000000999", p.SourceRunID.String())
			_, ok := p.Selector.(snapshot.SourcePinnedDAG)
			require.True(t, ok, "selector must be SourcePinnedDAG")

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

	h := handlers.NewHandleRerunHandler(uow, runRepo, snapSvc, newTestLogger())
	require.NoError(t, h.Handle(ctx, makeRerunCmd(), "msg-rerun-happy"))
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

// 2. ErrEmptyProjection from Snapshot → run.entries.dispatch_failed with
//    reason="empty_projection".
func TestHandleRerun_EmptyProjection_EmitsDispatchFailed(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	runRepo := &fakeRunRepository{}
	snapSvc := &rerunFakeSnapshotService{
		snapshotFn: func(_ context.Context, _ snapshot.Params) ([]snapshot.TaskProjection, error) {
			return nil, snapshot.ErrEmptyProjection
		},
	}

	h := handlers.NewHandleRerunHandler(uow, runRepo, snapSvc, newTestLogger())
	require.NoError(t, h.Handle(ctx, makeRerunCmd(), "msg-rerun-empty"))
	require.True(t, uow.CommittedTx)

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 1)
	require.Equal(t, "run.entries.dispatch_failed:v1", entries[0].StreamName)

	var failed pkgEvents.RunEntriesDispatchFailed
	require.NoError(t, json.Unmarshal(entries[0].Payload, &failed))
	assert.Equal(t, pkgEvents.DispatchFailedReasonEmptyProjection, failed.Reason)
}

// 3. Non-target terminal statuses (FAILED/CANCELLED/SKIPPED) inherited from the
//    source pinned DAG must be preserved verbatim in run.entries.dispatched:v1
//    and must NOT trigger query.model:v1 dispatch. Bug B2 (cycle 2): the
//    handler used to coerce any non-SUCCEEDED InitialStatus to "pending",
//    creating a zombie task_tracker row state would never finalize.
func TestHandleRerun_PreservesNonTargetTerminalStatuses(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name           string
		terminalStatus string
		wantStatusWire string
	}{
		{name: "failed", terminalStatus: "FAILED", wantStatusWire: "failed"},
		{name: "cancelled", terminalStatus: "CANCELLED", wantStatusWire: "cancelled"},
		{name: "skipped", terminalStatus: "SKIPPED", wantStatusWire: "skipped"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uow := newFakeUnitOfWork()

			targetTaskID := uuid.New()
			succTaskID := uuid.New()
			termTaskID := uuid.New()
			succInheritedRoot := uuid.New()
			termInheritedRoot := uuid.New()

			runRepo := &fakeRunRepository{}
			snapSvc := &rerunFakeSnapshotService{
				snapshotFn: func(_ context.Context, _ snapshot.Params) ([]snapshot.TaskProjection, error) {
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
							TaskID:              succTaskID,
							ServiceName:         "svc1",
							SchemaName:          "public",
							TableName:           "users",
							ScheduleName:        "daily",
							NodeType:            "dbt-model",
							InitialStatus:       "SUCCEEDED",
							ImageTag:            "v3-pinned",
							ManifestVersion:     "m3-pinned",
							InheritedFromTaskID: &succInheritedRoot,
							MaxRetries:          0,
						},
						{
							TaskID:              termTaskID,
							ServiceName:         "svc1",
							SchemaName:          "public",
							TableName:           "events",
							ScheduleName:        "daily",
							NodeType:            "dbt-model",
							InitialStatus:       tc.terminalStatus,
							ImageTag:            "v3-pinned",
							ManifestVersion:     "m3-pinned",
							InheritedFromTaskID: &termInheritedRoot,
							MaxRetries:          0,
						},
					}, nil
				},
			}

			h := handlers.NewHandleRerunHandler(uow, runRepo, snapSvc, newTestLogger())
			require.NoError(t, h.Handle(ctx, makeRerunCmd(), "msg-rerun-term-"+tc.name))
			require.True(t, uow.CommittedTx)

			entries := uow.outboxRepo.CreatedEntries
			// Expect: 1 run.entries.dispatched:v1 + 1 query.model:v1 (only PENDING row).
			require.Len(t, entries, 2, "expect 1 dispatched + 1 query.model (only the PENDING target)")

			require.Equal(t, "run.entries.dispatched:v1", entries[0].StreamName)
			var dispatched pkgEvents.RunEntriesDispatched
			require.NoError(t, json.Unmarshal(entries[0].Payload, &dispatched))
			require.Equal(t, int32(3), dispatched.TotalTaskCount)
			require.Len(t, dispatched.AllTasks, 3)

			byTable := make(map[string]pkgEvents.DispatchedTask, 3)
			for _, dt := range dispatched.AllTasks {
				byTable[dt.TableName] = dt
			}
			require.Contains(t, byTable, "orders")
			require.Contains(t, byTable, "users")
			require.Contains(t, byTable, "events")

			assert.Equal(t, "pending", byTable["orders"].Status, "PENDING (rebased target) → pending")
			assert.Equal(t, "succeeded", byTable["users"].Status, "SUCCEEDED → succeeded")
			assert.Equal(t, tc.wantStatusWire, byTable["events"].Status,
				"non-target terminal status %q must be preserved verbatim (lowercased), not coerced to pending",
				tc.terminalStatus)

			// query.model:v1 must fire ONLY for the PENDING row.
			require.Equal(t, "query.model:v1", entries[1].StreamName)
			var qevt domain.NodeReadyForExecution
			require.NoError(t, json.Unmarshal(entries[1].Payload, &qevt))
			assert.Equal(t, "orders", qevt.TableName,
				"only the rebased PENDING target should receive a query.model:v1 dispatch")
		})
	}
}

// 4. Second delivery of the same messageID is a no-op (dedup).
func TestHandleRerun_DedupSecondDelivery(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	taskID := uuid.New()
	runRepo := &fakeRunRepository{}
	snapSvc := &rerunFakeSnapshotService{
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

	h := handlers.NewHandleRerunHandler(uow, runRepo, snapSvc, newTestLogger())
	cmd := makeRerunCmd()

	// First delivery: processes normally.
	require.NoError(t, h.Handle(ctx, cmd, "msg-rerun-dup"))
	require.Equal(t, 1, snapSvc.snapshotCalls)
	firstCount := len(uow.outboxRepo.CreatedEntries)
	require.Equal(t, 2, firstCount, "1 dispatched + 1 query.model")

	// Second delivery with the same message ID: must be skipped.
	require.NoError(t, h.Handle(ctx, cmd, "msg-rerun-dup"))
	assert.Equal(t, 1, snapSvc.snapshotCalls, "Snapshot must NOT be called again")
	assert.Len(t, uow.outboxRepo.CreatedEntries, firstCount,
		"outbox count must not grow on duplicate delivery")
}
