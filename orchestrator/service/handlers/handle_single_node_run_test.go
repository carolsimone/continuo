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
	"github.com/stretchr/testify/require"
)

// ── fakeSnapshotService for single-node-run tests ─────────────────────────────

// singleNodeFakeSnapshotService satisfies handlers.SnapshotService for
// handle_single_node_run tests.
type singleNodeFakeSnapshotService struct {
	snapshotResult singleNodeSnapshotResult
	snapshotErr    error
	snapshotCalls  int
}

type singleNodeSnapshotResult struct {
	TaskID          string
	ImageTag        string
	ManifestVersion string
	NodeType        string
	ScheduleName    string
	ServiceName     string
	SchemaName      string
	TableName       string
}

func (f *singleNodeFakeSnapshotService) Snapshot(_ context.Context, params snapshot.Params) ([]snapshot.TaskProjection, error) {
	f.snapshotCalls++
	if f.snapshotErr != nil {
		return nil, f.snapshotErr
	}
	taskID, _ := uuid.Parse(f.snapshotResult.TaskID)
	if taskID == uuid.Nil {
		taskID = uuid.New()
	}
	// Pull identity from the SingleNode selector if present; fall back to
	// snapshotResult fields so existing test setups keep working.
	svc := f.snapshotResult.ServiceName
	schema := f.snapshotResult.SchemaName
	tbl := f.snapshotResult.TableName
	if sel, ok := params.Selector.(snapshot.SingleNode); ok {
		if svc == "" {
			svc = sel.ServiceName
		}
		if schema == "" {
			schema = sel.SchemaName
		}
		if tbl == "" {
			tbl = sel.TableName
		}
	}
	return []snapshot.TaskProjection{{
		TaskID:          taskID,
		ServiceName:     svc,
		SchemaName:      schema,
		TableName:       tbl,
		ScheduleName:    f.snapshotResult.ScheduleName,
		NodeType:        f.snapshotResult.NodeType,
		InitialStatus:   "PENDING",
		ImageTag:        f.snapshotResult.ImageTag,
		ManifestVersion: f.snapshotResult.ManifestVersion,
		MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
	}}, nil
}

// ── fixture ───────────────────────────────────────────────────────────────────

type singleNodeRunHandlerFixture struct {
	Handler *handlers.HandleSingleNodeRunHandler
	SnapSvc *singleNodeFakeSnapshotService
	UoW     *fakeUnitOfWork
}

func newSingleNodeRunHandlerFixture(_ *testing.T) *singleNodeRunHandlerFixture {
	uow := newFakeUnitOfWork()
	runRepo := &fakeRunRepository{}
	snapSvc := &singleNodeFakeSnapshotService{}
	h := handlers.NewHandleSingleNodeRunHandler(uow, runRepo, snapSvc, newTestLogger())
	return &singleNodeRunHandlerFixture{
		Handler: h,
		SnapSvc: snapSvc,
		UoW:     uow,
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func makeSingleNodeCmd(runID string) domainModel.SingleNodeRunInput {
	return domainModel.SingleNodeRunInput{
		RunID:          runID,
		ScheduleName:   "single-node-run-" + runID[:8],
		ServiceName:    "svcA",
		SchemaName:     "public",
		TableName:      "users",
		MetadataSource: "latest",
		SourceRunID:    "",
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. Happy path: Snapshot(SingleNode) returns concrete values.
//    Expect TWO outbox entries: run.entries.dispatched:v1 and query.model:v1.
func TestHandleSingleNodeRun_Latest_HappyPath(t *testing.T) {
	fx := newSingleNodeRunHandlerFixture(t)
	scheduleID := uuid.New().String()
	cmd := makeSingleNodeCmd(scheduleID)

	taskID := uuid.NewString()
	fx.SnapSvc.snapshotResult = singleNodeSnapshotResult{
		TaskID:          taskID,
		ImageTag:        "v3",
		ManifestVersion: "m3",
		NodeType:        "dbt-model",
	}

	require.NoError(t, fx.Handler.Handle(context.Background(), cmd, "msg-snr-1"))
	require.True(t, fx.UoW.CommittedTx, "transaction must be committed")

	entries := fx.UoW.outboxRepo.CreatedEntries
	require.Len(t, entries, 2, "expect 2 outbox entries: run.entries.dispatched + query.model")

	// Entry 0: run.entries.dispatched:v1
	require.Equal(t, "run.entries.dispatched:v1", entries[0].StreamName)
	require.Equal(t, "run_entries_dispatched", entries[0].EventType)
	require.Equal(t, "orchestrator", entries[0].AggregateType)
	require.Equal(t, "pending", entries[0].Status)

	var dispatchedEvt pkgEvents.RunEntriesDispatched
	require.NoError(t, json.Unmarshal(entries[0].Payload, &dispatchedEvt))
	require.Equal(t, scheduleID, dispatchedEvt.ScheduleID)
	require.Equal(t, cmd.ScheduleName, dispatchedEvt.ScheduleName)
	require.Len(t, dispatchedEvt.AllTasks, 1, "exactly one task for single-node run")
	require.Equal(t, int32(1), dispatchedEvt.TotalTaskCount)
	task := dispatchedEvt.AllTasks[0]
	require.Equal(t, taskID, task.TaskID)
	require.Equal(t, "svcA", task.ServiceName)
	require.Equal(t, "public", task.SchemaName)
	require.Equal(t, "users", task.TableName)
	require.Equal(t, "dbt-model", task.NodeType)
	require.Equal(t, pkgEvents.DefaultTaskMaxRetries, task.MaxRetries,
		"single-node DispatchedTask must carry the canonical k8s retry budget")
	require.Equal(t, "m3", task.ManifestVersion)
	require.Equal(t, "v3", task.ImageTag)

	// Entry 1: query.model:v1
	require.Equal(t, "query.model:v1", entries[1].StreamName)
	require.Equal(t, "node_ready_for_execution", entries[1].EventType)
	require.Equal(t, "pending", entries[1].Status)

	var queryEvt domain.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(entries[1].Payload, &queryEvt))
	require.Equal(t, scheduleID, queryEvt.ScheduleID)
	require.Equal(t, "svcA", queryEvt.ServiceName)
	require.Equal(t, "public", queryEvt.SchemaName)
	require.Equal(t, "users", queryEvt.TableName)
	require.Equal(t, taskID, queryEvt.TaskID)
	require.Equal(t, "v3", queryEvt.ImageTag)
	require.Equal(t, "m3", queryEvt.ManifestVersion)
	require.Equal(t, "dbt-model", queryEvt.NodeType)
}

// 2. Snapshot(SingleNode) returns ErrTargetNotFound.
//    Expect ONE outbox entry with stream run.entries.dispatch_failed:v1; handler returns nil.
func TestHandleSingleNodeRun_TargetNotFound(t *testing.T) {
	fx := newSingleNodeRunHandlerFixture(t)
	cmd := makeSingleNodeCmd(uuid.New().String())
	fx.SnapSvc.snapshotErr = snapshot.ErrTargetNotFound

	err := fx.Handler.Handle(context.Background(), cmd, "msg-snr-2")
	require.NoError(t, err, "ErrTargetNotFound must not surface as a handler error")
	require.True(t, fx.UoW.CommittedTx, "transaction must still be committed")

	entries := fx.UoW.outboxRepo.CreatedEntries
	require.Len(t, entries, 1, "expect exactly 1 outbox entry: run.entries.dispatch_failed:v1")
	require.Equal(t, "run.entries.dispatch_failed:v1", entries[0].StreamName)
	require.Equal(t, "run_entries_dispatch_failed", entries[0].EventType)
	require.Equal(t, "pending", entries[0].Status)

	var payload pkgEvents.RunEntriesDispatchFailed
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))
	require.Equal(t, cmd.RunID, payload.ScheduleID)
	require.Equal(t, cmd.ScheduleName, payload.ScheduleName)
	require.Equal(t, "target_not_found", payload.Reason)
}

// 3. Second delivery of the same messageID is a no-op (dedup).
//    After both calls the outbox count must not double.
func TestHandleSingleNodeRun_DedupSecondDelivery(t *testing.T) {
	fx := newSingleNodeRunHandlerFixture(t)
	cmd := makeSingleNodeCmd(uuid.New().String())
	fx.SnapSvc.snapshotResult = singleNodeSnapshotResult{
		TaskID:          uuid.NewString(),
		ImageTag:        "v1",
		ManifestVersion: "m1",
		NodeType:        "dbt-model",
	}

	// First delivery: processes normally.
	require.NoError(t, fx.Handler.Handle(context.Background(), cmd, "msg-snr-dup"))
	require.Equal(t, 1, fx.SnapSvc.snapshotCalls)
	firstCount := len(fx.UoW.outboxRepo.CreatedEntries)
	require.Equal(t, 2, firstCount)

	// Second delivery with the same message ID: must be skipped.
	require.NoError(t, fx.Handler.Handle(context.Background(), cmd, "msg-snr-dup"))
	require.Equal(t, 1, fx.SnapSvc.snapshotCalls, "Snapshot must NOT be called again")
	require.Len(t, fx.UoW.outboxRepo.CreatedEntries, firstCount, "outbox count must not grow on duplicate")
}
