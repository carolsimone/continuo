package handlers_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// ── fake run repository for single-node-run tests ─────────────────────────────

// singleNodeFakeRunRepository wraps fakeRunRepository, overriding
// SnapshotSingleNodeRun so the handler's snapshot call can be stubbed.
type singleNodeFakeRunRepository struct {
	fakeRunRepository

	snapshotResult singleNodeSnapshotResult
	snapshotErr    error
	snapshotCalls  int
}

type singleNodeSnapshotResult struct {
	TaskID          string
	ImageTag        string
	ManifestVersion string
	NodeType        string
}

func (f *singleNodeFakeRunRepository) SnapshotSingleNodeRun(
	_ context.Context,
	_, _ string,
	_ *uuid.UUID,
	_, _, _ string,
	_ string,
) (taskID, imageTag, manifestVersion, nodeType string, err error) {
	f.snapshotCalls++
	if f.snapshotErr != nil {
		return "", "", "", "", f.snapshotErr
	}
	return f.snapshotResult.TaskID, f.snapshotResult.ImageTag, f.snapshotResult.ManifestVersion, f.snapshotResult.NodeType, nil
}

// ── fixture ───────────────────────────────────────────────────────────────────

type singleNodeRunHandlerFixture struct {
	Handler *handlers.HandleSingleNodeRunHandler
	RunRepo *singleNodeFakeRunRepository
	UoW     *fakeUnitOfWork
}

func newSingleNodeRunHandlerFixture(_ *testing.T) *singleNodeRunHandlerFixture {
	uow := newFakeUnitOfWork()
	runRepo := &singleNodeFakeRunRepository{}
	h := handlers.NewHandleSingleNodeRunHandler(uow, runRepo, newTestLogger())
	return &singleNodeRunHandlerFixture{
		Handler: h,
		RunRepo: runRepo,
		UoW:     uow,
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func makeSingleNodeCmd(runID string) domainCmd.HandleSingleNodeRunCmd {
	return domainCmd.HandleSingleNodeRunCmd{
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

// 1. Happy path: SnapshotSingleNodeRun returns concrete values.
//    Expect TWO outbox entries: run.initialized:v1 and query.model:v1.
func TestHandleSingleNodeRun_Latest_HappyPath(t *testing.T) {
	fx := newSingleNodeRunHandlerFixture(t)
	scheduleID := uuid.New().String()
	cmd := makeSingleNodeCmd(scheduleID)

	taskID := uuid.NewString()
	fx.RunRepo.snapshotResult = singleNodeSnapshotResult{
		TaskID:          taskID,
		ImageTag:        "v3",
		ManifestVersion: "m3",
		NodeType:        "dbt-model",
	}

	require.NoError(t, fx.Handler.Handle(context.Background(), cmd, "msg-snr-1"))
	require.True(t, fx.UoW.CommittedTx, "transaction must be committed")

	entries := fx.UoW.outboxRepo.CreatedEntries
	require.Len(t, entries, 2, "expect 2 outbox entries: run.initialized + query.model")

	// Entry 0: run.initialized:v1
	require.Equal(t, "run.initialized:v1", entries[0].StreamName)
	require.Equal(t, "run_initialized", entries[0].EventType)
	require.Equal(t, "orchestrator", entries[0].AggregateType)
	require.Equal(t, "pending", entries[0].Status)

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

// 2. SnapshotSingleNodeRun returns ErrTargetNotFound.
//    Expect ONE outbox entry with stream run.failed:v1; handler returns nil.
func TestHandleSingleNodeRun_TargetNotFound(t *testing.T) {
	fx := newSingleNodeRunHandlerFixture(t)
	cmd := makeSingleNodeCmd(uuid.New().String())
	fx.RunRepo.snapshotErr = run.ErrTargetNotFound

	err := fx.Handler.Handle(context.Background(), cmd, "msg-snr-2")
	require.NoError(t, err, "ErrTargetNotFound must not surface as a handler error")
	require.True(t, fx.UoW.CommittedTx, "transaction must still be committed")

	entries := fx.UoW.outboxRepo.CreatedEntries
	require.Len(t, entries, 1, "expect exactly 1 outbox entry: run.failed:v1")
	require.Equal(t, "run.failed:v1", entries[0].StreamName)
	require.Equal(t, "run_failed", entries[0].EventType)
	require.Equal(t, "pending", entries[0].Status)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))
	require.Equal(t, cmd.RunID, payload["schedule_id"])
	require.Equal(t, "target_not_found", payload["reason"])
}

// 3. Second delivery of the same messageID is a no-op (dedup).
//    After both calls the outbox count must not double.
func TestHandleSingleNodeRun_DedupSecondDelivery(t *testing.T) {
	fx := newSingleNodeRunHandlerFixture(t)
	cmd := makeSingleNodeCmd(uuid.New().String())
	fx.RunRepo.snapshotResult = singleNodeSnapshotResult{
		TaskID:          uuid.NewString(),
		ImageTag:        "v1",
		ManifestVersion: "m1",
		NodeType:        "dbt-model",
	}

	// First delivery: processes normally.
	require.NoError(t, fx.Handler.Handle(context.Background(), cmd, "msg-snr-dup"))
	require.Equal(t, 1, fx.RunRepo.snapshotCalls)
	firstCount := len(fx.UoW.outboxRepo.CreatedEntries)
	require.Equal(t, 2, firstCount)

	// Second delivery with the same message ID: must be skipped.
	require.NoError(t, fx.Handler.Handle(context.Background(), cmd, "msg-snr-dup"))
	require.Equal(t, 1, fx.RunRepo.snapshotCalls, "SnapshotSingleNodeRun must NOT be called again")
	require.Len(t, fx.UoW.outboxRepo.CreatedEntries, firstCount, "outbox count must not grow on duplicate")
}
