package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DispatchDerivedRun is the shared pipeline behind HandleRerun and HandleRebase.
// These tests bite on the contract; the per-handler test files only assert
// their own kind/stream/event constants.

func TestDispatchDerivedRun_EmitsDispatchedAndQueryModel(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	pendingID := uuid.New()
	inheritedRoot := uuid.New()
	inheritedID := uuid.New()

	projection := []snapshot.TaskProjection{
		{TaskID: pendingID, ServiceName: "svc", SchemaName: "s", TableName: "tgt",
			ScheduleName: "daily", NodeType: "dbt-model", InitialStatus: "PENDING",
			ReadyToDispatch: true,
			ImageTag: "v1", ManifestVersion: "m1", MaxRetries: pkgEvents.DefaultTaskMaxRetries},
		{TaskID: inheritedID, ServiceName: "svc", SchemaName: "s", TableName: "ok",
			ScheduleName: "daily", NodeType: "dbt-model", InitialStatus: "SUCCEEDED",
			ImageTag: "v1", ManifestVersion: "m1", InheritedFromTaskID: &inheritedRoot},
	}

	msgProcID := uuid.New()
	err := handlers.DispatchDerivedRun(ctx, uow, newTestLogger(), handlers.DerivedRunDispatch{
		RunID:               "00000000-0000-0000-0000-000000000001",
		ScheduleName:        "daily",
		Kind:                "rerun",
		MessageProcessingID: msgProcID,
		Projection:          projection,
	})
	require.NoError(t, err)

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 2, "1 dispatched + 1 query.model (only the PENDING row)")

	require.Equal(t, streams.RunEntriesDispatchedV1, entries[0].StreamName)
	var dispatched pkgEvents.RunEntriesDispatched
	require.NoError(t, json.Unmarshal(entries[0].Payload, &dispatched))
	require.Equal(t, int32(2), dispatched.TotalTaskCount)

	byTable := map[string]pkgEvents.DispatchedTask{}
	for _, dt := range dispatched.AllTasks {
		byTable[dt.TableName] = dt
	}
	assert.Equal(t, "pending", byTable["tgt"].Status)
	assert.Equal(t, "succeeded", byTable["ok"].Status)
	assert.Equal(t, inheritedRoot.String(), byTable["ok"].InheritedFromTaskID)

	require.Equal(t, streams.QueryModelV1, entries[1].StreamName)
	var qevt domain.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(entries[1].Payload, &qevt))
	assert.Equal(t, "tgt", qevt.TableName)
}

func TestDispatchDerivedRun_PreservesTerminalInherits(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	cases := []struct {
		initial string
		wire    string
	}{
		{"FAILED", "failed"},
		{"CANCELLED", "cancelled"},
		{"SKIPPED", "skipped"},
	}

	for _, tc := range cases {
		t.Run(tc.initial, func(t *testing.T) {
			uow.outboxRepo.CreatedEntries = nil // reset

			root := uuid.New()
			projection := []snapshot.TaskProjection{
				{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "term",
					ScheduleName: "daily", InitialStatus: tc.initial, InheritedFromTaskID: &root},
			}
			err := handlers.DispatchDerivedRun(ctx, uow, newTestLogger(), handlers.DerivedRunDispatch{
				RunID:               "00000000-0000-0000-0000-000000000001",
				ScheduleName:        "daily",
				Kind:                "rerun",
				MessageProcessingID: uuid.New(),
				Projection:          projection,
			})
			require.NoError(t, err)

			require.Len(t, uow.outboxRepo.CreatedEntries, 1, "no query.model for non-PENDING rows")
			var dispatched pkgEvents.RunEntriesDispatched
			require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &dispatched))
			require.Equal(t, tc.wire, dispatched.AllTasks[0].Status,
				"%s must round-trip verbatim, not coerce to pending", tc.initial)
		})
	}
}

// A blocked rebased node (PENDING with a still-pending upstream) must NOT get a
// query.model dispatch — only the rebase frontier dispatches up front; the run
// aggregate unblocks/cascade-skips the rest as upstreams complete. Guards the
// regression where the whole rebase subtree was dispatched at once.
func TestDispatchDerivedRun_OnlyDispatchesReadyFrontier(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()

	frontier := uuid.New()
	blocked := uuid.New()
	projection := []snapshot.TaskProjection{
		{TaskID: frontier, ServiceName: "svc", SchemaName: "s", TableName: "e",
			ScheduleName: "daily", NodeType: "dbt-model", InitialStatus: "PENDING",
			ReadyToDispatch: true, MaxRetries: pkgEvents.DefaultTaskMaxRetries},
		{TaskID: blocked, ServiceName: "svc", SchemaName: "s", TableName: "f",
			ScheduleName: "daily", NodeType: "dbt-model", InitialStatus: "PENDING",
			ReadyToDispatch: false, MaxRetries: pkgEvents.DefaultTaskMaxRetries},
	}

	err := handlers.DispatchDerivedRun(ctx, uow, newTestLogger(), handlers.DerivedRunDispatch{
		RunID:               "00000000-0000-0000-0000-000000000001",
		ScheduleName:        "daily",
		Kind:                "rerun",
		MessageProcessingID: uuid.New(),
		Projection:          projection,
	})
	require.NoError(t, err)

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 2, "1 dispatched (both tasks) + 1 query.model (frontier only)")

	// run.entries.dispatched still covers BOTH pending tasks so state creates
	// both task_tracker rows (run finalize counts them).
	var dispatched pkgEvents.RunEntriesDispatched
	require.NoError(t, json.Unmarshal(entries[0].Payload, &dispatched))
	require.Equal(t, int32(2), dispatched.TotalTaskCount)

	require.Equal(t, streams.QueryModelV1, entries[1].StreamName)
	var qevt domain.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(entries[1].Payload, &qevt))
	assert.Equal(t, "e", qevt.TableName, "only the frontier node dispatches; f waits")
}

// sanity: returns a meaningful error if RunID is invalid.
func TestDispatchDerivedRun_InvalidRunID_Errors(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	err := handlers.DispatchDerivedRun(ctx, uow, newTestLogger(), handlers.DerivedRunDispatch{
		RunID:               "not-a-uuid",
		ScheduleName:        "daily",
		Kind:                "rerun",
		MessageProcessingID: uuid.New(),
		Projection:          nil,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.Unwrap(err)) || err != nil, "must propagate parse error")
}
