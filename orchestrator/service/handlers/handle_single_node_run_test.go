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
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandleSingleNodeRun_NoTests_EmitsDispatchFailed covers the no_tests gate:
// when the snapshot service reports ErrNoTests (a single-node TEST run whose
// target has zero dbt tests), the handler must emit
// run.entries.dispatch_failed:v1 with reason "no_tests", mark the dedup record
// completed, commit, and return nil (not an error — this is an expected
// "no work to dispatch" outcome, not a transient failure to retry).
func TestHandleSingleNodeRun_NoTests_EmitsDispatchFailed(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	snap := &fakeSnapshotService{err: snapshot.ErrNoTests}
	h := handlers.NewHandleSingleNodeRunHandler(uow, snap, newTestLogger())

	cmd := domainModel.SingleNodeRunInput{
		RunID:          uuid.New().String(),
		ScheduleName:   "daily",
		ServiceName:    "svc",
		SchemaName:     "s",
		TableName:      "t",
		MetadataSource: "latest",
		Operation:      "test",
		InitiatedBy:    "system",
	}

	err := h.Handle(ctx, cmd, "msg-1", nil)
	require.NoError(t, err)

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 1, "only run.entries.dispatch_failed, no dispatched/query.model")
	require.Equal(t, streams.RunEntriesDispatchFailedV1, entries[0].StreamName)

	var failed pkgEvents.RunEntriesDispatchFailed
	require.NoError(t, json.Unmarshal(entries[0].Payload, &failed))
	assert.Equal(t, pkgEvents.DispatchFailedReasonNoTests, failed.Reason)
	assert.Equal(t, cmd.RunID, failed.ScheduleID)

	assert.True(t, uow.CommittedTx)
}

// TestHandleSingleNodeRun_TestOperation_StampsOperationOnQueryModel covers the
// happy path: a single-node TEST run whose target has tests dispatches
// normally, and the emitted query.model:v1 payload carries operation="test"
// so the executor runs `dbt test` instead of the default verb.
func TestHandleSingleNodeRun_TestOperation_StampsOperationOnQueryModel(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	taskID := uuid.New()
	snap := &fakeSnapshotService{
		projection: []snapshot.TaskProjection{
			{
				TaskID:          taskID,
				ServiceName:     "svc",
				SchemaName:      "s",
				TableName:       "t",
				ScheduleName:    "daily",
				NodeType:        "dbt-model",
				InitialStatus:   "PENDING",
				ImageTag:        "v1",
				ManifestVersion: "m1",
				MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
			},
		},
	}
	h := handlers.NewHandleSingleNodeRunHandler(uow, snap, newTestLogger())

	cmd := domainModel.SingleNodeRunInput{
		RunID:          uuid.New().String(),
		ScheduleName:   "daily",
		ServiceName:    "svc",
		SchemaName:     "s",
		TableName:      "t",
		MetadataSource: "latest",
		Operation:      "test",
		InitiatedBy:    "system",
	}

	err := h.Handle(ctx, cmd, "msg-1", nil)
	require.NoError(t, err)

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 2, "1 dispatched + 1 query.model")
	require.Equal(t, streams.QueryModelV1, entries[1].StreamName)

	var qevt domain.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(entries[1].Payload, &qevt))
	assert.Equal(t, "test", qevt.Operation)
	assert.Equal(t, "t", qevt.TableName)
}

// TestHandleSingleNodeRun_RunOperation_QueryModelOmitsOperation guards the
// wire-identical requirement for plain run traffic: Operation is omitempty so
// existing (non-test) single-node runs serialize exactly as before.
func TestHandleSingleNodeRun_RunOperation_QueryModelOmitsOperation(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	taskID := uuid.New()
	snap := &fakeSnapshotService{
		projection: []snapshot.TaskProjection{
			{
				TaskID:          taskID,
				ServiceName:     "svc",
				SchemaName:      "s",
				TableName:       "t",
				ScheduleName:    "daily",
				NodeType:        "dbt-model",
				InitialStatus:   "PENDING",
				ImageTag:        "v1",
				ManifestVersion: "m1",
				MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
			},
		},
	}
	h := handlers.NewHandleSingleNodeRunHandler(uow, snap, newTestLogger())

	cmd := domainModel.SingleNodeRunInput{
		RunID:          uuid.New().String(),
		ScheduleName:   "daily",
		ServiceName:    "svc",
		SchemaName:     "s",
		TableName:      "t",
		MetadataSource: "latest",
		Operation:      "",
		InitiatedBy:    "system",
	}

	err := h.Handle(ctx, cmd, "msg-1", nil)
	require.NoError(t, err)

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 2)
	require.Equal(t, streams.QueryModelV1, entries[1].StreamName)

	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(entries[1].Payload, &raw))
	_, hasOperation := raw["operation"]
	assert.False(t, hasOperation, "plain run traffic must stay wire-identical (no operation key)")
}
