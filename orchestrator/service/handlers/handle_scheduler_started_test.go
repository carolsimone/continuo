package handlers_test

import (
	"context"
	"encoding/json"
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

// fakeSnapshotService returns a fixed projection (or error) from Snapshot.
// It captures the Params it was called with so tests can assert on what the
// handler threaded through (e.g. Operation).
type fakeSnapshotService struct {
	projection []snapshot.TaskProjection
	err        error

	sourceOp    string
	sourceOpErr error

	capturedParams snapshot.Params
}

func (f *fakeSnapshotService) Snapshot(_ context.Context, p snapshot.Params) ([]snapshot.TaskProjection, error) {
	f.capturedParams = p
	return f.projection, f.err
}

func (f *fakeSnapshotService) SourceOperation(_ context.Context, _ string) (string, error) {
	return f.sourceOp, f.sourceOpErr
}

// TestHandleSchedulerStarted_InvalidNodeType_FailsRun verifies that a dispatch
// frontier node with an unparseable node_type fails the run fast via
// run.entries.dispatch_failed:v1 instead of being silently skipped (which would
// leave the run to be cancelled by the watchdog). No dispatched/query.model
// entries must be written.
func TestHandleSchedulerStarted_InvalidNodeType_FailsRun(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()
	scheduleID := uuid.New()

	snap := &fakeSnapshotService{
		projection: []snapshot.TaskProjection{
			{
				TaskID:          uuid.New(),
				ServiceName:     "svc",
				SchemaName:      "p",
				TableName:       "t",
				ScheduleName:    "daily",
				NodeType:        "not-a-node-type",
				InitialStatus:   "PENDING",
				ReadyToDispatch: true,
				MaxRetries:      2,
			},
		},
	}

	h := handlers.NewHandleSchedulerStartedHandler(u, snap, newTestLogger())
	err := h.Handle(ctx, domain.SchedulerStarted{
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		Kind:         "cron",
	}, "msg-1", nil)
	require.NoError(t, err)

	// Exactly one outbox entry, and it is the dispatch_failed signal.
	require.Len(t, u.outboxRepo.CreatedEntries, 1)
	e := u.outboxRepo.CreatedEntries[0]
	assert.Equal(t, streams.RunEntriesDispatchFailedV1, e.StreamName)
	assert.Equal(t, "run_entries_dispatch_failed", e.EventType)
	assert.Equal(t, scheduleID, e.AggregateID)

	var payload pkgEvents.RunEntriesDispatchFailed
	require.NoError(t, json.Unmarshal(e.Payload, &payload))
	assert.Equal(t, pkgEvents.DispatchFailedReasonInvalidNodeType, payload.Reason)

	// No dispatched / query.model entries leaked.
	for _, entry := range u.outboxRepo.CreatedEntries {
		assert.NotEqual(t, streams.RunEntriesDispatchedV1, entry.StreamName,
			"must not write run.entries.dispatched when a node_type is invalid")
		assert.NotEqual(t, streams.QueryModelV1, entry.StreamName,
			"must not dispatch any node when a node_type is invalid")
	}
}

// TestHandleSchedulerStarted_ValidNodeType_Dispatches is the happy-path control:
// a valid node_type on the dispatch frontier produces a dispatched entry plus
// one query.model entry and no dispatch_failed.
func TestHandleSchedulerStarted_ValidNodeType_Dispatches(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()
	scheduleID := uuid.New()

	snap := &fakeSnapshotService{
		projection: []snapshot.TaskProjection{
			{
				TaskID:          uuid.New(),
				ServiceName:     "svc",
				SchemaName:      "p",
				TableName:       "t",
				ScheduleName:    "daily",
				NodeType:        "dbt-model",
				InitialStatus:   "PENDING",
				ReadyToDispatch: true,
				MaxRetries:      2,
			},
		},
	}

	h := handlers.NewHandleSchedulerStartedHandler(u, snap, newTestLogger())
	err := h.Handle(ctx, domain.SchedulerStarted{
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		Kind:         "cron",
	}, "msg-2", nil)
	require.NoError(t, err)

	var dispatched, queryModel, dispatchFailed int
	for _, e := range u.outboxRepo.CreatedEntries {
		switch e.StreamName {
		case streams.RunEntriesDispatchedV1:
			dispatched++
		case streams.QueryModelV1:
			queryModel++
		case streams.RunEntriesDispatchFailedV1:
			dispatchFailed++
		}
	}
	assert.Equal(t, 1, dispatched, "one run.entries.dispatched")
	assert.Equal(t, 1, queryModel, "one query.model dispatch")
	assert.Equal(t, 0, dispatchFailed, "no dispatch_failed on the happy path")
}

// TestHandleSchedulerStarted_Operation_ThreadedToSnapshotAndDispatch verifies
// that evt.Operation ("test") is passed through to snapshot.Params AND
// stamped on every emitted query.model NodeReadyForExecution payload — this
// is what lets a whole-DAG TEST run reach the executor.
func TestHandleSchedulerStarted_Operation_ThreadedToSnapshotAndDispatch(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()
	scheduleID := uuid.New()

	snap := &fakeSnapshotService{
		projection: []snapshot.TaskProjection{
			{
				TaskID:          uuid.New(),
				ServiceName:     "svc",
				SchemaName:      "p",
				TableName:       "t1",
				ScheduleName:    "daily",
				NodeType:        "dbt-model",
				InitialStatus:   "PENDING",
				ReadyToDispatch: true,
				MaxRetries:      2,
			},
			{
				TaskID:          uuid.New(),
				ServiceName:     "svc",
				SchemaName:      "p",
				TableName:       "t2",
				ScheduleName:    "daily",
				NodeType:        "dbt-model",
				InitialStatus:   "PENDING",
				ReadyToDispatch: true,
				MaxRetries:      2,
			},
		},
	}

	h := handlers.NewHandleSchedulerStartedHandler(u, snap, newTestLogger())
	err := h.Handle(ctx, domain.SchedulerStarted{
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		Kind:         "cron",
		Operation:    "test",
	}, "msg-3", nil)
	require.NoError(t, err)

	assert.Equal(t, "test", snap.capturedParams.Operation, "Operation must be threaded onto snapshot.Params")

	var queryModelCount int
	for _, e := range u.outboxRepo.CreatedEntries {
		if e.StreamName != streams.QueryModelV1 {
			continue
		}
		queryModelCount++
		var payload domain.NodeReadyForExecution
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.Equal(t, "test", payload.Operation, "every dispatched node.query.model payload must carry Operation")
	}
	assert.Equal(t, 2, queryModelCount)
}

// TestHandleSchedulerStarted_EmptyOperation_OmittedFromDispatch is the control:
// a normal (non-test) run has evt.Operation == "" and the dispatched
// query.model payloads must not carry an operation field at all (omitempty
// keeps normal-run messages wire-identical).
func TestHandleSchedulerStarted_EmptyOperation_OmittedFromDispatch(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()
	scheduleID := uuid.New()

	snap := &fakeSnapshotService{
		projection: []snapshot.TaskProjection{
			{
				TaskID:          uuid.New(),
				ServiceName:     "svc",
				SchemaName:      "p",
				TableName:       "t1",
				ScheduleName:    "daily",
				NodeType:        "dbt-model",
				InitialStatus:   "PENDING",
				ReadyToDispatch: true,
				MaxRetries:      2,
			},
		},
	}

	h := handlers.NewHandleSchedulerStartedHandler(u, snap, newTestLogger())
	err := h.Handle(ctx, domain.SchedulerStarted{
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		Kind:         "cron",
	}, "msg-4", nil)
	require.NoError(t, err)

	assert.Equal(t, "", snap.capturedParams.Operation)

	var queryModelCount int
	for _, e := range u.outboxRepo.CreatedEntries {
		if e.StreamName != streams.QueryModelV1 {
			continue
		}
		queryModelCount++
		assert.NotContains(t, string(e.Payload), `"operation"`, "omitempty must drop operation from the wire payload")
	}
	assert.Equal(t, 1, queryModelCount)
}
