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
type fakeSnapshotService struct {
	projection []snapshot.TaskProjection
	err        error
}

func (f *fakeSnapshotService) Snapshot(_ context.Context, _ snapshot.Params) ([]snapshot.TaskProjection, error) {
	return f.projection, f.err
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
