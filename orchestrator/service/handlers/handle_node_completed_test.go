package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/serialization"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAggregateRepository returns a prebuilt Run from Rehydrate and records Save.
type fakeAggregateRepository struct {
	agg       *run.Run
	saveCount int
}

func (f *fakeAggregateRepository) Rehydrate(_ context.Context, _ string, _ run.Scope) (*run.Run, error) {
	return f.agg, nil
}
func (f *fakeAggregateRepository) Save(_ context.Context, _ *run.Run) error {
	f.saveCount++
	return nil
}

var _ run.AggregateRepository = (*fakeAggregateRepository)(nil)

// fakeCancelledSchedules always reports the schedule as not cancelled.
type fakeCancelledSchedules struct{}

func (fakeCancelledSchedules) Insert(context.Context, uuid.UUID) error { return nil }
func (fakeCancelledSchedules) Exists(context.Context, uuid.UUID) (bool, error) {
	return false, nil
}
func (fakeCancelledSchedules) DeleteExpired(context.Context, time.Duration) (int64, error) {
	return 0, nil
}

// TestHandleNodeCompleted_UnblockedInvalidNodeType_FailsRun verifies that when
// completing a node unblocks a downstream node whose node_type is unparseable,
// the run is failed fast via run.entries.dispatch_failed:v1 instead of silently
// dropping the unblocked node (which would stall the run).
func TestHandleNodeCompleted_UnblockedInvalidNodeType_FailsRun(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()

	kA := run.NodeKey{ServiceName: "svc", SchemaName: "p", TableName: "a"}
	kC := run.NodeKey{ServiceName: "svc", SchemaName: "p", TableName: "c"}

	// A is RUNNING and unblocks C; C is PENDING with a bad node_type.
	agg := run.NewRun("run-1", "daily", []*run.RunNode{
		{Key: kA, TaskID: uuid.New(), Status: "RUNNING", ScheduleName: "daily",
			NodeType: "dbt-model", Downstreams: []run.NodeKey{kC}},
		{Key: kC, TaskID: uuid.New(), Status: "PENDING", ScheduleName: "daily",
			NodeType: "not-a-node-type", Upstreams: []run.NodeKey{kA}},
	})

	runs := &fakeAggregateRepository{agg: agg}
	h := handlers.NewHandleNodeCompletedHandler(u, runs, fakeCancelledSchedules{}, newTestLogger())

	err := h.Handle(ctx, domainModel.NodeCompletedInput{
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "daily",
		ServiceName:  "svc",
		SchemaName:   "p",
		TableName:    "a",
		Status:       "SUCCEEDED",
	}, "msg-1", nil)
	require.NoError(t, err)

	// A dispatch_failed entry was written; no query.model entry for the bad node.
	var dispatchFailed, queryModel int
	var failedPayload *pkgEvents.RunEntriesDispatchFailed
	for _, e := range u.outboxRepo.CreatedEntries {
		switch e.StreamName {
		case streams.RunEntriesDispatchFailedV1:
			dispatchFailed++
			var p pkgEvents.RunEntriesDispatchFailed
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			failedPayload = &p
		case streams.QueryModelV1:
			queryModel++
		}
	}
	require.Equal(t, 1, dispatchFailed, "exactly one dispatch_failed entry")
	assert.Equal(t, 0, queryModel, "must not dispatch the invalid-node-type node")
	require.NotNil(t, failedPayload)
	assert.Equal(t, pkgEvents.DispatchFailedReasonInvalidNodeType, failedPayload.Reason)
	assert.True(t, u.CommittedTx, "tx must commit so dispatch_failed is published")
}

// TestHandleNodeCompleted_UnblockedNodeCarriesPinnedSecretRef verifies that a
// downstream node unblocked by a completion is dispatched with the secret_ref
// pinned on its run node, so a python-api pod mounts the Secret its run pinned.
func TestHandleNodeCompleted_UnblockedNodeCarriesPinnedSecretRef(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()

	kA := run.NodeKey{ServiceName: "svc", SchemaName: "p", TableName: "a"}
	kC := run.NodeKey{ServiceName: "svc", SchemaName: "p", TableName: "fx"}

	agg := run.NewRun("run-1", "daily", []*run.RunNode{
		{Key: kA, TaskID: uuid.New(), Status: "RUNNING", ScheduleName: "daily",
			NodeType: "dbt-model", Downstreams: []run.NodeKey{kC}},
		{Key: kC, TaskID: uuid.New(), Status: "PENDING", ScheduleName: "daily", //nolint:gosec // G101: secret_ref names a Kubernetes Secret, not a credential
			NodeType: "python-api", ImageTag: "it1",
			SecretRef: "continuo-api-fx", Upstreams: []run.NodeKey{kA}},
	})

	runs := &fakeAggregateRepository{agg: agg}
	h := handlers.NewHandleNodeCompletedHandler(u, runs, fakeCancelledSchedules{}, newTestLogger())

	err := h.Handle(ctx, domainModel.NodeCompletedInput{
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "daily",
		ServiceName:  "svc",
		SchemaName:   "p",
		TableName:    "a",
		Status:       "SUCCEEDED",
	}, "msg-1", nil)
	require.NoError(t, err)

	var dispatched []serialization.NodeReadyForExecutionDTO
	for _, e := range u.outboxRepo.CreatedEntries {
		if e.StreamName != streams.QueryModelV1 {
			continue
		}
		var dto serialization.NodeReadyForExecutionDTO
		require.NoError(t, json.Unmarshal(e.Payload, &dto))
		dispatched = append(dispatched, dto)
	}
	require.Len(t, dispatched, 1, "the unblocked node is dispatched once")
	got := dispatched[0].ToDomain()
	assert.Equal(t, "fx", got.TableName)
	assert.Equal(t, "continuo-api-fx", got.SecretRef)
}

// TestHandleNodeCompleted_UnblockedNodeCarriesItsRetryBudget verifies that a
// downstream node dispatched through NodeUnblocked carries the retry budget its
// run node was rehydrated with, the same budget state holds for the task.
func TestHandleNodeCompleted_UnblockedNodeCarriesItsRetryBudget(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()

	kA := run.NodeKey{ServiceName: "svc", SchemaName: "p", TableName: "a"}
	kC := run.NodeKey{ServiceName: "svc", SchemaName: "p", TableName: "c"}

	agg := run.NewRun("run-1", "daily", []*run.RunNode{
		{Key: kA, TaskID: uuid.New(), Status: "RUNNING", ScheduleName: "daily",
			NodeType: "dbt-model", Downstreams: []run.NodeKey{kC}},
		{Key: kC, TaskID: uuid.New(), Status: "PENDING", ScheduleName: "daily",
			NodeType: "dbt-model", ImageTag: "it1", MaxRetries: 5, Upstreams: []run.NodeKey{kA}},
	})

	h := handlers.NewHandleNodeCompletedHandler(u, &fakeAggregateRepository{agg: agg}, fakeCancelledSchedules{}, newTestLogger())
	require.NoError(t, h.Handle(ctx, domainModel.NodeCompletedInput{
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "daily",
		ServiceName:  "svc",
		SchemaName:   "p",
		TableName:    "a",
		Status:       "SUCCEEDED",
	}, "msg-1", nil))

	dispatched := queryModelDispatches(t, u)
	require.Len(t, dispatched, 1, "the unblocked node is dispatched once")
	assert.Equal(t, "c", dispatched[0].TableName)
	assert.Equal(t, int32(5), dispatched[0].MaxRetries)
}
