package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/serialization"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeLivePromotion answers LivePromotionSeq from seqs, one value per read and
// the last one repeated once they run out, or with err. Empty seqs read 0.
type fakeLivePromotion struct {
	seqs  []int64
	err   error
	reads int
}

var _ repository.LivePromotionReader = (*fakeLivePromotion)(nil)

func (f *fakeLivePromotion) LivePromotionSeq(context.Context) (int64, error) {
	f.reads++
	if f.err != nil {
		return 0, f.err
	}
	if len(f.seqs) == 0 {
		return 0, nil
	}
	return f.seqs[min(f.reads, len(f.seqs))-1], nil
}

// gatedCmd is a one-seed promoted-seeds run planned against promotion seq.
func gatedCmd(seq int64) domainModel.PromotedSeedsRunInput {
	cmd := promotedSeedsCmd(uuid.New().String(), "seed_users")
	cmd.PromotionSeq = seq
	return cmd
}

func promotedSeedsCmd(runID string, tables ...string) domainModel.PromotedSeedsRunInput {
	nodes := make([]domainModel.PromotedSeedsNode, 0, len(tables))
	for _, tbl := range tables {
		nodes = append(nodes, domainModel.PromotedSeedsNode{
			ServiceName: "core", SchemaName: "analytics", TableName: tbl,
		})
	}
	return domainModel.PromotedSeedsRunInput{
		RunID:        runID,
		ScheduleName: "promote-seed-abc12345",
		ReleaseID:    "rel-1",
		Nodes:        nodes,
		InitiatedBy:  "system",
	}
}

func seedProjection(tables ...string) []snapshot.TaskProjection {
	out := make([]snapshot.TaskProjection, 0, len(tables))
	for _, tbl := range tables {
		out = append(out, snapshot.TaskProjection{
			TaskID:        uuid.New(),
			ServiceName:   "core",
			SchemaName:    "analytics",
			TableName:     tbl,
			ScheduleName:  "seed",
			NodeType:      "dbt-seed",
			InitialStatus: "PENDING",
			ImageTag:      "v1",
			MaxRetries:    pkgEvents.DefaultTaskMaxRetries,
		})
	}
	return out
}

// The run must be announced to state before its tasks are dispatched: without
// run.entries.dispatched:v1 there are no task rows, and the terminal events a
// failing seed Job produces would have no run to land against — which is exactly
// how a failed production seed build used to disappear.
func TestHandlePromotedSeedsRun_AnnouncesTheRunThenDispatchesEveryNode(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	runID := uuid.New().String()
	snap := &fakeSnapshotService{projection: seedProjection("seed_users", "seed_fx_transactions")}
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, snap, &fakeLivePromotion{}, 0, newTestLogger())

	require.NoError(t, h.Handle(ctx, promotedSeedsCmd(runID, "seed_users", "seed_fx_transactions"), "msg-1", nil))

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 3, "one run.entries.dispatched plus one query.model per seed")

	require.Equal(t, streams.RunEntriesDispatchedV1, entries[0].StreamName,
		"the run is announced before any node is dispatched")
	var dispatched pkgEvents.RunEntriesDispatched
	require.NoError(t, json.Unmarshal(entries[0].Payload, &dispatched))
	assert.Equal(t, runID, dispatched.ScheduleID)
	assert.Len(t, dispatched.AllTasks, 2)
	assert.Equal(t, int32(2), dispatched.TotalTaskCount,
		"state sizes the run from this; a wrong count leaves it unable to finalise")
	for _, task := range dispatched.AllTasks {
		assert.Equal(t, pkgEvents.DefaultTaskMaxRetries, task.MaxRetries,
			"each task carries the retry budget that makes a failed seed build recoverable")
	}

	seen := map[string]bool{}
	for _, entry := range entries[1:] {
		require.Equal(t, streams.QueryModelV1, entry.StreamName)
		var q serialization.NodeReadyForExecutionDTO
		require.NoError(t, json.Unmarshal(entry.Payload, &q))
		assert.Equal(t, runID, q.ScheduleID)
		assert.NotEmpty(t, q.JobName)
		seen[q.TableName] = true
	}
	assert.Equal(t, map[string]bool{"seed_users": true, "seed_fx_transactions": true}, seen)

	assert.True(t, uow.CommittedTx)
}

// A seed named on the trigger but absent from the topology degrades to the
// existing dispatch-failed path rather than erroring and redelivering forever.
func TestHandlePromotedSeedsRun_TargetNotFound_EmitsDispatchFailed(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	snap := &fakeSnapshotService{err: snapshot.ErrTargetNotFound}
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, snap, &fakeLivePromotion{}, 0, newTestLogger())

	require.NoError(t, h.Handle(ctx, promotedSeedsCmd(uuid.New().String(), "seed_gone"), "msg-2", nil))

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 1, "only the dispatch-failed row")
	assert.Equal(t, streams.RunEntriesDispatchFailedV1, entries[0].StreamName)
	assert.True(t, uow.CommittedTx)
}

func TestHandlePromotedSeedsRun_SingleSeed_StillAnnouncesTheRun(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	runID := uuid.New().String()
	snap := &fakeSnapshotService{projection: seedProjection("seed_users")}
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, snap, &fakeLivePromotion{}, 0, newTestLogger())

	require.NoError(t, h.Handle(ctx, promotedSeedsCmd(runID, "seed_users"), "msg-3", nil))

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 2)
	assert.Equal(t, streams.RunEntriesDispatchedV1, entries[0].StreamName)
	assert.Equal(t, streams.QueryModelV1, entries[1].StreamName)
}

// Each seed's dispatch carries the retry budget of its projection row, which
// is what lets a failed seed build be retried.
func TestHandlePromotedSeedsRun_DispatchCarriesTheTaskRetryBudget(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	runID := uuid.New().String()
	projection := seedProjection("seed_users")
	projection[0].MaxRetries = 4
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, &fakeSnapshotService{projection: projection}, &fakeLivePromotion{}, 0, newTestLogger())

	require.NoError(t, h.Handle(ctx, promotedSeedsCmd(runID, "seed_users"), "msg-1", nil))

	queries := queryModelDispatches(t, uow)
	require.Len(t, queries, 1)
	assert.Equal(t, int32(4), queries[0].MaxRetries)
}

// A run planned against a promotion the graph has not applied is retried
// without projecting anything, and the error names both seqs.
func TestHandlePromotedSeedsRun_GraphBehindIsRetriedNamingBothSeqs(t *testing.T) {
	uow := newFakeUnitOfWork()
	snap := &fakeSnapshotService{projection: seedProjection("seed_users")}
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, snap, &fakeLivePromotion{seqs: []int64{5}}, 0, newTestLogger())

	err := h.Handle(context.Background(), gatedCmd(7), "msg-behind", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "promotion 7")
	assert.Contains(t, err.Error(), "promotion 5")
	assert.False(t, errors.Is(err, pkgEvents.ErrPermanent))
	assert.Equal(t, pkgredis.ClassTransient, pkgredis.Classify(err), "five deliveries, then dead letter")
	assert.False(t, uow.BegunTx, "nothing is read or written before the graph is ready")
	assert.Empty(t, uow.outboxRepo.CreatedEntries)
	assert.Empty(t, snap.capturedParams.RunID)
}

// A graph at or past the run's promotion projects and dispatches the run.
func TestHandlePromotedSeedsRun_GraphAtOrPastThePromotionProjects(t *testing.T) {
	for name, seq := range map[string]int64{"reached": 7, "ahead": 9} {
		t.Run(name, func(t *testing.T) {
			uow := newFakeUnitOfWork()
			h := handlers.NewHandlePromotedSeedsRunHandler(uow, &fakeSnapshotService{projection: seedProjection("seed_users")},
				&fakeLivePromotion{seqs: []int64{seq}}, 0, newTestLogger())

			require.NoError(t, h.Handle(context.Background(), gatedCmd(7), "msg-"+name, nil))
			assert.Len(t, uow.outboxRepo.CreatedEntries, 2, "run.entries.dispatched and one query.model")
			assert.True(t, uow.CommittedTx)
		})
	}
}

// A swap that lands while the handler waits is picked up by a later read, so
// the run is projected on the first delivery.
func TestHandlePromotedSeedsRun_WaitsForTheSwapToLand(t *testing.T) {
	uow := newFakeUnitOfWork()
	live := &fakeLivePromotion{seqs: []int64{5, 7}}
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, &fakeSnapshotService{projection: seedProjection("seed_users")},
		live, 5*time.Second, newTestLogger())

	require.NoError(t, h.Handle(context.Background(), gatedCmd(7), "msg-wait", nil))
	assert.Equal(t, 2, live.reads)
	assert.True(t, uow.CommittedTx)
}

// A trigger without promotion_seq is projected at once, without reading the
// graph.
func TestHandlePromotedSeedsRun_NoPromotionSeqIsNotGated(t *testing.T) {
	uow := newFakeUnitOfWork()
	live := &fakeLivePromotion{seqs: []int64{0}}
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, &fakeSnapshotService{projection: seedProjection("seed_users")},
		live, 0, newTestLogger())

	require.NoError(t, h.Handle(context.Background(), gatedCmd(0), "msg-ungated", nil))
	assert.Zero(t, live.reads)
	assert.True(t, uow.CommittedTx)
}

// A failed graph read is returned with its cause, so an infrastructure outage
// is classified as one and pauses the consumer instead of spending a delivery.
func TestHandlePromotedSeedsRun_GraphReadFailureKeepsItsCause(t *testing.T) {
	uow := newFakeUnitOfWork()
	boom := errors.New("neo4j unavailable")
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, &fakeSnapshotService{projection: seedProjection("seed_users")},
		&fakeLivePromotion{err: boom}, 0, newTestLogger())

	err := h.Handle(context.Background(), gatedCmd(7), "msg-read-fails", nil)
	assert.ErrorIs(t, err, boom)
	assert.False(t, uow.BegunTx)
}

// A delivery cancelled while it waits for the swap stops waiting and returns the
// context error without projecting anything.
func TestHandlePromotedSeedsRun_CancelledWhileWaitingStopsWaiting(t *testing.T) {
	uow := newFakeUnitOfWork()
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, &fakeSnapshotService{projection: seedProjection("seed_users")},
		&fakeLivePromotion{seqs: []int64{5}}, time.Minute, newTestLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := h.Handle(ctx, gatedCmd(7), "msg-cancelled", nil)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, uow.BegunTx)
	assert.Empty(t, uow.outboxRepo.CreatedEntries)
}
