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
			TaskID:          uuid.New(),
			ServiceName:     "core",
			SchemaName:      "analytics",
			TableName:       tbl,
			ScheduleName:    "seed",
			NodeType:        "dbt-seed",
			InitialStatus:   "PENDING",
			ImageTag:        "v1",
			ManifestVersion: "rel-1",
			MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
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
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, snap, newTestLogger())

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
		var q domain.NodeReadyForExecution
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
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, snap, newTestLogger())

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
	h := handlers.NewHandlePromotedSeedsRunHandler(uow, snap, newTestLogger())

	require.NoError(t, h.Handle(ctx, promotedSeedsCmd(runID, "seed_users"), "msg-3", nil))

	entries := uow.outboxRepo.CreatedEntries
	require.Len(t, entries, 2)
	assert.Equal(t, streams.RunEntriesDispatchedV1, entries[0].StreamName)
	assert.Equal(t, streams.QueryModelV1, entries[1].StreamName)
}
