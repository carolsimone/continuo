package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes: repository.ReleasePromotionRepository ─────────────────────────────

// fakeReleasePromotionRepository records every swap and answers with outcome
// (PromotionApplied when unset) or err.
type fakeReleasePromotionRepository struct {
	outcome             topology.PromotionOutcome
	err                 error
	promoteReleaseCalls []promoteReleaseCall
}

type promoteReleaseCall struct {
	ReleaseID       string
	PromotionSeq    int64
	Nodes           []topology.ReleasePromotedTopologyNode
	ServiceMetadata map[string]map[string]string
}

func (f *fakeReleasePromotionRepository) PromoteRelease(
	_ context.Context,
	releaseID string,
	promotionSeq int64,
	nodes []topology.ReleasePromotedTopologyNode,
	serviceMetadata map[string]map[string]string,
	_ time.Time,
) (topology.PromotionOutcome, error) {
	f.promoteReleaseCalls = append(f.promoteReleaseCalls, promoteReleaseCall{
		ReleaseID: releaseID, PromotionSeq: promotionSeq, Nodes: nodes, ServiceMetadata: serviceMetadata,
	})
	if f.err != nil {
		return "", f.err
	}
	if f.outcome == "" {
		return topology.PromotionApplied, nil
	}
	return f.outcome, nil
}

var _ repository.ReleasePromotionRepository = (*fakeReleasePromotionRepository)(nil)

// ── helpers ───────────────────────────────────────────────────────────────────

func newReleasePromotedHandler(uow *fakeUnitOfWork, promRepo *fakeReleasePromotionRepository) *handlers.ReleasePromotedHandler {
	return handlers.NewReleasePromotedHandler(uow, promRepo, newTestLogger())
}

// twoNodeInput builds promotion 5 of release rA with 2 nodes (a → b).
func twoNodeInput() domainModel.PromoteReleaseInput {
	return domainModel.PromoteReleaseInput{
		ReleaseID:    "rA",
		PromotionSeq: 5,
		Topology: []domainEvent.ReleasePromotedNode{
			{
				UniqueID:          "svc-a.public.table_a",
				SchemaName:        "public",
				TableName:         "table_a",
				ServiceName:       "service-a",
				ImageTag:          "tag-a",
				Schedule:          "daily",
				UpstreamUniqueIDs: []string{},
			},
			{ //nolint:gosec // G101: secret_ref names a Kubernetes Secret, not a credential
				UniqueID:          "svc-b.public.table_b",
				SchemaName:        "public",
				TableName:         "table_b",
				ServiceName:       "service-b",
				ImageTag:          "tag-b",
				SecretRef:         "continuo-api-b",
				Schedule:          "hourly",
				UpstreamUniqueIDs: []string{"svc-a.public.table_a"},
			},
		},
		ImageTags: map[string]string{
			"service-a": "tag-a",
			"service-b": "tag-b",
		},
	}
}

// expectedEventID returns the deterministic schedules.loaded:v1 event_id for one
// promotion, using the same namespace constant as the handler.
func expectedEventID(releaseID string, promotionSeq int64) string {
	ns := uuid.MustParse("f0d20655-ae9f-4dc9-a512-99f7ce3955c8")
	return uuid.NewSHA1(ns, []byte(fmt.Sprintf("%s|%d", releaseID, promotionSeq))).String()
}

// expectedAggregateID returns the deterministic UUID v5 stamped on every
// outbox row for the given release_id.
func expectedAggregateID(releaseID string) uuid.UUID {
	ns := uuid.MustParse("f0d20655-ae9f-4dc9-a512-99f7ce3955c8")
	return uuid.NewSHA1(ns, []byte("aggregate:"+releaseID))
}

type schedulesLoadedPayload struct {
	EventID         string                       `json:"event_id"`
	ScheduleNames   []string                     `json:"schedule_names"`
	ServiceMetadata map[string]map[string]string `json:"service_metadata"`
	PromotionSeq    int64                        `json:"promotion_seq"`
}

func schedulesLoadedEntries(uow *fakeUnitOfWork) []*pkgoutbox.Entry {
	var out []*pkgoutbox.Entry
	for _, e := range uow.outboxRepo.CreatedEntries {
		if e.StreamName == streams.SchedulesLoadedV1 {
			out = append(out, e)
		}
	}
	return out
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestReleasePromoted_HappyPath_PromotesAndEmitsSchedulesLoaded(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	promRepo := &fakeReleasePromotionRepository{}
	h := newReleasePromotedHandler(uow, promRepo)

	require.NoError(t, h.Handle(ctx, "msg-rp-1", nil, twoNodeInput()))

	require.Len(t, promRepo.promoteReleaseCalls, 1)
	call := promRepo.promoteReleaseCalls[0]
	assert.Equal(t, "rA", call.ReleaseID)
	assert.Equal(t, int64(5), call.PromotionSeq, "the swap decides by the event's promotion seq")
	require.Len(t, call.Nodes, 2)
	assert.Equal(t, "continuo-api-b", call.Nodes[1].SecretRef, "secret_ref reaches the promotion repository")
	assert.Equal(t, map[string]map[string]string{
		"service-a": {"image_tag": "tag-a"},
		"service-b": {"image_tag": "tag-b"},
	}, call.ServiceMetadata, "service metadata is written by the swap, inside its transaction")
	assert.True(t, uow.CommittedTx)

	entries := schedulesLoadedEntries(uow)
	require.Len(t, entries, 1)
	assert.Equal(t, expectedAggregateID("rA"), entries[0].AggregateID)
	var payload schedulesLoadedPayload
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))
	assert.Equal(t, expectedEventID("rA", 5), payload.EventID)
	assert.Equal(t, []string{"daily", "hourly"}, payload.ScheduleNames)
	assert.Equal(t, int64(5), payload.PromotionSeq)

	mp, err := uow.msgProcRepo.GetByMessageIDAndStream(ctx, "msg-rp-1", streams.OrchestratorReleasePromoted)
	require.NoError(t, err)
	require.NotNil(t, mp)
	assert.Equal(t, "completed", mp.State)
}

// A redelivery after a crash between the Neo4j commit and the Postgres commit
// finds the promotion already live. The follow-up rows were never committed, so
// they are written again — with the same event_id.
func TestReleasePromoted_RedeliveryReemitsSchedulesLoadedWithTheSameEventID(t *testing.T) {
	uow := newFakeUnitOfWork()
	h := newReleasePromotedHandler(uow, &fakeReleasePromotionRepository{outcome: topology.PromotionRedelivered})

	require.NoError(t, h.Handle(context.Background(), "msg-rp-idem", nil, twoNodeInput()))

	entries := schedulesLoadedEntries(uow)
	require.Len(t, entries, 1)
	var payload schedulesLoadedPayload
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))
	assert.Equal(t, expectedEventID("rA", 5), payload.EventID)
	assert.Equal(t, int64(5), payload.PromotionSeq)
	assert.True(t, uow.CommittedTx)
}

// An older promotion arriving late leaves the topology alone and must not move
// state's schedule catalog back either.
func TestReleasePromoted_StalePromotionWritesNoSchedulesLoaded(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	h := newReleasePromotedHandler(uow, &fakeReleasePromotionRepository{outcome: topology.PromotionStale})

	require.NoError(t, h.Handle(ctx, "msg-rp-stale", nil, twoNodeInput()))

	assert.Empty(t, schedulesLoadedEntries(uow))
	assert.True(t, uow.CommittedTx, "the message is acknowledged")
	mp, err := uow.msgProcRepo.GetByMessageIDAndStream(ctx, "msg-rp-stale", streams.OrchestratorReleasePromoted)
	require.NoError(t, err)
	require.NotNil(t, mp)
	assert.Equal(t, "completed", mp.State)
}

// A re-announcement of the same release under a newer seq is a distinct event.
func TestReleasePromoted_EventIDDiffersPerPromotionSeq(t *testing.T) {
	first := newFakeUnitOfWork()
	require.NoError(t, newReleasePromotedHandler(first, &fakeReleasePromotionRepository{}).
		Handle(context.Background(), "msg-1", nil, twoNodeInput()))
	in := twoNodeInput()
	in.PromotionSeq = 6
	second := newFakeUnitOfWork()
	require.NoError(t, newReleasePromotedHandler(second, &fakeReleasePromotionRepository{}).
		Handle(context.Background(), "msg-2", nil, in))

	var a, b schedulesLoadedPayload
	require.NoError(t, json.Unmarshal(schedulesLoadedEntries(first)[0].Payload, &a))
	require.NoError(t, json.Unmarshal(schedulesLoadedEntries(second)[0].Payload, &b))
	assert.NotEqual(t, a.EventID, b.EventID)
}

func TestReleasePromoted_DedupHit_ShortCircuits(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	promRepo := &fakeReleasePromotionRepository{}
	h := newReleasePromotedHandler(uow, promRepo)

	require.NoError(t, h.Handle(ctx, "msg-rp-dup", nil, twoNodeInput()))
	promRepo.promoteReleaseCalls = nil
	uow.outboxRepo.CreatedEntries = nil

	require.NoError(t, h.Handle(ctx, "msg-rp-dup", nil, twoNodeInput()))
	assert.Empty(t, promRepo.promoteReleaseCalls, "PromoteRelease must NOT be called on dedup hit")
	assert.Empty(t, uow.outboxRepo.CreatedEntries, "no outbox on dedup hit")
}

// A Neo4j failure is retryable: the transaction is rolled back, so the dedup
// row is discarded and the message replays.
func TestReleasePromoted_Neo4jError_PropagatesAsRetryable(t *testing.T) {
	uow := newFakeUnitOfWork()
	h := newReleasePromotedHandler(uow, &fakeReleasePromotionRepository{err: errors.New("neo4j: connection unavailable")})

	err := h.Handle(context.Background(), "msg-rp-neo4j-err", nil, twoNodeInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, events.ErrPermanent))
	assert.False(t, uow.CommittedTx)
	assert.True(t, uow.RolledBackTx)
	assert.Empty(t, uow.outboxRepo.CreatedEntries)
}

func TestReleasePromoted_OutboxWriteError_PropagatesAsRetryable(t *testing.T) {
	uow := newFakeUnitOfWork()
	uow.outboxRepo.createErr = errors.New("outbox write failed")
	h := newReleasePromotedHandler(uow, &fakeReleasePromotionRepository{})

	err := h.Handle(context.Background(), "msg-rp-outbox-err", nil, twoNodeInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, events.ErrPermanent))
	assert.False(t, uow.CommittedTx)
}

func TestReleasePromoted_EmptyTopology_StillEmitsSchedulesLoadedWithEmptyArrays(t *testing.T) {
	uow := newFakeUnitOfWork()
	promRepo := &fakeReleasePromotionRepository{}
	h := newReleasePromotedHandler(uow, promRepo)

	in := domainModel.PromoteReleaseInput{ReleaseID: "rEmpty", PromotionSeq: 1, Topology: []domainEvent.ReleasePromotedNode{}}
	require.NoError(t, h.Handle(context.Background(), "msg-rp-empty", nil, in))

	require.Len(t, promRepo.promoteReleaseCalls, 1)
	assert.Empty(t, promRepo.promoteReleaseCalls[0].Nodes)
	entries := schedulesLoadedEntries(uow)
	require.Len(t, entries, 1)
	var payload schedulesLoadedPayload
	require.NoError(t, json.Unmarshal(entries[0].Payload, &payload))
	assert.Empty(t, payload.ScheduleNames)
	assert.Empty(t, payload.ServiceMetadata)
}

func TestReleasePromoted_OutboxEntryIDPassedThrough(t *testing.T) {
	uow := newFakeUnitOfWork()
	h := newReleasePromotedHandler(uow, &fakeReleasePromotionRepository{})

	knownID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	require.NoError(t, h.Handle(context.Background(), "msg-rp-oeid", &knownID, twoNodeInput()))

	var stored *uuid.UUID
	for _, mp := range uow.msgProcRepo.messages {
		if mp.MessageID == "msg-rp-oeid" {
			stored = mp.OutboxEntryID
		}
	}
	require.NotNil(t, stored, "outbox_entry_id must be threaded to the dedup layer")
	assert.Equal(t, knownID, *stored)
}

// The request to build a promotion's changed seeds is written in the same
// transaction as the dedup row, after the swap has committed the :Table nodes a
// run will be projected onto. Each node carries this release's image tag.
func TestReleasePromoted_EmitsSeedsPendingForChangedSeeds(t *testing.T) {
	uow := newFakeUnitOfWork()
	h := newReleasePromotedHandler(uow, &fakeReleasePromotionRepository{})

	in := twoNodeInput()
	in.Topology[0].NodeType = "dbt-seed"
	in.Topology[0].Changed = true
	in.Topology[1].NodeType = "dbt-model"
	in.Topology[1].Changed = true
	require.NoError(t, h.Handle(context.Background(), "msg-seeds-1", nil, in))

	var seedsEntry *pkgoutbox.Entry
	for _, e := range uow.outboxRepo.CreatedEntries {
		if e.StreamName == streams.ReleaseSeedsPendingV1 {
			seedsEntry = e
		}
	}
	require.NotNil(t, seedsEntry)
	var payload struct {
		ReleaseID string `json:"release_id"`
		Nodes     []struct {
			TableName string `json:"table_name"`
			ImageTag  string `json:"image_tag"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(seedsEntry.Payload, &payload))
	assert.Equal(t, "rA", payload.ReleaseID)
	require.Len(t, payload.Nodes, 1, "only the changed seed, not the changed model")
	assert.Equal(t, "table_a", payload.Nodes[0].TableName)
	assert.Equal(t, "tag-a", payload.Nodes[0].ImageTag)
}

// A release that changed no seeds must produce no request at all: state would
// otherwise mint a task-less run that could never reach a terminal state.
func TestReleasePromoted_NoChangedSeeds_EmitsNoSeedsPending(t *testing.T) {
	uow := newFakeUnitOfWork()
	h := newReleasePromotedHandler(uow, &fakeReleasePromotionRepository{})

	require.NoError(t, h.Handle(context.Background(), "msg-seeds-2", nil, twoNodeInput()))
	for _, e := range uow.outboxRepo.CreatedEntries {
		assert.NotEqual(t, streams.ReleaseSeedsPendingV1, e.StreamName)
	}
}
