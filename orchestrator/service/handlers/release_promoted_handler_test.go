package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"time"
)

// ── fakes: repository.ReleasePromotionRepository ─────────────────────────────

type fakeReleasePromotionRepository struct {
	promoteReleaseFn    func(ctx context.Context, releaseID string, nodes []topology.ReleasePromotedTopologyNode, now time.Time) (bool, error)
	promoteReleaseCalls []promoteReleaseCall
}

type promoteReleaseCall struct {
	ReleaseID string
	Nodes     []topology.ReleasePromotedTopologyNode
}

func (f *fakeReleasePromotionRepository) PromoteRelease(
	ctx context.Context,
	releaseID string,
	nodes []topology.ReleasePromotedTopologyNode,
	now time.Time,
) (bool, error) {
	f.promoteReleaseCalls = append(f.promoteReleaseCalls, promoteReleaseCall{
		ReleaseID: releaseID,
		Nodes:     nodes,
	})
	if f.promoteReleaseFn != nil {
		return f.promoteReleaseFn(ctx, releaseID, nodes, now)
	}
	return true, nil
}

var _ repository.ReleasePromotionRepository = (*fakeReleasePromotionRepository)(nil)

// ── helpers ───────────────────────────────────────────────────────────────────

// twoNodeInput builds a PromoteReleaseInput with 2 nodes (a → b).
func twoNodeInput() domainModel.PromoteReleaseInput {
	return domainModel.PromoteReleaseInput{
		ReleaseID: "rA",
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
			{
				UniqueID:          "svc-b.public.table_b",
				SchemaName:        "public",
				TableName:         "table_b",
				ServiceName:       "service-b",
				ImageTag:          "tag-b",
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

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. Happy path: topology is promoted and schedules.loaded:v1 outbox entry emitted.
func TestReleasePromoted_HappyPath_PromotesAndEmitsSchedulesLoaded(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeReleasePromotionRepository{}
	h := handlers.NewReleasePromotedHandler(uow, topoRepo, newTestLogger())

	in := twoNodeInput()
	err := h.Handle(ctx, "msg-rp-1", in)
	require.NoError(t, err)

	// PromoteRelease called exactly once with the correct release_id.
	require.Len(t, topoRepo.promoteReleaseCalls, 1, "PromoteRelease should be called once")
	call := topoRepo.promoteReleaseCalls[0]
	assert.Equal(t, "rA", call.ReleaseID)
	require.Len(t, call.Nodes, 2, "two domain nodes expected")
	assert.Equal(t, "svc-a.public.table_a", call.Nodes[0].UniqueID)
	assert.Equal(t, "svc-b.public.table_b", call.Nodes[1].UniqueID)

	// Transaction committed.
	assert.True(t, uow.CommittedTx, "transaction should be committed")

	// One outbox entry on schedules.loaded:v1.
	require.Len(t, uow.outboxRepo.CreatedEntries, 1, "one outbox entry expected")
	entry := uow.outboxRepo.CreatedEntries[0]
	assert.Equal(t, streams.SchedulesLoadedV1, entry.StreamName)
	assert.Equal(t, "pending", entry.Status)
	assert.Equal(t, "orchestrator", entry.AggregateType)

	// Payload shape: event_id present, sorted schedule_names, service_metadata
	// with manifest_version=release_id and correct image_tags.
	var payload struct {
		EventID         string                       `json:"event_id"`
		ScheduleNames   []string                     `json:"schedule_names"`
		ServiceMetadata map[string]map[string]string `json:"service_metadata"`
	}
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.NotEmpty(t, payload.EventID)
	assert.Equal(t, []string{"daily", "hourly"}, payload.ScheduleNames)
	assert.Equal(t, map[string]map[string]string{
		"service-a": {"manifest_version": "rA", "image_tag": "tag-a"},
		"service-b": {"manifest_version": "rA", "image_tag": "tag-b"},
	}, payload.ServiceMetadata)

	// Dedup row written for the inbound message.
	mp, err := uow.msgProcRepo.GetByMessageIDAndStream(ctx, "msg-rp-1", streams.ReleasePromotedV1)
	require.NoError(t, err)
	require.NotNil(t, mp, "dedup row should be written")
	assert.Equal(t, "completed", mp.State)
}

// 2. Idempotent redelivery: PromoteRelease returns changed=false → no outbox
// entry written, dedup row still marked processed, handler returns nil.
func TestReleasePromoted_IdempotentRedelivery_NoOpAndStillAcks(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeReleasePromotionRepository{
		promoteReleaseFn: func(_ context.Context, _ string, _ []topology.ReleasePromotedTopologyNode, _ time.Time) (bool, error) {
			return false, nil // already promoted — no-op
		},
	}
	h := handlers.NewReleasePromotedHandler(uow, topoRepo, newTestLogger())

	err := h.Handle(ctx, "msg-rp-idem", twoNodeInput())
	require.NoError(t, err)

	// No outbox entry: re-emitting schedules.loaded:v1 for an already-promoted
	// release would cause spurious projection refreshes in state.
	assert.Empty(t, uow.outboxRepo.CreatedEntries, "no outbox entry on idempotent no-op")

	// Dedup row still written so the message is not replayed again.
	mp, err := uow.msgProcRepo.GetByMessageIDAndStream(ctx, "msg-rp-idem", streams.ReleasePromotedV1)
	require.NoError(t, err)
	require.NotNil(t, mp, "dedup row should be written even on no-op")
	assert.Equal(t, "completed", mp.State)
}

// 3. Dedup hit: inbound (message_id, stream) already recorded → short-circuit.
func TestReleasePromoted_DedupHit_ShortCircuits(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeReleasePromotionRepository{}
	h := handlers.NewReleasePromotedHandler(uow, topoRepo, newTestLogger())

	in := twoNodeInput()
	// First call processes normally.
	require.NoError(t, h.Handle(ctx, "msg-rp-dup", in))

	// Reset call tracking.
	topoRepo.promoteReleaseCalls = nil
	uow.outboxRepo.CreatedEntries = nil
	uow.CommittedTx = false

	// Second call with same message ID.
	err := h.Handle(ctx, "msg-rp-dup", in)
	require.NoError(t, err)

	assert.Empty(t, topoRepo.promoteReleaseCalls, "PromoteRelease must NOT be called on dedup hit")
	assert.Empty(t, uow.outboxRepo.CreatedEntries, "no outbox on dedup hit")
}

// 4. Neo4j error from PromoteRelease propagates as retryable (not ErrPermanent)
// and does NOT write a dedup row so the message can replay.
func TestReleasePromoted_Neo4jError_PropagatesAsRetryable(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	neo4jErr := errors.New("neo4j: connection unavailable")
	topoRepo := &fakeReleasePromotionRepository{
		promoteReleaseFn: func(_ context.Context, _ string, _ []topology.ReleasePromotedTopologyNode, _ time.Time) (bool, error) {
			return false, neo4jErr
		},
	}
	h := handlers.NewReleasePromotedHandler(uow, topoRepo, newTestLogger())

	err := h.Handle(ctx, "msg-rp-neo4j-err", twoNodeInput())
	require.Error(t, err)
	// Must NOT be a permanent error — the binding must NACK so the message replays.
	assert.False(t, errors.Is(err, events.ErrPermanent), "neo4j error should be retryable, not permanent")

	// No dedup row written → replay will be processed fresh.
	mp, lookupErr := uow.msgProcRepo.GetByMessageIDAndStream(ctx, "msg-rp-neo4j-err", streams.ReleasePromotedV1)
	require.NoError(t, lookupErr)
	assert.Nil(t, mp, "dedup row must NOT be written when PromoteRelease fails")

	// No outbox written.
	assert.Empty(t, uow.outboxRepo.CreatedEntries)
}

// 5. Outbox write failure propagates as retryable; transaction not committed.
func TestReleasePromoted_OutboxWriteError_PropagatesAsRetryable(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	// Inject outbox error.
	outboxErr := errors.New("outbox write failed")
	uow.outboxRepo.createErr = outboxErr
	topoRepo := &fakeReleasePromotionRepository{} // returns changed=true, nil
	h := handlers.NewReleasePromotedHandler(uow, topoRepo, newTestLogger())

	err := h.Handle(ctx, "msg-rp-outbox-err", twoNodeInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, events.ErrPermanent), "outbox error should be retryable")

	// Commit must NOT have been called.
	assert.False(t, uow.CommittedTx, "transaction must not be committed on outbox error")
}

// 6. Empty topology: PromoteRelease called with empty slice, outbox emitted with
// empty schedule_names and service_metadata.
func TestReleasePromoted_EmptyTopology_StillEmitsSchedulesLoadedWithEmptyArrays(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeReleasePromotionRepository{}
	h := handlers.NewReleasePromotedHandler(uow, topoRepo, newTestLogger())

	emptyIn := domainModel.PromoteReleaseInput{
		ReleaseID: "rEmpty",
		Topology:  []domainEvent.ReleasePromotedNode{},
		ImageTags: map[string]string{},
	}
	err := h.Handle(ctx, "msg-rp-empty", emptyIn)
	require.NoError(t, err)

	require.Len(t, topoRepo.promoteReleaseCalls, 1)
	assert.Equal(t, "rEmpty", topoRepo.promoteReleaseCalls[0].ReleaseID)
	assert.Empty(t, topoRepo.promoteReleaseCalls[0].Nodes)

	require.Len(t, uow.outboxRepo.CreatedEntries, 1)
	var payload struct {
		ScheduleNames   []string                     `json:"schedule_names"`
		ServiceMetadata map[string]map[string]string `json:"service_metadata"`
	}
	require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &payload))
	assert.Empty(t, payload.ScheduleNames)
	assert.Empty(t, payload.ServiceMetadata)
}
