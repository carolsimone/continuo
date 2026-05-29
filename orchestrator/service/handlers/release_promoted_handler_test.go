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
	"github.com/google/uuid"
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
// fakeTopologyRepository and fakeTopologyStateRepository are shared with
// ingest_topology_test.go (same package); see that file for declarations.

// newReleasePromotedHandler wires up a ReleasePromotedHandler with the given
// fakes. topoState.generation is pre-seeded to the provided value so that
// IncrementGeneration returns generation+1 on the first call.
func newReleasePromotedHandler(
	uow *fakeUnitOfWork,
	topoRepo *fakeTopologyRepository,
	topoState *fakeTopologyStateRepository,
	promRepo *fakeReleasePromotionRepository,
) *handlers.ReleasePromotedHandler {
	return handlers.NewReleasePromotedHandler(uow, promRepo, topoRepo, topoState, newTestLogger())
}

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

// expectedEventID returns the deterministic UUID v5 for the given release_id
// using the same namespace constant as the handler.
func expectedEventID(releaseID string) string {
	ns := uuid.MustParse("f0d20655-ae9f-4dc9-a512-99f7ce3955c8")
	return uuid.NewSHA1(ns, []byte(releaseID)).String()
}

// expectedAggregateID returns the deterministic UUID v5 stamped on every
// outbox row for the given release_id. Same namespace as expectedEventID,
// disambiguated by an "aggregate:" prefix so the two values never collide.
func expectedAggregateID(releaseID string) uuid.UUID {
	ns := uuid.MustParse("f0d20655-ae9f-4dc9-a512-99f7ce3955c8")
	return uuid.NewSHA1(ns, []byte("aggregate:"+releaseID))
}

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. Happy path: topology is promoted, TopologyRoot updated, schedules.loaded:v1
// outbox entry emitted with deterministic event_id and topology_generation.
func TestReleasePromoted_HappyPath_PromotesAndEmitsSchedulesLoaded(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	topoState := &fakeTopologyStateRepository{generation: 4} // IncrementGeneration → 5
	promRepo := &fakeReleasePromotionRepository{}
	h := newReleasePromotedHandler(uow, topoRepo, topoState, promRepo)

	in := twoNodeInput()
	err := h.Handle(ctx, "msg-rp-1", nil, in)
	require.NoError(t, err)

	// PromoteRelease called exactly once with the correct release_id.
	require.Len(t, promRepo.promoteReleaseCalls, 1, "PromoteRelease should be called once")
	call := promRepo.promoteReleaseCalls[0]
	assert.Equal(t, "rA", call.ReleaseID)
	require.Len(t, call.Nodes, 2, "two domain nodes expected")
	assert.Equal(t, "svc-a.public.table_a", call.Nodes[0].UniqueID)
	assert.Equal(t, "svc-b.public.table_b", call.Nodes[1].UniqueID)

	// Transaction committed.
	assert.True(t, uow.CommittedTx, "transaction should be committed")

	// IncrementGeneration called once (changed=true path).
	assert.Equal(t, 1, topoState.incrementCalls, "IncrementGeneration must be called once")
	assert.Equal(t, 0, topoState.getCalls, "GetGeneration must NOT be called on changed=true")

	// SetServiceMetadata called with the correct generation.
	require.Len(t, topoRepo.setServiceMetadataCalls, 1, "SetServiceMetadata must be called once")
	smCall := topoRepo.setServiceMetadataCalls[0]
	assert.Equal(t, int64(5), smCall.TopologyGeneration)
	assert.Equal(t, map[string]map[string]string{
		"service-a": {"manifest_version": "rA", "image_tag": "tag-a"},
		"service-b": {"manifest_version": "rA", "image_tag": "tag-b"},
	}, smCall.ServiceMetadata)

	// One outbox entry on schedules.loaded:v1.
	require.Len(t, uow.outboxRepo.CreatedEntries, 1, "one outbox entry expected")
	entry := uow.outboxRepo.CreatedEntries[0]
	assert.Equal(t, streams.SchedulesLoadedV1, entry.StreamName)
	assert.Equal(t, "pending", entry.Status)
	assert.Equal(t, "orchestrator", entry.AggregateType)

	// Payload shape: deterministic event_id, sorted schedule_names,
	// service_metadata with manifest_version=release_id and correct image_tags,
	// and topology_generation present.
	var payload struct {
		EventID             string                       `json:"event_id"`
		ScheduleNames       []string                     `json:"schedule_names"`
		ServiceMetadata     map[string]map[string]string `json:"service_metadata"`
		TopologyGeneration  int64                        `json:"topology_generation"`
	}
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, expectedEventID("rA"), payload.EventID, "event_id must be deterministic")
	assert.Equal(t, []string{"daily", "hourly"}, payload.ScheduleNames)
	assert.Equal(t, map[string]map[string]string{
		"service-a": {"manifest_version": "rA", "image_tag": "tag-a"},
		"service-b": {"manifest_version": "rA", "image_tag": "tag-b"},
	}, payload.ServiceMetadata)
	assert.Equal(t, int64(5), payload.TopologyGeneration)

	// Dedup row written for the inbound message.
	mp, err := uow.msgProcRepo.GetByMessageIDAndStream(ctx, "msg-rp-1", streams.ReleasePromotedV1)
	require.NoError(t, err)
	require.NotNil(t, mp, "dedup row should be written")
	assert.Equal(t, "completed", mp.State)
}

// 2. Idempotent redelivery: PromoteRelease returns changed=false.
// The handler MUST still emit a schedules.loaded:v1 outbox row with the SAME
// deterministic event_id. GetGeneration is called (not IncrementGeneration)
// to include the current generation without bumping it.
func TestReleasePromoted_IdempotentRedelivery_ReemitsOutboxWithSameEventID(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	topoState := &fakeTopologyStateRepository{generation: 7} // GetGeneration returns 7
	promRepo := &fakeReleasePromotionRepository{
		promoteReleaseFn: func(_ context.Context, _ string, _ []topology.ReleasePromotedTopologyNode, _ time.Time) (bool, error) {
			return false, nil // already promoted — no-op
		},
	}
	h := newReleasePromotedHandler(uow, topoRepo, topoState, promRepo)

	in := twoNodeInput()
	err := h.Handle(ctx, "msg-rp-idem", nil, in)
	require.NoError(t, err)

	// Outbox entry IS emitted on redelivery (Fix #4).
	require.Len(t, uow.outboxRepo.CreatedEntries, 1, "outbox entry must be emitted on idempotent redelivery")
	entry := uow.outboxRepo.CreatedEntries[0]

	var payload struct {
		EventID            string `json:"event_id"`
		TopologyGeneration int64  `json:"topology_generation"`
	}
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))

	// event_id must be the SAME deterministic value as the happy-path call.
	assert.Equal(t, expectedEventID("rA"), payload.EventID,
		"event_id must be identical to the original emission so state dedups it")

	// AggregateID must be the SAME deterministic value as the happy-path emission
	// so audit queries can correlate every outbox row for this release.
	assert.Equal(t, expectedAggregateID("rA"), entry.AggregateID,
		"AggregateID must be deterministic per release_id, not random")

	// topology_generation is the current value (not incremented).
	assert.Equal(t, int64(7), payload.TopologyGeneration)

	// IncrementGeneration must NOT be called; GetGeneration must be called once.
	assert.Equal(t, 0, topoState.incrementCalls, "IncrementGeneration must not be called on changed=false")
	assert.Equal(t, 1, topoState.getCalls, "GetGeneration must be called once on changed=false")

	// SetServiceMetadata IS called on changed=false to catch up any cross-step
	// crash where attempt 1 incremented the counter but never wrote :TopologyRoot.
	// MERGE makes it idempotent so the no-crash redelivery case is a no-op
	// beyond an updated_at bump.
	require.Len(t, topoRepo.setServiceMetadataCalls, 1,
		"SetServiceMetadata must be called on changed=false to close the drift window")
	assert.Equal(t, int64(7), topoRepo.setServiceMetadataCalls[0].TopologyGeneration,
		"SetServiceMetadata must be called with the current generation, not an incremented one")

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
	topoRepo := &fakeTopologyRepository{}
	topoState := &fakeTopologyStateRepository{}
	promRepo := &fakeReleasePromotionRepository{}
	h := newReleasePromotedHandler(uow, topoRepo, topoState, promRepo)

	in := twoNodeInput()
	// First call processes normally.
	require.NoError(t, h.Handle(ctx, "msg-rp-dup", nil, in))

	// Reset call tracking.
	promRepo.promoteReleaseCalls = nil
	uow.outboxRepo.CreatedEntries = nil
	uow.CommittedTx = false

	// Second call with same message ID.
	err := h.Handle(ctx, "msg-rp-dup", nil, in)
	require.NoError(t, err)

	assert.Empty(t, promRepo.promoteReleaseCalls, "PromoteRelease must NOT be called on dedup hit")
	assert.Empty(t, uow.outboxRepo.CreatedEntries, "no outbox on dedup hit")
}

// 4. Neo4j error from PromoteRelease propagates as retryable (not ErrPermanent).
// The handler returns a non-nil error so the binding NACKs and the message
// replays. In a real Postgres transaction the dedup INSERT is rolled back;
// the fake UoW is not transaction-aware so we verify rollback was called and
// no commit occurred, which is the observable contract for replay eligibility.
func TestReleasePromoted_Neo4jError_PropagatesAsRetryable(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	topoState := &fakeTopologyStateRepository{}
	neo4jErr := errors.New("neo4j: connection unavailable")
	promRepo := &fakeReleasePromotionRepository{
		promoteReleaseFn: func(_ context.Context, _ string, _ []topology.ReleasePromotedTopologyNode, _ time.Time) (bool, error) {
			return false, neo4jErr
		},
	}
	h := newReleasePromotedHandler(uow, topoRepo, topoState, promRepo)

	err := h.Handle(ctx, "msg-rp-neo4j-err", nil, twoNodeInput())
	require.Error(t, err)
	// Must NOT be a permanent error — the binding must NACK so the message replays.
	assert.False(t, errors.Is(err, events.ErrPermanent), "neo4j error should be retryable, not permanent")

	// Transaction must NOT be committed — in the real system the Postgres tx
	// rolls back, discarding the dedup INSERT so the message can replay.
	assert.False(t, uow.CommittedTx, "transaction must not be committed on PromoteRelease failure")
	assert.True(t, uow.RolledBackTx, "rollback must be called on PromoteRelease failure")

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
	topoRepo := &fakeTopologyRepository{}
	topoState := &fakeTopologyStateRepository{}
	promRepo := &fakeReleasePromotionRepository{} // returns changed=true, nil
	h := newReleasePromotedHandler(uow, topoRepo, topoState, promRepo)

	err := h.Handle(ctx, "msg-rp-outbox-err", nil, twoNodeInput())
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
	topoRepo := &fakeTopologyRepository{}
	topoState := &fakeTopologyStateRepository{}
	promRepo := &fakeReleasePromotionRepository{}
	h := newReleasePromotedHandler(uow, topoRepo, topoState, promRepo)

	emptyIn := domainModel.PromoteReleaseInput{
		ReleaseID: "rEmpty",
		Topology:  []domainEvent.ReleasePromotedNode{},
		ImageTags: map[string]string{},
	}
	err := h.Handle(ctx, "msg-rp-empty", nil, emptyIn)
	require.NoError(t, err)

	require.Len(t, promRepo.promoteReleaseCalls, 1)
	assert.Equal(t, "rEmpty", promRepo.promoteReleaseCalls[0].ReleaseID)
	assert.Empty(t, promRepo.promoteReleaseCalls[0].Nodes)

	require.Len(t, uow.outboxRepo.CreatedEntries, 1)
	var payload struct {
		ScheduleNames   []string                     `json:"schedule_names"`
		ServiceMetadata map[string]map[string]string `json:"service_metadata"`
	}
	require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &payload))
	assert.Empty(t, payload.ScheduleNames)
	assert.Empty(t, payload.ServiceMetadata)
}

// 7. outbox_entry_id is threaded through to DedupWithOutboxEntryID.
// Injects a fake dedup via the message processing repo's InsertIfNotExists
// capture and verifies the outbox_entry_id reaches the dedup layer.
func TestReleasePromoted_OutboxEntryIDPassedThrough(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	topoState := &fakeTopologyStateRepository{}
	promRepo := &fakeReleasePromotionRepository{}
	h := newReleasePromotedHandler(uow, topoRepo, topoState, promRepo)

	knownID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	err := h.Handle(ctx, "msg-rp-oeid", &knownID, twoNodeInput())
	require.NoError(t, err)

	// Verify the outbox_entry_id was stored in the message_processing row.
	// The fake InsertIfNotExists stores the full MessageProcessing struct;
	// we look it up by message_id + stream and check OutboxEntryID.
	var stored *messageprocessingStored
	for _, mp := range uow.msgProcRepo.messages {
		if mp.MessageID == "msg-rp-oeid" {
			stored = &messageprocessingStored{outboxEntryID: mp.OutboxEntryID}
			break
		}
	}
	require.NotNil(t, stored, "message_processing row must be written")
	require.NotNil(t, stored.outboxEntryID, "outbox_entry_id must be threaded to the dedup layer")
	assert.Equal(t, knownID, *stored.outboxEntryID)
}

// helper struct for TestReleasePromoted_OutboxEntryIDPassedThrough.
type messageprocessingStored struct {
	outboxEntryID *uuid.UUID
}

// 8. TopologyRoot is updated after a successful swap.
func TestReleasePromoted_TopologyRootUpdatedAfterPromotion(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	topoState := &fakeTopologyStateRepository{generation: 41} // IncrementGeneration → 42
	promRepo := &fakeReleasePromotionRepository{}              // returns changed=true
	h := newReleasePromotedHandler(uow, topoRepo, topoState, promRepo)

	in := twoNodeInput()
	require.NoError(t, h.Handle(ctx, "msg-rp-topo", nil, in))

	// IncrementGeneration called once; GetGeneration not called.
	assert.Equal(t, 1, topoState.incrementCalls)
	assert.Equal(t, 0, topoState.getCalls)

	// SetServiceMetadata called with generation=42.
	require.Len(t, topoRepo.setServiceMetadataCalls, 1)
	smCall := topoRepo.setServiceMetadataCalls[0]
	assert.Equal(t, int64(42), smCall.TopologyGeneration)
	assert.Equal(t, map[string]map[string]string{
		"service-a": {"manifest_version": "rA", "image_tag": "tag-a"},
		"service-b": {"manifest_version": "rA", "image_tag": "tag-b"},
	}, smCall.ServiceMetadata)

	// Outbox payload carries topology_generation=42.
	require.Len(t, uow.outboxRepo.CreatedEntries, 1)
	var payload struct {
		TopologyGeneration int64 `json:"topology_generation"`
	}
	require.NoError(t, json.Unmarshal(uow.outboxRepo.CreatedEntries[0].Payload, &payload))
	assert.Equal(t, int64(42), payload.TopologyGeneration)
}
