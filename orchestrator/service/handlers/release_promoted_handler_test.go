package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
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
	ReleaseID    string
	PromotionSeq int64
	Nodes        []topology.ReleasePromotedTopologyNode
}

func (f *fakeReleasePromotionRepository) PromoteRelease(
	_ context.Context,
	releaseID string,
	promotionSeq int64,
	nodes []topology.ReleasePromotedTopologyNode,
	_ time.Time,
) (topology.PromotionOutcome, error) {
	f.promoteReleaseCalls = append(f.promoteReleaseCalls, promoteReleaseCall{
		ReleaseID: releaseID, PromotionSeq: promotionSeq, Nodes: nodes,
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

// ── fixtures ──────────────────────────────────────────────────────────────────

// releaseRA is the artifact of release rA: seed a ← model b, plus a dbt-test the
// live graph must never see.
func releaseRA() topologyartifact.Document {
	return topologyartifact.Document{
		SchemaVersion: topologyartifact.SchemaVersion,
		TenantID:      "default",
		ReleaseID:     "rA",
		Nodes: []topologyartifact.Node{
			{UniqueID: "svc-a.public.table_a", SchemaName: "public", TableName: "table_a",
				ServiceName: "service-a", NodeType: "dbt-seed", ContentHash: "sha256:a",
				ImageTag: "tag-a", Schedule: "daily", UpstreamUniqueIDs: []string{}},
			{UniqueID: "svc-b.public.table_b", SchemaName: "public", TableName: "table_b", //nolint:gosec // G101: secret_ref names a Kubernetes Secret, not a credential
				ServiceName: "service-b", NodeType: "python-api", ContentHash: "sha256:b",
				ImageTag: "tag-b", SecretRef: "continuo-api-b", Schedule: "hourly",
				UpstreamUniqueIDs: []string{"svc-a.public.table_a"}},
			{UniqueID: "test.not_null_table_b_id", ServiceName: "service-b", NodeType: "dbt-test",
				UpstreamUniqueIDs: []string{"svc-b.public.table_b"}},
		},
	}
}

// promotionOf builds promotion seq of doc, registering doc with reader.
func promotionOf(reader *fakeArtifactReader, doc topologyartifact.Document, seq int64, changed ...string) domainModel.PromoteReleaseInput {
	if changed == nil {
		changed = []string{}
	}
	return domainModel.PromoteReleaseInput{
		ReleaseID:      doc.ReleaseID,
		PromotionSeq:   seq,
		TopologyURI:    reader.artifactFor(doc),
		TopologySHA256: "sha-" + doc.ReleaseID,
		ChangedNodeIDs: changed,
		PromotedAt:     time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC),
	}
}

func newReleasePromotedHandler(uow *fakeUnitOfWork, reader *fakeArtifactReader, promRepo *fakeReleasePromotionRepository) *handlers.ReleasePromotedHandler {
	return handlers.NewReleasePromotedHandler(uow, reader, promRepo, newTestLogger())
}

// ── the swap ──────────────────────────────────────────────────────────────────

func TestReleasePromoted_HappyPath_PromotesTheArtifact(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	reader := &fakeArtifactReader{}
	promRepo := &fakeReleasePromotionRepository{}
	in := promotionOf(reader, releaseRA(), 5)

	require.NoError(t, newReleasePromotedHandler(uow, reader, promRepo).Handle(ctx, "msg-rp-1", nil, in))

	assert.Equal(t, []string{in.TopologyURI}, reader.calls, "the topology is read from the artifact")
	require.Len(t, promRepo.promoteReleaseCalls, 1)
	call := promRepo.promoteReleaseCalls[0]
	assert.Equal(t, "rA", call.ReleaseID)
	assert.Equal(t, int64(5), call.PromotionSeq)
	require.Len(t, call.Nodes, 2, "the dbt-test never reaches the live graph")
	assert.Equal(t, "svc-a.public.table_a", call.Nodes[0].UniqueID)
	assert.Equal(t, "sha256:a", call.Nodes[0].ContentHash)
	assert.Equal(t, "continuo-api-b", call.Nodes[1].SecretRef)
	assert.Equal(t, []string{"svc-a.public.table_a"}, call.Nodes[1].UpstreamUniqueIDs)
	assert.Equal(t, "daily", call.Nodes[0].Schedule, "each node keeps its schedule name in the live graph")
	assert.Equal(t, "hourly", call.Nodes[1].Schedule)

	assert.True(t, uow.CommittedTx)
	mp, err := uow.msgProcRepo.GetByMessageIDAndStream(ctx, "msg-rp-1", streams.OrchestratorReleasePromoted)
	require.NoError(t, err)
	require.NotNil(t, mp)
	assert.Equal(t, "completed", mp.State)
}

func TestReleasePromoted_EmptyTopology_PromotesNoNodes(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeArtifactReader{}
	doc := topologyartifact.Document{SchemaVersion: topologyartifact.SchemaVersion, TenantID: "default", ReleaseID: "rEmpty"}
	promRepo := &fakeReleasePromotionRepository{}

	require.NoError(t, newReleasePromotedHandler(uow, reader, promRepo).
		Handle(context.Background(), "msg-rp-empty", nil, promotionOf(reader, doc, 1)))

	require.Len(t, promRepo.promoteReleaseCalls, 1)
	assert.Empty(t, promRepo.promoteReleaseCalls[0].Nodes)
	assert.True(t, uow.CommittedTx)
}

// ── no event ──────────────────────────────────────────────────────────────────

// A promotion commits whether it applied, was redelivered or was stale: it
// publishes nothing, because run-controller reads the schedule names from its own copy
// of the release and plans seed loads from it, whatever seeds the promotion
// changed.
func TestReleasePromoted_CommitsForEveryOutcome(t *testing.T) {
	for _, outcome := range []topology.PromotionOutcome{
		topology.PromotionApplied, topology.PromotionRedelivered, topology.PromotionStale,
	} {
		t.Run(string(outcome), func(t *testing.T) {
			uow := newFakeUnitOfWork()
			reader := &fakeArtifactReader{}
			in := promotionOf(reader, releaseRA(), 5, "svc-a.public.table_a", "svc-b.public.table_b")

			require.NoError(t, newReleasePromotedHandler(uow, reader, &fakeReleasePromotionRepository{outcome: outcome}).
				Handle(context.Background(), "msg-none-"+string(outcome), nil, in))

			assert.True(t, uow.CommittedTx)
		})
	}
}

// ── the artifact ──────────────────────────────────────────────────────────────

// An artifact that does not match the promotion's checksum is permanent: the
// message is dead-lettered and the live topology is untouched.
func TestReleasePromoted_CorruptArtifactIsPermanent(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeArtifactReader{}
	in := promotionOf(reader, releaseRA(), 5)
	reader.err = fmt.Errorf("%w: expected sha256 aa, object has bb", ports.ErrTopologyArtifactCorrupt)
	promRepo := &fakeReleasePromotionRepository{}

	err := newReleasePromotedHandler(uow, reader, promRepo).Handle(context.Background(), "msg-corrupt", nil, in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, events.ErrPermanent))
	assert.Empty(t, promRepo.promoteReleaseCalls)
	assert.False(t, uow.CommittedTx)
}

// An artifact that is not there yet may still land: retry.
func TestReleasePromoted_MissingArtifactIsRetryable(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeArtifactReader{}
	in := promotionOf(reader, releaseRA(), 5)
	in.TopologyURI = "s3://continuo/tenants/default/topologies/elsewhere/topology.json.gz"

	err := newReleasePromotedHandler(uow, reader, &fakeReleasePromotionRepository{}).
		Handle(context.Background(), "msg-missing", nil, in)
	require.Error(t, err)
	assert.False(t, errors.Is(err, events.ErrPermanent))
	assert.True(t, errors.Is(err, ports.ErrTopologyArtifactNotFound))
	assert.True(t, uow.RolledBackTx)
}

func TestReleasePromoted_ArtifactOfAnotherReleaseIsPermanent(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeArtifactReader{}
	in := promotionOf(reader, releaseRA(), 5)
	other := releaseRA()
	other.ReleaseID = "rZ"
	reader.docs[in.TopologyURI] = other

	err := newReleasePromotedHandler(uow, reader, &fakeReleasePromotionRepository{}).
		Handle(context.Background(), "msg-other", nil, in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, events.ErrPermanent))
}

// How the consumer treats each way the artifact read can fail: a missing object
// counts toward the delivery limit, a corrupt one dead-letters at once, and an
// object-storage outage pauses and retries without counting.
func TestReleasePromoted_ArtifactFailuresAreClassifiedForTheConsumer(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want pkgredis.ErrorClass
	}{
		{"missing object", fmt.Errorf("%w: s3://continuo/k", ports.ErrTopologyArtifactNotFound), pkgredis.ClassTransient},
		{"checksum mismatch", fmt.Errorf("%w: expected sha256 aa, object has bb", ports.ErrTopologyArtifactCorrupt), pkgredis.ClassPermanent},
		{"object storage 5xx", fmt.Errorf("get topology artifact: %w", httpStatusError{status: 503}), pkgredis.ClassInfrastructure},
		{"object storage unreachable", fmt.Errorf("get topology artifact: %w", &net.OpError{Op: "dial", Err: errors.New("connection refused")}), pkgredis.ClassInfrastructure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uow := newFakeUnitOfWork()
			reader := &fakeArtifactReader{}
			in := promotionOf(reader, releaseRA(), 5)
			reader.err = tc.err

			err := newReleasePromotedHandler(uow, reader, &fakeReleasePromotionRepository{}).
				Handle(context.Background(), "msg-classify", nil, in)

			require.Error(t, err)
			assert.Equal(t, tc.want, pkgredis.Classify(err))
			assert.False(t, uow.CommittedTx, "nothing is committed, so the dedup row is discarded and the message replays")
		})
	}
}

// httpStatusError is an object-storage response error carrying an HTTP status.
type httpStatusError struct{ status int }

func (e httpStatusError) Error() string       { return fmt.Sprintf("http status %d", e.status) }
func (e httpStatusError) HTTPStatusCode() int { return e.status }

// ── transaction and dedup ─────────────────────────────────────────────────────

func TestReleasePromoted_DedupHit_ShortCircuits(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	reader := &fakeArtifactReader{}
	promRepo := &fakeReleasePromotionRepository{}
	h := newReleasePromotedHandler(uow, reader, promRepo)
	in := promotionOf(reader, releaseRA(), 5)

	require.NoError(t, h.Handle(ctx, "msg-rp-dup", nil, in))
	promRepo.promoteReleaseCalls = nil
	reader.calls = nil

	require.NoError(t, h.Handle(ctx, "msg-rp-dup", nil, in))
	assert.Empty(t, reader.calls, "a duplicate does not read the artifact")
	assert.Empty(t, promRepo.promoteReleaseCalls)
}

func TestReleasePromoted_Neo4jError_PropagatesAsRetryable(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeArtifactReader{}
	err := newReleasePromotedHandler(uow, reader,
		&fakeReleasePromotionRepository{err: errors.New("neo4j: connection unavailable")}).
		Handle(context.Background(), "msg-rp-neo4j-err", nil, promotionOf(reader, releaseRA(), 5))
	require.Error(t, err)
	assert.False(t, errors.Is(err, events.ErrPermanent))
	assert.False(t, uow.CommittedTx)
	assert.True(t, uow.RolledBackTx)
}

func TestReleasePromoted_OutboxEntryIDPassedThrough(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeArtifactReader{}
	knownID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	require.NoError(t, newReleasePromotedHandler(uow, reader, &fakeReleasePromotionRepository{}).
		Handle(context.Background(), "msg-rp-oeid", &knownID, promotionOf(reader, releaseRA(), 5)))

	var stored *uuid.UUID
	for _, mp := range uow.msgProcRepo.messages {
		if mp.MessageID == "msg-rp-oeid" {
			stored = mp.OutboxEntryID
		}
	}
	require.NotNil(t, stored, "outbox_entry_id must be threaded to the dedup layer")
	assert.Equal(t, knownID, *stored)
}
