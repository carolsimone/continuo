package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
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
	TenantID     string
	ReleaseID    string
	PromotionSeq int64
	Nodes        []topology.ReleasePromotedTopologyNode
}

func (f *fakeReleasePromotionRepository) PromoteRelease(
	_ context.Context,
	tenantID string,
	releaseID string,
	promotionSeq int64,
	nodes []topology.ReleasePromotedTopologyNode,
	_ time.Time,
) (topology.PromotionOutcome, error) {
	f.promoteReleaseCalls = append(f.promoteReleaseCalls, promoteReleaseCall{
		TenantID: tenantID, ReleaseID: releaseID, PromotionSeq: promotionSeq, Nodes: nodes,
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

// promotionOf builds promotion seq of doc for tenant default, registering doc
// with reader.
func promotionOf(reader *fakeArtifactReader, doc topologyartifact.Document, seq int64, changed ...string) domainModel.PromoteReleaseInput {
	if changed == nil {
		changed = []string{}
	}
	return domainModel.PromoteReleaseInput{
		TenantID:       "default",
		ReleaseID:      doc.ReleaseID,
		PromotionSeq:   seq,
		TopologyURI:    reader.artifactFor(doc),
		TopologySHA256: "sha-" + doc.ReleaseID,
		ChangedNodeIDs: changed,
		PromotedAt:     time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC),
	}
}

func newReleasePromotedHandler(u *fakeGraphUnitOfWork, reader *fakeArtifactReader) *handlers.ReleasePromotedHandler {
	return handlers.NewReleasePromotedHandler(u, reader, newTestLogger())
}

// topologyMarker is the marker the topology group records for messageID of
// tenant default.
func topologyMarker(messageID string, outboxEntryID *uuid.UUID) uow.Marker {
	return uow.Marker{TenantID: "default", Group: streams.CatalogReleaseTopology, MessageID: messageID, OutboxEntryID: outboxEntryID}
}

// ── the swap ──────────────────────────────────────────────────────────────────

func TestReleasePromotedHandler_PromotesTheArtifactWithTheMarker(t *testing.T) {
	ctx := context.Background()
	promRepo := &fakeReleasePromotionRepository{}
	u := newFakeGraphUnitOfWork(promRepo)
	reader := &fakeArtifactReader{}
	in := promotionOf(reader, releaseRA(), 5)

	require.NoError(t, newReleasePromotedHandler(u, reader).Handle(ctx, "msg-rp-1", nil, in))

	assert.Equal(t, []string{in.TopologyURI}, reader.calls, "the topology is read from the artifact")
	require.Len(t, promRepo.promoteReleaseCalls, 1)
	call := promRepo.promoteReleaseCalls[0]
	assert.Equal(t, "default", call.TenantID, "the swap runs under the message's tenant")
	assert.Equal(t, "rA", call.ReleaseID)
	assert.Equal(t, int64(5), call.PromotionSeq)
	require.Len(t, call.Nodes, 2, "the dbt-test never reaches the live graph")
	assert.Equal(t, "svc-a.public.table_a", call.Nodes[0].UniqueID)
	assert.Equal(t, "sha256:a", call.Nodes[0].ContentHash)
	assert.Equal(t, "continuo-api-b", call.Nodes[1].SecretRef)
	assert.Equal(t, []string{"svc-a.public.table_a"}, call.Nodes[1].UpstreamUniqueIDs)
	assert.Equal(t, "daily", call.Nodes[0].Schedule, "each node keeps its schedule name in the live graph")
	assert.Equal(t, "hourly", call.Nodes[1].Schedule)

	want := topologyMarker("msg-rp-1", nil)
	assert.Equal(t, []uow.Marker{want}, u.processed, "the marker is looked up before the artifact is read")
	assert.Equal(t, []fakeGraphWrite{{Marker: want, Final: true}}, u.writes, "one final write carries the swap and the marker")
}

func TestReleasePromotedHandler_EmptyTopologyPromotesNoNodes(t *testing.T) {
	promRepo := &fakeReleasePromotionRepository{}
	u := newFakeGraphUnitOfWork(promRepo)
	reader := &fakeArtifactReader{}
	doc := topologyartifact.Document{SchemaVersion: topologyartifact.SchemaVersion, TenantID: "default", ReleaseID: "rEmpty"}

	require.NoError(t, newReleasePromotedHandler(u, reader).
		Handle(context.Background(), "msg-rp-empty", nil, promotionOf(reader, doc, 1)))

	require.Len(t, promRepo.promoteReleaseCalls, 1)
	assert.Empty(t, promRepo.promoteReleaseCalls[0].Nodes)
	assert.True(t, u.isRecorded(topologyMarker("msg-rp-empty", nil)))
}

// A promotion records its marker whether it applied, was redelivered or was
// stale. It publishes nothing: run-controller reads the schedule names from
// its own copy of the release and plans seed loads from it, whatever seeds the
// promotion changed.
func TestReleasePromotedHandler_RecordsTheMarkerForEveryOutcome(t *testing.T) {
	for _, outcome := range []topology.PromotionOutcome{
		topology.PromotionApplied, topology.PromotionRedelivered, topology.PromotionStale,
	} {
		t.Run(string(outcome), func(t *testing.T) {
			u := newFakeGraphUnitOfWork(&fakeReleasePromotionRepository{outcome: outcome})
			reader := &fakeArtifactReader{}
			in := promotionOf(reader, releaseRA(), 5, "svc-a.public.table_a", "svc-b.public.table_b")

			require.NoError(t, newReleasePromotedHandler(u, reader).
				Handle(context.Background(), "msg-none-"+string(outcome), nil, in))

			assert.True(t, u.isRecorded(topologyMarker("msg-none-"+string(outcome), nil)))
		})
	}
}

// ── the marker ────────────────────────────────────────────────────────────────

// A message whose marker is recorded is acknowledged without reading object
// storage or opening a write.
func TestReleasePromotedHandler_FoundMarkerReadsNoObjectStorage(t *testing.T) {
	promRepo := &fakeReleasePromotionRepository{}
	u := newFakeGraphUnitOfWork(promRepo)
	reader := &fakeArtifactReader{}
	in := promotionOf(reader, releaseRA(), 5)
	u.record(topologyMarker("msg-rp-dup", nil))

	require.NoError(t, newReleasePromotedHandler(u, reader).Handle(context.Background(), "msg-rp-dup", nil, in))

	assert.Empty(t, reader.calls, "the artifact is never loaded")
	assert.Empty(t, u.writes)
	assert.Empty(t, promRepo.promoteReleaseCalls)
}

// The same outbox row re-sent under a fresh message id is found by its outbox
// entry id, and the marker the swap records carries it.
func TestReleasePromotedHandler_OutboxEntryIDReachesTheMarker(t *testing.T) {
	ctx := context.Background()
	u := newFakeGraphUnitOfWork(&fakeReleasePromotionRepository{})
	reader := &fakeArtifactReader{}
	in := promotionOf(reader, releaseRA(), 5)
	entry := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	h := newReleasePromotedHandler(u, reader)

	require.NoError(t, h.Handle(ctx, "msg-rp-oeid", &entry, in))
	require.Len(t, u.writes, 1)
	assert.Equal(t, topologyMarker("msg-rp-oeid", &entry), u.writes[0].Marker)

	reader.calls = nil
	require.NoError(t, h.Handle(ctx, "msg-rp-oeid-resent", &entry, in))
	assert.Empty(t, reader.calls, "the re-sent row is recognised by its outbox entry id")
	assert.Len(t, u.writes, 1)
}

// A failed marker lookup is returned before anything else happens, so the
// consumer retries the message.
func TestReleasePromotedHandler_MarkerLookupFailureIsReturned(t *testing.T) {
	lookup := errors.New("neo4j: connection refused")
	u := newFakeGraphUnitOfWork(&fakeReleasePromotionRepository{})
	u.processedErr = lookup
	reader := &fakeArtifactReader{}

	err := newReleasePromotedHandler(u, reader).Handle(context.Background(), "msg-rp-lookup", nil, promotionOf(reader, releaseRA(), 5))
	require.ErrorIs(t, err, lookup)
	assert.False(t, errors.Is(err, events.ErrPermanent))
	assert.Empty(t, reader.calls)
	assert.Empty(t, u.writes)
}

// A write paused by an export or a restore reaches the consumer as
// uow.ErrWritesPaused, which it backs off on as for an outage.
func TestReleasePromotedHandler_PausedWriteIsReturnedAsPaused(t *testing.T) {
	u := newFakeGraphUnitOfWork(&fakeReleasePromotionRepository{})
	u.writeErr = fmt.Errorf("tenant %q: %w", "default", uow.ErrWritesPaused)
	reader := &fakeArtifactReader{}

	err := newReleasePromotedHandler(u, reader).Handle(context.Background(), "msg-rp-paused", nil, promotionOf(reader, releaseRA(), 5))
	require.ErrorIs(t, err, uow.ErrWritesPaused)
	assert.False(t, u.isRecorded(topologyMarker("msg-rp-paused", nil)))
}

// ── the artifact ──────────────────────────────────────────────────────────────

// An artifact that does not match the promotion's checksum is permanent: the
// message is dead-lettered and the live topology is untouched.
func TestReleasePromotedHandler_CorruptArtifactIsPermanent(t *testing.T) {
	promRepo := &fakeReleasePromotionRepository{}
	u := newFakeGraphUnitOfWork(promRepo)
	reader := &fakeArtifactReader{}
	in := promotionOf(reader, releaseRA(), 5)
	reader.err = fmt.Errorf("%w: expected sha256 aa, object has bb", ports.ErrTopologyArtifactCorrupt)

	err := newReleasePromotedHandler(u, reader).Handle(context.Background(), "msg-corrupt", nil, in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, events.ErrPermanent))
	assert.Empty(t, promRepo.promoteReleaseCalls)
	assert.Empty(t, u.writes, "no write opens for a corrupt artifact")
}

// An artifact that is not there yet may still land: retry.
func TestReleasePromotedHandler_MissingArtifactIsRetryable(t *testing.T) {
	u := newFakeGraphUnitOfWork(&fakeReleasePromotionRepository{})
	reader := &fakeArtifactReader{}
	in := promotionOf(reader, releaseRA(), 5)
	in.TopologyURI = "s3://continuo/tenants/default/topologies/elsewhere/topology.json.gz"

	err := newReleasePromotedHandler(u, reader).Handle(context.Background(), "msg-missing", nil, in)
	require.Error(t, err)
	assert.False(t, errors.Is(err, events.ErrPermanent))
	assert.True(t, errors.Is(err, ports.ErrTopologyArtifactNotFound))
	assert.Empty(t, u.writes)
}

func TestReleasePromotedHandler_ArtifactOfAnotherReleaseIsPermanent(t *testing.T) {
	u := newFakeGraphUnitOfWork(&fakeReleasePromotionRepository{})
	reader := &fakeArtifactReader{}
	in := promotionOf(reader, releaseRA(), 5)
	other := releaseRA()
	other.ReleaseID = "rZ"
	reader.docs[in.TopologyURI] = other

	err := newReleasePromotedHandler(u, reader).Handle(context.Background(), "msg-other", nil, in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, events.ErrPermanent))
	assert.Empty(t, u.writes)
}

// How the consumer treats each way the artifact read can fail: a missing object
// counts toward the delivery limit, a corrupt one dead-letters at once, and an
// object-storage outage pauses and retries without counting.
func TestReleasePromotedHandler_ArtifactFailuresAreClassifiedForTheConsumer(t *testing.T) {
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
			u := newFakeGraphUnitOfWork(&fakeReleasePromotionRepository{})
			reader := &fakeArtifactReader{}
			in := promotionOf(reader, releaseRA(), 5)
			reader.err = tc.err

			err := newReleasePromotedHandler(u, reader).Handle(context.Background(), "msg-classify", nil, in)

			require.Error(t, err)
			assert.Equal(t, tc.want, pkgredis.Classify(err))
			assert.Empty(t, u.writes, "nothing is written, so the message replays")
		})
	}
}

// httpStatusError is an object-storage response error carrying an HTTP status.
type httpStatusError struct{ status int }

func (e httpStatusError) Error() string       { return fmt.Sprintf("http status %d", e.status) }
func (e httpStatusError) HTTPStatusCode() int { return e.status }

// ── the write ─────────────────────────────────────────────────────────────────

// A swap that fails rolls back with its marker, so the message replays.
func TestReleasePromotedHandler_SwapErrorIsRetryable(t *testing.T) {
	u := newFakeGraphUnitOfWork(&fakeReleasePromotionRepository{err: errors.New("neo4j: connection unavailable")})
	reader := &fakeArtifactReader{}
	err := newReleasePromotedHandler(u, reader).
		Handle(context.Background(), "msg-rp-neo4j-err", nil, promotionOf(reader, releaseRA(), 5))
	require.Error(t, err)
	assert.False(t, errors.Is(err, events.ErrPermanent))
	assert.False(t, u.isRecorded(topologyMarker("msg-rp-neo4j-err", nil)))
}
