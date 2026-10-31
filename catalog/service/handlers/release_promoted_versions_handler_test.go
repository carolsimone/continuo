package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/codebundle"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes: ports.CodeBundleReader ────────────────────────────────────────────

// fakeBundleReader serves bundles from memory: the one registered under the
// URI, or bundle when none is. err, when set, is returned for every read.
type fakeBundleReader struct {
	bundle  codebundle.Bundle
	bundles map[string]codebundle.Bundle
	err     error
	calls   []string
}

func (f *fakeBundleReader) Fetch(_ context.Context, uri string) (codebundle.Bundle, error) {
	f.calls = append(f.calls, uri)
	if f.err != nil {
		return codebundle.Bundle{}, f.err
	}
	if b, ok := f.bundles[uri]; ok {
		return b, nil
	}
	return f.bundle, nil
}

var _ ports.CodeBundleReader = (*fakeBundleReader)(nil)

// ── fakes: repository.CodeVersionRepository ──────────────────────────────────

// fakeCodeVersionRepository records every batch and answers with res, or with
// err on call failAt (counted from 1; 0 never fails).
type fakeCodeVersionRepository struct {
	res    codeversion.WriteResult
	err    error
	failAt int
	calls  []writeBatchCall
}

type writeBatchCall struct {
	TenantID string
	In       codeversion.WriteInput
}

func (f *fakeCodeVersionRepository) WriteBatch(_ context.Context, tenantID string, in codeversion.WriteInput) (codeversion.WriteResult, error) {
	f.calls = append(f.calls, writeBatchCall{TenantID: tenantID, In: in})
	if f.failAt != 0 && len(f.calls) == f.failAt {
		return codeversion.WriteResult{}, f.err
	}
	return f.res, nil
}

var _ repository.CodeVersionRepository = (*fakeCodeVersionRepository)(nil)

// ── fixtures ─────────────────────────────────────────────────────────────────

const versionsBundleURI = "s3://b/code-bundles/rel-1/bundle.json"

func versionsBundle() codebundle.Bundle {
	return codebundle.Bundle{
		ContractVersion: 1,
		ReleaseID:       "rel-1",
		Nodes: map[string]codebundle.Node{
			"analytics.revenue": {
				Runtime: "dbt", RawCode: "select 1", CompiledCode: "select 1",
				SourceHash: "s", SharedCodeHash: "m", ConfigHash: "c",
				ContentHash: "sha256:abc",
			},
		},
	}
}

// versionsArtifact is rel-1's topology: the node the bundle describes, plus a
// dbt-test the bundle does not carry.
func versionsArtifact() topologyartifact.Document {
	return topologyartifact.Document{
		SchemaVersion: topologyartifact.SchemaVersion,
		TenantID:      "default",
		ReleaseID:     "rel-1",
		Nodes: []topologyartifact.Node{
			{UniqueID: "analytics.revenue", SchemaName: "analytics", TableName: "revenue", ServiceName: "svc",
				NodeType: "dbt-model", ContentHash: "sha256:abc", Schedule: "daily", UpstreamUniqueIDs: []string{}},
			{UniqueID: "test.revenue_not_null", NodeType: "dbt-test", UpstreamUniqueIDs: []string{"analytics.revenue"}},
		},
	}
}

// versionsInput is promotion 3 of rel-1 for tenant default, which changed
// analytics.revenue.
func versionsInput(artifacts *fakeArtifactReader) domainModel.PromoteReleaseInput {
	return domainModel.PromoteReleaseInput{
		TenantID:       "default",
		ReleaseID:      "rel-1",
		PromotionSeq:   3,
		TopologyURI:    artifacts.artifactFor(versionsArtifact()),
		TopologySHA256: "sha-rel-1",
		ChangedNodeIDs: []string{"analytics.revenue"},
		Repo:           "org/svc",
		CommitSHA:      "deadbeef",
		PromotedAt:     time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC),
		CodeBundleURI:  versionsBundleURI,
	}
}

// wideRelease is a release of n models, each using shared unit svc:shared,
// and the last 50 also svc:late, with its bundle and its artifact (which also
// carries a dbt-test).
func wideRelease(n int) (codebundle.Bundle, topologyartifact.Document) {
	b := codebundle.Bundle{ContractVersion: 1, ReleaseID: "rel-1", Nodes: map[string]codebundle.Node{},
		SharedCode: map[string]codebundle.SharedCodeUnit{
			"svc:shared": {Source: "shared", Checksum: "s1"},
			"svc:late":   {Source: "late", Checksum: "l1"},
		}}
	doc := topologyartifact.Document{SchemaVersion: topologyartifact.SchemaVersion, TenantID: "default", ReleaseID: "rel-1"}
	for i := 0; i < n; i++ {
		uid := fmt.Sprintf("analytics.n%03d", i)
		units := []string{"svc:shared"}
		if i >= n-50 {
			units = append(units, "svc:late")
		}
		b.Nodes[uid] = codebundle.Node{Runtime: "dbt", ContentHash: "sha256:" + uid, CodeUnitIDs: units}
		doc.Nodes = append(doc.Nodes, topologyartifact.Node{UniqueID: uid, NodeType: "dbt-model",
			ContentHash: "sha256:" + uid, UpstreamUniqueIDs: []string{}})
	}
	doc.Nodes = append(doc.Nodes, topologyartifact.Node{UniqueID: "test.n000_not_null", NodeType: "dbt-test",
		UpstreamUniqueIDs: []string{"analytics.n000"}})
	return b, doc
}

// newVersionsUnitOfWork is a fake unit of work whose transaction hands out
// repo as its code-version repository.
func newVersionsUnitOfWork(repo repository.CodeVersionRepository) *fakeGraphUnitOfWork {
	u := newFakeGraphUnitOfWork(nil)
	u.tx.codeVersions = repo
	return u
}

func newVersionsHandler(u uow.UnitOfWork, reader *fakeBundleReader, artifacts *fakeArtifactReader) *handlers.ReleasePromotedVersionsHandler {
	return handlers.NewReleasePromotedVersionsHandler(u, reader, artifacts, newTestLogger())
}

// versionsMarker is the marker the versions group records for messageID of
// tenant default.
func versionsMarker(messageID string, outboxEntryID *uuid.UUID) uow.Marker {
	return uow.Marker{TenantID: "default", Group: streams.CatalogReleaseVersions, MessageID: messageID, OutboxEntryID: outboxEntryID}
}

func batchNodeIDs(in codeversion.WriteInput) []string {
	ids := make([]string, 0, len(in.Nodes))
	for _, n := range in.Nodes {
		ids = append(ids, n.UniqueID)
	}
	return ids
}

func batchUnitIDs(in codeversion.WriteInput) []string {
	ids := make([]string, 0, len(in.Units))
	for _, u := range in.Units {
		ids = append(ids, u.UnitID)
	}
	return ids
}

func topologyIDs(in codeversion.WriteInput) []string {
	ids := make([]string, 0, len(in.Topology))
	for id := range in.Topology {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// ── the write ────────────────────────────────────────────────────────────────

func TestVersionsHandler_WritesVersionsFromTheBundle(t *testing.T) {
	ctx := context.Background()
	repo := &fakeCodeVersionRepository{res: codeversion.WriteResult{NodeVersionsCreated: 1, CurrentPointersMoved: 1}}
	u := newVersionsUnitOfWork(repo)
	reader := &fakeBundleReader{bundle: versionsBundle()}
	artifacts := &fakeArtifactReader{}
	in := versionsInput(artifacts)

	require.NoError(t, newVersionsHandler(u, reader, artifacts).Handle(ctx, "1-0", nil, in))

	assert.Equal(t, []string{versionsBundleURI}, reader.calls)
	assert.Equal(t, []string{in.TopologyURI}, artifacts.calls)
	assert.Equal(t, []fakeGraphWrite{{Marker: versionsMarker("1-0", nil), Final: true}}, u.writes,
		"one batch, written with the marker")
	require.Len(t, repo.calls, 1)
	call := repo.calls[0]
	assert.Equal(t, "default", call.TenantID, "the batch is written under the message's tenant")
	assert.Equal(t, "rel-1", call.In.ReleaseID)
	assert.Equal(t, int64(3), call.In.PromotionSeq)
	assert.Equal(t, "org/svc", call.In.Repo)
	assert.Equal(t, "deadbeef", call.In.CommitSHA)
	assert.Equal(t, in.PromotedAt, call.In.PromotedAt)
	require.Len(t, call.In.Nodes, 1)
	assert.Equal(t, "analytics.revenue", call.In.Nodes[0].UniqueID)
	assert.False(t, call.In.Nodes[0].Healed, "a changed node of a non-bootstrap release is exact")
	assert.Equal(t, []string{"analytics.revenue"}, topologyIDs(call.In), "the dbt-test is not among the nodes the swap writes")
	assert.Equal(t, "dbt-model", call.In.Topology["analytics.revenue"].NodeType)
	assert.Equal(t, "daily", call.In.Topology["analytics.revenue"].Schedule)
	assert.True(t, u.isRecorded(versionsMarker("1-0", nil)))
}

// A release larger than one batch is written in batches of VersionsBatchSize
// nodes, one Write each, under the one marker, which only the last Write
// records. Each batch carries the shared code and artifact entries of its own
// nodes.
func TestVersionsHandler_SplitsThePlanIntoBatches(t *testing.T) {
	ctx := context.Background()
	repo := &fakeCodeVersionRepository{res: codeversion.WriteResult{NodeVersionsCreated: 1}}
	u := newVersionsUnitOfWork(repo)
	bundle, doc := wideRelease(2*handlers.VersionsBatchSize + 50)
	artifacts := &fakeArtifactReader{}
	in := promotionOf(artifacts, doc, 3)
	in.CodeBundleURI = versionsBundleURI

	require.NoError(t, newVersionsHandler(u, &fakeBundleReader{bundle: bundle}, artifacts).Handle(ctx, "1-0", nil, in))

	marker := versionsMarker("1-0", nil)
	assert.Equal(t, []fakeGraphWrite{{marker, false}, {marker, false}, {marker, true}}, u.writes)
	require.Len(t, repo.calls, 3)
	var seen []string
	for i, call := range repo.calls {
		ids := batchNodeIDs(call.In)
		assert.LessOrEqual(t, len(ids), handlers.VersionsBatchSize, "batch %d", i)
		assert.Equal(t, ids, topologyIDs(call.In), "batch %d carries the artifact entries of its own nodes", i)
		seen = append(seen, ids...)
	}
	assert.Len(t, repo.calls[2].In.Nodes, 50)
	assert.True(t, sort.StringsAreSorted(seen), "the batches follow the plan's order")
	assert.Len(t, seen, 2*handlers.VersionsBatchSize+50, "every node is written once")
	assert.Equal(t, []string{"svc:shared"}, batchUnitIDs(repo.calls[0].In))
	assert.Equal(t, []string{"svc:late", "svc:shared"}, batchUnitIDs(repo.calls[2].In),
		"a batch carries the shared code its nodes use")
	assert.True(t, u.isRecorded(marker))
}

// A release of exactly one or two batches' worth of nodes is written in that
// many Writes, each full, with the marker on the last: no empty batch trails
// it, and no node is left out.
func TestVersionsHandler_WritesExactMultiplesOfTheBatchSize(t *testing.T) {
	for _, batches := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d nodes", batches*handlers.VersionsBatchSize), func(t *testing.T) {
			repo := &fakeCodeVersionRepository{res: codeversion.WriteResult{NodeVersionsCreated: 1}}
			u := newVersionsUnitOfWork(repo)
			bundle, doc := wideRelease(batches * handlers.VersionsBatchSize)
			artifacts := &fakeArtifactReader{}
			in := promotionOf(artifacts, doc, 3)
			in.CodeBundleURI = versionsBundleURI

			require.NoError(t, newVersionsHandler(u, &fakeBundleReader{bundle: bundle}, artifacts).Handle(context.Background(), "1-0", nil, in))

			marker := versionsMarker("1-0", nil)
			want := make([]fakeGraphWrite, batches)
			for i := range want {
				want[i] = fakeGraphWrite{Marker: marker, Final: i == batches-1}
			}
			assert.Equal(t, want, u.writes)
			require.Len(t, repo.calls, batches)
			total := 0
			for i, call := range repo.calls {
				assert.Len(t, call.In.Nodes, handlers.VersionsBatchSize, "batch %d is full", i)
				total += len(call.In.Nodes)
			}
			assert.Equal(t, batches*handlers.VersionsBatchSize, total, "every node is written once")
			assert.True(t, u.isRecorded(marker))
		})
	}
}

// A batch that fails ends the message with no marker and writes no later
// batch; the redelivery writes every batch again and records the marker.
func TestVersionsHandler_FailedBatchStopsTheMessageWithoutMarker(t *testing.T) {
	ctx := context.Background()
	repo := &fakeCodeVersionRepository{err: errors.New("neo4j: connection reset"), failAt: 2}
	u := newVersionsUnitOfWork(repo)
	bundle, doc := wideRelease(2*handlers.VersionsBatchSize + 50)
	artifacts := &fakeArtifactReader{}
	in := promotionOf(artifacts, doc, 3)
	in.CodeBundleURI = versionsBundleURI
	h := newVersionsHandler(u, &fakeBundleReader{bundle: bundle}, artifacts)

	err := h.Handle(ctx, "1-0", nil, in)
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Len(t, u.writes, 2, "no batch after the failed one is written")
	assert.False(t, u.isRecorded(versionsMarker("1-0", nil)))

	repo.failAt = 0
	repo.calls = nil
	require.NoError(t, h.Handle(ctx, "1-0", nil, in))
	assert.Len(t, repo.calls, 3, "the redelivery writes every batch")
	assert.True(t, u.isRecorded(versionsMarker("1-0", nil)))
}

// An announcement (bench, e2e, upgrade re-announcement) carries no bundle; no
// retry can produce one, so its marker is recorded without reading anything.
func TestVersionsHandler_NoBundleCompletesWithMarker(t *testing.T) {
	repo := &fakeCodeVersionRepository{}
	u := newVersionsUnitOfWork(repo)
	reader := &fakeBundleReader{}
	artifacts := &fakeArtifactReader{}
	in := versionsInput(artifacts)
	in.CodeBundleURI = ""

	require.NoError(t, newVersionsHandler(u, reader, artifacts).Handle(context.Background(), "1-0", nil, in))

	assert.Empty(t, reader.calls)
	assert.Empty(t, artifacts.calls)
	assert.Empty(t, repo.calls)
	assert.Equal(t, []fakeGraphWrite{{Marker: versionsMarker("1-0", nil), Final: true}}, u.writes)
	assert.True(t, u.isRecorded(versionsMarker("1-0", nil)))
}

// A bundle with no node writes no batch and still records the marker.
func TestVersionsHandler_EmptyBundleRecordsTheMarker(t *testing.T) {
	repo := &fakeCodeVersionRepository{}
	u := newVersionsUnitOfWork(repo)
	artifacts := &fakeArtifactReader{}
	b := versionsBundle()
	b.Nodes = map[string]codebundle.Node{}

	require.NoError(t, newVersionsHandler(u, &fakeBundleReader{bundle: b}, artifacts).
		Handle(context.Background(), "1-0", nil, versionsInput(artifacts)))

	assert.Empty(t, repo.calls)
	assert.Equal(t, []fakeGraphWrite{{Marker: versionsMarker("1-0", nil), Final: true}}, u.writes)
	assert.True(t, u.isRecorded(versionsMarker("1-0", nil)))
}

// ── the marker ───────────────────────────────────────────────────────────────

func TestVersionsHandler_FoundMarkerReadsNoObjectStorage(t *testing.T) {
	repo := &fakeCodeVersionRepository{}
	u := newVersionsUnitOfWork(repo)
	u.record(versionsMarker("1-0", nil))
	reader := &fakeBundleReader{bundle: versionsBundle()}
	artifacts := &fakeArtifactReader{}

	require.NoError(t, newVersionsHandler(u, reader, artifacts).Handle(context.Background(), "1-0", nil, versionsInput(artifacts)))

	assert.Equal(t, []uow.Marker{versionsMarker("1-0", nil)}, u.processed)
	assert.Empty(t, reader.calls)
	assert.Empty(t, artifacts.calls)
	assert.Empty(t, u.writes)
}

// A re-sent outbox row arrives under a fresh message id; its outbox entry id
// finds the marker.
func TestVersionsHandler_OutboxEntryIDReachesTheMarker(t *testing.T) {
	entry := uuid.New()
	repo := &fakeCodeVersionRepository{}
	u := newVersionsUnitOfWork(repo)
	reader := &fakeBundleReader{bundle: versionsBundle()}
	artifacts := &fakeArtifactReader{}
	h := newVersionsHandler(u, reader, artifacts)
	in := versionsInput(artifacts)

	require.NoError(t, h.Handle(context.Background(), "1-0", &entry, in))
	require.NoError(t, h.Handle(context.Background(), "2-0", &entry, in))

	assert.Len(t, repo.calls, 1)
	assert.Len(t, reader.calls, 1)
	assert.Equal(t, versionsMarker("2-0", &entry), u.processed[1])
}

func TestVersionsHandler_DuplicateMessageIsSkipped(t *testing.T) {
	repo := &fakeCodeVersionRepository{}
	u := newVersionsUnitOfWork(repo)
	reader := &fakeBundleReader{bundle: versionsBundle()}
	artifacts := &fakeArtifactReader{}
	h := newVersionsHandler(u, reader, artifacts)
	in := versionsInput(artifacts)

	require.NoError(t, h.Handle(context.Background(), "1-0", nil, in))
	require.NoError(t, h.Handle(context.Background(), "1-0", nil, in))
	assert.Len(t, repo.calls, 1, "a redelivered message is not written again")
	assert.Len(t, reader.calls, 1, "nor its bundle read")
	assert.Len(t, artifacts.calls, 1, "nor its artifact")
}

// racingUnitOfWork answers Processed with false while the marker is already
// recorded, as when another delivery of the message commits between the
// lookup and the write.
type racingUnitOfWork struct{ *fakeGraphUnitOfWork }

func (r racingUnitOfWork) Processed(context.Context, uow.Marker) (bool, error) { return false, nil }

// A message another delivery finished meanwhile is acknowledged: its Write
// finds the marker and writes nothing, and no later batch is tried.
func TestVersionsHandler_ProcessedMeanwhileWritesNothing(t *testing.T) {
	repo := &fakeCodeVersionRepository{}
	u := newVersionsUnitOfWork(repo)
	u.record(versionsMarker("1-0", nil))
	bundle, doc := wideRelease(2 * handlers.VersionsBatchSize)
	artifacts := &fakeArtifactReader{}
	in := promotionOf(artifacts, doc, 3)
	in.CodeBundleURI = versionsBundleURI

	require.NoError(t, newVersionsHandler(racingUnitOfWork{u}, &fakeBundleReader{bundle: bundle}, artifacts).
		Handle(context.Background(), "1-0", nil, in))

	assert.Empty(t, repo.calls)
	assert.Len(t, u.writes, 1, "no batch after the one that found the marker")
}

func TestVersionsHandler_MarkerLookupFailureIsReturned(t *testing.T) {
	lookup := errors.New("neo4j: connection refused")
	u := newVersionsUnitOfWork(&fakeCodeVersionRepository{})
	u.processedErr = lookup
	reader := &fakeBundleReader{bundle: versionsBundle()}
	artifacts := &fakeArtifactReader{}

	err := newVersionsHandler(u, reader, artifacts).Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.ErrorIs(t, err, lookup)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Empty(t, reader.calls)
	assert.Empty(t, u.writes)
}

// A write paused by an export or a restore reaches the consumer as
// uow.ErrWritesPaused, which it backs off on as for an outage.
func TestVersionsHandler_PausedWriteIsReturnedAsPaused(t *testing.T) {
	u := newVersionsUnitOfWork(&fakeCodeVersionRepository{})
	u.writeErr = fmt.Errorf("tenant %q: %w", "default", uow.ErrWritesPaused)
	artifacts := &fakeArtifactReader{}

	err := newVersionsHandler(u, &fakeBundleReader{bundle: versionsBundle()}, artifacts).
		Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.ErrorIs(t, err, uow.ErrWritesPaused)
	assert.False(t, u.isRecorded(versionsMarker("1-0", nil)))
}

func TestVersionsHandler_GraphFailureLeavesNoMarker(t *testing.T) {
	repo := &fakeCodeVersionRepository{err: errors.New("neo4j unavailable"), failAt: 1}
	u := newVersionsUnitOfWork(repo)
	artifacts := &fakeArtifactReader{}

	err := newVersionsHandler(u, &fakeBundleReader{bundle: versionsBundle()}, artifacts).
		Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.False(t, u.isRecorded(versionsMarker("1-0", nil)), "the redelivery processes the message again")
}

// ── the bundle and the artifact ──────────────────────────────────────────────

func TestVersionsHandler_MissingBundleIsRetryable(t *testing.T) {
	u := newVersionsUnitOfWork(&fakeCodeVersionRepository{})
	artifacts := &fakeArtifactReader{}
	reader := &fakeBundleReader{err: fmt.Errorf("%w: %s", ports.ErrBundleNotFound, versionsBundleURI)}

	err := newVersionsHandler(u, reader, artifacts).Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Empty(t, u.writes)
}

func TestVersionsHandler_MalformedBundleIsPermanent(t *testing.T) {
	u := newVersionsUnitOfWork(&fakeCodeVersionRepository{})
	artifacts := &fakeArtifactReader{}
	reader := &fakeBundleReader{err: fmt.Errorf("%w: bad json", ports.ErrBundleMalformed)}

	err := newVersionsHandler(u, reader, artifacts).Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Empty(t, u.writes)
}

func TestVersionsHandler_OversizedBundleIsPermanent(t *testing.T) {
	u := newVersionsUnitOfWork(&fakeCodeVersionRepository{})
	artifacts := &fakeArtifactReader{}
	reader := &fakeBundleReader{err: fmt.Errorf("%w: too big", ports.ErrBundleTooLarge)}

	err := newVersionsHandler(u, reader, artifacts).Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Empty(t, u.writes)
}

func TestVersionsHandler_CorruptArtifactIsPermanent(t *testing.T) {
	u := newVersionsUnitOfWork(&fakeCodeVersionRepository{})
	artifacts := &fakeArtifactReader{}
	in := versionsInput(artifacts)
	artifacts.err = fmt.Errorf("%w: expected sha256 aa, object has bb", ports.ErrTopologyArtifactCorrupt)

	err := newVersionsHandler(u, &fakeBundleReader{bundle: versionsBundle()}, artifacts).
		Handle(context.Background(), "1-0", nil, in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Empty(t, u.writes)
}

func TestVersionsHandler_MissingArtifactIsRetryable(t *testing.T) {
	u := newVersionsUnitOfWork(&fakeCodeVersionRepository{})
	artifacts := &fakeArtifactReader{}
	in := versionsInput(artifacts)
	in.TopologyURI = "s3://continuo/tenants/default/topologies/absent/topology.json.gz"

	err := newVersionsHandler(u, &fakeBundleReader{bundle: versionsBundle()}, artifacts).
		Handle(context.Background(), "1-0", nil, in)
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Empty(t, u.writes)
}

// A bundle URI that resolves to another release's document must not be written:
// doing so would stamp this release's provenance onto code it never promoted.
func TestVersionsHandler_BundleForAnotherReleaseIsPermanent(t *testing.T) {
	u := newVersionsUnitOfWork(&fakeCodeVersionRepository{})
	artifacts := &fakeArtifactReader{}
	b := versionsBundle()
	b.ReleaseID = "rel-99"

	err := newVersionsHandler(u, &fakeBundleReader{bundle: b}, artifacts).
		Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Empty(t, u.writes)
}

// An artifact of another tenant than the message's is permanent: no version
// is written under the message's tenant from another tenant's topology.
func TestVersionsHandler_ArtifactOfAnotherTenantIsPermanent(t *testing.T) {
	u := newVersionsUnitOfWork(&fakeCodeVersionRepository{})
	artifacts := &fakeArtifactReader{}
	in := versionsInput(artifacts)
	other := versionsArtifact()
	other.TenantID = "acme"
	artifacts.docs[in.TopologyURI] = other

	err := newVersionsHandler(u, &fakeBundleReader{bundle: versionsBundle()}, artifacts).
		Handle(context.Background(), "1-0", nil, in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Contains(t, err.Error(), "acme")
	assert.Empty(t, u.writes)
}

func TestVersionsHandler_ArtifactOfAnotherReleaseIsPermanent(t *testing.T) {
	u := newVersionsUnitOfWork(&fakeCodeVersionRepository{})
	artifacts := &fakeArtifactReader{}
	in := versionsInput(artifacts)
	other := versionsArtifact()
	other.ReleaseID = "rel-99"
	artifacts.docs[in.TopologyURI] = other

	err := newVersionsHandler(u, &fakeBundleReader{bundle: versionsBundle()}, artifacts).
		Handle(context.Background(), "1-0", nil, in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Empty(t, u.writes)
}

// The bundle and the topology artifact come from one parse, so a per-node hash
// disagreement means the bundle is not this release's.
func TestVersionsHandler_BundleHashDisagreeingWithTheArtifactIsPermanent(t *testing.T) {
	u := newVersionsUnitOfWork(&fakeCodeVersionRepository{})
	artifacts := &fakeArtifactReader{}
	b := versionsBundle()
	n := b.Nodes["analytics.revenue"]
	n.ContentHash = "sha256:something-else"
	b.Nodes["analytics.revenue"] = n

	err := newVersionsHandler(u, &fakeBundleReader{bundle: b}, artifacts).
		Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Contains(t, err.Error(), "analytics.revenue")
	assert.Empty(t, u.writes)
}
