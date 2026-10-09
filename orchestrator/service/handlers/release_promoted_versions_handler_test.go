package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/pkg/codebundle"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes: ports.CodeBundleReader ────────────────────────────────────────────

type fakeBundleReader struct {
	bundle codebundle.Bundle
	err    error
	calls  []string
}

func (f *fakeBundleReader) Fetch(_ context.Context, uri string) (codebundle.Bundle, error) {
	f.calls = append(f.calls, uri)
	return f.bundle, f.err
}

var _ ports.CodeBundleReader = (*fakeBundleReader)(nil)

// ── fakes: repository.CodeVersionRepository ──────────────────────────────────

type fakeCodeVersionRepository struct {
	in     codeversion.WriteInput
	res    codeversion.WriteResult
	err    error
	called int
}

func (f *fakeCodeVersionRepository) WriteVersions(_ context.Context, in codeversion.WriteInput) (codeversion.WriteResult, error) {
	f.called++
	f.in = in
	return f.res, f.err
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
			{UniqueID: "analytics.revenue", NodeType: "dbt-model", ContentHash: "sha256:abc", UpstreamUniqueIDs: []string{}},
			{UniqueID: "test.revenue_not_null", NodeType: "dbt-test", UpstreamUniqueIDs: []string{"analytics.revenue"}},
		},
	}
}

// versionsInput is promotion 3 of rel-1, which changed analytics.revenue.
func versionsInput(artifacts *fakeArtifactReader) domainModel.PromoteReleaseInput {
	return domainModel.PromoteReleaseInput{
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

func newVersionsHandler(
	uow *fakeUnitOfWork,
	reader *fakeBundleReader,
	artifacts *fakeArtifactReader,
	repo *fakeCodeVersionRepository,
) *handlers.ReleasePromotedVersionsHandler {
	return handlers.NewReleasePromotedVersionsHandler(uow, reader, artifacts, repo, newTestLogger())
}

// ── tests ────────────────────────────────────────────────────────────────────

func TestVersionsHandler_WritesVersionsFromTheBundle(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{bundle: versionsBundle()}
	artifacts := &fakeArtifactReader{}
	repo := &fakeCodeVersionRepository{res: codeversion.WriteResult{
		NodeVersionsCreated: 1, CurrentPointersMoved: 1, GraphReleaseID: "rel-1", GraphPromotionSeq: 3,
	}}

	in := versionsInput(artifacts)
	require.NoError(t, newVersionsHandler(uow, reader, artifacts, repo).Handle(ctx, "1-0", nil, in))

	assert.Equal(t, []string{versionsBundleURI}, reader.calls)
	assert.Equal(t, []string{in.TopologyURI}, artifacts.calls)
	assert.Equal(t, 1, repo.called)
	assert.Equal(t, "rel-1", repo.in.ReleaseID)
	assert.Equal(t, int64(3), repo.in.PromotionSeq)
	assert.Equal(t, "org/svc", repo.in.Repo)
	assert.Equal(t, "deadbeef", repo.in.CommitSHA)
	assert.Equal(t, in.PromotedAt, repo.in.PromotedAt)
	require.Len(t, repo.in.Nodes, 1)
	assert.Equal(t, "analytics.revenue", repo.in.Nodes[0].UniqueID)
	assert.False(t, repo.in.Nodes[0].Healed, "a changed node of a non-bootstrap release is exact")
	assert.True(t, uow.CommittedTx)

	mp, err := uow.msgProcRepo.GetByMessageIDAndStream(ctx, "1-0", streams.OrchestratorReleasePromotedVersionsV2)
	require.NoError(t, err)
	require.NotNil(t, mp, "the dedup row is scoped by the v2 versions group")
}

// An announcement (bench, e2e, upgrade re-announcement) carries no bundle; no
// retry can produce one, so it is acknowledged without reading anything.
func TestVersionsHandler_EmptyBundleURIIsAcknowledged(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{}
	artifacts := &fakeArtifactReader{}
	repo := &fakeCodeVersionRepository{}

	in := versionsInput(artifacts)
	in.CodeBundleURI = ""
	require.NoError(t, newVersionsHandler(uow, reader, artifacts, repo).Handle(context.Background(), "1-0", nil, in))

	assert.Empty(t, reader.calls)
	assert.Empty(t, artifacts.calls)
	assert.Zero(t, repo.called)
	assert.True(t, uow.CommittedTx)
}

func TestVersionsHandler_MissingBundleIsRetryable(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	reader := &fakeBundleReader{err: fmt.Errorf("%w: %s", ports.ErrBundleNotFound, versionsBundleURI)}
	repo := &fakeCodeVersionRepository{}

	err := newVersionsHandler(uow, reader, artifacts, repo).Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Zero(t, repo.called)
	assert.False(t, uow.CommittedTx)
	assert.True(t, uow.RolledBackTx)
}

func TestVersionsHandler_MalformedBundleIsPermanent(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	reader := &fakeBundleReader{err: fmt.Errorf("%w: bad json", ports.ErrBundleMalformed)}
	repo := &fakeCodeVersionRepository{}

	err := newVersionsHandler(uow, reader, artifacts, repo).Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Zero(t, repo.called)
}

func TestVersionsHandler_OversizedBundleIsPermanent(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	reader := &fakeBundleReader{err: fmt.Errorf("%w: too big", ports.ErrBundleTooLarge)}
	repo := &fakeCodeVersionRepository{}

	err := newVersionsHandler(uow, reader, artifacts, repo).Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Zero(t, repo.called)
}

func TestVersionsHandler_CorruptArtifactIsPermanent(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	in := versionsInput(artifacts)
	artifacts.err = fmt.Errorf("%w: expected sha256 aa, object has bb", ports.ErrTopologyArtifactCorrupt)
	repo := &fakeCodeVersionRepository{}

	err := newVersionsHandler(uow, &fakeBundleReader{bundle: versionsBundle()}, artifacts, repo).
		Handle(context.Background(), "1-0", nil, in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Zero(t, repo.called)
}

func TestVersionsHandler_MissingArtifactIsRetryable(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	in := versionsInput(artifacts)
	in.TopologyURI = "s3://continuo/tenants/default/topologies/absent/topology.json.gz"
	repo := &fakeCodeVersionRepository{}

	err := newVersionsHandler(uow, &fakeBundleReader{bundle: versionsBundle()}, artifacts, repo).
		Handle(context.Background(), "1-0", nil, in)
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Zero(t, repo.called)
}

// The version group trails the topology swap; until the swap of this promotion
// lands, the nodes have no :Table to attach a version to.
func TestVersionsHandler_UnmatchedNodesBeforeTopologySwapAreRetryable(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	repo := &fakeCodeVersionRepository{res: codeversion.WriteResult{
		UnmatchedNodeIDs: []string{"analytics.revenue"}, GraphReleaseID: "rel-0", GraphPromotionSeq: 2,
	}}

	err := newVersionsHandler(uow, &fakeBundleReader{bundle: versionsBundle()}, artifacts, repo).
		Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Contains(t, err.Error(), "rel-1")
	assert.False(t, uow.CommittedTx)
}

// Once the swap of this promotion has landed, an unmatched node is genuinely
// absent from the topology and must not block the rest of the release's history.
func TestVersionsHandler_UnmatchedNodesAfterTopologySwapAreTolerated(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	repo := &fakeCodeVersionRepository{res: codeversion.WriteResult{
		NodeVersionsCreated: 1,
		UnmatchedNodeIDs:    []string{"analytics.dropped"},
		GraphReleaseID:      "rel-1",
		GraphPromotionSeq:   3,
	}}

	require.NoError(t, newVersionsHandler(uow, &fakeBundleReader{bundle: versionsBundle()}, artifacts, repo).
		Handle(context.Background(), "1-0", nil, versionsInput(artifacts)))
	assert.True(t, uow.CommittedTx)
}

// A late, older promotion whose nodes were retired in the meantime must not
// retry to the delivery ceiling and lose their history: the repository records
// it unattached and the handler acknowledges.
func TestVersionsHandler_GraphAheadIsAcknowledgedNotRetried(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	repo := &fakeCodeVersionRepository{res: codeversion.WriteResult{
		NodeVersionsCreated: 1,
		UnmatchedNodeIDs:    []string{"analytics.retired"},
		GraphReleaseID:      "rel-9",
		GraphPromotionSeq:   9,
		GraphAhead:          true,
	}}

	require.NoError(t, newVersionsHandler(uow, &fakeBundleReader{bundle: versionsBundle()}, artifacts, repo).
		Handle(context.Background(), "1-0", nil, versionsInput(artifacts)))
	assert.True(t, uow.CommittedTx, "the message is acknowledged rather than retried")
}

func TestVersionsHandler_DuplicateMessageIsSkipped(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{bundle: versionsBundle()}
	artifacts := &fakeArtifactReader{}
	repo := &fakeCodeVersionRepository{}
	handler := newVersionsHandler(uow, reader, artifacts, repo)
	in := versionsInput(artifacts)

	require.NoError(t, handler.Handle(context.Background(), "1-0", nil, in))
	require.NoError(t, handler.Handle(context.Background(), "1-0", nil, in))
	assert.Equal(t, 1, repo.called, "a redelivered message must not re-ingest")
	assert.Len(t, reader.calls, 1, "and must not re-fetch the bundle")
	assert.Len(t, artifacts.calls, 1, "or the artifact")
}

func TestVersionsHandler_GraphFailureLeavesNoDedupRow(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	repo := &fakeCodeVersionRepository{err: errors.New("neo4j unavailable")}

	err := newVersionsHandler(uow, &fakeBundleReader{bundle: versionsBundle()}, artifacts, repo).
		Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.False(t, uow.CommittedTx, "rolling back drops the dedup row so the retry reprocesses")
	assert.True(t, uow.RolledBackTx)
}

// A bundle URI that resolves to another release's document must not be written:
// doing so would stamp this release's provenance onto code it never promoted.
func TestVersionsHandler_BundleForAnotherReleaseIsPermanent(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	b := versionsBundle()
	b.ReleaseID = "rel-99"
	repo := &fakeCodeVersionRepository{}

	err := newVersionsHandler(uow, &fakeBundleReader{bundle: b}, artifacts, repo).
		Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Zero(t, repo.called)
}

func TestVersionsHandler_ArtifactOfAnotherReleaseIsPermanent(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	in := versionsInput(artifacts)
	other := versionsArtifact()
	other.ReleaseID = "rel-99"
	artifacts.docs[in.TopologyURI] = other
	repo := &fakeCodeVersionRepository{}

	err := newVersionsHandler(uow, &fakeBundleReader{bundle: versionsBundle()}, artifacts, repo).
		Handle(context.Background(), "1-0", nil, in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Zero(t, repo.called)
}

// The bundle and the topology artifact come from one parse, so a per-node hash
// disagreement means the bundle is not this release's.
func TestVersionsHandler_BundleHashDisagreeingWithTheArtifactIsPermanent(t *testing.T) {
	uow := newFakeUnitOfWork()
	artifacts := &fakeArtifactReader{}
	b := versionsBundle()
	n := b.Nodes["analytics.revenue"]
	n.ContentHash = "sha256:something-else"
	b.Nodes["analytics.revenue"] = n
	repo := &fakeCodeVersionRepository{}

	err := newVersionsHandler(uow, &fakeBundleReader{bundle: b}, artifacts, repo).
		Handle(context.Background(), "1-0", nil, versionsInput(artifacts))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Contains(t, err.Error(), "analytics.revenue")
	assert.Zero(t, repo.called)
}
