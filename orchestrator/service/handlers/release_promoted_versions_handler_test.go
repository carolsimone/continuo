package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/codebundle"
	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/orchestrator/service/ports"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
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

func versionsInput() domainModel.PromoteReleaseInput {
	return domainModel.PromoteReleaseInput{
		ReleaseID:     "rel-1",
		Repo:          "org/svc",
		CommitSHA:     "deadbeef",
		PromotedAt:    time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC),
		CodeBundleURI: versionsBundleURI,
		Topology: []domainEvent.ReleasePromotedNode{
			{UniqueID: "analytics.revenue", ContentHash: "sha256:abc", Changed: true},
		},
	}
}

func newVersionsHandler(
	uow *fakeUnitOfWork,
	reader *fakeBundleReader,
	repo *fakeCodeVersionRepository,
) *handlers.ReleasePromotedVersionsHandler {
	return handlers.NewReleasePromotedVersionsHandler(uow, reader, repo, newTestLogger())
}

// ── tests ────────────────────────────────────────────────────────────────────

func TestVersionsHandler_WritesVersionsFromTheBundle(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{bundle: versionsBundle()}
	repo := &fakeCodeVersionRepository{res: codeversion.WriteResult{
		NodeVersionsCreated: 1, CurrentPointersMoved: 1, GraphReleaseID: "rel-1",
	}}

	in := versionsInput()
	require.NoError(t, newVersionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, in))

	assert.Equal(t, []string{versionsBundleURI}, reader.calls)
	assert.Equal(t, 1, repo.called)
	assert.Equal(t, "rel-1", repo.in.ReleaseID)
	assert.Equal(t, "org/svc", repo.in.Repo)
	assert.Equal(t, "deadbeef", repo.in.CommitSHA)
	assert.Equal(t, in.PromotedAt, repo.in.PromotedAt)
	require.Len(t, repo.in.Nodes, 1)
	assert.Equal(t, "analytics.revenue", repo.in.Nodes[0].UniqueID)
	assert.Equal(t, "sha256:abc", repo.in.Nodes[0].ContentHash)
	assert.True(t, uow.CommittedTx)
}

// A release promoted before bundles existed can never gain one, so retrying it
// forever would wedge the consumer on a message nothing can fix.
func TestVersionsHandler_EmptyBundleURIIsAcknowledged(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{}
	repo := &fakeCodeVersionRepository{}

	in := versionsInput()
	in.CodeBundleURI = ""
	require.NoError(t, newVersionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, in))

	assert.Empty(t, reader.calls)
	assert.Zero(t, repo.called)
	assert.True(t, uow.CommittedTx)
}

func TestVersionsHandler_MissingBundleIsRetryable(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{err: fmt.Errorf("%w: %s", ports.ErrBundleNotFound, versionsBundleURI)}
	repo := &fakeCodeVersionRepository{}

	err := newVersionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, versionsInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent),
		"an absent bundle may still land, so the message must be retried")
	assert.Zero(t, repo.called)
	assert.False(t, uow.CommittedTx)
	assert.True(t, uow.RolledBackTx)
}

func TestVersionsHandler_MalformedBundleIsPermanent(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{err: fmt.Errorf("%w: bad json", ports.ErrBundleMalformed)}
	repo := &fakeCodeVersionRepository{}

	err := newVersionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, versionsInput())
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent),
		"re-reading the same bytes can never succeed, so the message is dropped")
	assert.Zero(t, repo.called)
}

// The version group trails the topology swap; until the swap lands the nodes
// have no :Table to attach a version to.
func TestVersionsHandler_UnmatchedNodesBeforeTopologySwapAreRetryable(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{bundle: versionsBundle()}
	repo := &fakeCodeVersionRepository{res: codeversion.WriteResult{
		UnmatchedNodeIDs: []string{"analytics.revenue"}, GraphReleaseID: "rel-0",
	}}

	err := newVersionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, versionsInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Contains(t, err.Error(), "rel-1")
	assert.False(t, uow.CommittedTx)
}

// Once the swap has landed, an unmatched node is genuinely absent from the
// topology and must not block the rest of the release's history.
func TestVersionsHandler_UnmatchedNodesAfterTopologySwapAreTolerated(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{bundle: versionsBundle()}
	repo := &fakeCodeVersionRepository{res: codeversion.WriteResult{
		NodeVersionsCreated: 1,
		UnmatchedNodeIDs:    []string{"analytics.dropped"},
		GraphReleaseID:      "rel-1",
	}}

	require.NoError(t, newVersionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, versionsInput()))
	assert.True(t, uow.CommittedTx)
}

func TestVersionsHandler_DuplicateMessageIsSkipped(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{bundle: versionsBundle()}
	repo := &fakeCodeVersionRepository{}
	handler := newVersionsHandler(uow, reader, repo)

	require.NoError(t, handler.Handle(context.Background(), "1-0", nil, versionsInput()))
	require.Equal(t, 1, repo.called)

	require.NoError(t, handler.Handle(context.Background(), "1-0", nil, versionsInput()))
	assert.Equal(t, 1, repo.called, "a redelivered message must not re-ingest")
	assert.Len(t, reader.calls, 1, "and must not re-fetch the bundle")
}

func TestVersionsHandler_GraphFailureLeavesNoDedupRow(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{bundle: versionsBundle()}
	repo := &fakeCodeVersionRepository{err: errors.New("neo4j unavailable")}

	err := newVersionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, versionsInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.False(t, uow.CommittedTx, "rolling back drops the dedup row so the retry reprocesses")
	assert.True(t, uow.RolledBackTx)
}
