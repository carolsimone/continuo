package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/casebase"
	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/pkg/codebundle"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes: repository.CaseBaseRepository ─────────────────────────────────────

type fakeCaseBaseRepository struct {
	recordRejectionCalls []casebase.Rejection
	recordRejectionErr   error
	recordProposalCalls  []casebase.Proposal
	recordProposalPRs    []casebase.PullRequest
	recordProposalErr    error
}

func (f *fakeCaseBaseRepository) RecordRejection(_ context.Context, r casebase.Rejection) error {
	f.recordRejectionCalls = append(f.recordRejectionCalls, r)
	return f.recordRejectionErr
}

func (f *fakeCaseBaseRepository) RecordProposal(_ context.Context, p casebase.Proposal, pr casebase.PullRequest) error {
	f.recordProposalCalls = append(f.recordProposalCalls, p)
	f.recordProposalPRs = append(f.recordProposalPRs, pr)
	return f.recordProposalErr
}

var _ repository.CaseBaseRepository = (*fakeCaseBaseRepository)(nil)

// ── fixtures ─────────────────────────────────────────────────────────────────

const rejectionsBundleURI = "s3://b/code-bundles/rel-1/bundle.json"

func rejectionsBundle() codebundle.Bundle {
	return codebundle.Bundle{
		ContractVersion: 1,
		ReleaseID:       "rel-1",
		Nodes: map[string]codebundle.Node{
			"analytics.revenue": {
				RawCode:     "select 1",
				ContentHash: "h1",
			},
		},
	}
}

func rejectionInput() domainEvent.RemediationRequested {
	return domainEvent.RemediationRequested{
		EventID:       "evt-1",
		Source:        "validation",
		ReleaseID:     "rel-1",
		CodeBundleURI: rejectionsBundleURI,
		ClassifiedAt:  "2026-08-12T09:00:00Z",
		Nodes: []domainEvent.RemediationRequestedNode{
			{
				NodeID:         "analytics.revenue",
				Category:       "sql_syntax_error",
				ErrorSignature: "sig-1",
				Reason:         `column "foo" does not exist`,
				ErrorExcerpt:   `ERROR: column "foo" does not exist`,
				DBTLogURI:      "s3://b/logs/rel-1/analytics.revenue.log",
			},
		},
	}
}

func newRejectionsHandler(
	uow *fakeUnitOfWork,
	reader *fakeBundleReader,
	repo *fakeCaseBaseRepository,
) *handlers.RemediationRequestedRejectionsHandler {
	return handlers.NewRemediationRequestedRejectionsHandler(uow, reader, repo, newTestLogger())
}

// ── tests ────────────────────────────────────────────────────────────────────

func TestRejectionsHandler_RecordsRejectionWithBundleCode(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{bundle: rejectionsBundle()}
	repo := &fakeCaseBaseRepository{}

	in := rejectionInput()
	require.NoError(t, newRejectionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, in))

	assert.Equal(t, []string{rejectionsBundleURI}, reader.calls)
	require.Len(t, repo.recordRejectionCalls, 1)
	rej := repo.recordRejectionCalls[0]
	assert.Equal(t, "rel-1", rej.ReleaseID)
	assert.Equal(t, "analytics.revenue", rej.NodeID)
	assert.Equal(t, "validation", rej.Stage)
	assert.Equal(t, "sql_syntax_error", rej.Category)
	assert.Equal(t, `column "foo" does not exist`, rej.Reason)
	assert.Equal(t, "sig-1", rej.Signature)
	assert.Equal(t, `ERROR: column "foo" does not exist`, rej.ErrorExcerpt)
	assert.Equal(t, "s3://b/logs/rel-1/analytics.revenue.log", rej.DBTLogURI)
	wantAt, err := time.Parse(time.RFC3339, in.ClassifiedAt)
	require.NoError(t, err)
	assert.Equal(t, wantAt, rej.At)
	assert.Equal(t, "select 1", rej.RawCode)
	assert.Equal(t, "h1", rej.ContentHash)
	assert.True(t, uow.CommittedTx)
}

// The batched trigger carries every healable node from one rejected release
// in a single message; the case base gets one :Rejection per node, and the
// bundle backing all of them is fetched exactly once.
func TestRejectionsHandler_RecordsOneRejectionPerNode(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{bundle: codebundle.Bundle{
		ContractVersion: 1,
		ReleaseID:       "rel-1",
		Nodes: map[string]codebundle.Node{
			"s.a": {RawCode: "select a", ContentHash: "ha"},
			"s.b": {RawCode: "select b", ContentHash: "hb"},
		},
	}}
	repo := &fakeCaseBaseRepository{}

	in := domainEvent.RemediationRequested{
		Source: "validation", ReleaseID: "rel-1", CodeBundleURI: "s3://b/code-bundles/rel-1/bundle.json",
		ClassifiedAt: "2026-08-28T10:00:00Z",
		Nodes: []domainEvent.RemediationRequestedNode{
			{NodeID: "s.a", Category: "logic", ErrorSignature: "sig", Reason: "logic:missing_object", DBTLogURI: "s3://l/a"},
			{NodeID: "s.b", Category: "logic", ErrorSignature: "sig", Reason: "logic:missing_object", DBTLogURI: "s3://l/b"},
		},
	}
	require.NoError(t, newRejectionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, in))
	require.Len(t, repo.recordRejectionCalls, 2)
	assert.Equal(t, "s.a", repo.recordRejectionCalls[0].NodeID)
	assert.Equal(t, "select a", repo.recordRejectionCalls[0].RawCode)
	assert.Equal(t, "s.b", repo.recordRejectionCalls[1].NodeID)
	assert.Equal(t, "hb", repo.recordRejectionCalls[1].ContentHash)
	assert.Len(t, reader.calls, 1, "the bundle is read once per message, not once per node")
}

func TestRejectionsHandler_EmptyNodesIsPermanent(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{bundle: rejectionsBundle()}
	repo := &fakeCaseBaseRepository{}

	err := newRejectionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil,
		domainEvent.RemediationRequested{ReleaseID: "rel-1", ClassifiedAt: "2026-08-28T10:00:00Z"})
	require.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.Empty(t, repo.recordRejectionCalls)
}

// A compile-stage failure happens before any parse produces a bundle, so
// CodeBundleURI is empty and there is nothing to fetch — the classification
// is still worth recording without code.
func TestRejectionsHandler_RecordsWithoutCodeWhenNoBundleURI(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{}
	repo := &fakeCaseBaseRepository{}

	in := rejectionInput()
	in.CodeBundleURI = ""
	require.NoError(t, newRejectionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, in))

	assert.Empty(t, reader.calls)
	require.Len(t, repo.recordRejectionCalls, 1)
	assert.Empty(t, repo.recordRejectionCalls[0].RawCode)
	assert.Empty(t, repo.recordRejectionCalls[0].ContentHash)
	assert.True(t, uow.CommittedTx)
}

// A bundle that is unreadable will still be unreadable on redelivery. Losing
// the code must not lose the precedent, so the rejection is recorded anyway.
func TestRejectionsHandler_MalformedBundleDegradesToNoCode(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{err: fmt.Errorf("%w: bad json", ports.ErrBundleMalformed)}
	repo := &fakeCaseBaseRepository{}

	err := newRejectionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, rejectionInput())
	require.NoError(t, err, "losing the code must not lose the precedent")
	require.Len(t, repo.recordRejectionCalls, 1)
	assert.Empty(t, repo.recordRejectionCalls[0].RawCode)
	assert.Empty(t, repo.recordRejectionCalls[0].ContentHash)
	assert.True(t, uow.CommittedTx)
}

// A bundle absent from object storage right now may still land, so the
// message is retried rather than recorded without code.
func TestRejectionsHandler_MissingBundleRetries(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{err: fmt.Errorf("%w: %s", ports.ErrBundleNotFound, rejectionsBundleURI)}
	repo := &fakeCaseBaseRepository{}

	err := newRejectionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, rejectionInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent),
		"an absent bundle may still land, so the message must be retried")
	assert.Empty(t, repo.recordRejectionCalls)
	assert.False(t, uow.CommittedTx)
	assert.True(t, uow.RolledBackTx)
}

// The bundle read fine but this particular node is not in it (e.g. it never
// reached parse). The rejection is still worth recording without code.
func TestRejectionsHandler_NodeAbsentFromBundleRecordsWithoutCode(t *testing.T) {
	uow := newFakeUnitOfWork()
	b := rejectionsBundle()
	delete(b.Nodes, "analytics.revenue")
	reader := &fakeBundleReader{bundle: b}
	repo := &fakeCaseBaseRepository{}

	err := newRejectionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, rejectionInput())
	require.NoError(t, err)
	require.Len(t, repo.recordRejectionCalls, 1)
	assert.Empty(t, repo.recordRejectionCalls[0].RawCode)
	assert.Empty(t, repo.recordRejectionCalls[0].ContentHash)
	assert.True(t, uow.CommittedTx)
}

// A payload missing an identity field, carrying no healable nodes, or an
// unparseable classified_at can never be fixed by redelivery: dropping it
// permanently is the only option, and it must never reach the repository. An
// unparseable classified_at is caught here rather than left to zero-value At,
// which would make the rejection eligible to be "resolved by" essentially any
// version.
func TestRejectionsHandler_PoisonPayloadIsPermanent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*domainEvent.RemediationRequested)
	}{
		{"empty release_id", func(in *domainEvent.RemediationRequested) { in.ReleaseID = "" }},
		{"no nodes", func(in *domainEvent.RemediationRequested) { in.Nodes = nil }},
		{"empty node_id", func(in *domainEvent.RemediationRequested) { in.Nodes[0].NodeID = "" }},
		{"empty error_signature", func(in *domainEvent.RemediationRequested) { in.Nodes[0].ErrorSignature = "" }},
		{"unparseable classified_at", func(in *domainEvent.RemediationRequested) { in.ClassifiedAt = "not-a-time" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uow := newFakeUnitOfWork()
			reader := &fakeBundleReader{bundle: rejectionsBundle()}
			repo := &fakeCaseBaseRepository{}

			in := rejectionInput()
			tc.mutate(&in)

			err := newRejectionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, in)
			require.Error(t, err)
			assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
			assert.Empty(t, repo.recordRejectionCalls)
		})
	}
}

// The URI resolved to a bundle for a different release. Unlike the versions
// handler (which drops the message — writing the wrong code would corrupt
// the version graph), losing the code here must not lose the precedent: the
// rejection is still recorded, without code, and the message ACKs rather
// than retrying (retrying the same URI would only fetch the same wrong
// bundle again).
func TestRejectionsHandler_BundleForAnotherReleaseRecordsWithoutCode(t *testing.T) {
	uow := newFakeUnitOfWork()
	b := rejectionsBundle()
	b.ReleaseID = "rel-99"
	reader := &fakeBundleReader{bundle: b}
	repo := &fakeCaseBaseRepository{}

	err := newRejectionsHandler(uow, reader, repo).Handle(context.Background(), "1-0", nil, rejectionInput())
	require.NoError(t, err)
	require.Len(t, repo.recordRejectionCalls, 1)
	assert.Empty(t, repo.recordRejectionCalls[0].RawCode)
	assert.Empty(t, repo.recordRejectionCalls[0].ContentHash)
	assert.True(t, uow.CommittedTx)
}

func TestRejectionsHandler_DedupSkips(t *testing.T) {
	uow := newFakeUnitOfWork()
	reader := &fakeBundleReader{bundle: rejectionsBundle()}
	repo := &fakeCaseBaseRepository{}
	handler := newRejectionsHandler(uow, reader, repo)

	require.NoError(t, handler.Handle(context.Background(), "1-0", nil, rejectionInput()))
	require.Len(t, repo.recordRejectionCalls, 1)

	require.NoError(t, handler.Handle(context.Background(), "1-0", nil, rejectionInput()))
	assert.Len(t, repo.recordRejectionCalls, 1, "a redelivered message must not re-record")
	assert.Len(t, reader.calls, 1, "and must not re-fetch the bundle")
}
