package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	domainEvent "github.com/carolsimone/continuo/catalog/domain/event"
	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/codebundle"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes: repository.CaseBaseRepository ─────────────────────────────────────

// fakeCaseBaseRepository records every call with the tenant it names, and
// fails each call with err when err is set.
type fakeCaseBaseRepository struct {
	tenants              []string
	recordRejectionCalls []casebase.Rejection
	recordProposalCalls  []casebase.Proposal
	recordProposalPRs    []casebase.PullRequest
	recordOutcomeCalls   []casebase.PullRequestOutcome
	err                  error
}

func (f *fakeCaseBaseRepository) RecordRejection(_ context.Context, tenantID string, r casebase.Rejection) error {
	f.tenants = append(f.tenants, tenantID)
	f.recordRejectionCalls = append(f.recordRejectionCalls, r)
	return f.err
}

func (f *fakeCaseBaseRepository) RecordProposal(_ context.Context, tenantID string, p casebase.Proposal, pr casebase.PullRequest) error {
	f.tenants = append(f.tenants, tenantID)
	f.recordProposalCalls = append(f.recordProposalCalls, p)
	f.recordProposalPRs = append(f.recordProposalPRs, pr)
	return f.err
}

func (f *fakeCaseBaseRepository) RecordPullRequestOutcome(_ context.Context, tenantID string, o casebase.PullRequestOutcome) error {
	f.tenants = append(f.tenants, tenantID)
	f.recordOutcomeCalls = append(f.recordOutcomeCalls, o)
	return f.err
}

var _ repository.CaseBaseRepository = (*fakeCaseBaseRepository)(nil)

// newCaseBaseUnitOfWork is a fake unit of work whose transaction hands out
// repo as its case-base repository.
func newCaseBaseUnitOfWork(repo repository.CaseBaseRepository) *fakeGraphUnitOfWork {
	u := newFakeGraphUnitOfWork(nil)
	u.tx.caseBase = repo
	return u
}

// caseBaseTenant is the tenant the case-base handler tests' messages name.
const caseBaseTenant = "tenant-7"

// caseBaseMarker is the marker group records for messageID of caseBaseTenant.
func caseBaseMarker(group, messageID string, outboxEntryID *uuid.UUID) uow.Marker {
	return uow.Marker{TenantID: caseBaseTenant, Group: group, MessageID: messageID, OutboxEntryID: outboxEntryID}
}

func rejectionsMarker(messageID string, outboxEntryID *uuid.UUID) uow.Marker {
	return caseBaseMarker(streams.CatalogRemediationRequestedRejections, messageID, outboxEntryID)
}

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

func newRejectionsHandler(u uow.UnitOfWork, reader *fakeBundleReader) *handlers.RemediationRequestedRejectionsHandler {
	return handlers.NewRemediationRequestedRejectionsHandler(u, reader, newTestLogger())
}

// ── the write ────────────────────────────────────────────────────────────────

// Every node's rejection is recorded under the message's tenant in one final
// Write, so the marker commits with the rejections.
func TestRejectionsHandler_RecordsRejectionWithBundleCode(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	reader := &fakeBundleReader{bundle: rejectionsBundle()}

	in := rejectionInput()
	require.NoError(t, newRejectionsHandler(u, reader).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))

	assert.Equal(t, []string{rejectionsBundleURI}, reader.calls)
	require.Len(t, repo.recordRejectionCalls, 1)
	assert.Equal(t, []string{caseBaseTenant}, repo.tenants, "the rejection is recorded under the message's tenant")
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
	assert.Equal(t, []fakeGraphWrite{{Marker: rejectionsMarker("1-0", nil), Final: true}}, u.writes,
		"one final Write: the marker commits with the rejections")
	assert.True(t, u.isRecorded(rejectionsMarker("1-0", nil)))
}

// The batched trigger carries every healable node from one rejected release
// in a single message; the case base gets one :Rejection per node, in the one
// Write, and the bundle backing all of them is fetched exactly once.
func TestRejectionsHandler_RecordsOneRejectionPerNode(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	reader := &fakeBundleReader{bundle: codebundle.Bundle{
		ContractVersion: 1,
		ReleaseID:       "rel-1",
		Nodes: map[string]codebundle.Node{
			"s.a": {RawCode: "select a", ContentHash: "ha"},
			"s.b": {RawCode: "select b", ContentHash: "hb"},
		},
	}}

	in := domainEvent.RemediationRequested{
		Source: "validation", ReleaseID: "rel-1", CodeBundleURI: "s3://b/code-bundles/rel-1/bundle.json",
		ClassifiedAt: "2026-08-28T10:00:00Z",
		Nodes: []domainEvent.RemediationRequestedNode{
			{NodeID: "s.a", Category: "logic", ErrorSignature: "sig", Reason: "logic:missing_object", DBTLogURI: "s3://l/a"},
			{NodeID: "s.b", Category: "logic", ErrorSignature: "sig", Reason: "logic:missing_object", DBTLogURI: "s3://l/b"},
		},
	}
	require.NoError(t, newRejectionsHandler(u, reader).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))
	require.Len(t, repo.recordRejectionCalls, 2)
	assert.Equal(t, "s.a", repo.recordRejectionCalls[0].NodeID)
	assert.Equal(t, "select a", repo.recordRejectionCalls[0].RawCode)
	assert.Equal(t, "s.b", repo.recordRejectionCalls[1].NodeID)
	assert.Equal(t, "hb", repo.recordRejectionCalls[1].ContentHash)
	assert.Len(t, reader.calls, 1, "the bundle is read once per message, not once per node")
	assert.Len(t, u.writes, 1, "every node's rejection is recorded in the one Write")
}

func TestRejectionsHandler_EmptyNodesIsPermanent(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	reader := &fakeBundleReader{bundle: rejectionsBundle()}

	err := newRejectionsHandler(u, reader).Handle(context.Background(), caseBaseTenant, "1-0", nil,
		domainEvent.RemediationRequested{ReleaseID: "rel-1", ClassifiedAt: "2026-08-28T10:00:00Z"})
	require.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.Empty(t, repo.recordRejectionCalls)
}

// A compile-stage failure happens before any parse produces a bundle, so
// CodeBundleURI is empty and there is nothing to fetch — the classification
// is still worth recording without code.
func TestRejectionsHandler_RecordsWithoutCodeWhenNoBundleURI(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	reader := &fakeBundleReader{}

	in := rejectionInput()
	in.CodeBundleURI = ""
	require.NoError(t, newRejectionsHandler(u, reader).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))

	assert.Empty(t, reader.calls)
	require.Len(t, repo.recordRejectionCalls, 1)
	assert.Empty(t, repo.recordRejectionCalls[0].RawCode)
	assert.Empty(t, repo.recordRejectionCalls[0].ContentHash)
	assert.True(t, u.isRecorded(rejectionsMarker("1-0", nil)))
}

// A bundle that is unreadable will still be unreadable on redelivery. Losing
// the code must not lose the precedent, so the rejection is recorded anyway.
func TestRejectionsHandler_MalformedBundleDegradesToNoCode(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	reader := &fakeBundleReader{err: fmt.Errorf("%w: bad json", ports.ErrBundleMalformed)}

	err := newRejectionsHandler(u, reader).Handle(context.Background(), caseBaseTenant, "1-0", nil, rejectionInput())
	require.NoError(t, err, "losing the code must not lose the precedent")
	require.Len(t, repo.recordRejectionCalls, 1)
	assert.Empty(t, repo.recordRejectionCalls[0].RawCode)
	assert.Empty(t, repo.recordRejectionCalls[0].ContentHash)
	assert.True(t, u.isRecorded(rejectionsMarker("1-0", nil)))
}

// A bundle absent from object storage right now may still land, so the
// message is retried rather than recorded without code: nothing is written.
func TestRejectionsHandler_MissingBundleRetries(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	reader := &fakeBundleReader{err: fmt.Errorf("%w: %s", ports.ErrBundleNotFound, rejectionsBundleURI)}

	err := newRejectionsHandler(u, reader).Handle(context.Background(), caseBaseTenant, "1-0", nil, rejectionInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent),
		"an absent bundle may still land, so the message must be retried")
	assert.Empty(t, repo.recordRejectionCalls)
	assert.Empty(t, u.writes)
	assert.False(t, u.isRecorded(rejectionsMarker("1-0", nil)))
}

// The bundle read fine but this particular node is not in it (e.g. it never
// reached parse). The rejection is still worth recording without code.
func TestRejectionsHandler_NodeAbsentFromBundleRecordsWithoutCode(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	b := rejectionsBundle()
	delete(b.Nodes, "analytics.revenue")
	reader := &fakeBundleReader{bundle: b}

	err := newRejectionsHandler(u, reader).Handle(context.Background(), caseBaseTenant, "1-0", nil, rejectionInput())
	require.NoError(t, err)
	require.Len(t, repo.recordRejectionCalls, 1)
	assert.Empty(t, repo.recordRejectionCalls[0].RawCode)
	assert.Empty(t, repo.recordRejectionCalls[0].ContentHash)
	assert.True(t, u.isRecorded(rejectionsMarker("1-0", nil)))
}

// A payload missing an identity field, carrying no healable nodes, or an
// unparseable classified_at can never be fixed by redelivery: it is refused
// as permanent before any marker lookup or transaction, and never reaches the
// repository. An unparseable classified_at is caught here rather than left to
// zero-value At, which would make the rejection eligible to be "resolved by"
// essentially any version.
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
			repo := &fakeCaseBaseRepository{}
			u := newCaseBaseUnitOfWork(repo)
			reader := &fakeBundleReader{bundle: rejectionsBundle()}

			in := rejectionInput()
			tc.mutate(&in)

			err := newRejectionsHandler(u, reader).Handle(context.Background(), caseBaseTenant, "1-0", nil, in)
			require.Error(t, err)
			assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
			assert.Empty(t, repo.recordRejectionCalls)
			assert.Empty(t, u.processed, "decided before any marker lookup")
			assert.Empty(t, u.writes, "decided before any transaction")
			assert.Empty(t, reader.calls)
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
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	b := rejectionsBundle()
	b.ReleaseID = "rel-99"
	reader := &fakeBundleReader{bundle: b}

	err := newRejectionsHandler(u, reader).Handle(context.Background(), caseBaseTenant, "1-0", nil, rejectionInput())
	require.NoError(t, err)
	require.Len(t, repo.recordRejectionCalls, 1)
	assert.Empty(t, repo.recordRejectionCalls[0].RawCode)
	assert.Empty(t, repo.recordRejectionCalls[0].ContentHash)
	assert.True(t, u.isRecorded(rejectionsMarker("1-0", nil)))
}

// ── the marker ───────────────────────────────────────────────────────────────

// A message whose marker is recorded is acknowledged without reading the
// bundle or opening a write.
func TestRejectionsHandler_FoundMarkerReadsNoObjectStorage(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	reader := &fakeBundleReader{bundle: rejectionsBundle()}
	handler := newRejectionsHandler(u, reader)

	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "1-0", nil, rejectionInput()))
	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "1-0", nil, rejectionInput()))

	assert.Len(t, repo.recordRejectionCalls, 1, "a redelivered message is not recorded again")
	assert.Len(t, reader.calls, 1, "nor its bundle read")
	assert.Len(t, u.writes, 1, "nor a write opened")
}

// The same outbox row re-sent under a fresh message id is found by its outbox
// entry id, and the marker the write records carries it.
func TestRejectionsHandler_OutboxEntryIDReachesTheMarker(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	reader := &fakeBundleReader{bundle: rejectionsBundle()}
	entry := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	handler := newRejectionsHandler(u, reader)

	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "1-0", &entry, rejectionInput()))
	require.Len(t, u.writes, 1)
	assert.Equal(t, rejectionsMarker("1-0", &entry), u.writes[0].Marker)

	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "2-0", &entry, rejectionInput()))
	assert.Len(t, repo.recordRejectionCalls, 1, "the re-sent row is recognised by its outbox entry id")
	assert.Len(t, reader.calls, 1)
}

// A message another delivery finished between the lookup and the write is
// acknowledged: its Write finds the marker and records nothing.
func TestRejectionsHandler_ProcessedMeanwhileWritesNothing(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	u.record(rejectionsMarker("1-0", nil))

	require.NoError(t, newRejectionsHandler(racingUnitOfWork{u}, &fakeBundleReader{bundle: rejectionsBundle()}).
		Handle(context.Background(), caseBaseTenant, "1-0", nil, rejectionInput()))
	assert.Empty(t, repo.recordRejectionCalls)
	assert.Len(t, u.writes, 1)
}

// A failed marker lookup is returned before anything else happens, so the
// consumer retries the message.
func TestRejectionsHandler_MarkerLookupFailureIsReturned(t *testing.T) {
	lookup := errors.New("neo4j: connection refused")
	u := newCaseBaseUnitOfWork(&fakeCaseBaseRepository{})
	u.processedErr = lookup
	reader := &fakeBundleReader{bundle: rejectionsBundle()}

	err := newRejectionsHandler(u, reader).Handle(context.Background(), caseBaseTenant, "1-0", nil, rejectionInput())
	require.ErrorIs(t, err, lookup)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.Empty(t, reader.calls)
	assert.Empty(t, u.writes)
}

// A rejection the graph fails to record fails the message with no marker, so
// the redelivery records every rejection again.
func TestRejectionsHandler_FailedRecordLeavesNoMarker(t *testing.T) {
	repo := &fakeCaseBaseRepository{err: errors.New("neo4j: transient failure")}
	u := newCaseBaseUnitOfWork(repo)

	err := newRejectionsHandler(u, &fakeBundleReader{bundle: rejectionsBundle()}).
		Handle(context.Background(), caseBaseTenant, "1-0", nil, rejectionInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.False(t, u.isRecorded(rejectionsMarker("1-0", nil)))
}

// A write paused by an export or a restore reaches the consumer as
// uow.ErrWritesPaused, which it backs off on as for an outage.
func TestRejectionsHandler_PausedWriteIsReturnedAsPaused(t *testing.T) {
	u := newCaseBaseUnitOfWork(&fakeCaseBaseRepository{})
	u.writeErr = fmt.Errorf("tenant %q: %w", caseBaseTenant, uow.ErrWritesPaused)

	err := newRejectionsHandler(u, &fakeBundleReader{bundle: rejectionsBundle()}).
		Handle(context.Background(), caseBaseTenant, "1-0", nil, rejectionInput())
	require.ErrorIs(t, err, uow.ErrWritesPaused)
	assert.False(t, u.isRecorded(rejectionsMarker("1-0", nil)))
}
