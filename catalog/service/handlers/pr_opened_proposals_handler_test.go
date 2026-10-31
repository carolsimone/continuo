package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	domainEvent "github.com/carolsimone/continuo/catalog/domain/event"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fixtures ─────────────────────────────────────────────────────────────────

func prOpenedInput() domainEvent.PROpened {
	return domainEvent.PROpened{
		ProposalID: "prop-1",
		ReleaseID:  "rel-1",
		NodeID:     "analytics.revenue",
		PrURL:      "https://github.com/org/repo/pull/42",
		PrNumber:   42,
		OpenedBy:   "agent-remediation",
		OpenedAt:   "2026-08-12T09:05:00Z",
		Service:    "core",
	}
}

func proposalsMarker(messageID string, outboxEntryID *uuid.UUID) uow.Marker {
	return caseBaseMarker(streams.CatalogRemediationPrOpenedProposals, messageID, outboxEntryID)
}

func newProposalsHandler(u uow.UnitOfWork) *handlers.PrOpenedProposalsHandler {
	return handlers.NewPrOpenedProposalsHandler(u, newTestLogger())
}

// ── tests ────────────────────────────────────────────────────────────────────

// The proposal is recorded under the message's tenant in one final Write, so
// the marker commits with it.
func TestProposalsHandler_RecordsProposal(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)

	in := prOpenedInput()
	require.NoError(t, newProposalsHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))

	require.Len(t, repo.recordProposalCalls, 1)
	assert.Equal(t, []string{caseBaseTenant}, repo.tenants, "the proposal is recorded under the message's tenant")
	p := repo.recordProposalCalls[0]
	assert.Equal(t, "prop-1", p.ProposalID)
	assert.Equal(t, "rel-1", p.ReleaseID)
	assert.Equal(t, "analytics.revenue", p.NodeID)

	require.Len(t, repo.recordProposalPRs, 1)
	pr := repo.recordProposalPRs[0]
	assert.Equal(t, "prop-1", pr.ProposalID)
	assert.Equal(t, "core", pr.Service)
	assert.Equal(t, "https://github.com/org/repo/pull/42", pr.PrURL)
	assert.Equal(t, 42, pr.PrNumber)
	assert.Equal(t, "open", pr.State)
	assert.Equal(t, "agent-remediation", pr.OpenedBy)
	wantAt, err := time.Parse(time.RFC3339, in.OpenedAt)
	require.NoError(t, err)
	assert.Equal(t, wantAt, pr.OpenedAt)
	assert.Equal(t, []fakeGraphWrite{{Marker: proposalsMarker("1-0", nil), Final: true}}, u.writes,
		"one final Write: the marker commits with the proposal")
	assert.True(t, u.isRecorded(proposalsMarker("1-0", nil)))
}

// A payload missing an identity field, or carrying an opened_at that cannot be
// parsed, can never be fixed by redelivery: it is refused as permanent before
// any marker lookup or transaction, and never reaches the repository. An
// unparseable opened_at is caught here rather than left to zero-value
// OpenedAt, which the adapter uses as the stub :Rejection's `at` — a zero
// value would silently attach the wrong fix to a rejection's history.
func TestProposalsHandler_PoisonPayloadIsPermanent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*domainEvent.PROpened)
	}{
		{"empty proposal_id", func(in *domainEvent.PROpened) { in.ProposalID = "" }},
		{"empty release_id", func(in *domainEvent.PROpened) { in.ReleaseID = "" }},
		{"empty node_id", func(in *domainEvent.PROpened) { in.NodeID = "" }},
		{"unparseable opened_at", func(in *domainEvent.PROpened) { in.OpenedAt = "not-a-time" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeCaseBaseRepository{}
			u := newCaseBaseUnitOfWork(repo)

			in := prOpenedInput()
			tc.mutate(&in)

			err := newProposalsHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in)
			require.Error(t, err)
			assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
			assert.Empty(t, repo.recordProposalCalls)
			assert.Empty(t, u.processed, "decided before any marker lookup")
			assert.Empty(t, u.writes, "decided before any transaction")
		})
	}
}

// A redelivered message is acknowledged by its marker and records nothing
// again; the same outbox row re-sent under a fresh message id is recognised by
// its outbox entry id.
func TestProposalsHandler_FoundMarkerSkips(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	entry := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	handler := newProposalsHandler(u)

	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "1-0", &entry, prOpenedInput()))
	require.Len(t, repo.recordProposalCalls, 1)
	assert.Equal(t, proposalsMarker("1-0", &entry), u.writes[0].Marker)

	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "1-0", &entry, prOpenedInput()))
	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "2-0", &entry, prOpenedInput()))
	assert.Len(t, repo.recordProposalCalls, 1, "a redelivered message must not re-record")
	assert.Len(t, u.writes, 1)
}

// TestProposalsHandler_RecordsOneProposalPerResolvedNode verifies that a batched
// fix PR — one PR resolving several failing nodes — records a :Proposal for
// every node it resolves, all in the one Write. Recording only the
// representative node would leave the other rejections without a PROPOSED
// edge, so precedent for them would stop reporting that a fix PR was ever
// opened.
func TestProposalsHandler_RecordsOneProposalPerResolvedNode(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)

	in := prOpenedInput()
	in.ResolvedNodeIDs = []string{"analytics.revenue", "analytics.margin"}

	require.NoError(t, newProposalsHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))

	require.Len(t, repo.recordProposalCalls, 2)
	nodes := []string{repo.recordProposalCalls[0].NodeID, repo.recordProposalCalls[1].NodeID}
	assert.ElementsMatch(t, []string{"analytics.revenue", "analytics.margin"}, nodes)
	for _, p := range repo.recordProposalCalls {
		assert.Equal(t, "prop-1", p.ProposalID, "every node shares the one PR's proposal id")
		assert.Equal(t, "rel-1", p.ReleaseID)
	}

	require.Len(t, repo.recordProposalPRs, 2)
	for _, pr := range repo.recordProposalPRs {
		assert.Equal(t, "prop-1", pr.ProposalID)
		assert.Equal(t, "core", pr.Service, "every resolved node shares the one PR's service")
		assert.Equal(t, "https://github.com/org/repo/pull/42", pr.PrURL)
		assert.Equal(t, 42, pr.PrNumber)
		assert.Equal(t, "open", pr.State)
		assert.Equal(t, "agent-remediation", pr.OpenedBy)
	}
	assert.Len(t, u.writes, 1, "every node's proposal is recorded in the one Write")
	assert.True(t, u.isRecorded(proposalsMarker("1-0", nil)))
}

// TestProposalsHandler_EmptyResolvedNodeIDsFallsBackToNodeID verifies a payload
// without resolved_node_ids — one emitted before the field existed — still
// records exactly the one proposal for its node_id.
func TestProposalsHandler_EmptyResolvedNodeIDsFallsBackToNodeID(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)

	in := prOpenedInput() // no ResolvedNodeIDs
	require.NoError(t, newProposalsHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))

	require.Len(t, repo.recordProposalCalls, 1)
	assert.Equal(t, "analytics.revenue", repo.recordProposalCalls[0].NodeID)
}

// A message another delivery finished between the lookup and the write is
// acknowledged: its Write finds the marker and records nothing.
func TestProposalsHandler_ProcessedMeanwhileWritesNothing(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	u.record(proposalsMarker("1-0", nil))

	require.NoError(t, newProposalsHandler(racingUnitOfWork{u}).
		Handle(context.Background(), caseBaseTenant, "1-0", nil, prOpenedInput()))
	assert.Empty(t, repo.recordProposalCalls)
	assert.Len(t, u.writes, 1)
}

// A proposal the graph fails to record fails the message with no marker, so
// the redelivery records it again.
func TestProposalsHandler_FailedRecordLeavesNoMarker(t *testing.T) {
	u := newCaseBaseUnitOfWork(&fakeCaseBaseRepository{err: errors.New("neo4j: transient failure")})

	err := newProposalsHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, prOpenedInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.False(t, u.isRecorded(proposalsMarker("1-0", nil)))
}

// A write paused by an export or a restore reaches the consumer as
// uow.ErrWritesPaused, which it backs off on as for an outage.
func TestProposalsHandler_PausedWriteIsReturnedAsPaused(t *testing.T) {
	u := newCaseBaseUnitOfWork(&fakeCaseBaseRepository{})
	u.writeErr = fmt.Errorf("tenant %q: %w", caseBaseTenant, uow.ErrWritesPaused)

	err := newProposalsHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, prOpenedInput())
	require.ErrorIs(t, err, uow.ErrWritesPaused)
	assert.False(t, u.isRecorded(proposalsMarker("1-0", nil)))
}
