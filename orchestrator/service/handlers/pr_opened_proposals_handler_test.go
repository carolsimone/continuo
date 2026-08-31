package handlers_test

import (
	"context"
	"errors"
	"testing"
	"time"

	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
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

func newProposalsHandler(
	uow *fakeUnitOfWork,
	repo *fakeCaseBaseRepository,
) *handlers.PrOpenedProposalsHandler {
	return handlers.NewPrOpenedProposalsHandler(uow, repo, newTestLogger())
}

// ── tests ────────────────────────────────────────────────────────────────────

func TestProposalsHandler_RecordsProposal(t *testing.T) {
	uow := newFakeUnitOfWork()
	repo := &fakeCaseBaseRepository{}

	in := prOpenedInput()
	require.NoError(t, newProposalsHandler(uow, repo).Handle(context.Background(), "1-0", nil, in))

	require.Len(t, repo.recordProposalCalls, 1)
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
	assert.True(t, uow.CommittedTx)
}

// A payload missing an identity field, or carrying an opened_at that cannot be
// parsed, can never be fixed by redelivery: dropping it permanently is the
// only option, and it must never reach the repository. An unparseable
// opened_at is caught here rather than left to zero-value OpenedAt, which the
// adapter uses as the stub :Rejection's `at` — a zero value would silently
// attach the wrong fix to a rejection's history.
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
			uow := newFakeUnitOfWork()
			repo := &fakeCaseBaseRepository{}

			in := prOpenedInput()
			tc.mutate(&in)

			err := newProposalsHandler(uow, repo).Handle(context.Background(), "1-0", nil, in)
			require.Error(t, err)
			assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
			assert.Empty(t, repo.recordProposalCalls)
		})
	}
}

func TestProposalsHandler_DedupSkips(t *testing.T) {
	uow := newFakeUnitOfWork()
	repo := &fakeCaseBaseRepository{}
	handler := newProposalsHandler(uow, repo)

	require.NoError(t, handler.Handle(context.Background(), "1-0", nil, prOpenedInput()))
	require.Len(t, repo.recordProposalCalls, 1)

	require.NoError(t, handler.Handle(context.Background(), "1-0", nil, prOpenedInput()))
	assert.Len(t, repo.recordProposalCalls, 1, "a redelivered message must not re-record")
}

// TestProposalsHandler_RecordsOneProposalPerResolvedNode verifies that a batched
// fix PR — one PR resolving several failing nodes — records a :Proposal for
// every node it resolves. Recording only the representative node would leave
// the other rejections without a PROPOSED edge, so precedent for them would
// stop reporting that a fix PR was ever opened.
func TestProposalsHandler_RecordsOneProposalPerResolvedNode(t *testing.T) {
	uow := newFakeUnitOfWork()
	repo := &fakeCaseBaseRepository{}

	in := prOpenedInput()
	in.ResolvedNodeIDs = []string{"analytics.revenue", "analytics.margin"}

	require.NoError(t, newProposalsHandler(uow, repo).Handle(context.Background(), "1-0", nil, in))

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
	assert.True(t, uow.CommittedTx)
}

// TestProposalsHandler_EmptyResolvedNodeIDsFallsBackToNodeID verifies a payload
// without resolved_node_ids — one emitted before the field existed — still
// records exactly the one proposal for its node_id.
func TestProposalsHandler_EmptyResolvedNodeIDsFallsBackToNodeID(t *testing.T) {
	uow := newFakeUnitOfWork()
	repo := &fakeCaseBaseRepository{}

	in := prOpenedInput() // no ResolvedNodeIDs
	require.NoError(t, newProposalsHandler(uow, repo).Handle(context.Background(), "1-0", nil, in))

	require.Len(t, repo.recordProposalCalls, 1)
	assert.Equal(t, "analytics.revenue", repo.recordProposalCalls[0].NodeID)
}
