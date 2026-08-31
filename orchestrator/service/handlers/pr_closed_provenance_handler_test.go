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

func prClosedMergedInput() domainEvent.PRClosed {
	return domainEvent.PRClosed{
		ProposalID:      "prop-1",
		ReleaseID:       "rel-1",
		NodeID:          "analytics.revenue",
		ResolvedNodeIDs: []string{"analytics.revenue", "analytics.margin"},
		Service:         "core",
		PrURL:           "https://github.com/org/repo/pull/42",
		PrNumber:        42,
		Outcome:         "merged",
		ClosedAt:        "2026-08-14T11:30:00Z",
		Edits: []domainEvent.PRClosedEdit{
			{Path: "models/revenue.sql", TargetNodeID: "analytics.revenue", Amended: true, Diff: "@@ -1 +1 @@"},
			{Path: "models/margin.sql", TargetNodeID: "analytics.margin", Amended: false},
		},
	}
}

func newPrClosedProvenanceHandler(
	uow *fakeUnitOfWork,
	repo *fakeCaseBaseRepository,
) *handlers.PrClosedProvenanceHandler {
	return handlers.NewPrClosedProvenanceHandler(uow, repo, newTestLogger())
}

// ── tests ────────────────────────────────────────────────────────────────────

// A valid merged payload records exactly one PullRequestOutcome carrying every
// field verbatim, with closed_at parsed to a time.Time and the edits mapped
// through to the domain outcome. The repository layer, not the handler, decides
// which edges a merged outcome draws.
func TestPrClosedProvenanceHandler_RecordsMergedOutcome(t *testing.T) {
	uow := newFakeUnitOfWork()
	repo := &fakeCaseBaseRepository{}

	in := prClosedMergedInput()
	require.NoError(t, newPrClosedProvenanceHandler(uow, repo).Handle(context.Background(), "1-0", nil, in))

	require.Len(t, repo.recordOutcomeCalls, 1)
	o := repo.recordOutcomeCalls[0]
	assert.Equal(t, "prop-1", o.ProposalID)
	assert.Equal(t, "rel-1", o.ReleaseID)
	assert.Equal(t, "core", o.Service)
	assert.Equal(t, "merged", o.Outcome)
	wantAt, err := time.Parse(time.RFC3339, in.ClosedAt)
	require.NoError(t, err)
	assert.Equal(t, wantAt, o.ClosedAt)
	assert.Equal(t, []string{"analytics.revenue", "analytics.margin"}, o.ResolvedNodeIDs)

	require.Len(t, o.Edits, 2)
	assert.Equal(t, "models/revenue.sql", o.Edits[0].Path)
	assert.Equal(t, "analytics.revenue", o.Edits[0].TargetNodeID)
	assert.True(t, o.Edits[0].Amended)
	assert.Equal(t, "@@ -1 +1 @@", o.Edits[0].Diff)
	assert.Equal(t, "models/margin.sql", o.Edits[1].Path)
	assert.False(t, o.Edits[1].Amended)
	assert.True(t, uow.CommittedTx)
}

// A rejected outcome records the same single PullRequestOutcome call — the
// handler never branches on outcome; skipping the RESOLVED_BY/EDITED edges is
// the repository layer's job. A genuine rejected payload carries no resolved
// nodes and no edits.
func TestPrClosedProvenanceHandler_RecordsRejectedOutcome(t *testing.T) {
	uow := newFakeUnitOfWork()
	repo := &fakeCaseBaseRepository{}

	in := domainEvent.PRClosed{
		ProposalID: "prop-1",
		ReleaseID:  "rel-1",
		NodeID:     "analytics.revenue",
		Service:    "core",
		PrURL:      "https://github.com/org/repo/pull/42",
		PrNumber:   42,
		Outcome:    "rejected",
		ClosedAt:   "2026-08-14T11:30:00Z",
	}
	require.NoError(t, newPrClosedProvenanceHandler(uow, repo).Handle(context.Background(), "1-0", nil, in))

	require.Len(t, repo.recordOutcomeCalls, 1)
	o := repo.recordOutcomeCalls[0]
	assert.Equal(t, "rejected", o.Outcome)
	assert.Empty(t, o.ResolvedNodeIDs)
	assert.Empty(t, o.Edits)
	assert.True(t, uow.CommittedTx)
}

// A merged payload emitted before the resolved/edits fields existed still
// records its outcome — pr_state must update — and passes empty resolved/edits
// so the repository draws nothing. It must not error.
func TestPrClosedProvenanceHandler_LegacyMergedDrawsNothing(t *testing.T) {
	uow := newFakeUnitOfWork()
	repo := &fakeCaseBaseRepository{}

	in := domainEvent.PRClosed{
		ProposalID: "prop-1",
		ReleaseID:  "rel-1",
		NodeID:     "analytics.revenue",
		Service:    "core",
		Outcome:    "merged",
		ClosedAt:   "2026-08-14T11:30:00Z",
	}
	require.NoError(t, newPrClosedProvenanceHandler(uow, repo).Handle(context.Background(), "1-0", nil, in))

	require.Len(t, repo.recordOutcomeCalls, 1)
	o := repo.recordOutcomeCalls[0]
	assert.Equal(t, "merged", o.Outcome)
	assert.Empty(t, o.ResolvedNodeIDs, "no NodeID fallback — a legacy merged PR draws no RESOLVED_BY edges")
	assert.Empty(t, o.Edits)
}

// A payload missing an identity field, or carrying a closed_at that cannot be
// parsed, can never be fixed by redelivery: dropping it permanently is the only
// option, and it must never reach the repository.
func TestPrClosedProvenanceHandler_PoisonPayloadIsPermanent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*domainEvent.PRClosed)
	}{
		{"empty proposal_id", func(in *domainEvent.PRClosed) { in.ProposalID = "" }},
		{"empty release_id", func(in *domainEvent.PRClosed) { in.ReleaseID = "" }},
		{"unparseable closed_at", func(in *domainEvent.PRClosed) { in.ClosedAt = "not-a-time" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uow := newFakeUnitOfWork()
			repo := &fakeCaseBaseRepository{}

			in := prClosedMergedInput()
			tc.mutate(&in)

			err := newPrClosedProvenanceHandler(uow, repo).Handle(context.Background(), "1-0", nil, in)
			require.Error(t, err)
			assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
			assert.Empty(t, repo.recordOutcomeCalls)
		})
	}
}

// A redelivered message — a routine Redis consumer-group occurrence — must
// dedup and not re-record the outcome.
func TestPrClosedProvenanceHandler_DedupSkips(t *testing.T) {
	uow := newFakeUnitOfWork()
	repo := &fakeCaseBaseRepository{}
	handler := newPrClosedProvenanceHandler(uow, repo)

	require.NoError(t, handler.Handle(context.Background(), "1-0", nil, prClosedMergedInput()))
	require.Len(t, repo.recordOutcomeCalls, 1)

	require.NoError(t, handler.Handle(context.Background(), "1-0", nil, prClosedMergedInput()))
	assert.Len(t, repo.recordOutcomeCalls, 1, "a redelivered message must not re-record")
}
