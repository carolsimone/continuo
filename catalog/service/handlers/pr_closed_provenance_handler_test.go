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

func provenanceMarker(messageID string, outboxEntryID *uuid.UUID) uow.Marker {
	return caseBaseMarker(streams.CatalogRemediationPrClosedProvenance, messageID, outboxEntryID)
}

func newPrClosedProvenanceHandler(u uow.UnitOfWork) *handlers.PrClosedProvenanceHandler {
	return handlers.NewPrClosedProvenanceHandler(u, newTestLogger())
}

// ── tests ────────────────────────────────────────────────────────────────────

// A valid merged payload records exactly one PullRequestOutcome carrying every
// field verbatim, with closed_at parsed to a time.Time and the edits mapped
// through to the domain outcome, under the message's tenant in one final
// Write. The repository layer, not the handler, decides which edges a merged
// outcome draws.
func TestPrClosedProvenanceHandler_RecordsMergedOutcome(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)

	in := prClosedMergedInput()
	require.NoError(t, newPrClosedProvenanceHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))

	require.Len(t, repo.recordOutcomeCalls, 1)
	assert.Equal(t, []string{caseBaseTenant}, repo.tenants, "the outcome is recorded under the message's tenant")
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
	assert.Equal(t, []fakeGraphWrite{{Marker: provenanceMarker("1-0", nil), Final: true}}, u.writes,
		"one final Write: the marker commits with the outcome")
	assert.True(t, u.isRecorded(provenanceMarker("1-0", nil)))
}

// A rejected outcome records the same single PullRequestOutcome call — the
// handler never branches on outcome; skipping the RESOLVED_BY/EDITED edges is
// the repository layer's job. A genuine rejected payload carries no resolved
// nodes and no edits.
func TestPrClosedProvenanceHandler_RecordsRejectedOutcome(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)

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
	require.NoError(t, newPrClosedProvenanceHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))

	require.Len(t, repo.recordOutcomeCalls, 1)
	o := repo.recordOutcomeCalls[0]
	assert.Equal(t, "rejected", o.Outcome)
	assert.Empty(t, o.ResolvedNodeIDs)
	assert.Empty(t, o.Edits)
	assert.True(t, u.isRecorded(provenanceMarker("1-0", nil)))
}

// A merged payload without resolved node ids and edits still records its
// outcome — pr_state must update — and passes empty resolved/edits so the
// repository draws nothing. It must not error.
func TestPrClosedProvenanceHandler_MergedWithoutResolvedNodesDrawsNothing(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)

	in := domainEvent.PRClosed{
		ProposalID: "prop-1",
		ReleaseID:  "rel-1",
		NodeID:     "analytics.revenue",
		Service:    "core",
		Outcome:    "merged",
		ClosedAt:   "2026-08-14T11:30:00Z",
	}
	require.NoError(t, newPrClosedProvenanceHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))

	require.Len(t, repo.recordOutcomeCalls, 1)
	o := repo.recordOutcomeCalls[0]
	assert.Equal(t, "merged", o.Outcome)
	assert.Empty(t, o.ResolvedNodeIDs, "no NodeID fallback — a merged PR without resolved nodes draws no RESOLVED_BY edges")
	assert.Empty(t, o.Edits)
}

// A payload missing an identity field, or carrying a closed_at that cannot be
// parsed, can never be fixed by redelivery: it is refused as permanent before
// any marker lookup or transaction, and never reaches the repository.
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
			repo := &fakeCaseBaseRepository{}
			u := newCaseBaseUnitOfWork(repo)

			in := prClosedMergedInput()
			tc.mutate(&in)

			err := newPrClosedProvenanceHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in)
			require.Error(t, err)
			assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
			assert.Empty(t, repo.recordOutcomeCalls)
			assert.Empty(t, u.processed, "decided before any marker lookup")
			assert.Empty(t, u.writes, "decided before any transaction")
		})
	}
}

// A redelivered message — a routine Redis consumer-group occurrence — is
// acknowledged by its marker and does not re-record the outcome; the same
// outbox row re-sent under a fresh message id is recognised by its outbox
// entry id.
func TestPrClosedProvenanceHandler_FoundMarkerSkips(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	entry := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	handler := newPrClosedProvenanceHandler(u)

	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "1-0", &entry, prClosedMergedInput()))
	require.Len(t, repo.recordOutcomeCalls, 1)
	assert.Equal(t, provenanceMarker("1-0", &entry), u.writes[0].Marker)

	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "1-0", &entry, prClosedMergedInput()))
	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "2-0", &entry, prClosedMergedInput()))
	assert.Len(t, repo.recordOutcomeCalls, 1, "a redelivered message must not re-record")
	assert.Len(t, u.writes, 1)
}

// A message another delivery finished between the lookup and the write is
// acknowledged: its Write finds the marker and records nothing.
func TestPrClosedProvenanceHandler_ProcessedMeanwhileWritesNothing(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	u.record(provenanceMarker("1-0", nil))

	require.NoError(t, newPrClosedProvenanceHandler(racingUnitOfWork{u}).
		Handle(context.Background(), caseBaseTenant, "1-0", nil, prClosedMergedInput()))
	assert.Empty(t, repo.recordOutcomeCalls)
	assert.Len(t, u.writes, 1)
}

// An outcome the graph fails to record fails the message with no marker, so
// the redelivery records it again.
func TestPrClosedProvenanceHandler_FailedRecordLeavesNoMarker(t *testing.T) {
	u := newCaseBaseUnitOfWork(&fakeCaseBaseRepository{err: errors.New("neo4j: transient failure")})

	err := newPrClosedProvenanceHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, prClosedMergedInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.False(t, u.isRecorded(provenanceMarker("1-0", nil)))
}

// A write paused by an export or a restore reaches the consumer as
// uow.ErrWritesPaused, which it backs off on as for an outage.
func TestPrClosedProvenanceHandler_PausedWriteIsReturnedAsPaused(t *testing.T) {
	u := newCaseBaseUnitOfWork(&fakeCaseBaseRepository{})
	u.writeErr = fmt.Errorf("tenant %q: %w", caseBaseTenant, uow.ErrWritesPaused)

	err := newPrClosedProvenanceHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, prClosedMergedInput())
	require.ErrorIs(t, err, uow.ErrWritesPaused)
	assert.False(t, u.isRecorded(provenanceMarker("1-0", nil)))
}
