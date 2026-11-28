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
		FixID:           "fix-1",
		ReleaseID:       "rel-1",
		NodeID:          "analytics.revenue",
		ResolvedNodeIDs: []string{"analytics.revenue"},
		PrURL:           "https://github.com/org/repo/pull/42",
		PrNumber:        42,
		OpenedBy:        "agent-remediation",
		OpenedAt:        "2026-08-12T09:05:00Z",
		Project:         "core",
	}
}

func fixesMarker(messageID string, outboxEntryID *uuid.UUID) uow.Marker {
	return caseBaseMarker(streams.CatalogFixPrOpenedFixes, messageID, outboxEntryID)
}

func newFixesHandler(u uow.UnitOfWork) *handlers.PrOpenedFixesHandler {
	return handlers.NewPrOpenedFixesHandler(u, newTestLogger())
}

// ── tests ────────────────────────────────────────────────────────────────────

// The fix is recorded under the message's tenant in one final Write, so
// the marker commits with it.
func TestFixesHandler_RecordsFix(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)

	in := prOpenedInput()
	require.NoError(t, newFixesHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))

	require.Len(t, repo.recordFixCalls, 1)
	assert.Equal(t, []string{caseBaseTenant}, repo.tenants, "the fix is recorded under the message's tenant")
	p := repo.recordFixCalls[0]
	assert.Equal(t, "fix-1", p.FixID)
	assert.Equal(t, "rel-1", p.ReleaseID)
	assert.Equal(t, "analytics.revenue", p.NodeID)

	require.Len(t, repo.recordFixPRs, 1)
	pr := repo.recordFixPRs[0]
	assert.Equal(t, "fix-1", pr.FixID)
	assert.Equal(t, "core", pr.Project)
	assert.Equal(t, "https://github.com/org/repo/pull/42", pr.PrURL)
	assert.Equal(t, 42, pr.PrNumber)
	assert.Equal(t, "open", pr.State)
	assert.Equal(t, "agent-remediation", pr.OpenedBy)
	wantAt, err := time.Parse(time.RFC3339, in.OpenedAt)
	require.NoError(t, err)
	assert.Equal(t, wantAt, pr.OpenedAt)
	assert.Equal(t, []fakeGraphWrite{{Marker: fixesMarker("1-0", nil), Final: true}}, u.writes,
		"one final Write: the marker commits with the fix")
	assert.True(t, u.isRecorded(fixesMarker("1-0", nil)))
}

// A payload missing an identity field, or carrying an opened_at that cannot be
// parsed, can never be fixed by redelivery: it is refused as permanent before
// any marker lookup or transaction, and never reaches the repository. An
// unparseable opened_at is caught here rather than left to zero-value
// OpenedAt, which the adapter uses as the stub :FailurePrecedent's `at` — a zero
// value would silently attach the wrong fix to a failure precedent's history.
func TestFixesHandler_PoisonPayloadIsPermanent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*domainEvent.PROpened)
	}{
		{"empty fix_id", func(in *domainEvent.PROpened) { in.FixID = "" }},
		{"empty release_id", func(in *domainEvent.PROpened) { in.ReleaseID = "" }},
		{"empty node_id", func(in *domainEvent.PROpened) { in.NodeID = "" }},
		{"unparseable opened_at", func(in *domainEvent.PROpened) { in.OpenedAt = "not-a-time" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeCaseBaseRepository{}
			u := newCaseBaseUnitOfWork(repo)

			in := prOpenedInput()
			tc.mutate(&in)

			err := newFixesHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in)
			require.Error(t, err)
			assert.True(t, errors.Is(err, pkgevents.ErrPermanent))
			assert.Empty(t, repo.recordFixCalls)
			assert.Empty(t, u.processed, "decided before any marker lookup")
			assert.Empty(t, u.writes, "decided before any transaction")
		})
	}
}

// A redelivered message is acknowledged by its marker and records nothing
// again; the same outbox row re-sent under a fresh message id is recognised by
// its outbox entry id.
func TestFixesHandler_FoundMarkerSkips(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	entry := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	handler := newFixesHandler(u)

	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "1-0", &entry, prOpenedInput()))
	require.Len(t, repo.recordFixCalls, 1)
	assert.Equal(t, fixesMarker("1-0", &entry), u.writes[0].Marker)

	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "1-0", &entry, prOpenedInput()))
	require.NoError(t, handler.Handle(context.Background(), caseBaseTenant, "2-0", &entry, prOpenedInput()))
	assert.Len(t, repo.recordFixCalls, 1, "a redelivered message must not re-record")
	assert.Len(t, u.writes, 1)
}

// TestFixesHandler_RecordsOneFixPerResolvedNode verifies that a batched
// fix PR — one PR resolving several failing nodes — records a :Fix for
// every node it resolves, all in the one Write. Recording only the
// representative node would leave the other failure precedents without a PROPOSED
// edge, so precedent for them would stop reporting that a fix PR was ever
// opened.
func TestFixesHandler_RecordsOneFixPerResolvedNode(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)

	in := prOpenedInput()
	in.ResolvedNodeIDs = []string{"analytics.revenue", "analytics.margin"}

	require.NoError(t, newFixesHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))

	require.Len(t, repo.recordFixCalls, 2)
	nodes := []string{repo.recordFixCalls[0].NodeID, repo.recordFixCalls[1].NodeID}
	assert.ElementsMatch(t, []string{"analytics.revenue", "analytics.margin"}, nodes)
	for _, p := range repo.recordFixCalls {
		assert.Equal(t, "fix-1", p.FixID, "every node shares the one PR's fix id")
		assert.Equal(t, "rel-1", p.ReleaseID)
	}

	require.Len(t, repo.recordFixPRs, 2)
	for _, pr := range repo.recordFixPRs {
		assert.Equal(t, "fix-1", pr.FixID)
		assert.Equal(t, "core", pr.Project, "every resolved node shares the one PR's project")
		assert.Equal(t, "https://github.com/org/repo/pull/42", pr.PrURL)
		assert.Equal(t, 42, pr.PrNumber)
		assert.Equal(t, "open", pr.State)
		assert.Equal(t, "agent-remediation", pr.OpenedBy)
	}
	assert.Len(t, u.writes, 1, "every node's fix is recorded in the one Write")
	assert.True(t, u.isRecorded(fixesMarker("1-0", nil)))
}

// TestFixesHandler_EmptyResolvedNodeIDsRecordsNoFix verifies that the
// resolved set is read verbatim: a payload naming no resolved node records no
// :Fix, and its marker still commits.
func TestFixesHandler_EmptyResolvedNodeIDsRecordsNoFix(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)

	in := prOpenedInput()
	in.ResolvedNodeIDs = nil
	require.NoError(t, newFixesHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, in))

	assert.Empty(t, repo.recordFixCalls, "node_id alone names no node to record")
	assert.True(t, u.isRecorded(fixesMarker("1-0", nil)))
}

// A message another delivery finished between the lookup and the write is
// acknowledged: its Write finds the marker and records nothing.
func TestFixesHandler_ProcessedMeanwhileWritesNothing(t *testing.T) {
	repo := &fakeCaseBaseRepository{}
	u := newCaseBaseUnitOfWork(repo)
	u.record(fixesMarker("1-0", nil))

	require.NoError(t, newFixesHandler(racingUnitOfWork{u}).
		Handle(context.Background(), caseBaseTenant, "1-0", nil, prOpenedInput()))
	assert.Empty(t, repo.recordFixCalls)
	assert.Len(t, u.writes, 1)
}

// A fix the graph fails to record fails the message with no marker, so
// the redelivery records it again.
func TestFixesHandler_FailedRecordLeavesNoMarker(t *testing.T) {
	u := newCaseBaseUnitOfWork(&fakeCaseBaseRepository{err: errors.New("neo4j: transient failure")})

	err := newFixesHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, prOpenedInput())
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent))
	assert.False(t, u.isRecorded(fixesMarker("1-0", nil)))
}

// A write paused by a backup or a restore reaches the consumer as
// uow.ErrWritesPaused, which it backs off on as for an outage.
func TestFixesHandler_PausedWriteIsReturnedAsPaused(t *testing.T) {
	u := newCaseBaseUnitOfWork(&fakeCaseBaseRepository{})
	u.writeErr = fmt.Errorf("tenant %q: %w", caseBaseTenant, uow.ErrWritesPaused)

	err := newFixesHandler(u).Handle(context.Background(), caseBaseTenant, "1-0", nil, prOpenedInput())
	require.ErrorIs(t, err, uow.ErrWritesPaused)
	assert.False(t, u.isRecorded(fixesMarker("1-0", nil)))
}
