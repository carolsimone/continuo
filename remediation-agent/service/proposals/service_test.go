package proposals_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/domain/repository"
	"github.com/carolsimone/continuo/remediation-agent/service/proposals"
	"github.com/carolsimone/continuo/remediation-agent/service/uow"
)

// fixedClock returns a fixed time for deterministic tests.
type fixedClock struct{}

func (fixedClock) Now() time.Time {
	t, _ := time.Parse(time.RFC3339, "2026-06-24T00:00:00Z")
	return t
}

// fakeRepo implements repository.ProposalRepository and doubles as a UoW factory.
type fakeRepo struct {
	// state fed to Get/List
	view proposal.View
	// captured outputs
	lastBranch    string
	lastClaimedAt time.Time
	lastOutbox    *outbox.Entry
	committed     bool
	// RecordPR capture
	lastRecordPR struct {
		id       string
		prURL    string
		prNumber int
		openedBy string
		openedAt time.Time
	}
	// recordPRCASHit controls RecordPR's returned hit value; defaults to true
	// so existing tests that do not care about the CAS outcome keep passing.
	recordPRCASHit bool
	// RecordPROutcome capture
	lastOutcome   proposal.PROutcome
	lastClosedAt  time.Time
	outcomeCASHit bool
	openPRs       []proposal.OpenPR
	// FailStuckOpeningPR capture
	lastFailStuckID        string
	lastFailStuckClaimedAt time.Time
	failStuckHit           bool
}

func (r *fakeRepo) CountAttempts(_ context.Context, _, _, _ string) (int, error)  { return 0, nil }
func (r *fakeRepo) InsertGenerating(_ context.Context, _ proposal.Proposal) error { return nil }
func (r *fakeRepo) Upsert(_ context.Context, _ proposal.Proposal) error           { return nil }
func (r *fakeRepo) Get(_ context.Context, _ string) (proposal.View, error) {
	return r.view, nil
}
func (r *fakeRepo) List(_ context.Context, _ repository.ProposalFilter) ([]proposal.View, error) {
	return []proposal.View{r.view}, nil
}
func (r *fakeRepo) BeginPR(_ context.Context, _ string, branch string, claimedAt time.Time) (proposal.PRClaim, error) {
	r.lastBranch = branch
	r.lastClaimedAt = claimedAt
	return proposal.PRClaim{
		ID:        r.view.ID,
		ReleaseID: r.view.ReleaseID,
		NodeID:    r.view.NodeID,
		Attempt:   r.view.Attempt,
	}, nil
}
func (r *fakeRepo) RecordPR(_ context.Context, id, prURL string, prNumber int, openedBy string, openedAt time.Time) (bool, error) {
	r.lastRecordPR.id = id
	r.lastRecordPR.prURL = prURL
	r.lastRecordPR.prNumber = prNumber
	r.lastRecordPR.openedBy = openedBy
	r.lastRecordPR.openedAt = openedAt
	return r.recordPRCASHit, nil
}
func (r *fakeRepo) FailStuckOpeningPR(_ context.Context, id string, observedClaimedAt time.Time) (bool, error) {
	r.lastFailStuckID = id
	r.lastFailStuckClaimedAt = observedClaimedAt
	return r.failStuckHit, nil
}
func (r *fakeRepo) ListOpenPullRequests(_ context.Context, _ int) ([]proposal.OpenPR, error) {
	return r.openPRs, nil
}
func (r *fakeRepo) ListStuckOpening(_ context.Context, _ int, _ *repository.OpeningCursor) ([]proposal.OpeningPR, *repository.OpeningCursor, error) {
	return nil, nil, nil
}
func (r *fakeRepo) RecordPROutcome(_ context.Context, _ string, outcome proposal.PROutcome, closedAt time.Time) (bool, error) {
	r.lastOutcome = outcome
	r.lastClosedAt = closedAt
	return r.outcomeCASHit, nil
}

// fakeUoW is a unit of work backed by the fakeRepo.
type fakeUoW struct {
	repo *fakeRepo
}

func (u *fakeUoW) Begin(_ context.Context) error                       { return nil }
func (u *fakeUoW) Commit() error                                       { u.repo.committed = true; return nil }
func (u *fakeUoW) Rollback() error                                     { return nil }
func (u *fakeUoW) ProposalRepo() repository.ProposalRepository         { return u.repo }
func (u *fakeUoW) OutboxRepo() outbox.Repository                       { return &fakeOutboxRepo{repo: u.repo} }
func (u *fakeUoW) MessageProcessingRepo() messageprocessing.Repository { return nil }

// fakeOutboxRepo captures the outbox entry passed to Create.
type fakeOutboxRepo struct {
	repo *fakeRepo
}

func (o *fakeOutboxRepo) Create(_ context.Context, e *outbox.Entry) error {
	o.repo.lastOutbox = e
	return nil
}
func (o *fakeOutboxRepo) GetPendingBatch(_ context.Context, _ int) ([]*outbox.Entry, error) {
	return nil, nil
}
func (o *fakeOutboxRepo) MarkProcessed(_ context.Context, _ uuid.UUID) error        { return nil }
func (o *fakeOutboxRepo) MarkProcessedBatch(_ context.Context, _ []uuid.UUID) error { return nil }
func (o *fakeOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error { return nil }
func (o *fakeOutboxRepo) IncrementRetry(_ context.Context, _ uuid.UUID) error       { return nil }

func (r *fakeRepo) uowFactory() uow.UnitOfWork {
	return &fakeUoW{repo: r}
}

// TestService_Begin_BuildsDeterministicBranch verifies that Begin derives the
// branch name remediation/<release_id>/<node_sanitized>-attempt<n> where every
// rune outside [A-Za-z0-9_-] is replaced with '-'.
func TestService_Begin_BuildsDeterministicBranch(t *testing.T) {
	repo := &fakeRepo{view: proposal.View{
		ReleaseID:      "r-1",
		NodeID:         "model.p.orders_d",
		Attempt:        1,
		SourceResolved: true,
	}}
	svc := proposals.New(proposals.Deps{
		Repo:   repo,
		NewUoW: repo.uowFactory,
		Clock:  fixedClock{},
	})
	_, err := svc.Begin(context.Background(), "p1")
	require.NoError(t, err)
	require.Equal(t, "remediation/r-1/model-p-orders_d-attempt1", repo.lastBranch)
	require.Equal(t, fixedClock{}.Now(), repo.lastClaimedAt, "Begin must stamp the claim with the service's clock")
}

// TestService_Record_EmitsPROpenedAtomically verifies that Record writes an
// outbox entry with StreamName == streams.RemediationPrOpenedV1 and commits
// the unit of work.
func TestService_Record_EmitsPROpenedAtomically(t *testing.T) {
	repo := &fakeRepo{
		view: proposal.View{
			ID:        "p1",
			ReleaseID: "r-1",
			NodeID:    "model.p.orders_d",
			Attempt:   1,
		},
		recordPRCASHit: true,
	}
	svc := proposals.New(proposals.Deps{
		Repo:   repo,
		NewUoW: repo.uowFactory,
		Clock:  fixedClock{},
	})
	err := svc.Record(context.Background(), proposals.RecordInput{
		ProposalID: "p1",
		PrURL:      "u",
		PrNumber:   7,
		OpenedBy:   "dev|local",
	})
	require.NoError(t, err)
	require.NotNil(t, repo.lastOutbox, "expected an outbox entry to be created")
	require.Equal(t, streams.RemediationPrOpenedV1, repo.lastOutbox.StreamName)
	require.True(t, repo.committed, "expected the unit of work to be committed")
	require.Equal(t, fixedClock{}.Now(), repo.lastRecordPR.openedAt,
		"an unset OpenedAt must fall back to the service clock, as the normal client-side flow relies on")
}

// TestService_Record_UsesProvidedOpenedAt verifies that a non-zero
// RecordInput.OpenedAt — as the opening sweep supplies from GitHub's own
// created_at when recovering a stranded PR — is written through to
// RecordPR instead of the current clock time.
func TestService_Record_UsesProvidedOpenedAt(t *testing.T) {
	repo := &fakeRepo{
		view: proposal.View{
			ID:        "p1",
			ReleaseID: "r-1",
			NodeID:    "model.p.orders_d",
			Attempt:   1,
		},
		recordPRCASHit: true,
	}
	svc := proposals.New(proposals.Deps{
		Repo:   repo,
		NewUoW: repo.uowFactory,
		Clock:  fixedClock{},
	})
	githubCreatedAt := fixedClock{}.Now().Add(-90 * time.Minute)
	err := svc.Record(context.Background(), proposals.RecordInput{
		ProposalID: "p1",
		PrURL:      "u",
		PrNumber:   7,
		OpenedBy:   "dev|local",
		OpenedAt:   githubCreatedAt,
	})
	require.NoError(t, err)
	require.Equal(t, githubCreatedAt, repo.lastRecordPR.openedAt,
		"a provided OpenedAt must be used verbatim, not overridden by the current clock time")
}

// TestService_Record_NoEventWhenCASMisses verifies a CAS miss (row no longer
// 'opening', e.g. already recorded by the reconciler's opening sweep or the
// ui-service route racing to record the same claim) produces no outbox entry,
// no commit, and no error — Record is idempotent under that race.
func TestService_Record_NoEventWhenCASMisses(t *testing.T) {
	repo := &fakeRepo{
		view:           proposal.View{ID: "p1", ReleaseID: "r-1", NodeID: "model.p.orders_d", Attempt: 1},
		recordPRCASHit: false,
	}
	svc := proposals.New(proposals.Deps{Repo: repo, NewUoW: repo.uowFactory, Clock: fixedClock{}})

	err := svc.Record(context.Background(), proposals.RecordInput{
		ProposalID: "p1",
		PrURL:      "u",
		PrNumber:   7,
		OpenedBy:   "dev|local",
	})
	require.NoError(t, err, "a CAS miss is an idempotent no-op, not an error")
	require.Nil(t, repo.lastOutbox, "no event may be emitted when the CAS misses")
	require.False(t, repo.committed, "the transaction must not commit when nothing was written")
}

// TestService_RecordOutcome_EmitsPRClosedAtomically verifies that a fired CAS
// writes a remediation.pr_closed:v1 outbox entry and commits the unit of work.
func TestService_RecordOutcome_EmitsPRClosedAtomically(t *testing.T) {
	repo := &fakeRepo{
		view: proposal.View{
			ID: "p1", ReleaseID: "r-1", NodeID: "model.p.orders_d",
			Attempt: 1, PrURL: "http://gh/pull/7", PrNumber: 7,
		},
		outcomeCASHit: true,
	}
	svc := proposals.New(proposals.Deps{Repo: repo, NewUoW: repo.uowFactory, Clock: fixedClock{}})

	closedAt, _ := time.Parse(time.RFC3339, "2026-07-03T10:00:00Z")
	require.NoError(t, svc.RecordOutcome(context.Background(), "p1", proposal.PROutcomeMerged, closedAt))

	require.Equal(t, proposal.PROutcomeMerged, repo.lastOutcome)
	require.Equal(t, closedAt, repo.lastClosedAt)
	require.NotNil(t, repo.lastOutbox, "expected a pr_closed outbox entry")
	require.Equal(t, streams.RemediationPrClosedV1, repo.lastOutbox.StreamName)
	require.True(t, repo.committed, "expected the unit of work to be committed")
}

// TestService_RecordOutcome_NoEventWhenAlreadyTerminal verifies a CAS miss
// (row no longer 'open') produces no outbox entry and no error.
func TestService_RecordOutcome_NoEventWhenAlreadyTerminal(t *testing.T) {
	repo := &fakeRepo{
		view:          proposal.View{ID: "p1", ReleaseID: "r-1", NodeID: "n", Attempt: 1},
		outcomeCASHit: false,
	}
	svc := proposals.New(proposals.Deps{Repo: repo, NewUoW: repo.uowFactory, Clock: fixedClock{}})

	err := svc.RecordOutcome(context.Background(), "p1", proposal.PROutcomeRejected, time.Now())
	require.NoError(t, err, "a CAS miss is an idempotent no-op, not an error")
	require.Nil(t, repo.lastOutbox, "no event may be emitted when the CAS misses")
}

// TestService_FailStuckClaim_PassesThroughIDAndObservedClaimedAt verifies
// FailStuckClaim delegates to the repository's CAS variant with the exact id
// and observedClaimedAt it was given, and returns the repository's hit value
// unchanged. Both of this method's callers rely on that pass-through to
// distinguish "released" from "a fresher claim raced ahead of me": the
// reconciler's opening sweep, and the gRPC FailPullRequest handler on the
// ui-service PR-creation route's own failure callback.
func TestService_FailStuckClaim_PassesThroughIDAndObservedClaimedAt(t *testing.T) {
	repo := &fakeRepo{failStuckHit: true}
	svc := proposals.New(proposals.Deps{Repo: repo, NewUoW: repo.uowFactory, Clock: fixedClock{}})

	observed := fixedClock{}.Now().Add(-time.Hour)
	hit, err := svc.FailStuckClaim(context.Background(), "p1", observed)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, "p1", repo.lastFailStuckID)
	require.Equal(t, observed, repo.lastFailStuckClaimedAt)

	// A repository-reported miss (re-claimed since observed) passes through
	// as false, not an error.
	repo.failStuckHit = false
	hit, err = svc.FailStuckClaim(context.Background(), "p1", observed)
	require.NoError(t, err)
	require.False(t, hit, "a CAS miss must surface as hit=false, never an error")
}
