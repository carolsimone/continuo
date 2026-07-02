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
	lastBranch  string
	lastOutbox  *outbox.Entry
	committed   bool
	// RecordPR capture
	lastRecordPR struct {
		id       string
		prURL    string
		prNumber int
		openedBy string
		openedAt time.Time
	}
}

func (r *fakeRepo) CountAttempts(_ context.Context, _, _, _ string) (int, error) { return 0, nil }
func (r *fakeRepo) InsertGenerating(_ context.Context, _ proposal.Proposal) error { return nil }
func (r *fakeRepo) Insert(_ context.Context, _ proposal.Proposal) error { return nil }
func (r *fakeRepo) Get(_ context.Context, _ string) (proposal.View, error) {
	return r.view, nil
}
func (r *fakeRepo) List(_ context.Context, _ repository.ProposalFilter) ([]proposal.View, error) {
	return []proposal.View{r.view}, nil
}
func (r *fakeRepo) BeginPR(_ context.Context, _ string, branch string) (proposal.PRClaim, error) {
	r.lastBranch = branch
	return proposal.PRClaim{
		ID:        r.view.ID,
		ReleaseID: r.view.ReleaseID,
		NodeID:    r.view.NodeID,
		Attempt:   r.view.Attempt,
	}, nil
}
func (r *fakeRepo) RecordPR(_ context.Context, id, prURL string, prNumber int, openedBy string, openedAt time.Time) error {
	r.lastRecordPR.id = id
	r.lastRecordPR.prURL = prURL
	r.lastRecordPR.prNumber = prNumber
	r.lastRecordPR.openedBy = openedBy
	r.lastRecordPR.openedAt = openedAt
	return nil
}
func (r *fakeRepo) FailPR(_ context.Context, _ string) error { return nil }

// fakeUoW is a unit of work backed by the fakeRepo.
type fakeUoW struct {
	repo *fakeRepo
}

func (u *fakeUoW) Begin(_ context.Context) error              { return nil }
func (u *fakeUoW) Commit() error                              { u.repo.committed = true; return nil }
func (u *fakeUoW) Rollback() error                            { return nil }
func (u *fakeUoW) ProposalRepo() repository.ProposalRepository { return u.repo }
func (u *fakeUoW) OutboxRepo() outbox.Repository              { return &fakeOutboxRepo{repo: u.repo} }
func (u *fakeUoW) MessageProcessingRepo() messageprocessing.Repository { return nil }

// fakeOutboxRepo captures the outbox entry passed to Create.
type fakeOutboxRepo struct {
	repo *fakeRepo
}

func (o *fakeOutboxRepo) Create(_ context.Context, e *outbox.Entry) error { o.repo.lastOutbox = e; return nil }
func (o *fakeOutboxRepo) GetPendingBatch(_ context.Context, _ int) ([]*outbox.Entry, error) {
	return nil, nil
}
func (o *fakeOutboxRepo) MarkProcessed(_ context.Context, _ uuid.UUID) error { return nil }
func (o *fakeOutboxRepo) MarkProcessedBatch(_ context.Context, _ []uuid.UUID) error { return nil }
func (o *fakeOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error { return nil }
func (o *fakeOutboxRepo) IncrementRetry(_ context.Context, _ uuid.UUID) error { return nil }

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
}

// TestService_Record_EmitsPROpenedAtomically verifies that Record writes an
// outbox entry with StreamName == streams.RemediationPrOpenedV1 and commits
// the unit of work.
func TestService_Record_EmitsPROpenedAtomically(t *testing.T) {
	repo := &fakeRepo{view: proposal.View{
		ID:        "p1",
		ReleaseID: "r-1",
		NodeID:    "model.p.orders_d",
		Attempt:   1,
	}}
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
}
