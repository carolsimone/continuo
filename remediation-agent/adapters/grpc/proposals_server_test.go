package grpc_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcadapter "github.com/carolsimone/continuo/remediation-agent/adapters/grpc"
	remediationv1 "github.com/carolsimone/continuo/remediation-agent/api/remediation/v1"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/domain/repository"
	"github.com/carolsimone/continuo/remediation-agent/service/proposals"
)

// fakeSvc is a test double that implements grpcadapter.ProposalService.
type fakeSvc struct {
	listViews   []proposal.View
	listErr     error
	getView     proposal.View
	getErr      error
	beginClaim  proposal.PRClaim
	beginErr    error
	existingURL string // returned by Get when beginErr == ErrPRConflict
	recordErr   error
	// FailStuckClaim capture/behavior
	failHit          bool
	failErr          error
	lastFailID       string
	lastFailObserved time.Time
}

func (f *fakeSvc) List(_ context.Context, _ repository.ProposalFilter) ([]proposal.View, error) {
	return f.listViews, f.listErr
}

func (f *fakeSvc) Get(_ context.Context, _ string) (proposal.View, error) {
	if f.beginErr == repository.ErrPRConflict && f.existingURL != "" {
		return proposal.View{PrURL: f.existingURL}, nil
	}
	return f.getView, f.getErr
}

func (f *fakeSvc) Begin(_ context.Context, _ string) (proposal.PRClaim, error) {
	return f.beginClaim, f.beginErr
}

func (f *fakeSvc) Record(_ context.Context, _ proposals.RecordInput) error {
	return f.recordErr
}

func (f *fakeSvc) FailStuckClaim(_ context.Context, id string, observedClaimedAt time.Time) (bool, error) {
	f.lastFailID = id
	f.lastFailObserved = observedClaimedAt
	return f.failHit, f.failErr
}

// ---- ListProposals ----

func TestProposalsServer_ListProposals_HappyPath(t *testing.T) {
	ts := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	fakeViews := []proposal.View{
		{
			ID:             "p1",
			Source:         "test-source",
			ReleaseID:      "rel-1",
			NodeID:         "node-1",
			ErrorSignature: "sig",
			Attempt:        1,
			Status:         proposal.Status("pending"),
			Confidence:     proposal.Confidence("high"),
			Rationale:      "reason",
			ProposedSQLURI: "s3://bucket/proposed.sql",
			DiffURI:        "s3://bucket/diff.txt",
			SourceResolved: true,
			Repo:           "my-repo",
			CommitSHA:      "abc123",
			FilePath:       "models/foo.sql",
			Model:          "my_model",
			CreatedAt:      ts,
			PrURL:          "https://gh/pr/1",
			PrNumber:       42,
			PrState:        "open",
			PrOpenedBy:     "user1",
		},
	}

	svc := &fakeSvc{listViews: fakeViews}
	s := grpcadapter.NewProposalsServer(svc)

	resp, err := s.ListProposals(context.Background(), &remediationv1.ListProposalsRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Proposals, 1)

	p := resp.Proposals[0]
	assert.Equal(t, "p1", p.Id)
	assert.Equal(t, "test-source", p.Source)
	assert.Equal(t, "rel-1", p.ReleaseId)
	assert.Equal(t, "node-1", p.NodeId)
	assert.Equal(t, int32(1), p.Attempt)
	assert.Equal(t, "pending", p.Status)
	assert.Equal(t, "high", p.Confidence)
	assert.Equal(t, "reason", p.Rationale)
	assert.Equal(t, "s3://bucket/proposed.sql", p.ProposedSqlUri)
	assert.Equal(t, "s3://bucket/diff.txt", p.DiffUri)
	assert.True(t, p.SourceResolved)
	assert.Equal(t, "my-repo", p.Repo)
	assert.Equal(t, "abc123", p.CommitSha)
	assert.Equal(t, "models/foo.sql", p.FilePath)
	assert.Equal(t, "my_model", p.Model)
	assert.Equal(t, ts.Format(time.RFC3339), p.CreatedAt)
	assert.Equal(t, "https://gh/pr/1", p.PrUrl)
	assert.Equal(t, int32(42), p.PrNumber)
	assert.Equal(t, "open", p.PrState)
	assert.Equal(t, "user1", p.PrOpenedBy)
	assert.Equal(t, "", p.PrOpenedAt, "nil pr_opened_at must produce empty string")
}

func TestProposalsServer_ListProposals_InternalError(t *testing.T) {
	svc := &fakeSvc{listErr: assert.AnError}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.ListProposals(context.Background(), &remediationv1.ListProposalsRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// ---- GetProposal ----

func TestProposalsServer_GetProposal_HappyPath(t *testing.T) {
	ts := time.Date(2024, 5, 10, 8, 0, 0, 0, time.UTC)
	svc := &fakeSvc{
		getView: proposal.View{
			ID:        "p2",
			ReleaseID: "rel-2",
			CreatedAt: ts,
		},
	}
	s := grpcadapter.NewProposalsServer(svc)

	p, err := s.GetProposal(context.Background(), &remediationv1.GetProposalRequest{Id: "p2"})
	require.NoError(t, err)
	assert.Equal(t, "p2", p.Id)
	assert.Equal(t, "rel-2", p.ReleaseId)
	assert.Equal(t, ts.Format(time.RFC3339), p.CreatedAt)
}

func TestProposalsServer_GetProposal_NotFound(t *testing.T) {
	svc := &fakeSvc{getErr: repository.ErrNotFound}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.GetProposal(context.Background(), &remediationv1.GetProposalRequest{Id: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestProposalsServer_GetProposal_InternalError(t *testing.T) {
	svc := &fakeSvc{getErr: assert.AnError}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.GetProposal(context.Background(), &remediationv1.GetProposalRequest{Id: "p99"})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// ---- BeginPullRequest ----

func TestProposalsServer_BeginPullRequest_HappyPath(t *testing.T) {
	claimedAt := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	claim := proposal.PRClaim{
		ID:             "p3",
		Repo:           "repo-x",
		CommitSHA:      "sha999",
		FilePath:       "models/bar.sql",
		ProposedSQLURI: "s3://bucket/bar.sql",
		DiffURI:        "s3://bucket/bar.diff",
		ReleaseID:      "rel-3",
		NodeID:         "node-3",
		Attempt:        2,
		Rationale:      "fix bar",
		Confidence:     proposal.Confidence("medium"),
		Model:          "bar_model",
		ClaimedAt:      claimedAt,
		Branch:         "remediation/rel-3/node-3-attempt2",
	}
	svc := &fakeSvc{beginClaim: claim}
	s := grpcadapter.NewProposalsServer(svc)

	resp, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p3"})
	require.NoError(t, err)
	assert.Equal(t, "p3", resp.ProposalId)
	assert.Equal(t, "repo-x", resp.Repo)
	assert.Equal(t, "sha999", resp.CommitSha)
	assert.Equal(t, "models/bar.sql", resp.FilePath)
	assert.Equal(t, "s3://bucket/bar.sql", resp.ProposedSqlUri)
	assert.Equal(t, "s3://bucket/bar.diff", resp.DiffUri)
	assert.Equal(t, "rel-3", resp.ReleaseId)
	assert.Equal(t, "node-3", resp.NodeId)
	assert.Equal(t, int32(2), resp.Attempt)
	assert.Equal(t, "fix bar", resp.Rationale)
	assert.Equal(t, "medium", resp.Confidence)
	assert.Equal(t, "bar_model", resp.Model)
	assert.Equal(t, "remediation/rel-3/node-3-attempt2", resp.Branch)
	assert.Equal(t, claimedAt.Format(time.RFC3339), resp.ClaimedAt,
		"the response must carry the claim's persisted ClaimedAt so FailPullRequest can CAS on it later")
}

func TestProposalsServer_BeginPullRequest_ConflictMapsToFailedPrecondition(t *testing.T) {
	svc := &fakeSvc{beginErr: repository.ErrPRConflict, existingURL: "https://gh/pr/7"}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p1"})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	// The error message should carry the existing PR URL.
	assert.Contains(t, err.Error(), "https://gh/pr/7")
}

func TestProposalsServer_BeginPullRequest_NotSourceResolved(t *testing.T) {
	svc := &fakeSvc{beginErr: repository.ErrNotSourceResolved}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p2"})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestProposalsServer_BeginPullRequest_NotFound(t *testing.T) {
	svc := &fakeSvc{beginErr: repository.ErrNotFound}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestProposalsServer_BeginPullRequest_InternalError(t *testing.T) {
	svc := &fakeSvc{beginErr: assert.AnError}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p9"})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// ---- RecordPullRequest ----

func TestProposalsServer_RecordPullRequest_HappyPath(t *testing.T) {
	svc := &fakeSvc{}
	s := grpcadapter.NewProposalsServer(svc)

	resp, err := s.RecordPullRequest(context.Background(), &remediationv1.RecordPullRequestRequest{
		Id:       "p4",
		PrUrl:    "https://gh/pr/42",
		PrNumber: 42,
		OpenedBy: "bot",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestProposalsServer_RecordPullRequest_NotFound(t *testing.T) {
	svc := &fakeSvc{recordErr: repository.ErrNotFound}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.RecordPullRequest(context.Background(), &remediationv1.RecordPullRequestRequest{Id: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestProposalsServer_RecordPullRequest_InternalError(t *testing.T) {
	svc := &fakeSvc{recordErr: assert.AnError}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.RecordPullRequest(context.Background(), &remediationv1.RecordPullRequestRequest{Id: "p4"})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// ---- FailPullRequest ----

func TestProposalsServer_FailPullRequest_HappyPath(t *testing.T) {
	claimedAt := time.Date(2026, 6, 24, 9, 0, 0, 0, time.UTC)
	svc := &fakeSvc{failHit: true}
	s := grpcadapter.NewProposalsServer(svc)

	resp, err := s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id:        "p5",
		ClaimedAt: claimedAt.Format(time.RFC3339),
	})
	require.NoError(t, err)
	assert.True(t, resp.Released)
	assert.Equal(t, "p5", svc.lastFailID)
	assert.True(t, claimedAt.Equal(svc.lastFailObserved),
		"the handler must parse claimed_at and forward the exact instant to FailStuckClaim")
}

// TestProposalsServer_FailPullRequest_CASMiss_ReturnsReleasedFalseNoError
// verifies that a CAS miss — the claim already moved on, e.g. released and
// re-claimed by the reconciler's opening sweep between this caller's own
// BeginPullRequest and its failure callback — surfaces as released=false
// with no gRPC error, never as NOT_FOUND or any other error code: a caller
// that lost the race must never be told its own request failed.
func TestProposalsServer_FailPullRequest_CASMiss_ReturnsReleasedFalseNoError(t *testing.T) {
	svc := &fakeSvc{failHit: false}
	s := grpcadapter.NewProposalsServer(svc)

	resp, err := s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id:        "p5",
		ClaimedAt: time.Now().UTC().Format(time.RFC3339),
	})
	require.NoError(t, err)
	assert.False(t, resp.Released)
}

func TestProposalsServer_FailPullRequest_MissingClaimedAt_InvalidArgument(t *testing.T) {
	svc := &fakeSvc{failHit: true}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{Id: "p5"})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestProposalsServer_FailPullRequest_UnparseableClaimedAt_InvalidArgument(t *testing.T) {
	svc := &fakeSvc{failHit: true}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id: "p5", ClaimedAt: "not-a-timestamp",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestProposalsServer_FailPullRequest_InternalError(t *testing.T) {
	svc := &fakeSvc{failErr: assert.AnError}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id: "p5", ClaimedAt: time.Now().UTC().Format(time.RFC3339),
	})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// ---- PrOpenedAt non-nil ----

func TestProposalsServer_ListProposals_PrOpenedAtNonNil(t *testing.T) {
	ts := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	openedAt := time.Date(2024, 3, 16, 12, 30, 0, 0, time.UTC)
	fakeViews := []proposal.View{
		{
			ID:         "p10",
			CreatedAt:  ts,
			PrOpenedAt: &openedAt,
		},
	}

	svc := &fakeSvc{listViews: fakeViews}
	s := grpcadapter.NewProposalsServer(svc)

	resp, err := s.ListProposals(context.Background(), &remediationv1.ListProposalsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Proposals, 1)
	assert.Equal(t, openedAt.Format(time.RFC3339), resp.Proposals[0].PrOpenedAt)
}
