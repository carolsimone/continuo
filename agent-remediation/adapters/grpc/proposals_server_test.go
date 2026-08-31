package grpc_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcadapter "github.com/carolsimone/continuo/agent-remediation/adapters/grpc"
	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/proposals"
)

// fakeSvc is a test double that implements grpcadapter.ProposalService.
type fakeSvc struct {
	listViews []proposal.View
	listErr   error
	// lastFilter captures the filter List was called with, so a test can
	// verify the gRPC request's fields reach the service unchanged.
	lastFilter  repository.ProposalFilter
	getView     proposal.View
	getErr      error
	beginClaim  proposal.PRClaim
	beginErr    error
	existingURL string // returned by Get when beginErr == ErrPRConflict
	recordErr   error
	// Begin capture
	lastBeginID      string
	lastBeginService string
	// Record capture
	lastRecordInput proposals.RecordInput
	// FailStuckClaim capture/behavior
	failHit          bool
	failErr          error
	lastFailID       string
	lastFailService  string
	lastFailObserved time.Time
	// prServices is returned by PRServices; lastPRServicesView captures the
	// view it was called with.
	prServices         []string
	lastPRServicesView proposal.View
}

func (f *fakeSvc) List(_ context.Context, filter repository.ProposalFilter) ([]proposal.View, error) {
	f.lastFilter = filter
	return f.listViews, f.listErr
}

func (f *fakeSvc) Get(_ context.Context, _ string) (proposal.View, error) {
	if f.beginErr == repository.ErrPRConflict && f.existingURL != "" {
		return proposal.View{PrURL: f.existingURL}, nil
	}
	return f.getView, f.getErr
}

func (f *fakeSvc) Begin(_ context.Context, id, service string) (proposal.PRClaim, error) {
	f.lastBeginID = id
	f.lastBeginService = service
	return f.beginClaim, f.beginErr
}

func (f *fakeSvc) Record(_ context.Context, in proposals.RecordInput) error {
	f.lastRecordInput = in
	return f.recordErr
}

func (f *fakeSvc) FailStuckClaim(_ context.Context, id, service string, observedClaimedAt time.Time) (bool, error) {
	f.lastFailID = id
	f.lastFailService = service
	f.lastFailObserved = observedClaimedAt
	return f.failHit, f.failErr
}

func (f *fakeSvc) PRServices(v proposal.View) []string {
	f.lastPRServicesView = v
	return f.prServices
}

// ---- ListProposals ----

func TestProposalsServer_ListProposals_HappyPath(t *testing.T) {
	ts := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	fakeViews := []proposal.View{
		{
			ID:               "p1",
			Source:           "test-source",
			ReleaseID:        "rel-1",
			RemediationRound: 2,
			NodeID:           "node-1",
			ResolvedNodeIDs:  []string{"s.a", "s.b"},
			ErrorSignature:   "sig",
			Attempt:          1,
			Status:           proposal.Status("pending"),
			NodeOutcomes: map[string]proposal.NodeOutcome{
				"s.a": {Status: proposal.StatusProposed, Reason: "fixed"},
			},
			Verifications: []proposal.Verification{
				{Service: "service-1", Kind: "dbt", ShadowReleaseID: "shadow-x"},
			},
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
			Edits: []proposal.FileEdit{
				{Path: "models/foo.sql", ContentURI: "s3://bucket/foo.sql", DiffURI: "s3://bucket/foo.diff", TargetNodeID: "s.u"},
				{Path: "models/bar.sql", ContentURI: "s3://bucket/bar.sql", DiffURI: "s3://bucket/bar.diff"},
			},
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
	assert.Equal(t, int32(2), p.RemediationRound, "viewToProto must copy RemediationRound")
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

	require.Len(t, p.Edits, 2, "both proposed file edits must be carried onto the wire")
	assert.Equal(t, "models/foo.sql", p.Edits[0].Path)
	assert.Equal(t, "s3://bucket/foo.sql", p.Edits[0].ContentUri)
	assert.Equal(t, "s3://bucket/foo.diff", p.Edits[0].DiffUri)
	assert.Equal(t, "s.u", p.Edits[0].TargetNodeId, "edits[0]'s target_node_id must be carried onto the wire")
	assert.Equal(t, "models/bar.sql", p.Edits[1].Path)
	assert.Equal(t, "s3://bucket/bar.sql", p.Edits[1].ContentUri)
	assert.Equal(t, "s3://bucket/bar.diff", p.Edits[1].DiffUri)
	assert.Equal(t, "", p.Edits[1].TargetNodeId, "an edit with no TargetNodeID must produce an empty string")

	assert.Equal(t, []string{"s.a", "s.b"}, p.ResolvedNodeIds, "resolved_node_ids must be carried onto the wire")
	require.Contains(t, p.NodeOutcomes, "s.a")
	assert.Equal(t, "proposed", p.NodeOutcomes["s.a"].Status)
	assert.Equal(t, "fixed", p.NodeOutcomes["s.a"].Reason)
	require.Len(t, p.Verifications, 1)
	assert.Equal(t, "service-1", p.Verifications[0].Service)
	assert.Equal(t, "dbt", p.Verifications[0].Kind)
	assert.Equal(t, "shadow-x", p.Verifications[0].ShadowReleaseId)
}

func TestProposalsServer_ListProposals_InternalError(t *testing.T) {
	svc := &fakeSvc{listErr: assert.AnError}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.ListProposals(context.Background(), &remediationv1.ListProposalsRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// TestProposalsServer_ListProposals_FiltersByReleaseID verifies that
// ListProposalsRequest.release_id reaches the service as
// ProposalFilter.ReleaseID unchanged, so a release page can list only its own
// proposals.
func TestProposalsServer_ListProposals_FiltersByReleaseID(t *testing.T) {
	svc := &fakeSvc{}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.ListProposals(context.Background(), &remediationv1.ListProposalsRequest{ReleaseId: "rel-1"})
	require.NoError(t, err)
	assert.Equal(t, "rel-1", svc.lastFilter.ReleaseID)
}

// ---- GetProposal ----

func TestProposalsServer_GetProposal_HappyPath(t *testing.T) {
	ts := time.Date(2024, 5, 10, 8, 0, 0, 0, time.UTC)
	svc := &fakeSvc{
		getView: proposal.View{
			ID:        "p2",
			ReleaseID: "rel-2",
			CreatedAt: ts,
			Edits: []proposal.FileEdit{
				{Path: "models/a.sql", ContentURI: "s3://bucket/a.sql", DiffURI: "s3://bucket/a.diff"},
				{Path: "models/b.sql", ContentURI: "s3://bucket/b.sql", DiffURI: "s3://bucket/b.diff"},
			},
		},
	}
	s := grpcadapter.NewProposalsServer(svc)

	p, err := s.GetProposal(context.Background(), &remediationv1.GetProposalRequest{Id: "p2"})
	require.NoError(t, err)
	assert.Equal(t, "p2", p.Id)
	assert.Equal(t, "rel-2", p.ReleaseId)
	assert.Equal(t, ts.Format(time.RFC3339), p.CreatedAt)

	require.Len(t, p.Edits, 2, "both proposed file edits must be carried onto the wire")
	assert.Equal(t, "models/a.sql", p.Edits[0].Path)
	assert.Equal(t, "s3://bucket/a.sql", p.Edits[0].ContentUri)
	assert.Equal(t, "s3://bucket/a.diff", p.Edits[0].DiffUri)
	assert.Equal(t, "models/b.sql", p.Edits[1].Path)
	assert.Equal(t, "s3://bucket/b.sql", p.Edits[1].ContentUri)
	assert.Equal(t, "s3://bucket/b.diff", p.Edits[1].DiffUri)
}

// TestProposalsServer_GetProposal_PullRequestsAndPrServices verifies that
// GetProposal maps View.PullRequests onto the wire's pull_requests field and
// asks the service for pr_services, so a UI reading a split proposal sees
// every per-service PR and the set of services it split into.
func TestProposalsServer_GetProposal_PullRequestsAndPrServices(t *testing.T) {
	openedAt := time.Date(2026, 6, 24, 9, 0, 0, 0, time.UTC)
	closedAt := time.Date(2026, 6, 25, 9, 0, 0, 0, time.UTC)
	view := proposal.View{
		ID: "p1",
		PullRequests: []proposal.PullRequest{
			{
				Service:    "core",
				Repo:       "org/core",
				Branch:     "remediation/rel-1/attempt1/core",
				PrURL:      "https://gh/pr/1",
				PrNumber:   1,
				PrState:    "open",
				PrOpenedAt: &openedAt,
				PrOpenedBy: "bot",
				PrClosedAt: &closedAt,
			},
			{
				Service:  "billing",
				Repo:     "org/billing",
				Branch:   "remediation/rel-1/attempt1/billing",
				PrURL:    "https://gh/pr/2",
				PrNumber: 2,
				PrState:  "merged",
			},
		},
	}
	svc := &fakeSvc{getView: view, prServices: []string{"billing", "core"}}
	s := grpcadapter.NewProposalsServer(svc)

	p, err := s.GetProposal(context.Background(), &remediationv1.GetProposalRequest{Id: "p1"})
	require.NoError(t, err)

	require.Len(t, p.PullRequests, 2, "every (proposal, service) pull request must be carried onto the wire")
	assert.Equal(t, "core", p.PullRequests[0].Service)
	assert.Equal(t, "org/core", p.PullRequests[0].Repo)
	assert.Equal(t, "remediation/rel-1/attempt1/core", p.PullRequests[0].Branch)
	assert.Equal(t, "https://gh/pr/1", p.PullRequests[0].PrUrl)
	assert.Equal(t, int32(1), p.PullRequests[0].PrNumber)
	assert.Equal(t, "open", p.PullRequests[0].PrState)
	assert.Equal(t, openedAt.Format(time.RFC3339), p.PullRequests[0].PrOpenedAt)
	assert.Equal(t, "bot", p.PullRequests[0].PrOpenedBy)
	assert.Equal(t, closedAt.Format(time.RFC3339), p.PullRequests[0].PrClosedAt)

	assert.Equal(t, "billing", p.PullRequests[1].Service)
	assert.Equal(t, "", p.PullRequests[1].PrOpenedAt, "nil pr_opened_at must produce empty string")
	assert.Equal(t, "", p.PullRequests[1].PrClosedAt, "nil pr_closed_at must produce empty string")

	assert.Equal(t, []string{"billing", "core"}, p.PrServices, "pr_services must come from the service's PRServices call")
	assert.Equal(t, "p1", svc.lastPRServicesView.ID, "PRServices must be called with the fetched view")
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
		ID:              "p3",
		Repo:            "repo-x",
		CommitSHA:       "sha999",
		FilePath:        "models/bar.sql",
		ProposedSQLURI:  "s3://bucket/bar.sql",
		DiffURI:         "s3://bucket/bar.diff",
		ReleaseID:       "rel-3",
		NodeID:          "node-3",
		ResolvedNodeIDs: []string{"node-3", "node-4"},
		Attempt:         2,
		Rationale:       "fix bar",
		Confidence:      proposal.Confidence("medium"),
		Model:           "bar_model",
		ClaimedAt:       claimedAt,
		Branch:          "remediation/rel-3/node-3-attempt2",
		Edits: []proposal.FileEdit{
			{Path: "models/bar.sql", ContentURI: "s3://bucket/bar.sql", DiffURI: "s3://bucket/bar.diff"},
			{Path: "models/baz.sql", ContentURI: "s3://bucket/baz.sql", DiffURI: "s3://bucket/baz.diff"},
		},
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
	assert.Equal(t, []string{"node-3", "node-4"}, resp.ResolvedNodeIds, "resolved_node_ids must be carried onto the wire")
	assert.Equal(t, claimedAt.Format(time.RFC3339), resp.ClaimedAt,
		"the response must carry the claim's persisted ClaimedAt so FailPullRequest can CAS on it later")

	require.Len(t, resp.Edits, 2, "both proposed file edits must be carried onto the wire")
	assert.Equal(t, "models/bar.sql", resp.Edits[0].Path)
	assert.Equal(t, "s3://bucket/bar.sql", resp.Edits[0].ContentUri)
	assert.Equal(t, "s3://bucket/bar.diff", resp.Edits[0].DiffUri)
	assert.Equal(t, "models/baz.sql", resp.Edits[1].Path)
	assert.Equal(t, "s3://bucket/baz.sql", resp.Edits[1].ContentUri)
	assert.Equal(t, "s3://bucket/baz.diff", resp.Edits[1].DiffUri)
}

// TestProposalsServer_BeginPullRequest_ThreadsServiceThrough verifies that
// BeginPullRequest{service:"core"} reaches Begin(ctx, id, "core") and that
// the response echoes the claimed service back.
func TestProposalsServer_BeginPullRequest_ThreadsServiceThrough(t *testing.T) {
	svc := &fakeSvc{beginClaim: proposal.PRClaim{ID: "p1", Service: "core"}}
	s := grpcadapter.NewProposalsServer(svc)

	resp, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p1", Service: "core"})
	require.NoError(t, err)
	assert.Equal(t, "p1", svc.lastBeginID)
	assert.Equal(t, "core", svc.lastBeginService, "the request's service must reach Service.Begin unchanged")
	assert.Equal(t, "core", resp.Service, "the response must echo the claimed service")
}

// TestProposalsServer_BeginPullRequest_EmptyServiceStaysEmpty verifies the
// legacy path: an empty service on the request reaches Begin as "" and the
// response echoes "" back, unchanged.
func TestProposalsServer_BeginPullRequest_EmptyServiceStaysEmpty(t *testing.T) {
	svc := &fakeSvc{beginClaim: proposal.PRClaim{ID: "p1"}}
	s := grpcadapter.NewProposalsServer(svc)

	resp, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p1"})
	require.NoError(t, err)
	assert.Equal(t, "", svc.lastBeginService)
	assert.Equal(t, "", resp.Service)
}

// TestProposalsServer_BeginPullRequest_UnknownServiceMapsToInvalidArgument
// verifies that proposals.ErrUnknownService — the requested service is not
// one of the proposal's PRServices — maps to INVALID_ARGUMENT: it is a bad
// request argument, not a missing resource or a state conflict.
func TestProposalsServer_BeginPullRequest_UnknownServiceMapsToInvalidArgument(t *testing.T) {
	svc := &fakeSvc{beginErr: proposals.ErrUnknownService}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p1", Service: "bogus"})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
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

// TestProposalsServer_RecordPullRequest_ThreadsServiceThrough verifies that
// RecordPullRequest's service field reaches proposals.RecordInput.Service
// unchanged, and that an empty service (the legacy path) stays empty.
func TestProposalsServer_RecordPullRequest_ThreadsServiceThrough(t *testing.T) {
	svc := &fakeSvc{}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.RecordPullRequest(context.Background(), &remediationv1.RecordPullRequestRequest{
		Id: "p4", PrUrl: "https://gh/pr/42", PrNumber: 42, OpenedBy: "bot", Service: "core",
	})
	require.NoError(t, err)
	assert.Equal(t, "core", svc.lastRecordInput.Service)

	_, err = s.RecordPullRequest(context.Background(), &remediationv1.RecordPullRequestRequest{
		Id: "p4", PrUrl: "https://gh/pr/42", PrNumber: 42, OpenedBy: "bot",
	})
	require.NoError(t, err)
	assert.Equal(t, "", svc.lastRecordInput.Service, "an empty service must map to \"\" unchanged")
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

// TestProposalsServer_FailPullRequest_ThreadsServiceThrough verifies that
// FailPullRequest's service field reaches Service.FailStuckClaim unchanged,
// and that an empty service (the legacy path) stays empty.
func TestProposalsServer_FailPullRequest_ThreadsServiceThrough(t *testing.T) {
	claimedAt := time.Date(2026, 6, 24, 9, 0, 0, 0, time.UTC)
	svc := &fakeSvc{failHit: true}
	s := grpcadapter.NewProposalsServer(svc)

	_, err := s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id: "p5", ClaimedAt: claimedAt.Format(time.RFC3339), Service: "core",
	})
	require.NoError(t, err)
	assert.Equal(t, "core", svc.lastFailService)

	_, err = s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id: "p5", ClaimedAt: claimedAt.Format(time.RFC3339),
	})
	require.NoError(t, err)
	assert.Equal(t, "", svc.lastFailService, "an empty service must map to \"\" unchanged")
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
