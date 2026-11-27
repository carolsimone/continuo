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
	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/fixes"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// fakeSvc is a test double that implements grpcadapter.FixService.
type fakeSvc struct {
	listViews []fix.View
	listErr   error
	// lastFilter captures the filter List was called with, so a test can
	// verify the gRPC request's fields reach the service unchanged.
	lastFilter repository.FixFilter
	getView    fix.View
	getErr     error
	beginClaim fix.PRClaim
	beginErr   error
	recordErr  error
	// Begin capture
	lastBeginID      string
	lastBeginProject string
	// Record capture
	lastRecordInput fixes.RecordInput
	// FailStuckClaim capture/behavior
	failHit          bool
	failErr          error
	lastFailID       string
	lastFailProject  string
	lastFailObserved time.Time
	// prProjects is returned by PRProjects; lastPRProjectsView captures the
	// view it was called with.
	prProjects         []string
	lastPRProjectsView fix.View
}

func (f *fakeSvc) List(_ context.Context, filter repository.FixFilter) ([]fix.View, error) {
	f.lastFilter = filter
	return f.listViews, f.listErr
}

func (f *fakeSvc) Get(_ context.Context, _ string) (fix.View, error) {
	return f.getView, f.getErr
}

func (f *fakeSvc) Begin(_ context.Context, id, project string) (fix.PRClaim, error) {
	f.lastBeginID = id
	f.lastBeginProject = project
	return f.beginClaim, f.beginErr
}

func (f *fakeSvc) Record(_ context.Context, in fixes.RecordInput) error {
	f.lastRecordInput = in
	return f.recordErr
}

func (f *fakeSvc) FailStuckClaim(_ context.Context, id, project string, observedClaimedAt time.Time) (bool, error) {
	f.lastFailID = id
	f.lastFailProject = project
	f.lastFailObserved = observedClaimedAt
	return f.failHit, f.failErr
}

func (f *fakeSvc) PRProjects(v fix.View) []string {
	f.lastPRProjectsView = v
	return f.prProjects
}

// ---- ListFixes ----

func TestFixesServer_ListFixes_HappyPath(t *testing.T) {
	ts := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	fakeViews := []fix.View{
		{
			ID:               "p1",
			Source:           "test-source",
			ReleaseID:        "rel-1",
			RemediationRound: 2,
			NodeID:           "node-1",
			ResolvedNodeIDs:  []string{"s.a", "s.b"},
			ErrorSignature:   "sig",
			Try:              1,
			Status:           fix.Status("pending"),
			NodeOutcomes: map[string]fix.NodeOutcome{
				"s.a": {Status: fix.StatusProposed, Reason: "fixed"},
			},
			ReleaseChecks: []fix.ReleaseCheck{
				{Project: "service-1", Kind: "dbt", CheckID: "verify-x", Phase: fix.PhasePassed},
			},
			Confidence:     fix.Confidence("high"),
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
			Edits: []fix.FileEdit{
				{Path: "models/foo.sql", ContentURI: "s3://bucket/foo.sql", DiffURI: "s3://bucket/foo.diff", TargetNodeID: "s.u"},
				{Path: "models/bar.sql", ContentURI: "s3://bucket/bar.sql", DiffURI: "s3://bucket/bar.diff"},
			},
		},
	}

	svc := &fakeSvc{listViews: fakeViews}
	s := grpcadapter.NewFixesServer(svc)

	resp, err := s.ListFixes(context.Background(), &remediationv1.ListFixesRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Fixes, 1)

	p := resp.Fixes[0]
	assert.Equal(t, "p1", p.Id)
	assert.Equal(t, "test-source", p.Source)
	assert.Equal(t, "rel-1", p.ReleaseId)
	assert.Equal(t, int32(2), p.RemediationRound, "viewToProto must copy RemediationRound")
	assert.Equal(t, "node-1", p.NodeId)
	assert.Equal(t, int32(1), p.Try)
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
	require.Len(t, p.ReleaseChecks, 1)
	assert.Equal(t, "service-1", p.ReleaseChecks[0].Project)
	assert.Equal(t, "dbt", p.ReleaseChecks[0].Kind)
	assert.Equal(t, "verify-x", p.ReleaseChecks[0].CheckId)
	assert.Equal(t, "passed", p.ReleaseChecks[0].Phase)
}

func TestFixesServer_ListFixes_InternalError(t *testing.T) {
	svc := &fakeSvc{listErr: assert.AnError}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.ListFixes(context.Background(), &remediationv1.ListFixesRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// TestFixesServer_ListFixes_FiltersByReleaseID verifies that
// ListFixesRequest.release_id reaches the service as
// FixFilter.ReleaseID unchanged, so a release page can list only its own
// fixes.
func TestFixesServer_ListFixes_FiltersByReleaseID(t *testing.T) {
	svc := &fakeSvc{}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.ListFixes(context.Background(), &remediationv1.ListFixesRequest{ReleaseId: "rel-1"})
	require.NoError(t, err)
	assert.Equal(t, "rel-1", svc.lastFilter.ReleaseID)
}

// TestFixesServer_ListFixes_FiltersByProject verifies that
// ListFixesRequest.project reaches the service as FixFilter.Project
// unchanged, so a listing can be narrowed to one team's work.
func TestFixesServer_ListFixes_FiltersByProject(t *testing.T) {
	svc := &fakeSvc{}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.ListFixes(context.Background(), &remediationv1.ListFixesRequest{Project: "core"})
	require.NoError(t, err)
	assert.Equal(t, "core", svc.lastFilter.Project)
}

// ---- GetFix ----

func TestFixesServer_GetFix_HappyPath(t *testing.T) {
	ts := time.Date(2024, 5, 10, 8, 0, 0, 0, time.UTC)
	svc := &fakeSvc{
		getView: fix.View{
			ID:        "p2",
			ReleaseID: "rel-2",
			CreatedAt: ts,
			Edits: []fix.FileEdit{
				{Path: "models/a.sql", ContentURI: "s3://bucket/a.sql", DiffURI: "s3://bucket/a.diff"},
				{Path: "models/b.sql", ContentURI: "s3://bucket/b.sql", DiffURI: "s3://bucket/b.diff"},
			},
		},
	}
	s := grpcadapter.NewFixesServer(svc)

	p, err := s.GetFix(context.Background(), &remediationv1.GetFixRequest{Id: "p2"})
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

// TestFixesServer_MapsTheNodeOutcomeCode verifies that GetFix and
// ListFixes put the reason code of each node outcome on the wire beside its
// status and reason, and that an outcome with no code leaves the field empty.
func TestFixesServer_MapsTheNodeOutcomeCode(t *testing.T) {
	view := fix.View{
		ID:        "p-code",
		ReleaseID: "rel-code",
		NodeOutcomes: map[string]fix.NodeOutcome{
			"s.handed": {Status: fix.StatusEscalated, Reason: "no type matches", Code: model.HandoverReasonNoType},
			"s.fixed":  {Status: fix.StatusProposed},
		},
	}
	svc := &fakeSvc{getView: view, listViews: []fix.View{view}}
	s := grpcadapter.NewFixesServer(svc)

	got, err := s.GetFix(context.Background(), &remediationv1.GetFixRequest{Id: "p-code"})
	require.NoError(t, err)
	require.Len(t, got.NodeOutcomes, 2)
	assert.Equal(t, "escalated", got.NodeOutcomes["s.handed"].Status)
	assert.Equal(t, "no type matches", got.NodeOutcomes["s.handed"].Reason)
	assert.Equal(t, string(model.HandoverReasonNoType), got.NodeOutcomes["s.handed"].Code)
	assert.Empty(t, got.NodeOutcomes["s.fixed"].Code, "an outcome with no code carries an empty code")

	list, err := s.ListFixes(context.Background(), &remediationv1.ListFixesRequest{})
	require.NoError(t, err)
	require.Len(t, list.Fixes, 1)
	assert.Equal(t, string(model.HandoverReasonNoType), list.Fixes[0].NodeOutcomes["s.handed"].Code)
}

// TestFixesServer_MapsTheApprovers verifies that GetFix and
// ListFixes put each approver of a fix on the wire with its node, owner
// and source, in the row's order, and that a fix with none leaves the list
// empty.
func TestFixesServer_MapsTheApprovers(t *testing.T) {
	view := fix.View{
		ID:        "p-approvers",
		ReleaseID: "rel-approvers",
		Approvers: []fix.Approver{
			{NodeID: "s.a", Owner: "finance", Source: model.ApproverSourceBundle},
			{NodeID: "s.b", Source: model.ApproverSourceNone},
		},
	}
	svc := &fakeSvc{getView: view, listViews: []fix.View{view, {ID: "p-legacy"}}}
	s := grpcadapter.NewFixesServer(svc)

	got, err := s.GetFix(context.Background(), &remediationv1.GetFixRequest{Id: "p-approvers"})
	require.NoError(t, err)
	require.Len(t, got.Approvers, 2)
	assert.Equal(t, "s.a", got.Approvers[0].NodeId)
	assert.Equal(t, "finance", got.Approvers[0].Owner)
	assert.Equal(t, "bundle", got.Approvers[0].Source)
	assert.Equal(t, "s.b", got.Approvers[1].NodeId)
	assert.Empty(t, got.Approvers[1].Owner, "a node with no owner carries an empty owner")
	assert.Equal(t, "none", got.Approvers[1].Source)

	list, err := s.ListFixes(context.Background(), &remediationv1.ListFixesRequest{})
	require.NoError(t, err)
	require.Len(t, list.Fixes, 2)
	require.Len(t, list.Fixes[0].Approvers, 2)
	assert.Empty(t, list.Fixes[1].Approvers)
}

// TestFixesServer_GetFix_PullRequestsAndPrProjects verifies that
// GetFix maps View.PullRequests onto the wire's pull_requests field and
// asks the service for pr_projects, so a UI reading a split fix sees
// every per-project PR and the set of projects it split into.
func TestFixesServer_GetFix_PullRequestsAndPrProjects(t *testing.T) {
	openedAt := time.Date(2026, 6, 24, 9, 0, 0, 0, time.UTC)
	closedAt := time.Date(2026, 6, 25, 9, 0, 0, 0, time.UTC)
	view := fix.View{
		ID: "p1",
		PullRequests: []fix.PullRequest{
			{
				Project:    "core",
				Repo:       "org/core",
				Branch:     "remediation/rel-1/try1/core",
				PrURL:      "https://gh/pr/1",
				PrNumber:   1,
				PrState:    "open",
				PrOpenedAt: &openedAt,
				PrOpenedBy: "bot",
				PrClosedAt: &closedAt,
			},
			{
				Project:  "billing",
				Repo:     "org/billing",
				Branch:   "remediation/rel-1/try1/billing",
				PrURL:    "https://gh/pr/2",
				PrNumber: 2,
				PrState:  "merged",
			},
		},
	}
	svc := &fakeSvc{getView: view, prProjects: []string{"billing", "core"}}
	s := grpcadapter.NewFixesServer(svc)

	p, err := s.GetFix(context.Background(), &remediationv1.GetFixRequest{Id: "p1"})
	require.NoError(t, err)

	require.Len(t, p.PullRequests, 2, "every (fix, project) pull request must be carried onto the wire")
	assert.Equal(t, "core", p.PullRequests[0].Project)
	assert.Equal(t, "org/core", p.PullRequests[0].Repo)
	assert.Equal(t, "remediation/rel-1/try1/core", p.PullRequests[0].Branch)
	assert.Equal(t, "https://gh/pr/1", p.PullRequests[0].PrUrl)
	assert.Equal(t, int32(1), p.PullRequests[0].PrNumber)
	assert.Equal(t, "open", p.PullRequests[0].PrState)
	assert.Equal(t, openedAt.Format(time.RFC3339), p.PullRequests[0].PrOpenedAt)
	assert.Equal(t, "bot", p.PullRequests[0].PrOpenedBy)
	assert.Equal(t, closedAt.Format(time.RFC3339), p.PullRequests[0].PrClosedAt)

	assert.Equal(t, "billing", p.PullRequests[1].Project)
	assert.Equal(t, "", p.PullRequests[1].PrOpenedAt, "nil pr_opened_at must produce empty string")
	assert.Equal(t, "", p.PullRequests[1].PrClosedAt, "nil pr_closed_at must produce empty string")

	assert.Equal(t, []string{"billing", "core"}, p.PrProjects, "pr_projects must come from the service's PRProjects call")
	assert.Equal(t, "p1", svc.lastPRProjectsView.ID, "PRProjects must be called with the fetched view")
}

func TestFixesServer_GetFix_NotFound(t *testing.T) {
	svc := &fakeSvc{getErr: repository.ErrNotFound}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.GetFix(context.Background(), &remediationv1.GetFixRequest{Id: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestFixesServer_GetFix_InternalError(t *testing.T) {
	svc := &fakeSvc{getErr: assert.AnError}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.GetFix(context.Background(), &remediationv1.GetFixRequest{Id: "p99"})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// ---- BeginPullRequest ----

func TestFixesServer_BeginPullRequest_HappyPath(t *testing.T) {
	claimedAt := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	claim := fix.PRClaim{
		ID:              "p3",
		Repo:            "repo-x",
		CommitSHA:       "sha999",
		FilePath:        "models/bar.sql",
		ProposedSQLURI:  "s3://bucket/bar.sql",
		DiffURI:         "s3://bucket/bar.diff",
		ReleaseID:       "rel-3",
		NodeID:          "node-3",
		ResolvedNodeIDs: []string{"node-3", "node-4"},
		Try:             2,
		Rationale:       "fix bar",
		Confidence:      fix.Confidence("medium"),
		Model:           "bar_model",
		ClaimedAt:       claimedAt,
		Branch:          "remediation/rel-3/node-3-try2",
		Edits: []fix.FileEdit{
			{Path: "models/bar.sql", ContentURI: "s3://bucket/bar.sql", DiffURI: "s3://bucket/bar.diff"},
			{Path: "models/baz_v2.sql", RenamedFrom: "models/baz.sql", ContentURI: "s3://bucket/baz.sql", DiffURI: "s3://bucket/baz.diff"},
		},
	}
	svc := &fakeSvc{beginClaim: claim}
	s := grpcadapter.NewFixesServer(svc)

	resp, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p3"})
	require.NoError(t, err)
	assert.Equal(t, "p3", resp.FixId)
	assert.Equal(t, "repo-x", resp.Repo)
	assert.Equal(t, "sha999", resp.CommitSha)
	assert.Equal(t, "models/bar.sql", resp.FilePath)
	assert.Equal(t, "s3://bucket/bar.sql", resp.ProposedSqlUri)
	assert.Equal(t, "s3://bucket/bar.diff", resp.DiffUri)
	assert.Equal(t, "rel-3", resp.ReleaseId)
	assert.Equal(t, "node-3", resp.NodeId)
	assert.Equal(t, int32(2), resp.Try)
	assert.Equal(t, "fix bar", resp.Rationale)
	assert.Equal(t, "medium", resp.Confidence)
	assert.Equal(t, "bar_model", resp.Model)
	assert.Equal(t, "remediation/rel-3/node-3-try2", resp.Branch)
	assert.Equal(t, []string{"node-3", "node-4"}, resp.ResolvedNodeIds, "resolved_node_ids must be carried onto the wire")
	assert.Equal(t, claimedAt.Format(time.RFC3339), resp.ClaimedAt,
		"the response must carry the claim's persisted ClaimedAt so FailPullRequest can CAS on it later")

	require.Len(t, resp.Edits, 2, "both proposed file edits must be carried onto the wire")
	assert.Equal(t, "models/bar.sql", resp.Edits[0].Path)
	assert.Equal(t, "s3://bucket/bar.sql", resp.Edits[0].ContentUri)
	assert.Equal(t, "s3://bucket/bar.diff", resp.Edits[0].DiffUri)
	assert.Empty(t, resp.Edits[0].RenamedFrom, "an edit in place moves no file")
	assert.Equal(t, "models/baz_v2.sql", resp.Edits[1].Path)
	assert.Equal(t, "models/baz.sql", resp.Edits[1].RenamedFrom, "a moved file names the path it moves from")
	assert.Equal(t, "s3://bucket/baz.sql", resp.Edits[1].ContentUri)
	assert.Equal(t, "s3://bucket/baz.diff", resp.Edits[1].DiffUri)
}

// TestBeginPR_ReturnsTheClaimNodesApprovers verifies that BeginPullRequest puts
// the approvers of the claim's nodes on the wire, so the opener of the pull
// request can name who must approve it.
func TestBeginPR_ReturnsTheClaimNodesApprovers(t *testing.T) {
	claim := fix.PRClaim{
		ID:              "p-claim",
		ResolvedNodeIDs: []string{"s.a", "s.b"},
		Approvers: []fix.Approver{
			{NodeID: "s.a", Owner: "finance", Source: model.ApproverSourceBundle},
			{NodeID: "s.b", Owner: "growth", Source: model.ApproverSourceContractFile},
		},
	}
	s := grpcadapter.NewFixesServer(&fakeSvc{beginClaim: claim})

	resp, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p-claim"})
	require.NoError(t, err)
	require.Len(t, resp.Approvers, 2)
	assert.Equal(t, "s.a", resp.Approvers[0].NodeId)
	assert.Equal(t, "finance", resp.Approvers[0].Owner)
	assert.Equal(t, "bundle", resp.Approvers[0].Source)
	assert.Equal(t, "s.b", resp.Approvers[1].NodeId)
	assert.Equal(t, "growth", resp.Approvers[1].Owner)
	assert.Equal(t, "contract_file", resp.Approvers[1].Source)

	none, err := grpcadapter.NewFixesServer(&fakeSvc{beginClaim: fix.PRClaim{ID: "p-legacy"}}).
		BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p-legacy"})
	require.NoError(t, err)
	assert.Empty(t, none.Approvers)
}

// TestFixesServer_BeginPullRequest_ThreadsProjectThrough verifies that
// BeginPullRequest{project:"core"} reaches Begin(ctx, id, "core") and that
// the response echoes the claimed project back.
func TestFixesServer_BeginPullRequest_ThreadsProjectThrough(t *testing.T) {
	svc := &fakeSvc{beginClaim: fix.PRClaim{ID: "p1", Project: "core"}}
	s := grpcadapter.NewFixesServer(svc)

	resp, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p1", Project: "core"})
	require.NoError(t, err)
	assert.Equal(t, "p1", svc.lastBeginID)
	assert.Equal(t, "core", svc.lastBeginProject, "the request's project must reach Service.Begin unchanged")
	assert.Equal(t, "core", resp.Project, "the response must echo the claimed project")
}

// TestFixesServer_BeginPullRequest_EmptyProjectStaysEmpty verifies the
// legacy path: an empty project on the request reaches Begin as "" and the
// response echoes "" back, unchanged.
func TestFixesServer_BeginPullRequest_EmptyProjectStaysEmpty(t *testing.T) {
	svc := &fakeSvc{beginClaim: fix.PRClaim{ID: "p1"}}
	s := grpcadapter.NewFixesServer(svc)

	resp, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p1"})
	require.NoError(t, err)
	assert.Equal(t, "", svc.lastBeginProject)
	assert.Equal(t, "", resp.Project)
}

// TestFixesServer_BeginPullRequest_UnknownProjectMapsToInvalidArgument
// verifies that fixes.ErrUnknownProject — the requested project is not
// one of the fix's PRProjects — maps to INVALID_ARGUMENT: it is a bad
// request argument, not a missing resource or a state conflict.
func TestFixesServer_BeginPullRequest_UnknownProjectMapsToInvalidArgument(t *testing.T) {
	svc := &fakeSvc{beginErr: fixes.ErrUnknownProject}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p1", Project: "bogus"})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestFixesServer_BeginPullRequest_ConflictMapsToFailedPrecondition(t *testing.T) {
	svc := &fakeSvc{
		beginErr: repository.ErrPRConflict,
		getView: fix.View{
			PrURL:        "https://gh/pr/7",
			PullRequests: []fix.PullRequest{{Project: "", PrURL: "https://gh/pr/7", PrState: "open"}},
		},
	}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p1"})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	// The error message should carry the existing PR URL.
	assert.Contains(t, err.Error(), "https://gh/pr/7")
}

// TestFixesServer_BeginPullRequest_ConflictEmbedsTheConflictingProjectsOwnURL
// verifies that a conflict on a named project embeds THAT project's own
// pr_url from v.PullRequests, never a different project's. The fix's
// singular pr_url field mirrors the first child row ordered by project
// (here, "core"), so a naive read of it would report core's URL for a
// finance conflict even though the two are unrelated pull requests.
func TestFixesServer_BeginPullRequest_ConflictEmbedsTheConflictingProjectsOwnURL(t *testing.T) {
	view := fix.View{
		PrURL: "https://gh/pr/core", // singular fields mirror the first child, "core"
		PullRequests: []fix.PullRequest{
			{Project: "core", PrURL: "https://gh/pr/core", PrState: "open"},
			{Project: "finance", PrState: "opening"}, // claimed but not yet opened: no URL
		},
	}

	t.Run("finance conflict embeds finance's own state, never core's URL", func(t *testing.T) {
		svc := &fakeSvc{beginErr: repository.ErrPRConflict, getView: view}
		s := grpcadapter.NewFixesServer(svc)

		_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p1", Project: "finance"})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.NotContains(t, err.Error(), "https://gh/pr/core",
			"a finance conflict must never surface core's PR URL")
	})

	t.Run("core conflict embeds core's own URL", func(t *testing.T) {
		svc := &fakeSvc{beginErr: repository.ErrPRConflict, getView: view}
		s := grpcadapter.NewFixesServer(svc)

		_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p1", Project: "core"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "https://gh/pr/core")
	})

	t.Run("legacy project embeds the singular pr_url", func(t *testing.T) {
		legacyView := fix.View{
			PrURL:        "https://gh/pr/legacy",
			PullRequests: []fix.PullRequest{{Project: "", PrURL: "https://gh/pr/legacy", PrState: "open"}},
		}
		svc := &fakeSvc{beginErr: repository.ErrPRConflict, getView: legacyView}
		s := grpcadapter.NewFixesServer(svc)

		_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "https://gh/pr/legacy")
	})
}

func TestFixesServer_BeginPullRequest_NotSourceResolved(t *testing.T) {
	svc := &fakeSvc{beginErr: repository.ErrNotSourceResolved}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p2"})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestFixesServer_BeginPullRequest_NoRepositoryMapsToFailedPrecondition
// verifies fixes.ErrNoRepository maps to FAILED_PRECONDITION, carrying
// its message, rather than falling through to the generic Internal case.
func TestFixesServer_BeginPullRequest_NoRepositoryMapsToFailedPrecondition(t *testing.T) {
	svc := &fakeSvc{beginErr: fixes.ErrNoRepository}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p2"})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "no repository recorded for this fix")
}

func TestFixesServer_BeginPullRequest_NotFound(t *testing.T) {
	svc := &fakeSvc{beginErr: repository.ErrNotFound}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestFixesServer_BeginPullRequest_InternalError(t *testing.T) {
	svc := &fakeSvc{beginErr: assert.AnError}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p9"})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// ---- RecordPullRequest ----

func TestFixesServer_RecordPullRequest_HappyPath(t *testing.T) {
	svc := &fakeSvc{}
	s := grpcadapter.NewFixesServer(svc)

	resp, err := s.RecordPullRequest(context.Background(), &remediationv1.RecordPullRequestRequest{
		Id:       "p4",
		PrUrl:    "https://gh/pr/42",
		PrNumber: 42,
		OpenedBy: "bot",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

// TestFixesServer_RecordPullRequest_ThreadsProjectThrough verifies that
// RecordPullRequest's project field reaches fixes.RecordInput.Project
// unchanged, and that an empty project (the legacy path) stays empty.
func TestFixesServer_RecordPullRequest_ThreadsProjectThrough(t *testing.T) {
	svc := &fakeSvc{}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.RecordPullRequest(context.Background(), &remediationv1.RecordPullRequestRequest{
		Id: "p4", PrUrl: "https://gh/pr/42", PrNumber: 42, OpenedBy: "bot", Project: "core",
	})
	require.NoError(t, err)
	assert.Equal(t, "core", svc.lastRecordInput.Project)

	_, err = s.RecordPullRequest(context.Background(), &remediationv1.RecordPullRequestRequest{
		Id: "p4", PrUrl: "https://gh/pr/42", PrNumber: 42, OpenedBy: "bot",
	})
	require.NoError(t, err)
	assert.Equal(t, "", svc.lastRecordInput.Project, "an empty project must map to \"\" unchanged")
}

func TestFixesServer_RecordPullRequest_NotFound(t *testing.T) {
	svc := &fakeSvc{recordErr: repository.ErrNotFound}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.RecordPullRequest(context.Background(), &remediationv1.RecordPullRequestRequest{Id: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestFixesServer_RecordPullRequest_InternalError(t *testing.T) {
	svc := &fakeSvc{recordErr: assert.AnError}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.RecordPullRequest(context.Background(), &remediationv1.RecordPullRequestRequest{Id: "p4"})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// ---- FailPullRequest ----

func TestFixesServer_FailPullRequest_HappyPath(t *testing.T) {
	claimedAt := time.Date(2026, 6, 24, 9, 0, 0, 0, time.UTC)
	svc := &fakeSvc{failHit: true}
	s := grpcadapter.NewFixesServer(svc)

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

// TestFixesServer_ClaimedAtRoundTripsExactly pins the claim's compare-and-set
// across the wire: pr_claimed_at is stored to the microsecond, and the
// claimed_at BeginPullRequest hands out is what the ui sends back on
// FailPullRequest. A claimed_at that dropped the fraction would never equal
// the stored value, so a failed Create PR could not release its claim and
// every retry would be refused until the opening sweep ran.
func TestFixesServer_ClaimedAtRoundTripsExactly(t *testing.T) {
	claimedAt := time.Date(2026, 11, 23, 22, 35, 24, 277067000, time.UTC)
	svc := &fakeSvc{
		beginClaim: fix.PRClaim{ID: "p7", Repo: "owner/repo", ClaimedAt: claimedAt, Branch: "remediation/rel-7/try1/core"},
		failHit:    true,
	}
	s := grpcadapter.NewFixesServer(svc)

	claim, err := s.BeginPullRequest(context.Background(), &remediationv1.BeginPullRequestRequest{Id: "p7", Project: "core"})
	require.NoError(t, err)
	_, err = s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id: "p7", ClaimedAt: claim.ClaimedAt, Project: "core",
	})
	require.NoError(t, err)
	assert.True(t, claimedAt.Equal(svc.lastFailObserved),
		"FailPullRequest must forward the very instant BeginPullRequest persisted; got %s, want %s",
		svc.lastFailObserved.Format(time.RFC3339Nano), claimedAt.Format(time.RFC3339Nano))
}

// TestFixesServer_FailPullRequest_ThreadsProjectThrough verifies that
// FailPullRequest's project field reaches Service.FailStuckClaim unchanged,
// and that an empty project (the legacy path) stays empty.
func TestFixesServer_FailPullRequest_ThreadsProjectThrough(t *testing.T) {
	claimedAt := time.Date(2026, 6, 24, 9, 0, 0, 0, time.UTC)
	svc := &fakeSvc{failHit: true}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id: "p5", ClaimedAt: claimedAt.Format(time.RFC3339), Project: "core",
	})
	require.NoError(t, err)
	assert.Equal(t, "core", svc.lastFailProject)

	_, err = s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id: "p5", ClaimedAt: claimedAt.Format(time.RFC3339),
	})
	require.NoError(t, err)
	assert.Equal(t, "", svc.lastFailProject, "an empty project must map to \"\" unchanged")
}

// TestFixesServer_FailPullRequest_CASMiss_ReturnsReleasedFalseNoError
// verifies that a CAS miss — the claim already moved on, e.g. released and
// re-claimed by the reconciler's opening sweep between this caller's own
// BeginPullRequest and its failure callback — surfaces as released=false
// with no gRPC error, never as NOT_FOUND or any other error code: a caller
// that lost the race must never be told its own request failed.
func TestFixesServer_FailPullRequest_CASMiss_ReturnsReleasedFalseNoError(t *testing.T) {
	svc := &fakeSvc{failHit: false}
	s := grpcadapter.NewFixesServer(svc)

	resp, err := s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id:        "p5",
		ClaimedAt: time.Now().UTC().Format(time.RFC3339),
	})
	require.NoError(t, err)
	assert.False(t, resp.Released)
}

func TestFixesServer_FailPullRequest_MissingClaimedAt_InvalidArgument(t *testing.T) {
	svc := &fakeSvc{failHit: true}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{Id: "p5"})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestFixesServer_FailPullRequest_UnparseableClaimedAt_InvalidArgument(t *testing.T) {
	svc := &fakeSvc{failHit: true}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id: "p5", ClaimedAt: "not-a-timestamp",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestFixesServer_FailPullRequest_InternalError(t *testing.T) {
	svc := &fakeSvc{failErr: assert.AnError}
	s := grpcadapter.NewFixesServer(svc)

	_, err := s.FailPullRequest(context.Background(), &remediationv1.FailPullRequestRequest{
		Id: "p5", ClaimedAt: time.Now().UTC().Format(time.RFC3339),
	})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// ---- PrOpenedAt non-nil ----

func TestFixesServer_ListFixes_PrOpenedAtNonNil(t *testing.T) {
	ts := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	openedAt := time.Date(2024, 3, 16, 12, 30, 0, 0, time.UTC)
	fakeViews := []fix.View{
		{
			ID:         "p10",
			CreatedAt:  ts,
			PrOpenedAt: &openedAt,
		},
	}

	svc := &fakeSvc{listViews: fakeViews}
	s := grpcadapter.NewFixesServer(svc)

	resp, err := s.ListFixes(context.Background(), &remediationv1.ListFixesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Fixes, 1)
	assert.Equal(t, openedAt.Format(time.RFC3339), resp.Fixes[0].PrOpenedAt)
}
