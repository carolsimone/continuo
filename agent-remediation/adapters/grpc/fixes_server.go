// Package grpc contains gRPC adapter implementations for clients of other components
// and the RemediationFixes gRPC server.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/fixes"
	"github.com/carolsimone/continuo/pkg/num"
)

// FixService is the subset of fixes.Service methods consumed by the
// gRPC server. Declaring it here (rather than accepting *fixes.Service
// directly) keeps the adapter testable without a real database: any
// struct implementing these methods satisfies the interface.
type FixService interface {
	List(ctx context.Context, filter repository.FixFilter) ([]fix.View, error)
	Get(ctx context.Context, id string) (fix.View, error)
	// Begin claims the (fix, project) pull request for creation; project
	// selects the owning-project group ("" is the legacy whole-fix group).
	Begin(ctx context.Context, id, project string) (fix.PRClaim, error)
	Record(ctx context.Context, in fixes.RecordInput) error
	// FailStuckClaim releases the 'opening' claim identified by (id, project)
	// back to 'failed', but only if the row's current pr_claimed_at still equals
	// observedClaimedAt — the compare-and-set guard that keeps this call from
	// resetting a claim someone else already took over. hit reports whether
	// the CAS fired; false is not an error.
	FailStuckClaim(ctx context.Context, id, project string, observedClaimedAt time.Time) (hit bool, err error)
	// PRProjects returns the owning-project groups v's pull requests split
	// into, sorted; [""] for a legacy (unsplit) fix. It is a pure
	// passthrough over v, requiring no context or lookup.
	PRProjects(v fix.View) []string
}

// Compile-time assertion: *fixes.Service satisfies FixService.
var _ FixService = (*fixes.Service)(nil)

// FixesServer implements remediationv1.RemediationFixesServer by
// delegating to a FixService and mapping domain errors to gRPC status
// codes.
type FixesServer struct {
	remediationv1.UnimplementedRemediationFixesServer
	svc FixService
}

// NewFixesServer returns a FixesServer backed by the given service.
// The concrete *fixes.Service satisfies the FixService interface,
// so callers typically pass svc directly. Tests may pass any fake.
func NewFixesServer(svc FixService) *FixesServer {
	return &FixesServer{svc: svc}
}

// ListFixes returns fixes matching the request filter — status,
// pr_state, release_id, and project — ordered by created_at DESC. An empty
// filter returns all stored fixes up to Limit.
func (s *FixesServer) ListFixes(ctx context.Context, req *remediationv1.ListFixesRequest) (*remediationv1.ListFixesResponse, error) {
	filter := repository.FixFilter{
		Status:    req.Status,
		PRState:   req.PrState,
		ReleaseID: req.ReleaseId,
		Project:   req.Project,
		Limit:     int(req.Limit),
	}
	views, err := s.svc.List(ctx, filter)
	if err != nil {
		return nil, toGRPCError(err)
	}
	proto := make([]*remediationv1.Fix, 0, len(views))
	for i := range views {
		proto = append(proto, viewToProto(views[i], s.svc.PRProjects(views[i])))
	}
	return &remediationv1.ListFixesResponse{Fixes: proto}, nil
}

// GetFix returns a single fix by ID. Returns NOT_FOUND when the
// fix does not exist.
func (s *FixesServer) GetFix(ctx context.Context, req *remediationv1.GetFixRequest) (*remediationv1.Fix, error) {
	v, err := s.svc.Get(ctx, req.Id)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return viewToProto(v, s.svc.PRProjects(v)), nil
}

// BeginPullRequest atomically claims a fix's (or one owning-project
// group's) pull request for creation and returns the data needed to open the
// GitHub pull-request, including claimed_at — the persisted pr_claimed_at for
// this claim in RFC 3339 with its fraction of a second (pr_claimed_at is stored
// to the microsecond), which the caller must present back to FailPullRequest
// to release this exact claim. req.Project selects the owning-project group to
// claim; "" is the legacy whole-fix group. Returns INVALID_ARGUMENT when
// req.Project is not one of the fix's PRProjects. Returns
// FAILED_PRECONDITION when the fix is already claimed (carrying the
// existing pr_url in the message), when source resolution has not completed,
// when the try has not reached 'proposed' — a fix still being generated
// or verified, or one a release check rejected, is not one a pull request
// may be opened for — or when the fix records no repository. Returns
// NOT_FOUND when the fix does not exist.
func (s *FixesServer) BeginPullRequest(ctx context.Context, req *remediationv1.BeginPullRequestRequest) (*remediationv1.BeginPullRequestResponse, error) {
	claim, err := s.svc.Begin(ctx, req.Id, req.Project)
	if err != nil {
		if errors.Is(err, repository.ErrPRConflict) {
			// Try a best-effort fetch of the current pr_url so the caller
			// can surface it in the UI without an extra round-trip. It must be
			// req.Project's own PR: a split fix's other projects can be at
			// any state, so the conflicting project is not necessarily the one
			// whose row the singular fields mirror.
			msg := "fix PR already claimed"
			if v, getErr := s.svc.Get(ctx, req.Id); getErr == nil {
				if url := prURLForProject(v, req.Project); url != "" {
					msg = fmt.Sprintf("fix PR already claimed: existing pr_url=%s", url)
				}
			}
			return nil, status.Error(codes.FailedPrecondition, msg)
		}
		return nil, toGRPCError(err)
	}
	return claimToProto(claim), nil
}

// prURLForProject returns the pr_url of the pull request v records for
// project. "" is the legacy whole-fix group, whose URL is v's singular
// PrURL field (kept for a row with no child PullRequests at all). A named
// project looks up its own entry in v.PullRequests, so a conflict on one
// project can never surface a different project's URL.
func prURLForProject(v fix.View, project string) string {
	if project == "" {
		return v.PrURL
	}
	for _, pr := range v.PullRequests {
		if pr.Project == project {
			return pr.PrURL
		}
	}
	return ""
}

// RecordPullRequest stores the PR URL, number, and opener after a PR is
// opened. req.Project identifies which owning-project group's PR this is;
// "" is the legacy whole-fix group. Returns NOT_FOUND when the fix
// does not exist.
func (s *FixesServer) RecordPullRequest(ctx context.Context, req *remediationv1.RecordPullRequestRequest) (*remediationv1.RecordPullRequestResponse, error) {
	in := fixes.RecordInput{
		FixID:    req.Id,
		Project:  req.Project,
		PrURL:    req.PrUrl,
		PrNumber: int(req.PrNumber),
		OpenedBy: req.OpenedBy,
	}
	if err := s.svc.Record(ctx, in); err != nil {
		return nil, toGRPCError(err)
	}
	return &remediationv1.RecordPullRequestResponse{}, nil
}

// FailPullRequest releases the (req.Id, req.Project) 'opening' claim back to
// 'failed' so it can be retried, but only if the row's current pr_claimed_at
// still equals req.ClaimedAt — the compare-and-set guard that keeps a caller
// from resetting a claim someone else (a re-claim, or the reconciler's
// opening sweep) has already taken over since. req.Project is "" for the
// legacy whole-fix claim. req.ClaimedAt must be the value
// BeginPullRequest returned for this same claim, fraction of a second
// included, or the compare-and-set never matches. Returns INVALID_ARGUMENT
// when claimed_at is missing or not RFC3339. The response's released field is
// false, not an error, when the CAS did not fire because the claim had
// already moved on.
func (s *FixesServer) FailPullRequest(ctx context.Context, req *remediationv1.FailPullRequestRequest) (*remediationv1.FailPullRequestResponse, error) {
	if req.ClaimedAt == "" {
		return nil, status.Error(codes.InvalidArgument, "claimed_at is required")
	}
	claimedAt, err := time.Parse(time.RFC3339, req.ClaimedAt)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid claimed_at: %v", err)
	}
	hit, err := s.svc.FailStuckClaim(ctx, req.Id, req.Project, claimedAt)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &remediationv1.FailPullRequestResponse{Released: hit}, nil
}

// toGRPCError converts domain sentinel errors to the appropriate gRPC status
// codes. Unknown errors become codes.Internal.
func toGRPCError(err error) error {
	switch {
	case errors.Is(err, repository.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, repository.ErrPRConflict):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, repository.ErrNotSourceResolved):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, repository.ErrNotProposed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, fixes.ErrNoRepository):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, fixes.ErrUnknownProject):
		// The caller passed a project argument that names none of this
		// fix's PRProjects — a malformed request, not a missing
		// resource or a state conflict.
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// editsToProto converts a slice of domain FileEdit values to the proto
// representation. A nil or empty input produces a nil slice rather than a
// list containing a zero-valued element.
func editsToProto(edits []fix.FileEdit) []*remediationv1.FileEdit {
	if len(edits) == 0 {
		return nil
	}
	out := make([]*remediationv1.FileEdit, 0, len(edits))
	for _, e := range edits {
		out = append(out, &remediationv1.FileEdit{
			Path:         e.Path,
			ContentUri:   e.ContentURI,
			DiffUri:      e.DiffURI,
			TargetNodeId: e.TargetNodeID,
			RenamedFrom:  e.RenamedFrom,
		})
	}
	return out
}

// nodeOutcomesToProto converts the per-node outcome map to its proto
// representation. A nil or empty input produces a nil map rather than an
// empty non-nil one.
func nodeOutcomesToProto(outcomes map[string]fix.NodeOutcome) map[string]*remediationv1.NodeOutcome {
	if len(outcomes) == 0 {
		return nil
	}
	out := make(map[string]*remediationv1.NodeOutcome, len(outcomes))
	for nodeID, o := range outcomes {
		out[nodeID] = &remediationv1.NodeOutcome{
			Status: string(o.Status),
			Reason: o.Reason,
			Code:   string(o.Code),
		}
	}
	return out
}

// approversToProto converts a slice of domain Approver values to the proto
// representation. A nil or empty input produces a nil slice rather than a list
// containing a zero-valued element.
func approversToProto(approvers []fix.Approver) []*remediationv1.Approver {
	if len(approvers) == 0 {
		return nil
	}
	out := make([]*remediationv1.Approver, 0, len(approvers))
	for _, a := range approvers {
		out = append(out, &remediationv1.Approver{NodeId: a.NodeID, Owner: a.Owner, Source: string(a.Source)})
	}
	return out
}

// releaseChecksToProto converts a slice of domain ReleaseCheck values to the
// proto representation. A nil or empty input produces a nil slice rather
// than a list containing a zero-valued element. ActivatedAt is formatted as
// RFC3339, or "" when nil.
func releaseChecksToProto(releaseChecks []fix.ReleaseCheck) []*remediationv1.ReleaseCheck {
	if len(releaseChecks) == 0 {
		return nil
	}
	out := make([]*remediationv1.ReleaseCheck, 0, len(releaseChecks))
	for _, v := range releaseChecks {
		var activatedAt string
		if v.ActivatedAt != nil {
			activatedAt = v.ActivatedAt.Format(time.RFC3339)
		}
		out = append(out, &remediationv1.ReleaseCheck{
			Project:     v.Project,
			Kind:        v.Kind,
			CheckId:     v.CheckID,
			Phase:       string(v.Phase),
			ActivatedAt: activatedAt,
			Error:       v.Error,
		})
	}
	return out
}

// pullRequestsToProto converts a slice of domain fix.PullRequest values
// to the proto representation. A nil or empty input produces a nil slice
// rather than a list containing a zero-valued element — a fix that never
// entered the PR lifecycle reaches this path.
func pullRequestsToProto(prs []fix.PullRequest) []*remediationv1.PullRequest {
	if len(prs) == 0 {
		return nil
	}
	out := make([]*remediationv1.PullRequest, 0, len(prs))
	for _, p := range prs {
		var openedAt string
		if p.PrOpenedAt != nil {
			openedAt = p.PrOpenedAt.Format(time.RFC3339)
		}
		var closedAt string
		if p.PrClosedAt != nil {
			closedAt = p.PrClosedAt.Format(time.RFC3339)
		}
		out = append(out, &remediationv1.PullRequest{
			Project:    p.Project,
			Repo:       p.Repo,
			Branch:     p.Branch,
			PrUrl:      p.PrURL,
			PrNumber:   num.ClampInt32(p.PrNumber),
			PrState:    p.PrState,
			PrOpenedAt: openedAt,
			PrOpenedBy: p.PrOpenedBy,
			PrClosedAt: closedAt,
		})
	}
	return out
}

// viewToProto converts a domain fix.View to the proto Fix message.
// Timestamps are formatted as RFC3339 strings; a nil PrOpenedAt produces an
// empty string. pull_requests is mapped from v.PullRequests; prProjects — the
// caller's s.svc.PRProjects(v) result — becomes pr_projects verbatim. The
// singular pr_* fields above are already the first child row's view (the
// repository's applyChildPRs overrides them from PullRequests[0], ordered by
// project), so they need no extra mapping here.
func viewToProto(v fix.View, prProjects []string) *remediationv1.Fix {
	var prOpenedAt string
	if v.PrOpenedAt != nil {
		prOpenedAt = v.PrOpenedAt.Format(time.RFC3339)
	}
	var prClosedAt string
	if v.PrClosedAt != nil {
		prClosedAt = v.PrClosedAt.Format(time.RFC3339)
	}
	return &remediationv1.Fix{
		Id:                  v.ID,
		Source:              v.Source,
		ReleaseId:           v.ReleaseID,
		NodeId:              v.NodeID,
		ErrorSignature:      v.ErrorSignature,
		Try:                 num.ClampInt32(v.Try),
		Status:              string(v.Status),
		Confidence:          string(v.Confidence),
		Rationale:           v.Rationale,
		ProposedSqlUri:      v.ProposedSQLURI,
		DiffUri:             v.DiffURI,
		CandidateFixSqlUri:  v.CandidateFixSQLURI,
		CandidateFixDiffUri: v.CandidateFixDiffURI,
		SourceResolved:      v.SourceResolved,
		Repo:                v.Repo,
		CommitSha:           v.CommitSHA,
		FilePath:            v.FilePath,
		Model:               v.Model,
		CreatedAt:           v.CreatedAt.Format(time.RFC3339),
		PrUrl:               v.PrURL,
		PrNumber:            num.ClampInt32(v.PrNumber),
		PrState:             v.PrState,
		PrOpenedAt:          prOpenedAt,
		PrOpenedBy:          v.PrOpenedBy,
		PrClosedAt:          prClosedAt,
		Edits:               editsToProto(v.Edits),
		ReleaseCheckId:      v.ReleaseCheckID,
		VerifyError:         v.VerifyError,
		RemediationRound:    num.ClampInt32(v.RemediationRound),
		ResolvedNodeIds:     v.ResolvedNodeIDs,
		NodeOutcomes:        nodeOutcomesToProto(v.NodeOutcomes),
		ReleaseChecks:       releaseChecksToProto(v.ReleaseChecks),
		PullRequests:        pullRequestsToProto(v.PullRequests),
		PrProjects:          prProjects,
		Projects:            v.Projects,
		Approvers:           approversToProto(v.Approvers),
	}
}

// claimToProto converts a domain fix.PRClaim to the BeginPullRequestResponse.
func claimToProto(c fix.PRClaim) *remediationv1.BeginPullRequestResponse {
	return &remediationv1.BeginPullRequestResponse{
		FixId:            c.ID,
		Repo:             c.Repo,
		CommitSha:        c.CommitSHA,
		FilePath:         c.FilePath,
		ProposedSqlUri:   c.ProposedSQLURI,
		DiffUri:          c.DiffURI,
		ReleaseId:        c.ReleaseID,
		NodeId:           c.NodeID,
		RemediationRound: num.ClampInt32(c.RemediationRound),
		Try:              num.ClampInt32(c.Try),
		Rationale:        c.Rationale,
		Confidence:       string(c.Confidence),
		Model:            c.Model,
		Branch:           c.Branch,
		ClaimedAt:        c.ClaimedAt.Format(time.RFC3339Nano),
		Edits:            editsToProto(c.Edits),
		ResolvedNodeIds:  c.ResolvedNodeIDs,
		Project:          c.Project,
		Approvers:        approversToProto(c.Approvers),
	}
}
