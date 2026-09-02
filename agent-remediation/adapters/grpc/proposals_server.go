// Package grpc contains gRPC adapter implementations for external service clients
// and the RemediationProposals gRPC server.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/proposals"
	"github.com/carolsimone/continuo/pkg/num"
)

// ProposalService is the subset of proposals.Service methods consumed by the
// gRPC server. Declaring it here (rather than accepting *proposals.Service
// directly) keeps the adapter testable without a real database: any
// struct implementing these methods satisfies the interface.
type ProposalService interface {
	List(ctx context.Context, filter repository.ProposalFilter) ([]proposal.View, error)
	Get(ctx context.Context, id string) (proposal.View, error)
	// Begin claims the (proposal, service) pull request for creation; service
	// selects the owning-service group ("" is the legacy whole-proposal group).
	Begin(ctx context.Context, id, service string) (proposal.PRClaim, error)
	Record(ctx context.Context, in proposals.RecordInput) error
	// FailStuckClaim releases the 'opening' claim identified by (id, service)
	// back to 'failed', but only if the row's current pr_claimed_at still equals
	// observedClaimedAt — the compare-and-set guard that keeps this call from
	// resetting a claim someone else already took over. hit reports whether
	// the CAS fired; false is not an error.
	FailStuckClaim(ctx context.Context, id, service string, observedClaimedAt time.Time) (hit bool, err error)
	// PRServices returns the owning-service groups v's pull requests split
	// into, sorted; [""] for a legacy (unsplit) proposal. It is a pure
	// passthrough over v, requiring no context or lookup.
	PRServices(v proposal.View) []string
}

// Compile-time assertion: *proposals.Service satisfies ProposalService.
var _ ProposalService = (*proposals.Service)(nil)

// ProposalsServer implements remediationv1.RemediationProposalsServer by
// delegating to a ProposalService and mapping domain errors to gRPC status
// codes.
type ProposalsServer struct {
	remediationv1.UnimplementedRemediationProposalsServer
	svc ProposalService
}

// NewProposalsServer returns a ProposalsServer backed by the given service.
// The concrete *proposals.Service satisfies the ProposalService interface,
// so callers typically pass svc directly. Tests may pass any fake.
func NewProposalsServer(svc ProposalService) *ProposalsServer {
	return &ProposalsServer{svc: svc}
}

// ListProposals returns proposals matching the request filter — status,
// pr_state, and release_id — ordered by created_at DESC. An empty filter
// returns all stored proposals up to Limit.
func (s *ProposalsServer) ListProposals(ctx context.Context, req *remediationv1.ListProposalsRequest) (*remediationv1.ListProposalsResponse, error) {
	filter := repository.ProposalFilter{
		Status:    req.Status,
		PRState:   req.PrState,
		ReleaseID: req.ReleaseId,
		Service:   req.Service,
		Limit:     int(req.Limit),
	}
	views, err := s.svc.List(ctx, filter)
	if err != nil {
		return nil, toGRPCError(err)
	}
	proto := make([]*remediationv1.Proposal, 0, len(views))
	for i := range views {
		proto = append(proto, viewToProto(views[i], s.svc.PRServices(views[i])))
	}
	return &remediationv1.ListProposalsResponse{Proposals: proto}, nil
}

// GetProposal returns a single proposal by ID. Returns NOT_FOUND when the
// proposal does not exist.
func (s *ProposalsServer) GetProposal(ctx context.Context, req *remediationv1.GetProposalRequest) (*remediationv1.Proposal, error) {
	v, err := s.svc.Get(ctx, req.Id)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return viewToProto(v, s.svc.PRServices(v)), nil
}

// BeginPullRequest atomically claims a proposal's (or one owning-service
// group's) pull request for creation and returns the data needed to open the
// GitHub pull-request, including claimed_at — the persisted pr_claimed_at for
// this claim, which the caller must present back to FailPullRequest to
// release this exact claim. req.Service selects the owning-service group to
// claim; "" is the legacy whole-proposal group. Returns INVALID_ARGUMENT when
// req.Service is not one of the proposal's PRServices. Returns
// FAILED_PRECONDITION when the proposal is already claimed (carrying the
// existing pr_url in the message), when source resolution has not completed,
// or when the attempt has not reached 'proposed' — a fix still being
// generated or verified, or one a verification run rejected, is not one a
// pull request may be opened for. Returns NOT_FOUND when the proposal does not
// exist.
func (s *ProposalsServer) BeginPullRequest(ctx context.Context, req *remediationv1.BeginPullRequestRequest) (*remediationv1.BeginPullRequestResponse, error) {
	claim, err := s.svc.Begin(ctx, req.Id, req.Service)
	if err != nil {
		if errors.Is(err, repository.ErrPRConflict) {
			// Attempt a best-effort fetch of the current pr_url so the caller
			// can surface it in the UI without an extra round-trip. It must be
			// req.Service's own PR: a split proposal's other services can be at
			// any state, so the conflicting service is not necessarily the one
			// whose row the singular fields mirror.
			msg := "proposal PR already claimed"
			if v, getErr := s.svc.Get(ctx, req.Id); getErr == nil {
				if url := prURLForService(v, req.Service); url != "" {
					msg = fmt.Sprintf("proposal PR already claimed: existing pr_url=%s", url)
				}
			}
			return nil, status.Error(codes.FailedPrecondition, msg)
		}
		return nil, toGRPCError(err)
	}
	return claimToProto(claim), nil
}

// prURLForService returns the pr_url of the pull request v records for
// service. "" is the legacy whole-proposal group, whose URL is v's singular
// PrURL field (kept for a row with no child PullRequests at all). A named
// service looks up its own entry in v.PullRequests, so a conflict on one
// service can never surface a different service's URL.
func prURLForService(v proposal.View, service string) string {
	if service == "" {
		return v.PrURL
	}
	for _, pr := range v.PullRequests {
		if pr.Service == service {
			return pr.PrURL
		}
	}
	return ""
}

// RecordPullRequest stores the PR URL, number, and opener after a PR is
// opened. req.Service identifies which owning-service group's PR this is;
// "" is the legacy whole-proposal group. Returns NOT_FOUND when the proposal
// does not exist.
func (s *ProposalsServer) RecordPullRequest(ctx context.Context, req *remediationv1.RecordPullRequestRequest) (*remediationv1.RecordPullRequestResponse, error) {
	in := proposals.RecordInput{
		ProposalID: req.Id,
		Service:    req.Service,
		PrURL:      req.PrUrl,
		PrNumber:   int(req.PrNumber),
		OpenedBy:   req.OpenedBy,
	}
	if err := s.svc.Record(ctx, in); err != nil {
		return nil, toGRPCError(err)
	}
	return &remediationv1.RecordPullRequestResponse{}, nil
}

// FailPullRequest releases the (req.Id, req.Service) 'opening' claim back to
// 'failed' so it can be retried, but only if the row's current pr_claimed_at
// still equals req.ClaimedAt — the compare-and-set guard that keeps a caller
// from resetting a claim someone else (a re-claim, or the reconciler's
// opening sweep) has already taken over since. req.Service is "" for the
// legacy whole-proposal claim. req.ClaimedAt must be the value
// BeginPullRequest returned for this same claim. Returns INVALID_ARGUMENT
// when claimed_at is missing or not RFC3339. The response's released field is
// false, not an error, when the CAS did not fire because the claim had
// already moved on.
func (s *ProposalsServer) FailPullRequest(ctx context.Context, req *remediationv1.FailPullRequestRequest) (*remediationv1.FailPullRequestResponse, error) {
	if req.ClaimedAt == "" {
		return nil, status.Error(codes.InvalidArgument, "claimed_at is required")
	}
	claimedAt, err := time.Parse(time.RFC3339, req.ClaimedAt)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid claimed_at: %v", err)
	}
	hit, err := s.svc.FailStuckClaim(ctx, req.Id, req.Service, claimedAt)
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
	case errors.Is(err, proposals.ErrUnknownService):
		// The caller passed a service argument that names none of this
		// proposal's PRServices — a malformed request, not a missing
		// resource or a state conflict.
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// editsToProto converts a slice of domain FileEdit values to the proto
// representation. A nil or empty input produces a nil slice rather than a
// list containing a zero-valued element.
func editsToProto(edits []proposal.FileEdit) []*remediationv1.FileEdit {
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
		})
	}
	return out
}

// nodeOutcomesToProto converts the per-node outcome map to its proto
// representation. A nil or empty input produces a nil map rather than an
// empty non-nil one.
func nodeOutcomesToProto(outcomes map[string]proposal.NodeOutcome) map[string]*remediationv1.NodeOutcome {
	if len(outcomes) == 0 {
		return nil
	}
	out := make(map[string]*remediationv1.NodeOutcome, len(outcomes))
	for nodeID, o := range outcomes {
		out[nodeID] = &remediationv1.NodeOutcome{
			Status: string(o.Status),
			Reason: o.Reason,
		}
	}
	return out
}

// verificationsToProto converts a slice of domain Verification values to the
// proto representation. A nil or empty input produces a nil slice rather
// than a list containing a zero-valued element. ActivatedAt is formatted as
// RFC3339, or "" when nil.
func verificationsToProto(verifications []proposal.Verification) []*remediationv1.Verification {
	if len(verifications) == 0 {
		return nil
	}
	out := make([]*remediationv1.Verification, 0, len(verifications))
	for _, v := range verifications {
		var activatedAt string
		if v.ActivatedAt != nil {
			activatedAt = v.ActivatedAt.Format(time.RFC3339)
		}
		out = append(out, &remediationv1.Verification{
			Service:     v.Service,
			Kind:        v.Kind,
			RunId:       v.RunID,
			Phase:       string(v.Phase),
			ActivatedAt: activatedAt,
			Error:       v.Error,
		})
	}
	return out
}

// pullRequestsToProto converts a slice of domain proposal.PullRequest values
// to the proto representation. A nil or empty input produces a nil slice
// rather than a list containing a zero-valued element — a proposal that never
// entered the PR lifecycle reaches this path.
func pullRequestsToProto(prs []proposal.PullRequest) []*remediationv1.PullRequest {
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
			Service:    p.Service,
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

// viewToProto converts a domain proposal.View to the proto Proposal message.
// Timestamps are formatted as RFC3339 strings; a nil PrOpenedAt produces an
// empty string. pull_requests is mapped from v.PullRequests; prServices — the
// caller's s.svc.PRServices(v) result — becomes pr_services verbatim. The
// singular pr_* fields above are already the first child row's view (the
// repository's applyChildPRs overrides them from PullRequests[0], ordered by
// service), so they need no extra mapping here.
func viewToProto(v proposal.View, prServices []string) *remediationv1.Proposal {
	var prOpenedAt string
	if v.PrOpenedAt != nil {
		prOpenedAt = v.PrOpenedAt.Format(time.RFC3339)
	}
	var prClosedAt string
	if v.PrClosedAt != nil {
		prClosedAt = v.PrClosedAt.Format(time.RFC3339)
	}
	return &remediationv1.Proposal{
		Id:                  v.ID,
		Source:              v.Source,
		ReleaseId:           v.ReleaseID,
		NodeId:              v.NodeID,
		ErrorSignature:      v.ErrorSignature,
		Attempt:             num.ClampInt32(v.Attempt),
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
		VerificationRunId:   v.VerificationRunID,
		VerifyError:         v.VerifyError,
		RemediationRound:    num.ClampInt32(v.RemediationRound),
		ResolvedNodeIds:     v.ResolvedNodeIDs,
		NodeOutcomes:        nodeOutcomesToProto(v.NodeOutcomes),
		Verifications:       verificationsToProto(v.Verifications),
		PullRequests:        pullRequestsToProto(v.PullRequests),
		PrServices:          prServices,
	}
}

// claimToProto converts a domain proposal.PRClaim to the BeginPullRequestResponse.
func claimToProto(c proposal.PRClaim) *remediationv1.BeginPullRequestResponse {
	return &remediationv1.BeginPullRequestResponse{
		ProposalId:      c.ID,
		Repo:            c.Repo,
		CommitSha:       c.CommitSHA,
		FilePath:        c.FilePath,
		ProposedSqlUri:  c.ProposedSQLURI,
		DiffUri:         c.DiffURI,
		ReleaseId:       c.ReleaseID,
		NodeId:          c.NodeID,
		Attempt:         num.ClampInt32(c.Attempt),
		Rationale:       c.Rationale,
		Confidence:      string(c.Confidence),
		Model:           c.Model,
		Branch:          c.Branch,
		ClaimedAt:       c.ClaimedAt.Format(time.RFC3339),
		Edits:           editsToProto(c.Edits),
		ResolvedNodeIds: c.ResolvedNodeIDs,
		Service:         c.Service,
	}
}
