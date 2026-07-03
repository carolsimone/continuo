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

	"github.com/carolsimone/continuo/pkg/num"
	remediationv1 "github.com/carolsimone/continuo/remediation-agent/api/remediation/v1"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/domain/repository"
	"github.com/carolsimone/continuo/remediation-agent/service/proposals"
)

// ProposalService is the subset of proposals.Service methods consumed by the
// gRPC server. Declaring it here (rather than accepting *proposals.Service
// directly) keeps the adapter testable without a real database: any
// struct implementing these five methods satisfies the interface.
type ProposalService interface {
	List(ctx context.Context, filter repository.ProposalFilter) ([]proposal.View, error)
	Get(ctx context.Context, id string) (proposal.View, error)
	Begin(ctx context.Context, id string) (proposal.PRClaim, error)
	Record(ctx context.Context, in proposals.RecordInput) error
	Fail(ctx context.Context, id string) error
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

// ListProposals returns proposals matching the request filter, ordered by
// created_at DESC. An empty filter returns all stored proposals up to Limit.
func (s *ProposalsServer) ListProposals(ctx context.Context, req *remediationv1.ListProposalsRequest) (*remediationv1.ListProposalsResponse, error) {
	filter := repository.ProposalFilter{
		Status:  req.Status,
		PRState: req.PrState,
		Limit:   int(req.Limit),
	}
	views, err := s.svc.List(ctx, filter)
	if err != nil {
		return nil, toGRPCError(err)
	}
	proto := make([]*remediationv1.Proposal, 0, len(views))
	for i := range views {
		proto = append(proto, viewToProto(views[i]))
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
	return viewToProto(v), nil
}

// BeginPullRequest atomically claims a proposal for PR creation and returns
// the data needed to open the GitHub pull-request. Returns FAILED_PRECONDITION
// when the proposal is already claimed (carrying the existing pr_url in the
// message) or when source resolution has not completed. Returns NOT_FOUND when
// the proposal does not exist.
func (s *ProposalsServer) BeginPullRequest(ctx context.Context, req *remediationv1.BeginPullRequestRequest) (*remediationv1.BeginPullRequestResponse, error) {
	claim, err := s.svc.Begin(ctx, req.Id)
	if err != nil {
		if errors.Is(err, repository.ErrPRConflict) {
			// Attempt a best-effort fetch of the current pr_url so the caller
			// can surface it in the UI without an extra round-trip.
			msg := "proposal PR already claimed"
			if v, getErr := s.svc.Get(ctx, req.Id); getErr == nil && v.PrURL != "" {
				msg = fmt.Sprintf("proposal PR already claimed: existing pr_url=%s", v.PrURL)
			}
			return nil, status.Error(codes.FailedPrecondition, msg)
		}
		return nil, toGRPCError(err)
	}
	return claimToProto(claim), nil
}

// RecordPullRequest stores the PR URL, number, and opener after a PR is
// opened. Returns NOT_FOUND when the proposal does not exist.
func (s *ProposalsServer) RecordPullRequest(ctx context.Context, req *remediationv1.RecordPullRequestRequest) (*remediationv1.RecordPullRequestResponse, error) {
	in := proposals.RecordInput{
		ProposalID: req.Id,
		PrURL:      req.PrUrl,
		PrNumber:   int(req.PrNumber),
		OpenedBy:   req.OpenedBy,
	}
	if err := s.svc.Record(ctx, in); err != nil {
		return nil, toGRPCError(err)
	}
	return &remediationv1.RecordPullRequestResponse{}, nil
}

// FailPullRequest marks a proposal's PR attempt as failed so it can be
// retried. Returns NOT_FOUND when the proposal does not exist.
func (s *ProposalsServer) FailPullRequest(ctx context.Context, req *remediationv1.FailPullRequestRequest) (*remediationv1.FailPullRequestResponse, error) {
	if err := s.svc.Fail(ctx, req.Id); err != nil {
		return nil, toGRPCError(err)
	}
	return &remediationv1.FailPullRequestResponse{}, nil
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
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// viewToProto converts a domain proposal.View to the proto Proposal message.
// Timestamps are formatted as RFC3339 strings; a nil PrOpenedAt produces an
// empty string.
func viewToProto(v proposal.View) *remediationv1.Proposal {
	var prOpenedAt string
	if v.PrOpenedAt != nil {
		prOpenedAt = v.PrOpenedAt.Format(time.RFC3339)
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
	}
}

// claimToProto converts a domain proposal.PRClaim to the BeginPullRequestResponse.
func claimToProto(c proposal.PRClaim) *remediationv1.BeginPullRequestResponse {
	return &remediationv1.BeginPullRequestResponse{
		ProposalId:     c.ID,
		Repo:           c.Repo,
		CommitSha:      c.CommitSHA,
		FilePath:       c.FilePath,
		ProposedSqlUri: c.ProposedSQLURI,
		DiffUri:        c.DiffURI,
		ReleaseId:      c.ReleaseID,
		NodeId:         c.NodeID,
		Attempt:        num.ClampInt32(c.Attempt),
		Rationale:      c.Rationale,
		Confidence:     string(c.Confidence),
		Model:          c.Model,
		Branch:         c.Branch,
	}
}
