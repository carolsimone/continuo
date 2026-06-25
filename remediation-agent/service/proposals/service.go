// Package proposals holds the PR-lifecycle application service. It orchestrates
// the proposal repository and outbox to manage the full lifecycle from proposal
// creation through PR open/fail.
package proposals

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation-agent/domain/event"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/domain/repository"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
	"github.com/carolsimone/continuo/remediation-agent/service/uow"
)

// RecordInput carries the data required to record a successfully opened PR.
type RecordInput struct {
	ProposalID string
	PrURL      string
	PrNumber   int
	OpenedBy   string
}

// Deps holds every collaborator the Service needs, all behind ports or
// interfaces so no adapter or infrastructure package is imported.
type Deps struct {
	Repo   repository.ProposalRepository
	NewUoW func() uow.UnitOfWork
	Clock  ports.Clock
}

// Service is the PR-lifecycle application service. It coordinates proposal
// reads, the PR state machine (claim → record / fail), and outbox emission.
type Service struct {
	repo   repository.ProposalRepository
	newUoW func() uow.UnitOfWork
	clock  ports.Clock
}

// New constructs a Service from the provided Deps.
func New(d Deps) *Service {
	return &Service{
		repo:   d.Repo,
		newUoW: d.NewUoW,
		clock:  d.Clock,
	}
}

// List returns proposals matching filter, ordered by created_at DESC.
func (s *Service) List(ctx context.Context, filter repository.ProposalFilter) ([]proposal.View, error) {
	return s.repo.List(ctx, filter)
}

// Get returns the full View for a single proposal by id.
func (s *Service) Get(ctx context.Context, id string) (proposal.View, error) {
	return s.repo.Get(ctx, id)
}

// Begin atomically claims a proposal for PR creation and returns the data
// needed to open the GitHub pull-request. It builds the deterministic branch
// name remediation/<release_id>/<node_sanitized>-attempt<n> before delegating
// to repo.BeginPR. The returned PRClaim carries the computed Branch field set.
func (s *Service) Begin(ctx context.Context, id string) (proposal.PRClaim, error) {
	v, err := s.repo.Get(ctx, id)
	if err != nil {
		return proposal.PRClaim{}, fmt.Errorf("get proposal: %w", err)
	}
	branch := fmt.Sprintf("remediation/%s/%s-attempt%d",
		v.ReleaseID,
		sanitizeBranchSegment(v.NodeID),
		v.Attempt,
	)
	claim, err := s.repo.BeginPR(ctx, id, branch)
	if err != nil {
		return proposal.PRClaim{}, fmt.Errorf("begin pr: %w", err)
	}
	claim.Branch = branch
	return claim, nil
}

// Record records a successfully opened PR inside a single transaction: it
// calls RecordPR on the proposal row and creates a remediation.pr_opened:v1
// outbox entry atomically. The outbox entry ID is deterministic so a
// re-emission of the same PR-opened fact dedups to one downstream event.
func (s *Service) Record(ctx context.Context, in RecordInput) error {
	// Fetch the proposal to obtain release_id, node_id, and attempt — required
	// for deterministic outbox entry id and event payload construction.
	v, err := s.repo.Get(ctx, in.ProposalID)
	if err != nil {
		return fmt.Errorf("get proposal: %w", err)
	}

	now := s.clock.Now()

	u := s.newUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	if err := u.ProposalRepo().RecordPR(ctx, in.ProposalID, in.PrURL, in.PrNumber, in.OpenedBy, now); err != nil {
		return fmt.Errorf("record pr: %w", err)
	}

	if err := s.enqueuePROpened(ctx, u, v, in, now); err != nil {
		return err
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Fail resets a stuck 'opening' claim back to 'failed' so the action can be
// retried. It delegates directly to the non-transactional repository.
func (s *Service) Fail(ctx context.Context, id string) error {
	return s.repo.FailPR(ctx, id)
}

// enqueuePROpened builds the deterministic remediation.pr_opened:v1 outbox
// entry and creates it on the repository bound to the caller's transaction.
func (s *Service) enqueuePROpened(ctx context.Context, u uow.UnitOfWork, v proposal.View, in RecordInput, now time.Time) error {
	eventID := event.PROpenedEventID(v.ReleaseID, v.NodeID, v.Attempt)
	payload := event.PROpened{
		ProposalID: in.ProposalID,
		ReleaseID:  v.ReleaseID,
		NodeID:     v.NodeID,
		PrURL:      in.PrURL,
		PrNumber:   in.PrNumber,
		OpenedBy:   in.OpenedBy,
		OpenedAt:   now.Format("2006-01-02T15:04:05Z07:00"),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal pr_opened event: %w", err)
	}
	entry := &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(eventID.String())),
		AggregateType: "remediation_agent",
		AggregateID:   event.AggregateIDForRelease(v.ReleaseID),
		EventType:     event.PROpenedEventType,
		Payload:       body,
		StreamName:    streams.RemediationPrOpenedV1,
		Status:        "pending",
		MaxRetries:    outbox.DefaultMaxRetries,
		CreatedAt:     now,
	}
	return u.OutboxRepo().Create(ctx, entry)
}

// sanitizeBranchSegment replaces every rune that is not in [A-Za-z0-9_-] with
// '-', so a node id like "model.p.orders_d" becomes "model-p-orders_d".
func sanitizeBranchSegment(s string) string {
	b := make([]byte, 0, len(s))
	for _, r := range s {
		if r > unicode.MaxASCII || (!isAlphaNum(r) && r != '_' && r != '-') {
			b = append(b, '-')
		} else {
			b = append(b, byte(r))
		}
	}
	return string(b)
}

func isAlphaNum(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
}
