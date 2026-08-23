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
	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// RecordInput carries the data required to record a successfully opened PR.
type RecordInput struct {
	ProposalID string
	PrURL      string
	PrNumber   int
	OpenedBy   string
	// OpenedAt is the true moment the PR was created, when the caller knows
	// it — e.g. GitHub's own created_at for a PR the opening sweep recovers,
	// which can predate the recovery pass by minutes or hours. Zero means
	// "unknown": Record falls back to the current clock time, which is what
	// the normal client-side flow uses, since it has no better value (the PR
	// was just created by that same call).
	OpenedAt time.Time
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
	branch := BuildBranch(v.ReleaseID, v.NodeID, v.Attempt)
	claim, err := s.repo.BeginPR(ctx, id, branch, s.clock.Now())
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
// RecordPR's CAS guard makes this method itself idempotent: two callers can
// race to record the same claim — the ui PR-creation route and the
// reconciler's opening sweep, when the sweep finds a PR on GitHub for a claim
// it read as stuck before the route's own recording call lands — and only
// the first to reach the row writes anything or emits the event; the second
// is a no-op, not an error.
func (s *Service) Record(ctx context.Context, in RecordInput) error {
	// Fetch the proposal to obtain release_id, node_id, and attempt — required
	// for deterministic outbox entry id and event payload construction.
	v, err := s.repo.Get(ctx, in.ProposalID)
	if err != nil {
		return fmt.Errorf("get proposal: %w", err)
	}

	now := s.clock.Now()
	openedAt := in.OpenedAt
	if openedAt.IsZero() {
		openedAt = now
	}

	u := s.newUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	hit, err := u.ProposalRepo().RecordPR(ctx, in.ProposalID, in.PrURL, in.PrNumber, in.OpenedBy, openedAt)
	if err != nil {
		return fmt.Errorf("record pr: %w", err)
	}
	if !hit {
		return nil
	}

	if err := s.enqueuePROpened(ctx, u, v, in, now, openedAt); err != nil {
		return err
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// FailStuckClaim releases a stuck 'opening' claim back to 'failed', but only
// if the row's pr_claimed_at still matches observedClaimedAt — the compare-
// and-set guard that lets a caller release exactly the claim it itself
// acquired or observed, never a fresher one taken by someone else since. Two
// callers use this: the ui PR-creation route, immediately after its
// own Begin call in the same request, when a downstream S3 or GitHub step
// fails — passing the ClaimedAt that Begin returned; and the reconciler's
// opening sweep, passing the ClaimedAt it read while listing stuck claims
// earlier in the same pass. A mismatch (the claim was released and
// re-claimed between the caller's own claim/observation and this call) is
// reported via hit=false rather than an error: neither caller may ever
// overwrite a claim it does not currently hold.
func (s *Service) FailStuckClaim(ctx context.Context, id string, observedClaimedAt time.Time) (bool, error) {
	return s.repo.FailStuckOpeningPR(ctx, id, observedClaimedAt)
}

// RecordOutcome mirrors a terminal PR outcome observed on GitHub onto the
// proposal inside a single transaction: the pr_state CAS 'open' -> outcome and
// the remediation.pr_closed:v1 outbox entry commit together. A CAS miss (the
// row is no longer 'open') is an idempotent no-op: nothing is written and no
// event is emitted, so repeated observers of the same fact converge safely.
func (s *Service) RecordOutcome(ctx context.Context, id string, outcome proposal.PROutcome, closedAt time.Time) error {
	v, err := s.repo.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("get proposal: %w", err)
	}

	u := s.newUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	transitioned, err := u.ProposalRepo().RecordPROutcome(ctx, id, outcome, closedAt)
	if err != nil {
		return fmt.Errorf("record pr outcome: %w", err)
	}
	if !transitioned {
		return nil
	}

	if err := s.enqueuePRClosed(ctx, u, v, outcome, closedAt); err != nil {
		return err
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// enqueuePRClosed builds the deterministic remediation.pr_closed:v1 outbox
// entry and creates it on the repository bound to the caller's transaction.
func (s *Service) enqueuePRClosed(ctx context.Context, u uow.UnitOfWork, v proposal.View, outcome proposal.PROutcome, closedAt time.Time) error {
	eventID := event.PRClosedEventID(v.ReleaseID, v.NodeID, v.Attempt)
	payload := event.PRClosed{
		ProposalID: v.ID,
		ReleaseID:  v.ReleaseID,
		NodeID:     v.NodeID,
		PrURL:      v.PrURL,
		PrNumber:   v.PrNumber,
		Outcome:    string(outcome),
		ClosedAt:   closedAt.Format(time.RFC3339),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal pr_closed event: %w", err)
	}
	entry := &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(eventID.String())),
		AggregateType: "remediation_agent",
		AggregateID:   event.AggregateIDForRelease(v.ReleaseID),
		EventType:     event.PRClosedEventType,
		Payload:       body,
		StreamName:    streams.RemediationPrClosedV1,
		Status:        "pending",
		MaxRetries:    outbox.DefaultMaxRetries,
		CreatedAt:     s.clock.Now(),
	}
	return u.OutboxRepo().Create(ctx, entry)
}

// enqueuePROpened builds the deterministic remediation.pr_opened:v1 outbox
// entry and creates it on the repository bound to the caller's transaction.
// now is the outbox row's own bookkeeping timestamp; openedAt is the PR's
// actual creation time, which the two can legitimately differ on — recovering
// a stranded PR through the opening sweep resolves it long after GitHub
// created it.
func (s *Service) enqueuePROpened(ctx context.Context, u uow.UnitOfWork, v proposal.View, in RecordInput, now, openedAt time.Time) error {
	eventID := event.PROpenedEventID(v.ReleaseID, v.NodeID, v.Attempt)
	payload := event.PROpened{
		ProposalID: in.ProposalID,
		ReleaseID:  v.ReleaseID,
		NodeID:     v.NodeID,
		PrURL:      in.PrURL,
		PrNumber:   in.PrNumber,
		OpenedBy:   in.OpenedBy,
		OpenedAt:   openedAt.Format("2006-01-02T15:04:05Z07:00"),
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

// BuildBranch returns the deterministic remediation branch name for a
// proposal's release/node/attempt: remediation/<release_id>/<node_sanitized>-attempt<n>.
// Both Begin (computing the branch to claim) and the reconciler's opening
// sweep (recomputing the same branch to look a stuck claim up on GitHub) call
// this so the two never drift apart.
func BuildBranch(releaseID, nodeID string, attempt int) string {
	return fmt.Sprintf("remediation/%s/%s-attempt%d", releaseID, sanitizeBranchSegment(nodeID), attempt)
}

// sanitizeBranchSegment replaces every rune that is not in [A-Za-z0-9_-] with
// '-', so a node id like "model.p.orders_d" becomes "model-p-orders_d".
func sanitizeBranchSegment(s string) string {
	b := make([]byte, 0, len(s))
	for _, r := range s {
		if r > unicode.MaxASCII || (!isAlphaNum(r) && r != '_' && r != '-') {
			b = append(b, '-')
		} else {
			// Safe: the branch above already excludes any r > unicode.MaxASCII
			// (127), so r fits in a byte without truncation.
			b = append(b, byte(r)) //nolint:gosec // G115: r <= unicode.MaxASCII is guaranteed above.
		}
	}
	return string(b)
}

func isAlphaNum(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
}
