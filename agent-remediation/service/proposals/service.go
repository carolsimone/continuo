// Package proposals holds the PR-lifecycle application service. It orchestrates
// the proposal repository and outbox to manage the full lifecycle from proposal
// creation through PR open/fail.
package proposals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

// ErrUnknownService is returned by Begin when the requested owning-service group
// is not one of the proposal's — a split proposal can only be claimed on a
// service its edits actually attribute members to, and a legacy (unsplit)
// proposal only on the "" whole-proposal group.
var ErrUnknownService = errors.New("no edits for that service")

// RecordInput carries the data required to record a successfully opened PR.
type RecordInput struct {
	ProposalID string
	// Service is the owning-service group this PR covers; "" is the legacy
	// whole-proposal PR.
	Service  string
	PrURL    string
	PrNumber int
	OpenedBy string
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
	// ServiceRepoPaths maps a dbt service_name to its project root within the
	// repository. It is how the service splits a proposal's edits by owning
	// service (PRServices) and narrows a per-service PR's resolved node set
	// (resolvedForService). An empty map means every edit lands under the
	// legacy "" group — one PR for the whole proposal.
	ServiceRepoPaths map[string]string
}

// Service is the PR-lifecycle application service. It coordinates proposal
// reads, the PR state machine (claim → record / fail), and outbox emission.
type Service struct {
	repo             repository.ProposalRepository
	newUoW           func() uow.UnitOfWork
	clock            ports.Clock
	serviceRepoPaths map[string]string
}

// New constructs a Service from the provided Deps.
func New(d Deps) *Service {
	return &Service{
		repo:             d.Repo,
		newUoW:           d.NewUoW,
		clock:            d.Clock,
		serviceRepoPaths: d.ServiceRepoPaths,
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

// PRServices returns the owning-service groups a proposal's pull requests split
// into, sorted. A proposal whose edits carry NO cluster members — one written
// before the per-service split — is never split: it returns the single legacy
// [""] group, so its per-service claim path is never entered and it keeps one
// pull request for the whole proposal. Once at least one edit attributes
// members, the groups are the sorted keys of GroupEditsByService, so each
// owning service gets its own pull request.
func (s *Service) PRServices(v proposal.View) []string {
	hasMembers := false
	for _, e := range v.Edits {
		if len(e.MemberNodeIDs) > 0 {
			hasMembers = true
			break
		}
	}
	if !hasMembers {
		return []string{""}
	}
	groups := proposal.GroupEditsByService(s.serviceRepoPaths, v.Edits)
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// resolvedForService is the failing-node set a per-service pull request names:
// for a legacy "" claim it is the attempt's whole fixed set; for a real service
// it is the members that service's edits attribute, intersected with the
// attempt's fixed set (nil fallback, so an edit written before the member codec
// never lets one service claim nodes it did not touch). Both enqueuePROpened
// and enqueuePRClosed derive the resolved set through here, so pr_opened and
// pr_closed name the same nodes as the claim the repository handed the caller.
func (s *Service) resolvedForService(v proposal.View, service string) []string {
	if service == "" {
		return v.FixedNodeIDs()
	}
	edits := proposal.GroupEditsByService(s.serviceRepoPaths, v.Edits)[service]
	return proposal.IntersectSorted(proposal.MembersOfEdits(edits, nil), v.FixedNodeIDs())
}

// Begin atomically claims a proposal's per-service pull request for creation and
// returns the data needed to open the GitHub pull-request. service selects which
// owning-service group to claim ("" is the legacy whole-proposal group); it must
// be one of PRServices(v), else ErrUnknownService. It builds the deterministic
// branch remediation/<release_id>/attempt<n>(/<service> when non-empty) before
// delegating to repo.BeginPR. The returned PRClaim carries the computed Branch.
func (s *Service) Begin(ctx context.Context, id, service string) (proposal.PRClaim, error) {
	v, err := s.repo.Get(ctx, id)
	if err != nil {
		return proposal.PRClaim{}, fmt.Errorf("get proposal: %w", err)
	}
	if !slices.Contains(s.PRServices(v), service) {
		return proposal.PRClaim{}, fmt.Errorf("%w: %q", ErrUnknownService, service)
	}
	branch := BuildBranch(v.ReleaseID, v.Attempt, service)
	claim, err := s.repo.BeginPR(ctx, id, service, branch, s.clock.Now())
	if err != nil {
		return proposal.PRClaim{}, fmt.Errorf("begin pr: %w", err)
	}
	claim.Branch = branch
	return claim, nil
}

// Record records a successfully opened PR inside a single transaction: it
// calls RecordPR on the (proposal, service) child row and creates a
// remediation.pr_opened:v1 outbox entry atomically. The outbox entry ID is
// deterministic so a re-emission of the same PR-opened fact dedups to one
// downstream event. RecordPR's CAS guard makes this method itself idempotent:
// two callers can race to record the same claim — the ui PR-creation route and
// the reconciler's opening sweep, when the sweep finds a PR on GitHub for a
// claim it read as stuck before the route's own recording call lands — and only
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

	hit, err := u.ProposalRepo().RecordPR(ctx, in.ProposalID, in.Service, in.PrURL, in.PrNumber, in.OpenedBy, openedAt)
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
// acquired or observed, never a fresher one taken by someone else since. service
// selects which per-service child row to release. Two callers use this: the ui
// PR-creation route, immediately after its own Begin call in the same request,
// when a downstream S3 or GitHub step fails — passing the ClaimedAt that Begin
// returned; and the reconciler's opening sweep, passing the ClaimedAt it read
// while listing stuck claims earlier in the same pass. A mismatch (the claim was
// released and re-claimed between the caller's own claim/observation and this
// call) is reported via hit=false rather than an error: neither caller may ever
// overwrite a claim it does not currently hold.
func (s *Service) FailStuckClaim(ctx context.Context, id, service string, observedClaimedAt time.Time) (bool, error) {
	return s.repo.FailStuckOpeningPR(ctx, id, service, observedClaimedAt)
}

// RecordOutcome mirrors a terminal PR outcome observed on GitHub onto the
// (proposal, service) child row inside a single transaction: the pr_state CAS
// 'open' -> outcome and the remediation.pr_closed:v1 outbox entry commit
// together. A CAS miss (the row is no longer 'open') is an idempotent no-op:
// nothing is written and no event is emitted, so repeated observers of the same
// fact converge safely. edits carries this PR's per-file close detail (which
// edits a human amended before merge) and resolved the failing-node subset the
// PR fixes; both are threaded verbatim into the pr_closed payload. An empty
// resolved falls back to the same per-service set pr_opened named, so the two
// events always agree on which nodes the PR fixed.
func (s *Service) RecordOutcome(ctx context.Context, id, service string, outcome proposal.PROutcome, closedAt time.Time, edits []event.ClosedEdit, resolved []string) error {
	v, err := s.repo.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("get proposal: %w", err)
	}

	u := s.newUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	transitioned, err := u.ProposalRepo().RecordPROutcome(ctx, id, service, outcome, closedAt)
	if err != nil {
		return fmt.Errorf("record pr outcome: %w", err)
	}
	if !transitioned {
		return nil
	}

	if err := s.enqueuePRClosed(ctx, u, v, service, outcome, closedAt, edits, resolved); err != nil {
		return err
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// enqueuePRClosed builds the deterministic remediation.pr_closed:v1 outbox
// entry and creates it on the repository bound to the caller's transaction. It
// names the nodes the PR actually fixed, not every node the attempt addressed:
// a node the attempt skipped or failed carries no fix, so the pull request's
// outcome says nothing about its rejection. The resolved subset the caller
// passes is used verbatim; an empty one falls back to the per-service set,
// matching pr_opened exactly.
func (s *Service) enqueuePRClosed(ctx context.Context, u uow.UnitOfWork, v proposal.View, service string, outcome proposal.PROutcome, closedAt time.Time, edits []event.ClosedEdit, resolved []string) error {
	if len(resolved) == 0 {
		resolved = s.resolvedForService(v, service)
	}
	eventID := event.PRClosedEventID(v.ReleaseID, v.Attempt, service)
	payload := event.PRClosed{
		ProposalID:      v.ID,
		ReleaseID:       v.ReleaseID,
		NodeID:          v.NodeID,
		ResolvedNodeIDs: resolved,
		Service:         service,
		PrURL:           v.PrURL,
		PrNumber:        v.PrNumber,
		Outcome:         string(outcome),
		ClosedAt:        closedAt.Format(time.RFC3339),
		Edits:           edits,
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
// entry and creates it on the repository bound to the caller's transaction. As
// with pr_closed, it names only the nodes this per-service PR fixed.
// now is the outbox row's own bookkeeping timestamp; openedAt is the PR's
// actual creation time, which the two can legitimately differ on — recovering
// a stranded PR through the opening sweep resolves it long after GitHub
// created it.
func (s *Service) enqueuePROpened(ctx context.Context, u uow.UnitOfWork, v proposal.View, in RecordInput, now, openedAt time.Time) error {
	eventID := event.PROpenedEventID(v.ReleaseID, v.Attempt, in.Service)
	payload := event.PROpened{
		ProposalID:      in.ProposalID,
		ReleaseID:       v.ReleaseID,
		NodeID:          v.NodeID,
		ResolvedNodeIDs: s.resolvedForService(v, in.Service),
		Service:         in.Service,
		PrURL:           in.PrURL,
		PrNumber:        in.PrNumber,
		OpenedBy:        in.OpenedBy,
		OpenedAt:        openedAt.Format("2006-01-02T15:04:05Z07:00"),
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
// proposal's release/attempt/service:
// remediation/<release_id>/attempt<n>, with "/<service>" appended when service
// is non-empty (a per-service PR of a split proposal). The legacy "" service —
// one PR for the whole proposal — carries no service segment, so its branch is
// exactly the pre-split name. Both Begin (computing the branch to claim) and the
// reconciler's opening sweep (recomputing the same branch to look a stuck claim
// up on GitHub) call this so the two never drift apart.
func BuildBranch(releaseID string, attempt int, service string) string {
	branch := fmt.Sprintf("remediation/%s/attempt%d", releaseID, attempt)
	if service != "" {
		branch += "/" + service
	}
	return branch
}
