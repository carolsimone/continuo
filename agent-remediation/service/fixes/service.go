// Package fixes holds the PR-lifecycle application service. It orchestrates
// the fix repository and outbox to manage the full lifecycle from fix
// creation through PR open/fail.
package fixes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

// ErrUnknownProject is returned by Begin when the requested owning-project group
// is not one of the fix's — a split fix can only be claimed on a
// project its edits actually attribute members to, and a legacy (unsplit)
// fix only on the "" whole-fix group.
var ErrUnknownProject = errors.New("no edits for that project")

// ErrNoRepository is returned by Begin when the fix records no
// owner/name repository: there is nowhere to open a pull request, and a claim
// would only park the row in 'opening' for the reconciler to fail again.
var ErrNoRepository = errors.New("no repository recorded for this fix")

// RecordInput carries the data required to record a successfully opened PR.
type RecordInput struct {
	FixID string
	// Project is the owning-project group this PR covers; "" is the legacy
	// whole-fix PR.
	Project  string
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
	Repo   repository.FixRepository
	NewUoW func() uow.UnitOfWork
	Clock  ports.Clock
	// ProjectRepoPaths maps a dbt project_name to its project root within the
	// repository. It is how the service splits a fix's edits by owning
	// project (PRProjects) and narrows a per-project PR's resolved node set
	// (resolvedForProject). An empty map means every edit lands under the
	// legacy "" group — one PR for the whole fix.
	ProjectRepoPaths map[string]string
}

// Service is the PR-lifecycle application service. It coordinates fix
// reads, the PR state machine (claim → record / fail), and outbox emission.
type Service struct {
	repo             repository.FixRepository
	newUoW           func() uow.UnitOfWork
	clock            ports.Clock
	projectRepoPaths map[string]string
}

// New constructs a Service from the provided Deps.
func New(d Deps) *Service {
	return &Service{
		repo:             d.Repo,
		newUoW:           d.NewUoW,
		clock:            d.Clock,
		projectRepoPaths: d.ProjectRepoPaths,
	}
}

// List returns fixes matching filter, ordered by created_at DESC.
func (s *Service) List(ctx context.Context, filter repository.FixFilter) ([]fix.View, error) {
	return s.repo.List(ctx, filter)
}

// Get returns the full View for a single fix by id.
func (s *Service) Get(ctx context.Context, id string) (fix.View, error) {
	return s.repo.Get(ctx, id)
}

// PRProjects returns the owning-project groups a fix's pull requests split
// into, sorted. A fix whose edits carry NO cluster members — one written
// before the per-project split — is never split: it returns the single legacy
// [""] group, so its per-project claim path is never entered and it keeps one
// pull request for the whole fix. Once at least one edit attributes
// members, the groups are the sorted keys of GroupEditsByProject, so each
// owning project gets its own pull request — unless ProjectRepoPaths does not
// cover every edit's path, in which case the whole fix collapses back to
// the single legacy group (see below).
func (s *Service) PRProjects(v fix.View) []string {
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
	groups := fix.GroupEditsByProject(s.projectRepoPaths, v.Edits)
	if _, unmapped := groups[""]; unmapped && len(groups) > 1 {
		// The "" key is overloaded: it means both "this fix was never
		// split" (handled above) and "this edit's path matched no configured
		// project" (here). An unmapped edit alongside a mapped one would open
		// a "" pull request — which the repository's claim path treats as the
		// whole-fix group, every edit included — next to a named-project
		// pull request holding only the mapped edit, so that edit would be
		// claimed by both. Collapse to the single legacy group instead: one
		// pull request, carrying every edit, is the safe degradation for an
		// incomplete map.
		return []string{""}
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// resolvedForProject is the failing-node set a per-project pull request names:
// for a legacy "" claim it is the try's whole fixed set; for a real project
// it is the members that project's edits attribute, intersected with the
// try's fixed set (nil fallback, so an edit written before the member codec
// never lets one project claim nodes it did not touch). Both enqueuePROpened
// and enqueuePRClosed derive the resolved set through here, so pr_opened and
// pr_closed name the same nodes as the claim the repository handed the caller.
func (s *Service) resolvedForProject(v fix.View, project string) []string {
	return resolvedNodesForProject(s.projectRepoPaths, v, project)
}

// resolvedNodesForProject is the single implementation of the per-project
// resolved-node subset, shared by the Service (pr_opened / pr_closed emission)
// and the reconciler (which computes it directly from the View it loads when
// mirroring a terminal outcome). Keeping one implementation guarantees
// pr_opened and pr_closed name the same nodes however the outcome is recorded.
func resolvedNodesForProject(projectRepoPaths map[string]string, v fix.View, project string) []string {
	if project == "" {
		return v.FixedNodeIDs()
	}
	edits := fix.GroupEditsByProject(projectRepoPaths, v.Edits)[project]
	return fix.IntersectSorted(fix.MembersOfEdits(edits, nil), v.FixedNodeIDs())
}

// Begin atomically claims a fix's per-project pull request for creation and
// returns the data needed to open the GitHub pull-request. project selects which
// owning-project group to claim ("" is the legacy whole-fix group); it must
// be one of PRProjects(v), else ErrUnknownProject. A fix whose Repo is
// not owner/name is refused with ErrNoRepository before any write. It builds
// the deterministic branch remediation/<release_id>/round<r>/try<t>
// (/<project> when non-empty) before delegating to repo.BeginPR. The returned PRClaim
// carries the computed Branch.
func (s *Service) Begin(ctx context.Context, id, project string) (fix.PRClaim, error) {
	v, err := s.repo.Get(ctx, id)
	if err != nil {
		return fix.PRClaim{}, fmt.Errorf("get fix: %w", err)
	}
	if !slices.Contains(s.PRProjects(v), project) {
		return fix.PRClaim{}, fmt.Errorf("%w: %q", ErrUnknownProject, project)
	}
	if !strings.Contains(v.Repo, "/") {
		return fix.PRClaim{}, fmt.Errorf("%w: %q", ErrNoRepository, v.Repo)
	}
	branch := BuildBranch(v.ReleaseID, v.Round, v.Try, project)
	claim, err := s.repo.BeginPR(ctx, id, project, branch, s.clock.Now())
	if err != nil {
		return fix.PRClaim{}, fmt.Errorf("begin pr: %w", err)
	}
	claim.Branch = branch
	return claim, nil
}

// Record records a successfully opened PR inside a single transaction: it
// calls RecordPR on the (fix, project) child row and creates a
// fix.pr_opened:v1 outbox entry atomically. The outbox entry ID is
// deterministic so a re-emission of the same PR-opened fact dedups to one
// downstream event. RecordPR's CAS guard makes this method itself idempotent:
// two callers can race to record the same claim — the ui PR-creation route and
// the reconciler's opening sweep, when the sweep finds a PR on GitHub for a
// claim it read as stuck before the route's own recording call lands — and only
// the first to reach the row writes anything or emits the event; the second
// is a no-op, not an error.
func (s *Service) Record(ctx context.Context, in RecordInput) error {
	// Fetch the fix to obtain release_id, node_id, and try — required
	// for deterministic outbox entry id and event payload construction.
	v, err := s.repo.Get(ctx, in.FixID)
	if err != nil {
		return fmt.Errorf("get fix: %w", err)
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

	hit, err := u.FixRepo().RecordPR(ctx, in.FixID, in.Project, in.PrURL, in.PrNumber, in.OpenedBy, openedAt)
	if err != nil {
		return fmt.Errorf("record pr: %w", err)
	}
	if !hit {
		return nil
	}

	if err := s.enqueuePROpened(ctx, u, v, in, openedAt); err != nil {
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
// acquired or observed, never a fresher one taken by someone else since. project
// selects which per-project child row to release. Two callers use this: the ui
// PR-creation route, immediately after its own Begin call in the same request,
// when a downstream S3 or GitHub step fails — passing the ClaimedAt that Begin
// returned; and the reconciler's opening sweep, passing the ClaimedAt it read
// while listing stuck claims earlier in the same pass. A mismatch (the claim was
// released and re-claimed between the caller's own claim/observation and this
// call) is reported via hit=false rather than an error: neither caller may ever
// overwrite a claim it does not currently hold.
func (s *Service) FailStuckClaim(ctx context.Context, id, project string, observedClaimedAt time.Time) (bool, error) {
	return s.repo.FailStuckOpeningPR(ctx, id, project, observedClaimedAt)
}

// RecordOutcome mirrors a terminal PR outcome observed on GitHub onto the
// (fix, project) child row inside a single transaction: the pr_state CAS
// 'open' -> outcome and the fix.pr_closed:v1 outbox entry commit
// together. A CAS miss (the row is no longer 'open') is an idempotent no-op:
// nothing is written and no event is emitted, so repeated observers of the same
// fact converge safely. edits carries this PR's per-file close detail (which
// edits a human amended before merge) and resolved the failing-node subset the
// PR fixes; both are threaded verbatim into the pr_closed payload. An empty
// resolved falls back to the same per-project set pr_opened named, so the two
// events always agree on which nodes the PR fixed.
func (s *Service) RecordOutcome(ctx context.Context, id, project string, outcome fix.PROutcome, closedAt time.Time, edits []event.ClosedEdit, resolved []string) error {
	v, err := s.repo.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("get fix: %w", err)
	}

	u := s.newUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	transitioned, err := u.FixRepo().RecordPROutcome(ctx, id, project, outcome, closedAt)
	if err != nil {
		return fmt.Errorf("record pr outcome: %w", err)
	}
	if !transitioned {
		return nil
	}

	if err := s.enqueuePRClosed(ctx, u, v, project, outcome, closedAt, edits, resolved); err != nil {
		return err
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// enqueuePRClosed builds the deterministic fix.pr_closed:v1 outbox
// entry and creates it on the repository bound to the caller's transaction. It
// names the nodes the PR actually fixed, not every node the try addressed:
// a node the try skipped or failed carries no fix, so the pull request's
// outcome says nothing about its rejection. The resolved subset the caller
// passes is used verbatim; an empty one falls back to the per-project set,
// matching pr_opened exactly.
func (s *Service) enqueuePRClosed(ctx context.Context, u uow.UnitOfWork, v fix.View, project string, outcome fix.PROutcome, closedAt time.Time, edits []event.ClosedEdit, resolved []string) error {
	if len(resolved) == 0 {
		resolved = s.resolvedForProject(v, project)
	}
	prURL, prNumber := closingProjectPR(v, project)
	eventID := event.PRClosedEventID(v.ReleaseID, v.Round, v.Try, project)
	payload := event.PRClosed{
		FixID:           v.ID,
		ReleaseID:       v.ReleaseID,
		NodeID:          v.NodeID,
		ResolvedNodeIDs: resolved,
		Project:         project,
		PrURL:           prURL,
		PrNumber:        prNumber,
		Outcome:         string(outcome),
		ClosedAt:        closedAt.Format(time.RFC3339),
		Edits:           edits,
	}
	body, err := json.Marshal(serialization.PRClosedFromDomain(payload))
	if err != nil {
		return fmt.Errorf("marshal pr_closed event: %w", err)
	}
	entry := &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(eventID.String())),
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateIDForRelease(v.ReleaseID),
		EventType:     event.PRClosedEventType,
		Payload:       body,
		StreamName:    streams.FixPrClosedV1,
		Status:        "pending",
	}
	return u.OutboxRepo().Create(ctx, entry)
}

// closingProjectPR is the URL and number of the pull request closing project
// actually owns, read off v.PullRequests; it falls back to the View's
// singular Pr* fields (the first child row — see applyChildPRs) when project
// has no entry of its own, which is always true for a legacy (unsplit)
// fix.
func closingProjectPR(v fix.View, project string) (string, int) {
	for _, pr := range v.PullRequests {
		if pr.Project == project {
			return pr.PrURL, pr.PrNumber
		}
	}
	return v.PrURL, v.PrNumber
}

// enqueuePROpened builds the deterministic fix.pr_opened:v1 outbox
// entry and creates it on the repository bound to the caller's transaction. As
// with pr_closed, it names only the nodes this per-project PR fixed.
// openedAt is the PR's actual creation time, which can be long before this
// call: recovering a stranded PR through the opening sweep resolves it long
// after GitHub created it.
func (s *Service) enqueuePROpened(ctx context.Context, u uow.UnitOfWork, v fix.View, in RecordInput, openedAt time.Time) error {
	eventID := event.PROpenedEventID(v.ReleaseID, v.Round, v.Try, in.Project)
	payload := event.PROpened{
		FixID:           in.FixID,
		ReleaseID:       v.ReleaseID,
		NodeID:          v.NodeID,
		ResolvedNodeIDs: s.resolvedForProject(v, in.Project),
		Project:         in.Project,
		PrURL:           in.PrURL,
		PrNumber:        in.PrNumber,
		OpenedBy:        in.OpenedBy,
		OpenedAt:        openedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
	body, err := json.Marshal(serialization.PROpenedFromDomain(payload))
	if err != nil {
		return fmt.Errorf("marshal pr_opened event: %w", err)
	}
	entry := &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(eventID.String())),
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateIDForRelease(v.ReleaseID),
		EventType:     event.PROpenedEventType,
		Payload:       body,
		StreamName:    streams.FixPrOpenedV1,
		Status:        "pending",
	}
	return u.OutboxRepo().Create(ctx, entry)
}

// BuildBranch returns the deterministic remediation branch name for a
// fix's release, round, try and project:
// remediation/<release_id>/round<r>/try<t>, with "/<project>" appended when
// project is non-empty (a per-project PR of a split fix). The legacy ""
// project — one PR for the whole fix — carries no project segment. Both Begin (computing the branch to claim) and the
// reconciler's opening sweep (recomputing the same branch to look a stuck claim
// up on GitHub) call this so the two never drift apart.
func BuildBranch(releaseID string, round, try int, project string) string {
	branch := fmt.Sprintf("remediation/%s/round%d/try%d", releaseID, round, try)
	if project != "" {
		branch += "/" + project
	}
	return branch
}
