// Package repository declares the agent-remediation domain repository ports;
// implementations live in adapters/postgres.
package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

// Sentinel errors returned by ProposalRepository methods.
var (
	// ErrNotFound is returned when a proposal row does not exist for the given id.
	ErrNotFound = errors.New("proposal not found")
	// ErrPRConflict is returned by BeginPR when the (proposal, service) pull
	// request is already claimed for creation (its child pr_state is 'opening'
	// or 'open').
	ErrPRConflict = errors.New("proposal PR already claimed")
	// ErrNotSourceResolved is returned by BeginPR when the proposal has not
	// completed source resolution (source_resolved=false), so a PR cannot be opened.
	ErrNotSourceResolved = errors.New("proposal source not resolved")
	// ErrNotProposed is returned by BeginPR when the attempt has not reached
	// 'proposed'. A fix still being generated, still being verified by a
	// verification run, or already failed by one is not a fix anyone may open
	// a pull request for, and status is the only thing that says so: a python
	// contract fix carries source_resolved=true from the moment it is
	// submitted for verification, and a failed verification changes nothing
	// but the status.
	ErrNotProposed = errors.New("proposal is not in the proposed state")
)

// ProposalFilter constrains the rows returned by ProposalRepository.List.
// Zero-value fields are treated as "no filter" for that dimension.
type ProposalFilter struct {
	Status  string
	PRState string
	// ReleaseID restricts the listing to one release. Source, when set with
	// it, restricts the listing to the attempts made for one source of failure
	// in that release.
	ReleaseID string
	Source    string
	// Service, when set, matches proposals whose services contain it.
	Service string
	Limit   int
}

// AttemptLister is the repository slice the previous-tries step reads: the
// attempts already recorded for a release, which show the model what earlier
// attempts tried and why they were rejected. It is a narrower view of the List
// method ProposalRepository already declares, so a consumer that only needs
// this one slice does not have to depend on the full repository.
type AttemptLister interface {
	List(ctx context.Context, filter ProposalFilter) ([]proposal.View, error)
}

// OpeningCursor is the keyset position for paginating stuck 'opening' claims
// oldest-first by (created_at, id, service), mirroring release-controller's
// ListCursor. It lets ListStuckOpening resume after the last row a previous
// page returned instead of always re-reading the same oldest prefix: the
// reconciler's opening sweep carries the cursor its previous pass returned
// into the next call, so a persistently unresolvable row (a standing GitHub
// error, for instance) never keeps every row behind it out of every pass.
// Service is part of the key because one proposal can now have several
// 'opening' children — one per owning service — that share a (created_at, id);
// without the service tiebreak, resuming after one child would skip past a
// sibling sharing that key and strand it. A nil cursor requests the first page.
type OpeningCursor struct {
	CreatedAt time.Time
	ID        string
	Service   string
}

// OpenPRLister is the repository slice the reconciler's outcome-mirroring
// pass reads: proposals whose PR awaits a terminal outcome, oldest-opened
// first. It is a narrower view of the ListOpenPullRequests method
// ProposalRepository already declares, so a consumer that only needs this one
// slice does not have to depend on the full repository.
type OpenPRLister interface {
	ListOpenPullRequests(ctx context.Context, limit int) ([]proposal.OpenPR, error)
}

// OpeningLister is the repository slice the reconciler's opening sweep reads:
// proposals claimed for PR creation but never recorded, paginated by
// OpeningCursor so a pass resumes where the previous one left off. It is a
// narrower view of the ListStuckOpening method ProposalRepository already
// declares, so a consumer that only needs this one slice does not have to
// depend on the full repository.
type OpeningLister interface {
	ListStuckOpening(ctx context.Context, limit int, cursor *OpeningCursor) (items []proposal.OpeningPR, next *OpeningCursor, err error)
}

// ProposalRepository persists fix-proposal attempts and answers the attempt-cap
// query. The repository is bound to its transaction at construction by the
// UnitOfWork, so methods take only ctx + domain types.
type ProposalRepository interface {
	// CountAttempts returns the number of TERMINAL proposal attempts recorded for
	// one remediation round of one release: the attempt cap is a budget per
	// (release_id, remediation_round), counted over the release's whole failing
	// set rather than per node, since one attempt addresses every node it
	// covers at once. The release is part of the key because the attempt cap
	// bounds how often one release's failure is retried: a later release is new
	// code, and its attempts are its own, never charged to an earlier release's
	// budget. The remediation round is part of the key for the same reason
	// within one release: a human's "try again" on a rejected release starts a
	// fresh round, and its attempts are never charged to an earlier round's
	// exhausted budget. In-flight rows — 'generating' (the model call has not
	// resolved) and 'verifying' (a verification run is still validating a
	// proposed fix) — are excluded so an attempt that has not yet concluded
	// neither inflates the attempt cap nor shifts the attempt number on a
	// redelivery.
	CountAttempts(ctx context.Context, releaseID string, remediationRound int) (int, error)

	// InsertGenerating persists an in-flight 'generating' row for the attempt just
	// before the model is called. It is idempotent: a redelivery that re-runs the
	// same attempt collides on (release_id, attempt) and is a no-op, so at most
	// one generating row exists per attempt.
	InsertGenerating(ctx context.Context, p proposal.Proposal) error

	// FailGenerating finalizes releaseID's in-flight 'generating' row,
	// transitioning it to 'failed' with reason as its rationale, and returns how
	// many rows it moved. It carries 'failed' into the per-node outcomes too:
	// every node_outcomes entry still marked 'generating' is rewritten to
	// 'failed' with reason, while any already-terminal entry is left as it is.
	// This is load-bearing, not cosmetic — the release page reads a node's own
	// outcome in preference to the row status, so an entry left at 'generating'
	// would keep that node reporting a fix as still being generated even with the
	// row failed.
	//
	// The runner calls it when a remediation ends as a system failure while its
	// attempt row is still generating: the row would stay in flight with nothing
	// that will ever finish it, and every reader, the release page included,
	// would keep reporting a fix as still being generated.
	//
	// The match is releaseID alone: one attempt addresses the release's whole
	// failing set, so at most one row can be generating for a release at a
	// time, and the row it must finalize is unambiguous without a node or
	// error-signature match.
	FailGenerating(ctx context.Context, releaseID, reason string) (int, error)

	// FailLegacyInFlight fails every attempt row in flight that belongs to no
	// remediation, and returns how many rows it moved. Such a row is
	// 'generating' or 'verifying' with no remediation id: the runner stamps the
	// remediation on every attempt row it writes, so nothing drives a row that
	// has none, and left alone it would stay in flight for good and keep the
	// release page from offering "Try again".
	//
	// A runner row whose remediation the retention sweeper deleted also has no
	// remediation id (the column is cleared when the remediation goes), so the
	// selection cannot tell it from a row that never had one. That is safe: the
	// sweeper deletes only 'done' and 'system_failure' remediations, whose rows
	// are terminal, never 'generating' or 'verifying', so no row of a deleted
	// remediation is in flight.
	//
	// Each row moves to 'failed' with reason on it. A row that was generating
	// takes reason as its rationale. A row that was verifying takes it as its
	// verify error and keeps its rationale, edits and verifications. On both,
	// every node_outcomes entry still 'generating' or 'verifying' is rewritten
	// to 'failed' with reason, and any other entry is left as it is, so a
	// reader of a node's own outcome agrees with the row.
	//
	// It is idempotent: a row it moved is no longer in flight, so a second call
	// moves none. A row of a remediation, and a row that is not in flight, are
	// never touched.
	FailLegacyInFlight(ctx context.Context, reason string) (int, error)

	// Upsert records the terminal outcome of an attempt on the natural key
	// (release_id, attempt): it finalizes the in-flight generating row when one
	// exists (ON CONFLICT … DO UPDATE), or plain-inserts for instant paths that
	// never marked generating (e.g. the attempt-cap escalation). A proposal
	// that carries approvers replaces the row's, and one that carries none
	// keeps the approvers an existing row holds.
	Upsert(ctx context.Context, p proposal.Proposal) error

	// Get returns the full View for the given proposal id.
	// Returns ErrNotFound if no row exists for the id.
	Get(ctx context.Context, id string) (proposal.View, error)

	// List returns proposals matching the filter, ordered by created_at DESC.
	// A zero ProposalFilter returns all rows (up to Limit, if set).
	List(ctx context.Context, filter ProposalFilter) ([]proposal.View, error)

	// BeginPR atomically claims one (proposal, service) pull request for
	// creation by transitioning the child proposal_pull_request row's pr_state
	// from '' or 'failed' to 'opening', stamping pr_claimed_at with claimedAt.
	// service selects which per-service PR to claim ("" is the legacy sentinel:
	// one PR covering the whole proposal); the child row is created on first
	// claim. It returns the data needed to open the PR, narrowed to that
	// service's edits and the nodes it fixed, including the claimed ClaimedAt
	// read back from the row — the caller carries this value forward and hands
	// it to FailStuckOpeningPR if it later needs to release this exact claim.
	// Claiming requires the parent's status='proposed' as well as
	// source_resolved, so a fix still being generated or verified — or one a
	// verification run already failed — can never be turned into a pull request.
	// Returns ErrNotSourceResolved if source_resolved=false, ErrNotProposed if
	// the attempt has not reached 'proposed', ErrPRConflict if that service is
	// already claimed, ErrNotFound if the id is unknown.
	BeginPR(ctx context.Context, id, service, branch string, claimedAt time.Time) (proposal.PRClaim, error)

	// RecordPR atomically transitions the (proposal, service) child row's
	// pr_state 'opening' -> 'open', writing pr_url, pr_number, pr_opened_by,
	// and pr_opened_at, and clearing pr_claimed_at back to NULL since the claim
	// is resolved. hit reports whether the CAS fired; false means the row was no
	// longer 'opening' — already recorded or failed by another caller since this
	// one's own BeginPR claim — the same compare-and-set contract
	// FailStuckOpeningPR and RecordPROutcome apply to every other write against
	// an in-flight PR claim. Two callers race to record the same claim: the ui
	// PR-creation route, right after the GitHub PR it just created, and the
	// reconciler's opening sweep, after finding that same PR already exists on
	// GitHub for a claim it read as stuck. Whichever reaches the row first wins
	// the CAS; the other's call is a harmless no-op, since both would have
	// written the same pr_url and pr_number for the same branch.
	RecordPR(ctx context.Context, id, service, prURL string, prNumber int, openedBy string, openedAt time.Time) (hit bool, err error)

	// FailStuckOpeningPR resets a stuck 'opening' claim on the (proposal,
	// service) child row back to 'failed', clearing pr_claimed_at, but only when
	// the row's current pr_claimed_at still equals observedClaimedAt — the
	// compare-and-set guard that lets a caller release exactly the claim it
	// itself acquired or observed, never a fresher one taken by someone else
	// since. Two callers use it: the ui PR-creation route, with the ClaimedAt
	// its own BeginPR returned, when a downstream S3 or GitHub step fails after
	// the claim; and the reconciler's opening sweep, with the ClaimedAt it read
	// while listing stuck claims. hit reports whether the CAS fired; false
	// covers both "no longer opening" and "re-claimed with a different
	// pr_claimed_at" — neither is an error, and in both cases the call has
	// correctly left the row alone.
	FailStuckOpeningPR(ctx context.Context, id, service string, observedClaimedAt time.Time) (hit bool, err error)

	// ListOpenPullRequests returns one entry per child pull request awaiting a
	// terminal outcome (pr_state='open'), oldest-opened first; a proposal split
	// into several per-service PRs yields one entry per open service. Each row
	// carries its Service. limit<=0 means no limit.
	ListOpenPullRequests(ctx context.Context, limit int) ([]proposal.OpenPR, error)

	// ListStuckOpening returns up to limit child pull requests claimed for
	// creation but not yet recorded (pr_state='opening'), ordered oldest-created
	// first by (parent proposal created_at, parent id, child service), resuming
	// strictly after cursor (nil for the first page). Service is part of the key
	// so a proposal's several per-service 'opening' children, which share a
	// (created_at, id), are each visited rather than one skipping the others.
	// Each row carries its Service. next is the
	// cursor of the last row in this page, non-nil only when more rows exist
	// beyond it — nil signals "reached the end of the 'opening' set", which the
	// reconciler's opening sweep uses to wrap back to the first page so a full
	// rotation through every stuck row repeats indefinitely rather than starving
	// rows past the batch limit. limit<=0 defaults to 50. Each row carries its
	// ClaimedAt (pr_claimed_at) — see proposal.OpeningPR for why it is
	// essentially always non-nil.
	ListStuckOpening(ctx context.Context, limit int, cursor *OpeningCursor) (items []proposal.OpeningPR, next *OpeningCursor, err error)

	// RecordPROutcome atomically transitions the (proposal, service) child row's
	// pr_state 'open' -> outcome and sets pr_closed_at. Returns true when the
	// transition fired; false when the row is no longer in 'open' (already
	// terminal or never opened) — an idempotent no-op.
	RecordPROutcome(ctx context.Context, id, service string, outcome proposal.PROutcome, closedAt time.Time) (bool, error)

	// MarkFixVerified finalizes a proposal the runner verified, every
	// verification run having passed, transitioning status 'verifying' ->
	// 'proposed'. It also rewrites every node_outcomes entry still at
	// 'verifying' to 'proposed', so a per-node reader agrees with the row's own
	// status. The proposal id and the final per-run summary come from v, and
	// the summary is written in the same statement. hit reports whether the CAS
	// fired; false means the row was no longer 'verifying', already finalized
	// by a concurrent or repeated pass. It refuses a zero Verified with an
	// error and writes nothing.
	MarkFixVerified(ctx context.Context, v remediation.Verified) (hit bool, err error)

	// MarkVerifyFailed finalizes a proposal a verification run rejected,
	// transitioning status 'verifying' -> 'failed' and recording verifyErr so
	// the next attempt can use it as evidence. It also rewrites every
	// node_outcomes entry still at 'verifying' to 'failed'. verifications is
	// the final per-run summary, written in the same statement. hit reports
	// whether the CAS fired, with the same semantics as MarkFixVerified.
	MarkVerifyFailed(ctx context.Context, id, verifyErr string, verifications []proposal.Verification) (hit bool, err error)

	// ReopenTryAttempt moves the failed row of one attempt of the remediation
	// remediationID back in flight, to status, which is generating or
	// verifying, with no failure on it. A row reopened generating undoes
	// FailGenerating: it loses its rationale, and each per-node outcome
	// FailGenerating failed (status failed, the row's rationale as reason, no
	// code) is generating again, every other outcome kept as it is; outcomes
	// must be nil. A row reopened verifying loses its verify error, keeps its
	// rationale, edits and verifications, and takes outcomes as its per-node
	// outcomes. It changes only the row of that attempt whose status is
	// failed, and reports whether it changed one. Any other status, or
	// outcomes given for a row reopened generating, is an error, with nothing
	// written.
	ReopenTryAttempt(ctx context.Context, remediationID uuid.UUID, attempt int, status proposal.Status, outcomes map[string]proposal.NodeOutcome) (reopened bool, err error)

	// RecordApprovers records approvers on the row of one attempt of a release:
	// the row keeps one entry per node, an approver given for a node taking the
	// place of the entry the row holds for it, and its approvers stay sorted by
	// node id. Approvers the row holds for other nodes are kept, so the fixed
	// nodes and the handed-over nodes of one attempt add up. It returns
	// ErrNotFound, writing nothing, when the release has no row for the attempt.
	RecordApprovers(ctx context.Context, releaseID string, attempt int, approvers []proposal.Approver) error

	// UpdateVerificationPhase records the phase (and, once known, the
	// activation time) of one of the proposal's verification runs, leaving
	// the others untouched. Called by the gate poller only when the phase it
	// read differs from the stored one.
	UpdateVerificationPhase(ctx context.Context, id, runID string, phase proposal.Phase, activatedAt *time.Time) error
}
