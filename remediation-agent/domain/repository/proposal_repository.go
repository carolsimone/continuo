// Package repository declares the remediation-agent domain repository ports;
// implementations live in adapters/postgres.
package repository

import (
	"context"
	"errors"
	"time"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
)

// Sentinel errors returned by ProposalRepository methods.
var (
	// ErrNotFound is returned when a proposal row does not exist for the given id.
	ErrNotFound = errors.New("proposal not found")
	// ErrPRConflict is returned by BeginPR when the proposal is already claimed
	// for PR creation (pr_state is 'opening' or 'open').
	ErrPRConflict = errors.New("proposal PR already claimed")
	// ErrNotSourceResolved is returned by BeginPR when the proposal has not
	// completed source resolution (source_resolved=false), so a PR cannot be opened.
	ErrNotSourceResolved = errors.New("proposal source not resolved")
)

// ProposalFilter constrains the rows returned by ProposalRepository.List.
// Zero-value fields are treated as "no filter" for that dimension.
type ProposalFilter struct {
	Status  string
	PRState string
	Limit   int
}

// OpeningCursor is the keyset position for paginating stuck 'opening' claims
// oldest-first (created_at, id), mirroring release-controller's ListCursor.
// It lets ListStuckOpening resume after the last row a previous page
// returned instead of always re-reading the same oldest prefix: the
// reconciler's opening sweep carries the cursor its previous pass returned
// into the next call, so a persistently unresolvable row (a standing GitHub
// error, for instance) never keeps every row behind it out of every pass. A
// nil cursor requests the first page.
type OpeningCursor struct {
	CreatedAt time.Time
	ID        string
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
	// the (source, nodeID, errorSignature) triplet. In-flight 'generating' rows
	// are excluded so the in-progress attempt neither inflates the attempt cap nor
	// double-counts on a redelivery.
	CountAttempts(ctx context.Context, source, nodeID, errorSignature string) (int, error)

	// InsertGenerating persists an in-flight 'generating' row for the attempt just
	// before the model is called. It is idempotent: a redelivery that re-runs the
	// same attempt collides on (release_id, source, node_id, attempt) and is a
	// no-op, so at most one generating row exists per attempt.
	InsertGenerating(ctx context.Context, p proposal.Proposal) error

	// Upsert records the terminal outcome of an attempt on the natural key
	// (release_id, source, node_id, attempt): it finalizes the in-flight
	// generating row when one exists (ON CONFLICT … DO UPDATE), or plain-inserts
	// for instant paths that never marked generating (e.g. the attempt-cap
	// escalation).
	Upsert(ctx context.Context, p proposal.Proposal) error

	// Get returns the full View for the given proposal id.
	// Returns ErrNotFound if no row exists for the id.
	Get(ctx context.Context, id string) (proposal.View, error)

	// List returns proposals matching the filter, ordered by created_at DESC.
	// A zero ProposalFilter returns all rows (up to Limit, if set).
	List(ctx context.Context, filter ProposalFilter) ([]proposal.View, error)

	// BeginPR atomically claims a proposal for PR creation by transitioning
	// pr_state from '' or 'failed' to 'opening', stamping pr_claimed_at with
	// claimedAt. It returns the data needed to open the PR, including the
	// claimed ClaimedAt read back from the row — the caller carries this value
	// forward and hands it to FailStuckOpeningPR if it later needs to release
	// this exact claim. Returns ErrNotSourceResolved if source_resolved=false,
	// ErrPRConflict if already claimed, ErrNotFound if the id is unknown.
	BeginPR(ctx context.Context, id, branch string, claimedAt time.Time) (proposal.PRClaim, error)

	// RecordPR atomically transitions pr_state 'opening' -> 'open', writing
	// pr_url, pr_number, pr_opened_by, and pr_opened_at, and clearing
	// pr_claimed_at back to NULL since the claim is resolved. hit reports
	// whether the CAS fired; false means the row was no longer 'opening' —
	// already recorded or failed by another caller since this one's own
	// BeginPR claim — the same compare-and-set contract FailStuckOpeningPR and
	// RecordPROutcome apply to every other write against an in-flight PR
	// claim. Two callers race to record the same claim: the ui-service
	// PR-creation route, right after the GitHub PR it just created, and the
	// reconciler's opening sweep, after finding that same PR already exists
	// on GitHub for a claim it read as stuck. Whichever reaches the row first
	// wins the CAS; the other's call is a harmless no-op, since both would
	// have written the same pr_url and pr_number for the same branch.
	RecordPR(ctx context.Context, id, prURL string, prNumber int, openedBy string, openedAt time.Time) (hit bool, err error)

	// FailStuckOpeningPR resets a stuck 'opening' claim back to 'failed',
	// clearing pr_claimed_at, but only when the row's current pr_claimed_at
	// still equals observedClaimedAt — the compare-and-set guard that lets a
	// caller release exactly the claim it itself acquired or observed, never a
	// fresher one taken by someone else since. Two callers use it: the
	// ui-service PR-creation route, with the ClaimedAt its own BeginPR
	// returned, when a downstream S3 or GitHub step fails after the claim; and
	// the reconciler's opening sweep, with the ClaimedAt it read while listing
	// stuck claims. hit reports whether the CAS fired; false covers both "no
	// longer opening" and "re-claimed with a different pr_claimed_at" — neither
	// is an error, and in both cases the call has correctly left the row alone.
	FailStuckOpeningPR(ctx context.Context, id string, observedClaimedAt time.Time) (hit bool, err error)

	// ListOpenPullRequests returns proposals whose PR awaits a terminal outcome
	// (pr_state='open'), oldest-opened first. limit<=0 means no limit.
	ListOpenPullRequests(ctx context.Context, limit int) ([]proposal.OpenPR, error)

	// ListStuckOpening returns up to limit proposals claimed for PR creation
	// but not yet recorded (pr_state='opening'), ordered oldest-created first
	// (created_at, id), resuming strictly after cursor (nil for the first
	// page). next is the cursor of the last row in this page, non-nil only
	// when more rows exist beyond it — nil signals "reached the end of the
	// 'opening' set", which the reconciler's opening sweep uses to wrap back to
	// the first page so a full rotation through every stuck row repeats
	// indefinitely rather than starving rows past the batch limit. limit<=0
	// defaults to 50. Each row carries its ClaimedAt (pr_claimed_at) — see
	// proposal.OpeningPR for why it is essentially always non-nil.
	ListStuckOpening(ctx context.Context, limit int, cursor *OpeningCursor) (items []proposal.OpeningPR, next *OpeningCursor, err error)

	// RecordPROutcome atomically transitions pr_state 'open' -> outcome and sets
	// pr_closed_at. Returns true when the transition fired; false when the row is
	// no longer in 'open' (already terminal or never opened) — an idempotent no-op.
	RecordPROutcome(ctx context.Context, id string, outcome proposal.PROutcome, closedAt time.Time) (bool, error)
}
