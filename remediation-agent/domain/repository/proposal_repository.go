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
	// pr_state from '' or 'failed' to 'opening'. It returns the data needed
	// to open the PR. Returns ErrNotSourceResolved if source_resolved=false,
	// ErrPRConflict if already claimed, ErrNotFound if the id is unknown.
	BeginPR(ctx context.Context, id, branch string) (proposal.PRClaim, error)

	// RecordPR records a successfully opened PR: sets pr_state='open', pr_url,
	// pr_number, pr_opened_by, and pr_opened_at on the proposal row.
	RecordPR(ctx context.Context, id, prURL string, prNumber int, openedBy string, openedAt time.Time) error

	// FailPR resets a stuck 'opening' claim back to 'failed' so the action can
	// be retried. It is a no-op when pr_state is not 'opening'.
	FailPR(ctx context.Context, id string) error

	// ListOpenPullRequests returns proposals whose PR awaits a terminal outcome
	// (pr_state='open'), oldest-opened first. limit<=0 means no limit.
	ListOpenPullRequests(ctx context.Context, limit int) ([]proposal.OpenPR, error)

	// RecordPROutcome atomically transitions pr_state 'open' -> outcome and sets
	// pr_closed_at. Returns true when the transition fired; false when the row is
	// no longer in 'open' (already terminal or never opened) — an idempotent no-op.
	RecordPROutcome(ctx context.Context, id string, outcome proposal.PROutcome, closedAt time.Time) (bool, error)
}
