package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

// Sentinel errors returned by RemediationRepository methods.
var (
	// ErrLeaseLost is returned by Hold when the caller no longer owns the
	// remediation's lease: another worker claimed it after the lease ended, or
	// the lease ended before the caller extended it.
	ErrLeaseLost = errors.New("remediation lease lost")
	// ErrRemediationNotFound is returned when no remediation row has the given id.
	ErrRemediationNotFound = errors.New("remediation not found")
)

// RemediationRepository persists the runner's state: remediations, their tries,
// the clusters of each try and the steps that work on a cluster. A worker owns
// a remediation through a lease, which only the lease methods change.
//
// An implementation bound to a unit of work's transaction is what a step's
// commit uses (Hold and the Save methods). One bound to the database serves the
// worker, the verification poller and the retention sweeper, which call
// ClaimNext, Claim, ReleaseLease, the list methods and DeleteFinishedBefore
// outside any transaction; ClaimNext and Claim are each one atomic statement,
// so they are safe in both places.
type RemediationRepository interface {
	// Create inserts r. created is false when a remediation already exists for
	// the same (tenant, release, round); that row is left as it was.
	Create(ctx context.Context, r remediation.Remediation) (created bool, err error)

	// FindByRelease returns the remediation of one tenant, release and round.
	FindByRelease(ctx context.Context, tenantID, releaseID string, round int) (remediation.Remediation, bool, error)

	// Load returns the remediation with its current try, and that try's
	// clusters (ordered by cluster) and steps (ordered by id). Try is nil while
	// the remediation has made no try. It returns ErrRemediationNotFound for an
	// unknown id.
	Load(ctx context.Context, id uuid.UUID) (remediation.Snapshot, error)

	// ClaimNext leases the oldest running remediation whose lease is free or
	// ended before now and whose current try is not 'verifying' (the poller owns
	// those). The lease lasts until now+lease. found is false when none is
	// claimable. Concurrent callers never claim the same row.
	ClaimNext(ctx context.Context, owner string, now time.Time, lease time.Duration) (id uuid.UUID, found bool, err error)

	// Claim leases one remediation, regardless of its try's status, when it is
	// running and its lease is free or ended before now. It reports whether it
	// took the lease.
	Claim(ctx context.Context, id uuid.UUID, owner string, now time.Time, lease time.Duration) (bool, error)

	// Hold extends owner's lease to now+lease. It returns ErrLeaseLost when
	// owner does not hold an unexpired lease on the remediation. Every runner
	// commit calls it first, in the same transaction.
	Hold(ctx context.Context, id uuid.UUID, owner string, now time.Time, lease time.Duration) error

	// ReleaseLease frees the lease when owner holds it, and does nothing
	// otherwise.
	ReleaseLease(ctx context.Context, id uuid.UUID, owner string) error

	// SaveRemediation writes r's status, flow, trigger, current try, failure and
	// UpdatedAt. It never touches the lease. It returns ErrRemediationNotFound
	// for an unknown id.
	SaveRemediation(ctx context.Context, r remediation.Remediation) error

	// SaveTry inserts the try or updates it on (remediation, try).
	SaveTry(ctx context.Context, t remediation.Try) error

	// SaveCluster inserts the cluster or updates it on (remediation, try,
	// cluster).
	SaveCluster(ctx context.Context, c remediation.Cluster) error

	// SaveStep inserts the step run or updates it on (remediation, try,
	// cluster, step, round). The row's id is assigned on insert and kept on
	// update; s.ID is ignored.
	SaveStep(ctx context.Context, s remediation.StepRun) error

	// ListVerifying returns up to limit running remediations whose current try
	// is 'verifying', each as Load returns it, oldest first.
	ListVerifying(ctx context.Context, limit int) ([]remediation.Snapshot, error)

	// ListPastDeadline returns the ids of up to limit running remediations whose
	// current try is still open (drafting, checking or verifying) and whose
	// deadline ended before now, oldest deadline first.
	ListPastDeadline(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error)

	// DeleteFinishedBefore deletes the done and system_failure remediations
	// last updated before cutoff, with their tries, clusters and steps, and
	// returns how many it deleted. The proposals of a deleted remediation stay,
	// with no remediation.
	DeleteFinishedBefore(ctx context.Context, cutoff time.Time) (int64, error)
}
