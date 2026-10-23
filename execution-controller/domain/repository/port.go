// Package repository holds execution-controller's domain repository ports.
// Implementations live in the adapter layer.
package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/google/uuid"
)

// DeploymentRepository persists and loads Deployment aggregates from the
// deployments command queue.
type DeploymentRepository interface {
	// Add inserts a new pending Deployment.
	Add(ctx context.Context, d *model.Deployment) error
	// AddAttempt inserts a production Deployment unless its task already has
	// one of the same attempt, and reports whether it did.
	AddAttempt(ctx context.Context, d *model.Deployment) (added bool, err error)
	// LoadByTaskAttempt returns the production deployment of the task's
	// attempt, locked for the rest of the transaction. found is false, with no
	// error, when the task has no production deployment of that attempt.
	LoadByTaskAttempt(ctx context.Context, taskID string, attempt int) (d *model.Deployment, found bool, err error)
	// HasAttemptAfter reports whether the task has a production deployment of
	// a later attempt than attempt.
	HasAttemptAfter(ctx context.Context, taskID string, attempt int) (bool, error)
	// LoadByJobName returns the most recently created deployment that created
	// or adopted a Job of jobName (its DeployedAt is set), locked for the rest
	// of the transaction, or sql.ErrNoRows when none exists. A deployment that
	// only carries the name — refused, failed before its Job was created, or
	// not created yet — is never returned, so it never stands for the Job of
	// another.
	LoadByJobName(ctx context.Context, jobName string) (*model.Deployment, error)
	// LockOpenOfRun returns the production deployments of runID that have not
	// ended (pending, reserved, starting, running or terminating), ordered by
	// id, each locked for the rest of the transaction. A row another
	// transaction holds, such as a launch creating its Job, is waited for and
	// returned as that transaction left it, or not at all once it has ended.
	LockOpenOfRun(ctx context.Context, runID string) ([]*model.Deployment, error)
	// Save persists the mutated state of an existing Deployment.
	Save(ctx context.Context, d *model.Deployment) error
	// GetByReleaseNode returns the (mode, release_id, node_id) Deployment, or
	// sql.ErrNoRows when none exists. mode scopes the lookup so the validation
	// and seed-build legs of one release (which share release_id) never read each
	// other's rows: outcomes.Recorder passes the terminal Job's mode (ModeValidation,
	// ModeSeedBuild, or ModeCompile).
	GetByReleaseNode(ctx context.Context, releaseID, nodeID string, mode model.Mode) (*model.Deployment, error)
	// PendingValidationCount counts rows of the given mode for releaseID whose
	// outcome is not yet recorded, whatever their status (pending, blocked, in
	// flight or done). 'blocked' rows are waiting for in-set upstreams to
	// complete before they can be dispatched; counting every row without an
	// outcome keeps the aggregate-emit gate from firing before every node has
	// settled.
	// mode scopes the count so a release's validation and seed-build legs are
	// counted independently.
	PendingValidationCount(ctx context.Context, releaseID string, mode model.Mode) (int, error)
	// ListValidationResults returns all rows of the given mode for releaseID
	// whose outcome is non-NULL. The aggregate gate uses this to build the
	// per-node results array on the per-leg completion emission.
	ListValidationResults(ctx context.Context, releaseID string, mode model.Mode) ([]*model.Deployment, error)
	// ListValidationByRelease returns every row of the given mode for releaseID
	// as reconstituted aggregates (status, outcome, and UpstreamNodeIDs from
	// job_params). service/validation's propagateGating uses it to compute
	// downstream readiness and to skip transitive downstreams on failure.
	ListValidationByRelease(ctx context.Context, releaseID string, mode model.Mode) ([]*model.Deployment, error)
}

// ValidationAggregateRepository guards single emission of a per-release leg
// completion event (validation.result:v1 kind=complete / seed.build.completed:v1) via the
// validation_aggregates sentinel table. Both legs share one release_id but run
// sequentially (seed-build BEFORE validation), so the sentinel and the advisory
// lock are keyed on (release_id, mode): the seed-build claim must NOT block the
// later validation claim for the same release.
type ValidationAggregateRepository interface {
	// LockRelease takes a transaction-scoped advisory lock keyed on
	// (releaseID, mode), serializing one leg's aggregate-emit gate
	// (pending-count -> claim -> emit) across concurrent transactions. It
	// auto-releases at commit/rollback. Without it, two overlapping last-node
	// terminals could each read the other as still pending under READ COMMITTED
	// and both no-op, hanging the leg with no aggregate ever emitted. Keying on
	// mode keeps the validation and seed-build legs of one release from
	// serializing against each other. Must be called inside a transaction,
	// before PendingValidationCount.
	LockRelease(ctx context.Context, releaseID string, mode model.Mode) error
	// ClaimEmission inserts a sentinel row for (releaseID, mode). Returns
	// (true, nil) if this caller won the race and should emit the leg's
	// completion event; returns (false, nil) on conflict (another caller already
	// emitted that leg). Keying on mode lets the same release emit both
	// seed.build.completed:v1 and validation.result:v1 (kind=complete).
	ClaimEmission(ctx context.Context, releaseID string, mode model.Mode, now time.Time) (bool, error)
}

// CancelledRunsRepository holds the run fence: every run state cancelled,
// in cancelled_runs. No attempt of a fenced run starts, and every Job of it is
// deleted.
type CancelledRunsRepository interface {
	// Insert fences runID. Fencing a fenced run again keeps one fence and
	// restarts its age, so a fence outlives the last cancel state sent for
	// the run by DeleteOlderThan's age.
	Insert(ctx context.Context, runID uuid.UUID) error
	// Exists reports whether runID is fenced.
	Exists(ctx context.Context, runID uuid.UUID) (bool, error)
	// LockRun takes runID's lock for the rest of the current transaction:
	// shared (exclusive false) by a dispatch before it reads the fence and may
	// store an attempt of the run, exclusive by the fence before it inserts
	// the fence and stops the run's rows. The fence therefore sees every
	// attempt a dispatch stored without meeting it, and a dispatch that waits
	// for a fence meets it. Call it as its own statement, before the read it
	// guards: under READ COMMITTED that read then sees what the other
	// transaction committed.
	LockRun(ctx context.Context, runID uuid.UUID, exclusive bool) error
	// DeleteOlderThan removes the fences set more than age ago, by the
	// database clock, and returns how many it removed.
	DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error)
}
