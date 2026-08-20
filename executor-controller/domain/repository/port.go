// Package repository holds executor-controller's domain repository ports.
// Implementations live in the adapter layer.
package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/google/uuid"
)

// DeploymentRepository persists and loads Deployment aggregates from the
// executor_deployments command queue.
//
// GetDueBatch MUST be called inside a transaction the caller holds until the
// per-aggregate Save completes, because the batch locks rows with
// FOR UPDATE SKIP LOCKED.
type DeploymentRepository interface {
	// Add inserts a new pending Deployment.
	Add(ctx context.Context, d *model.Deployment) error
	// GetDueBatch returns up to limit pending Deployments whose next attempt is
	// due, oldest first, locked FOR UPDATE SKIP LOCKED.
	GetDueBatch(ctx context.Context, limit int) ([]*model.Deployment, error)
	// Save persists the mutated state of an existing Deployment.
	Save(ctx context.Context, d *model.Deployment) error
	// GetByReleaseNode returns the (mode, release_id, node_id) Deployment, or
	// sql.ErrNoRows when none exists. mode scopes the lookup so the validation
	// and seed-build legs of one release (which share release_id) never read each
	// other's rows: the validation.node.completed handler passes ModeValidation,
	// the seed.build.node.completed handler passes ModeSeedBuild.
	GetByReleaseNode(ctx context.Context, releaseID, nodeID string, mode model.Mode) (*model.Deployment, error)
	// PendingValidationCount counts rows of the given mode for releaseID that
	// are not yet terminal — i.e. status IN ('pending','blocked','deployed') AND
	// outcome IS NULL. 'blocked' rows are not terminal: they are waiting for
	// in-set upstreams to complete before they can be dispatched. Including them
	// prevents the aggregate-emit gate from firing before all nodes have settled.
	// mode scopes the count so a release's validation and seed-build legs are
	// counted independently.
	PendingValidationCount(ctx context.Context, releaseID string, mode model.Mode) (int, error)
	// ListValidationResults returns all rows of the given mode for releaseID
	// whose outcome is non-NULL. The aggregate gate uses this to build the
	// per-node results array on the per-leg completion emission.
	ListValidationResults(ctx context.Context, releaseID string, mode model.Mode) ([]*model.Deployment, error)
	// ListValidationByRelease returns every row of the given mode for releaseID
	// as reconstituted aggregates (status, outcome, and UpstreamNodeIDs from
	// job_params). The node.completed handlers use it to compute downstream
	// readiness and to skip transitive downstreams on failure.
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

// CancelledSchedulesRepository tracks schedule IDs whose deploys should be
// dropped on receipt. Implementations operate against the cancelled_schedules
// table (id, schedule_id UNIQUE, cancelled_at).
type CancelledSchedulesRepository interface {
	Insert(ctx context.Context, scheduleID uuid.UUID) error
	Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error)
	DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error)
}
