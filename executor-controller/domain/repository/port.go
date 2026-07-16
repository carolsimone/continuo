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
// GetDueJobs MUST be called inside a transaction the caller holds until the
// per-aggregate Save completes, because the batch locks rows with
// FOR UPDATE SKIP LOCKED.
type DeploymentRepository interface {
	// Add inserts a new pending Deployment.
	Add(ctx context.Context, d *model.Deployment) error
	// GetDueJobs returns up to limit pending Jobs-mode Deployments whose next
	// attempt is due, oldest first, locked FOR UPDATE SKIP LOCKED. Worker-mode
	// rows are excluded; they are claimed through GetDueWorkerForPool.
	GetDueJobs(ctx context.Context, limit int) ([]*model.Deployment, error)
	// Save persists the mutated state of an existing Deployment.
	Save(ctx context.Context, d *model.Deployment) error
	// GetByID returns one Deployment, or sql.ErrNoRows when it does not exist.
	GetByID(ctx context.Context, id uuid.UUID) (*model.Deployment, error)
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

	// LockCapacity takes a transaction-scoped lock serializing the executor's
	// capacity accounting. Every transaction that reserves an execution slot
	// takes it before ActiveSlotCount, so concurrent Jobs dispatch and worker
	// claims cannot both read the same free slot. Must be called in a
	// transaction; it releases at commit/rollback.
	LockCapacity(ctx context.Context) error
	// ActiveSlotCount counts the execution slots currently held, by Jobs-mode
	// and worker-mode work alike.
	ActiveSlotCount(ctx context.Context) (int, error)
	// ReleaseSlot frees the execution slot held by a Jobs-mode Deployment whose
	// Kubernetes Job reached a terminal status — the one release with no
	// aggregate transition of its own. Worker transitions release their slot
	// inside the aggregate and never call this. Returns false when the row held
	// no slot, so a duplicate terminal event is a no-op.
	ReleaseSlot(ctx context.Context, id uuid.UUID, now time.Time) (bool, error)
	// GetDueWorkerForPool returns one due pending worker-mode Deployment for
	// poolKey locked FOR UPDATE SKIP LOCKED, or nil when the pool has no due
	// work. Must be called inside the transaction that claims it.
	GetDueWorkerForPool(ctx context.Context, poolKey string) (*model.Deployment, error)
	// GetExpiredLeaseForUpdate returns one Deployment whose lease deadline has
	// passed, locked FOR UPDATE SKIP LOCKED, or nil when none has expired.
	GetExpiredLeaseForUpdate(ctx context.Context, now time.Time) (*model.Deployment, error)
	// GetStaleDispatchingForUpdate returns one Deployment that has held a
	// 'dispatching' reservation since before the given instant, locked
	// FOR UPDATE SKIP LOCKED, or nil when none is stale.
	GetStaleDispatchingForUpdate(ctx context.Context, before time.Time) (*model.Deployment, error)
	// ListPoolDemand reports each registered worker pool's backlog and
	// in-flight load at now, so the pool reconciler can size its replicas.
	ListPoolDemand(ctx context.Context, now time.Time) ([]model.PoolDemand, error)
	// DemotePendingPoolToJobs converts a pool's not-yet-started work back to the
	// Kubernetes Job path, returning how many rows moved. Work parked after a
	// retryable failure keeps its backoff and serves it out on the Jobs path.
	// Leased and running work is never converted.
	DemotePendingPoolToJobs(ctx context.Context, poolKey string, now time.Time) (int64, error)
	// CancelSchedule marks a schedule's not-yet-terminal Deployments cancelled
	// and returns the leases that were active, so the caller can terminate
	// exactly those worker pods.
	CancelSchedule(ctx context.Context, scheduleID uuid.UUID, now time.Time) ([]model.ActiveLease, error)
}

// ValidationAggregateRepository guards single emission of a per-release leg
// completion event (validation.completed:v1 / seed.build.completed:v1) via the
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
	// seed.build.completed:v1 and validation.completed:v1.
	ClaimEmission(ctx context.Context, releaseID string, mode model.Mode, now time.Time) (bool, error)
}
