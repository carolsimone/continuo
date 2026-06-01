// Package repository holds executor-controller's domain repository ports.
// Implementations live in the adapter layer.
package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
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
	// GetByReleaseNode returns the validation Deployment for (release_id, node_id),
	// or sql.ErrNoRows when none exists. Used by the validation.node.completed
	// handler to attach an outcome to the right row.
	GetByReleaseNode(ctx context.Context, releaseID, nodeID string) (*model.Deployment, error)
	// PendingValidationCount counts mode='validation' rows for releaseID that
	// are not yet terminal — i.e. status IN ('pending','blocked','deployed') AND
	// outcome IS NULL. 'blocked' rows are not terminal: they are waiting for
	// in-set upstreams to complete before they can be dispatched. Including them
	// prevents the aggregate-emit gate from firing before all nodes have settled.
	PendingValidationCount(ctx context.Context, releaseID string) (int, error)
	// ListValidationResults returns all mode='validation' rows for releaseID
	// whose outcome is non-NULL. The dispatcher uses this to build the per-node
	// results array on the aggregate validation.completed:v1 emission.
	ListValidationResults(ctx context.Context, releaseID string) ([]*model.Deployment, error)
	// ListValidationByRelease returns every mode='validation' row for releaseID
	// as reconstituted aggregates (status, outcome, and UpstreamNodeIDs from
	// job_params). The validation.node.completed handler uses it to compute
	// downstream readiness and to skip transitive downstreams on failure.
	ListValidationByRelease(ctx context.Context, releaseID string) ([]*model.Deployment, error)
}

// ValidationAggregateRepository guards single emission of
// validation.completed:v1 via the validation_aggregates sentinel table.
type ValidationAggregateRepository interface {
	// LockRelease takes a transaction-scoped advisory lock keyed on releaseID,
	// serializing the aggregate-emit gate (pending-count -> claim -> emit)
	// across concurrent transactions for the same release. It auto-releases at
	// commit/rollback. Without it, two overlapping last-node terminals could
	// each read the other as still pending under READ COMMITTED and both no-op,
	// hanging the release with no aggregate ever emitted. Must be called inside
	// a transaction, before PendingValidationCount.
	LockRelease(ctx context.Context, releaseID string) error
	// ClaimEmission inserts a sentinel row for releaseID. Returns (true, nil)
	// if this caller won the race and should emit validation.completed:v1;
	// returns (false, nil) on PK conflict (another caller already emitted).
	ClaimEmission(ctx context.Context, releaseID string, now time.Time) (bool, error)
}
