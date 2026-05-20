// Package repository holds executor-controller's domain repository ports.
// Implementations live in the adapter layer.
package repository

import (
	"context"

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
}
