package repository

import (
	"context"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
)

// WorkerPoolRepository is the collection of registered worker pools.
type WorkerPoolRepository interface {
	// Get returns the pool registered under poolKey, or nil when no pool is.
	// An unregistered pool is not an error: the worker API asks about pool keys
	// its callers supply, and answers a missing one exactly as it answers a
	// wrong credential.
	Get(ctx context.Context, poolKey string) (*model.WorkerPool, error)
	// Add registers a new pool.
	Add(ctx context.Context, pool model.WorkerPool) error
	// Save writes back an existing pool. Saving a pool that was never
	// registered is an error rather than a silent no-op.
	Save(ctx context.Context, pool model.WorkerPool) error
	// List returns every registered pool, ordered by pool key.
	List(ctx context.Context) ([]model.WorkerPool, error)
}
