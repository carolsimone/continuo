package repository

import (
	"context"
	"time"

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
	// SaveInitializationError writes back only a pool's initialization
	// outcome, leaving every other part of the pool as registered.
	//
	// This is the write a worker's own report drives, and a worker's report
	// says nothing about the rest of the pool: the credential, the replica
	// count, and the runtime artifact are all set by whoever registers the
	// pool. Writing the whole pool back from a snapshot read before the
	// report would let a report undo a credential rotation that landed in
	// between. Saving an unregistered pool is an error rather than a silent
	// no-op.
	SaveInitializationError(ctx context.Context, poolKey, initializationError string, at time.Time) error
	// List returns every registered pool, ordered by pool key.
	List(ctx context.Context) ([]model.WorkerPool, error)
}
