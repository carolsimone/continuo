package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/run-controller/domain/seeds"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
)

// ViewOutageReader reads the open outage of a tenant's node.
type ViewOutageReader interface {
	// Get returns the node's open outage; found is false when it has none.
	Get(ctx context.Context, tenantID string, key topology.NodeKey) (seeds.ViewOutage, bool, error)
}

// ViewOutageRepository records, per tenant and node, the views a seed load
// left missing or stale, from the commit of that load until a task of the view
// succeeds.
type ViewOutageRepository interface {
	// OpenForRebuilds opens an outage in state, since since, naming seedKey,
	// for each task of run runID that rebuilds a view the load of seedKey
	// drops, whichever seed the task is stamped for, and returns how many it
	// opened or updated. An outage already open keeps its since and takes
	// the new state, seed and run.
	OpenForRebuilds(ctx context.Context, tenantID string, runID uuid.UUID, seedKey topology.NodeKey,
		state seeds.OutageState, since time.Time) (int, error)

	// Close ends the node's outage and returns when it opened; found is false
	// when the node has none.
	Close(ctx context.Context, tenantID string, key topology.NodeKey) (since time.Time, found bool, err error)

	ViewOutageReader

	// CountByState counts the open outages of every tenant by state; a state
	// with none is absent.
	CountByState(ctx context.Context) (map[seeds.OutageState]int, error)

	// DeleteAbsentFrom deletes the outages of the tenant's nodes the release
	// does not hold and returns how many it deleted.
	DeleteAbsentFrom(ctx context.Context, tenantID, releaseID string) (int64, error)
}
