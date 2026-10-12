package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/state/domain/seeds"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// MaterializationRepository records, per tenant and node, the version last
// loaded into the warehouse and the seed reconcile's latest build request.
type MaterializationRepository interface {
	// List returns every recorded node of the tenant. AttemptState reflects
	// the status of the latest request's run: pending or running is in
	// flight; succeeded is succeeded; failed, cancelled or skipped is failed;
	// no request, or a run that no longer exists, is none.
	List(ctx context.Context, tenantID string) (map[topology.NodeKey]seeds.Materialization, error)

	// RecordAttempt records that runID was created to load contentHash into
	// the node, leaving its recorded version as it is.
	RecordAttempt(ctx context.Context, tenantID string, key topology.NodeKey, contentHash string, runID uuid.UUID, at time.Time) error

	// RecordBuilt records b as the node's loaded version, leaving its latest
	// request as it is.
	RecordBuilt(ctx context.Context, tenantID string, b seeds.Built) error

	// DeleteAbsentFrom deletes the rows of nodes the release does not have and
	// returns how many it deleted.
	DeleteAbsentFrom(ctx context.Context, tenantID, releaseID string) (int64, error)
}
