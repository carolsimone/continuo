package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/google/uuid"
)

// AdmissionRepository admits deployments to execution slots and finds the ones
// whose launch or observation was lost. LockScope, CountInFlight and
// ReserveNext run in one transaction the caller holds: LockScope serialises
// every claimer of the scope until that transaction ends, so the count stays
// true until the reservation commits.
type AdmissionRepository interface {
	// LockScope locks scope's capacity record for the rest of the transaction.
	// It fails when the record does not exist.
	LockScope(ctx context.Context, scope string) error
	// CountInFlight counts the deployments holding a slot (reserved, starting,
	// running).
	CountInFlight(ctx context.Context) (int, error)
	// ReserveNext moves up to limit due pending deployments to reserved and
	// returns their ids. Production and candidate deployments alternate; within
	// production, runs (schedule_id) take turns, and within candidates, releases
	// (release_id) take turns; within one run or release the earliest due goes
	// first.
	ReserveNext(ctx context.Context, limit int) ([]uuid.UUID, error)
	// GetReserved returns the reserved deployment id, locked for the rest of
	// the transaction, or sql.ErrNoRows when it is not reserved or another
	// transaction holds it.
	GetReserved(ctx context.Context, id uuid.UUID) (*model.Deployment, error)
	// ReturnStaleReserved moves deployments reserved for longer than olderThan
	// back to pending and returns their ids. A reservation that old lost the
	// dispatcher that took it; returning it frees the slot, and the claim
	// re-admits it in fair order. A reservation a launcher holds is skipped.
	ReturnStaleReserved(ctx context.Context, olderThan time.Duration) ([]uuid.UUID, error)
	// ListStarted returns the starting and running deployments whose status
	// last changed more than olderThan ago.
	ListStarted(ctx context.Context, olderThan time.Duration) ([]*model.Deployment, error)
	// GetStarted returns the starting or running deployment id, locked for the
	// rest of the transaction, or sql.ErrNoRows when it is no longer started or
	// another transaction holds it.
	GetStarted(ctx context.Context, id uuid.UUID) (*model.Deployment, error)
	// TouchStateChanged restarts id's staleness clock without changing its
	// status.
	TouchStateChanged(ctx context.Context, id uuid.UUID) error
}
