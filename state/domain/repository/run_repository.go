// Package repository declares state's domain repository ports: the
// collection-like abstractions over Run and ScheduleCatalog aggregates.
// Implementations live in state/adapters/postgres.
package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// RunRepository is the port the application layer uses to load/save Run
// aggregates and to ask narrow cross-Run queries (HasActiveSchedule,
// GetActiveScheduler, GetLastRunPerSchedule). Implementations live in
// state/adapters/postgres.
type RunRepository interface {
	// GetRun loads a Run snapshot for read-only use. No row lock. Returns
	// the adapter's ErrNotFound when the run does not exist; the adapter
	// wraps repo-level not-found into a domain-friendly form at the
	// application boundary.
	GetRun(ctx context.Context, id uuid.UUID) (*run.Run, error)

	// LoadRunForUpdate loads a Run inside the repository's bound transaction
	// with SELECT ... FOR UPDATE. The aggregate is intended to be mutated; call
	// SaveRun before tx commit.
	LoadRunForUpdate(ctx context.Context, id uuid.UUID) (*run.Run, error)

	// SaveRun persists every dirty field of r. The adapter consults
	// r.Changes() to dispatch to the existing tuned SQL methods. After a
	// successful save the adapter calls r.ResetChanges() so the aggregate
	// can be saved again within the same tx if necessary. Saving a newly
	// constructed Run whose id is already persisted writes nothing and returns
	// run.ErrRunAlreadyExists; the transaction stays usable.
	SaveRun(ctx context.Context, r *run.Run) error

	// HasActiveSchedule returns true when a PENDING or RUNNING Run exists
	// for `name`. Cross-aggregate query used by SchedulePolicy.
	HasActiveSchedule(ctx context.Context, name string) (bool, error)

	// GetActiveScheduler returns the active Run on `name`, or nil when
	// none exists. Used by CancelSchedule to find which run to cancel.
	GetActiveScheduler(ctx context.Context, name string) (*run.Run, error)

	// GetLastRunPerSchedule returns the most recent Run per schedule name.
	// Read-only projection for ListAllSchedules.
	GetLastRunPerSchedule(ctx context.Context) (map[string]LastRunSummary, error)

	// RunExists reports whether a run with id is persisted. It reads inside the
	// bound transaction and needs one.
	RunExists(ctx context.Context, id uuid.UUID) (bool, error)

	// ListUnplannedActive returns the ids of the PENDING or RUNNING runs whose
	// tasks state did not plan (planned_at is NULL), oldest first. It reads
	// inside the bound transaction and needs one.
	ListUnplannedActive(ctx context.Context) ([]uuid.UUID, error)

	// CountReleaseRuns counts the runs of kind whose release_id is releaseID.
	// It reads inside the bound transaction, so it sees the runs the
	// transaction wrote, and needs one.
	CountReleaseRuns(ctx context.Context, releaseID string, kind run.Kind) (int, error)
}

// LastRunSummary is the projection row returned by GetLastRunPerSchedule.
type LastRunSummary struct {
	ScheduleName string
	ScheduleID   uuid.UUID
	Status       run.SchedulerStatus
	CreatedAt    time.Time
	IsRunning    bool
}
