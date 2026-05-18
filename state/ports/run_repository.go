package ports

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
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

	// LoadRunForUpdate loads a Run inside an existing transaction with
	// SELECT ... FOR UPDATE. The aggregate is intended to be mutated; call
	// SaveRun before tx commit.
	LoadRunForUpdate(ctx context.Context, tx *sqlx.Tx, id uuid.UUID) (*run.Run, error)

	// SaveRun persists every dirty field of r. The adapter consults
	// r.Changes() to dispatch to the existing tuned SQL methods. After a
	// successful save the adapter calls r.ResetChanges() so the aggregate
	// can be saved again within the same tx if necessary.
	SaveRun(ctx context.Context, tx *sqlx.Tx, r *run.Run) error

	// HasActiveSchedule returns true when a PENDING or RUNNING Run exists
	// for `name`. Cross-aggregate query used by SchedulePolicy.
	HasActiveSchedule(ctx context.Context, name string) (bool, error)

	// GetActiveScheduler returns the active Run on `name`, or nil when
	// none exists. Used by CancelSchedule to find which run to cancel.
	GetActiveScheduler(ctx context.Context, name string) (*run.Run, error)

	// GetLastRunPerSchedule returns the most recent Run per schedule name.
	// Read-only projection for ListAllSchedules.
	GetLastRunPerSchedule(ctx context.Context) (map[string]LastRunSummary, error)
}

// LastRunSummary is the projection row returned by GetLastRunPerSchedule.
type LastRunSummary struct {
	ScheduleName string
	ScheduleID   uuid.UUID
	Status       run.SchedulerStatus
	CreatedAt    time.Time
	IsRunning    bool
}
