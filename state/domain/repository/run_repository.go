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
// aggregates and to ask narrow cross-Run queries (HasActiveRun,
// GetActiveRun, GetLastRunPerSchedule). Every read names the tenant it reads
// for and sees only that tenant's runs: another tenant's run reads as not
// found. A save writes the run under the run's own tenant. Implementations
// live in state/adapters/postgres.
type RunRepository interface {
	// GetRun loads the tenant's Run snapshot for read-only use. No row lock.
	// Returns the adapter's ErrNotFound when the tenant has no such run; the
	// adapter wraps repo-level not-found into a domain-friendly form at the
	// application boundary.
	GetRun(ctx context.Context, tenantID string, id uuid.UUID) (*run.Run, error)

	// LoadRunForUpdate loads the tenant's Run inside the repository's bound
	// transaction with SELECT ... FOR UPDATE. The aggregate is intended to be
	// mutated; call SaveRun before tx commit.
	LoadRunForUpdate(ctx context.Context, tenantID string, id uuid.UUID) (*run.Run, error)

	// SaveRun persists every dirty field of r under r's tenant. The adapter
	// consults r.Changes() to dispatch to the existing tuned SQL methods.
	// After a successful save the adapter calls r.ResetChanges() so the
	// aggregate can be saved again within the same tx if necessary. Saving a
	// newly constructed Run whose id is already persisted writes nothing and
	// returns run.ErrRunAlreadyExists; the transaction stays usable.
	SaveRun(ctx context.Context, r *run.Run) error

	// HasActiveRun returns true when the tenant has a PENDING, RUNNING or
	// CANCELLING Run for `name`. Cross-aggregate query used by
	// SchedulePolicy.
	HasActiveRun(ctx context.Context, tenantID, name string) (bool, error)

	// GetActiveRun returns the tenant's active Run on `name`, or nil when
	// none exists. Used by CancelSchedule to find which run to cancel.
	GetActiveRun(ctx context.Context, tenantID, name string) (*run.Run, error)

	// GetLastRunPerSchedule returns the tenant's most recent Run per schedule
	// name. Read-only projection for ListAllSchedules.
	GetLastRunPerSchedule(ctx context.Context, tenantID string) (map[string]LastRunSummary, error)

	// RunExists reports whether the tenant has a persisted run with id. It
	// reads inside the bound transaction and needs one.
	RunExists(ctx context.Context, tenantID string, id uuid.UUID) (bool, error)

	// ListUnplannedActive returns the ids of the tenant's PENDING, RUNNING or
	// CANCELLING runs whose tasks state did not plan (planned_at is NULL),
	// oldest first. It reads inside the bound transaction and needs one.
	ListUnplannedActive(ctx context.Context, tenantID string) ([]uuid.UUID, error)

	// CountReleaseRuns counts the tenant's runs of kind whose release_id is
	// releaseID. It reads inside the bound transaction, so it sees the runs
	// the transaction wrote, and needs one.
	CountReleaseRuns(ctx context.Context, tenantID, releaseID string, kind run.Kind) (int, error)
}

// LastRunSummary is the projection row returned by GetLastRunPerSchedule.
type LastRunSummary struct {
	ScheduleName string
	RunID        uuid.UUID
	Status       run.RunStatus
	CreatedAt    time.Time
	IsActive     bool
}
