package deployer

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Repository operates on the executor_deployments command queue.
//
// GetDueBatch MUST be called inside a transaction the caller holds until the
// per-row MarkDeployed / Reschedule / MarkFailed completes, because the batch
// uses FOR UPDATE SKIP LOCKED.
type Repository interface {
	Create(ctx context.Context, d *Deployment) error
	// GetDueBatch returns up to limit pending rows whose next_attempt_at has
	// passed, oldest first, locked FOR UPDATE SKIP LOCKED.
	GetDueBatch(ctx context.Context, limit int) ([]*Deployment, error)
	MarkDeployed(ctx context.Context, id uuid.UUID) error
	// Reschedule increments retry_count, sets next_attempt_at, and records the
	// transient error. The row stays 'pending'.
	Reschedule(ctx context.Context, id uuid.UUID, nextAttemptAt time.Time, errMsg string) error
	MarkFailed(ctx context.Context, id uuid.UUID, errMsg string) error
}
