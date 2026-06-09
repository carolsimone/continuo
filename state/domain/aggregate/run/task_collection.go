package run

import (
	"context"

	"github.com/google/uuid"
)

// TaskCollection is the port the Run aggregate calls to look up or mutate
// child task state without loading the full collection. The adapter that
// satisfies it is bound to the current Unit-of-Work transaction; the
// aggregate is unaware of the transaction.
//
// Read methods return narrow predicates (one row by id, or a boolean).
// Write methods are targeted SQL operations the aggregate orchestrates
// inside its own method bodies.
type TaskCollection interface {
	// LoadStatusAndAttempt returns the current status and stored retry_count
	// (the attempt number) of a single task, locking the row FOR UPDATE so
	// concurrent task.status.updated deliveries for the same task serialize.
	// The attempt is the discriminator the aggregate uses to order updates:
	// a strictly newer attempt supersedes, an older one is stale, and the same
	// attempt distinguishes a first terminal from a replay/late duplicate. The
	// third return is false when the row does not exist; err is non-nil only
	// for genuine I/O failures.
	LoadStatusAndAttempt(ctx context.Context, taskID uuid.UUID) (status TaskStatus, retryCount int32, exists bool, err error)

	// Exists checks whether a task_tracker row with the given id is present.
	Exists(ctx context.Context, taskID uuid.UUID) (bool, error)

	// HasFailed reports whether any task in the given run is in status
	// FAILED.
	HasFailed(ctx context.Context, runID uuid.UUID) (bool, error)

	// HasRetryableFailed reports whether any task in the given run is
	// FAILED and still has retries left (retry_count < max_retries).
	HasRetryableFailed(ctx context.Context, runID uuid.UUID) (bool, error)

	// HasNonSucceeded reports whether any task in the given run is in a
	// status other than SUCCEEDED. Used by rerun/rebase eligibility checks.
	HasNonSucceeded(ctx context.Context, runID uuid.UUID) (bool, error)

	// GetByNode returns the task at the (service, schema, table) node within
	// the given run. Returns ErrTaskNotFound if no row matches.
	GetByNode(ctx context.Context, runID uuid.UUID, node NodeID) (Task, error)

	// SetStatusAndAttempt writes status and retry_count for one task. It does
	// not overwrite a cancelled task. Returns rowsAffected (0 if the task is
	// cancelled or the row is missing). It applies no attempt logic — the Run
	// aggregate decides when a write is warranted; this just persists it.
	SetStatusAndAttempt(ctx context.Context, taskID uuid.UUID, status TaskStatus, retryCount int32) (rowsAffected int, err error)

	// BulkCreate inserts every task in one statement.
	BulkCreate(ctx context.Context, tasks []Task) error

	// BulkCancel marks every non-terminal task of the given run as
	// CANCELLED, stamping cancelled_by. Returns the number of rows updated.
	BulkCancel(ctx context.Context, runID uuid.UUID, cancelledBy string) (rowsAffected int, err error)
}
