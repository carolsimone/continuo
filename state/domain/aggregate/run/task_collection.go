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
	// GetStatus returns the current status of a single task. The second
	// return is false when the row does not exist; err is non-nil only
	// for genuine I/O failures.
	GetStatus(ctx context.Context, taskID uuid.UUID) (status TaskStatus, exists bool, err error)

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

	// UpdateStatusIfChanged updates the status (and retry_count) of one task
	// when the new values differ from current. Returns the number of rows
	// affected (0 if unchanged or missing).
	UpdateStatusIfChanged(ctx context.Context, taskID uuid.UUID, status TaskStatus, retryCount int32) (rowsAffected int, err error)

	// BulkCreate inserts every task in one statement.
	BulkCreate(ctx context.Context, tasks []Task) error

	// BulkCancel marks every non-terminal task of the given run as
	// CANCELLED, stamping cancelled_by. Returns the number of rows updated.
	BulkCancel(ctx context.Context, runID uuid.UUID, cancelledBy string) (rowsAffected int, err error)

	// Update persists the mutated fields of one Task. Used by the
	// ResetTaskToPending path.
	Update(ctx context.Context, t Task) error
}
