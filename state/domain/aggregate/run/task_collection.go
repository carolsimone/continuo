package run

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/state/domain/topology"
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
	// LoadAttemptState returns what a status update needs of one task: its
	// status, its stored attempt number, its retry budget, its node key, its
	// identity (job name and node) and its first dispatch time, locking the
	// row FOR UPDATE so concurrent
	// task.status.updated deliveries for the same task serialize. The attempt
	// is the discriminator the aggregate uses to order updates: a strictly
	// newer attempt supersedes, an older one is stale, and the same attempt
	// distinguishes a first terminal from a replay/late duplicate. The second
	// return is false when the row does not exist; err is non-nil only for
	// genuine I/O failures.
	LoadAttemptState(ctx context.Context, taskID uuid.UUID) (state TaskAttemptState, exists bool, err error)

	// LoadAttempt returns attempt number attempt of the task, locking its row
	// FOR UPDATE so concurrent writers of that attempt serialize. The second
	// return is false when the task has no such attempt; err is non-nil only
	// for genuine I/O failures. A caller that also locks the task row
	// (LoadAttemptState) takes that lock first.
	LoadAttempt(ctx context.Context, taskID uuid.UUID, attempt int32) (Attempt, bool, error)

	// SaveAttempt inserts the attempt or replaces the stored one with the same
	// (task, number). A new attempt with no DispatchedAt is stamped with the
	// database clock; a stored dispatch time is never changed. The attempt's
	// last report time is the database clock and never moves backwards, and an
	// attempt that is final has no next check.
	SaveAttempt(ctx context.Context, a Attempt) error

	// CancelOpenAttempts marks every dispatched, queued and running attempt of
	// the tasks of the given run as cancelled, with no next check, and returns
	// how many it changed. Attempts that have ended are left as they are.
	CancelOpenAttempts(ctx context.Context, runID uuid.UUID) (int, error)

	// HasFailed reports whether any task in the given run ended FAILED or
	// CANCELLED — a terminal unsuccessful task. Pending and running tasks do
	// not match. The run's finalized outcome is failed when it is true.
	HasFailed(ctx context.Context, runID uuid.UUID) (bool, error)

	// HasRetryableFailed reports whether any task in the given run is
	// FAILED and still has retries left (attempt < max_retries).
	HasRetryableFailed(ctx context.Context, runID uuid.UUID) (bool, error)

	// HasNonSucceeded reports whether any task in the given run is in a
	// status other than SUCCEEDED. Used by rerun/rebase eligibility checks.
	HasNonSucceeded(ctx context.Context, runID uuid.UUID) (bool, error)

	// GetByNode returns the task at the (service, schema, table) node within
	// the given run. Returns ErrTaskNotFound if no row matches.
	GetByNode(ctx context.Context, runID uuid.UUID, node NodeID) (Task, error)

	// ListRunTasks returns every task of the given run, in node-key order.
	ListRunTasks(ctx context.Context, runID uuid.UUID) ([]Task, error)

	// RunReleasesOf returns, for each task of taskIDs, the release its run
	// is stamped with. A task that does not exist, or whose run names no
	// release, is absent from the map.
	RunReleasesOf(ctx context.Context, taskIDs []uuid.UUID) (map[uuid.UUID]string, error)

	// GetByID returns one task by id. Returns ErrTaskNotFound if no row
	// matches.
	GetByID(ctx context.Context, taskID uuid.UUID) (Task, error)

	// SetStatusAndAttempt writes status and attempt for one task. It does
	// not overwrite a cancelled task. Returns rowsAffected (0 if the task is
	// cancelled or the row is missing). It applies no attempt logic — the Run
	// aggregate decides when a write is warranted; this just persists it.
	SetStatusAndAttempt(ctx context.Context, taskID uuid.UUID, status TaskStatus, retryCount int32) (rowsAffected int, err error)

	// BulkCreate inserts every task of a run inside the caller's
	// transaction, so the whole set commits or rolls back together.
	BulkCreate(ctx context.Context, tasks []Task) error

	// MarkDispatched stamps dispatched_at with at on each task of taskIDs
	// that is still pending and undispatched, and returns the ids it stamped.
	MarkDispatched(ctx context.Context, taskIDs []uuid.UUID, at time.Time) ([]uuid.UUID, error)

	// MarkSkipped sets status skipped on each task of taskIDs that is still
	// pending and undispatched, and returns how many tasks it changed.
	MarkSkipped(ctx context.Context, taskIDs []uuid.UUID) (int, error)

	// BulkCancel marks every non-terminal task of the given run as
	// CANCELLED, stamping cancelled_by and cancelled_at with cancelledAt — the
	// same authoritative instant the Run aggregate used for the run-level
	// cancellation. Returns the number of rows updated.
	BulkCancel(ctx context.Context, runID uuid.UUID, cancelledBy string, cancelledAt time.Time) (rowsAffected int, err error)
}

// TaskAttemptState is one task as a status update reads it: its status, the
// attempt it is at, its retry budget, its node in the run's release, its
// identity (the job name and node it was planned with) and when it was first
// dispatched. NodeKey is empty on a task whose run names no release;
// DispatchedAt is nil on a task that was never dispatched.
type TaskAttemptState struct {
	Status       TaskStatus
	RetryCount   int32
	MaxRetries   int32
	NodeKey      topology.NodeKey
	JobName      string
	ServiceName  string
	SchemaName   string
	TableName    string
	DispatchedAt *time.Time
}
