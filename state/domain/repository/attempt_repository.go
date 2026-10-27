package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// AttemptKey names one attempt of one task of one tenant.
type AttemptKey struct {
	TenantID string
	TaskID   uuid.UUID
	Attempt  int32
}

// AttemptRepository serves state's attempt reconcile: the attempts due to be
// asked about, the runs that stalled, and, inside the unit of work's
// transaction, the lock and the record of one question. Due times compare
// against the database clock. Its scans (DueAttempts, StalledRuns, DueStops)
// read every tenant and name each attempt or run with its tenant; every lock
// and write acts on the tenant its key or run names. CountCancelling counts
// the runs of every tenant.
type AttemptRepository interface {
	// DueAttempts returns up to limit attempts of any tenant that are
	// dispatched, queued or running, whose next check is due and whose run
	// has not ended, the longest due first.
	DueAttempts(ctx context.Context, limit int) ([]AttemptKey, error)

	// StalledRuns returns up to limit pending, running or cancelling runs of
	// any tenant that have no task dispatched, queued, running or stopping,
	// no task waiting for a node lease whose holder is one of those, and
	// whose progress clock is older than idle, the oldest first: a task
	// waiting behind another run's task in flight counts as in flight. A
	// lease, a holder and an older waiter count only within the run's tenant.
	StalledRuns(ctx context.Context, idle time.Duration, limit int) ([]RunRef, error)

	// LockQuestion locks the attempt FOR UPDATE SKIP LOCKED and returns what
	// a question about it carries, read from its task, its run and the node
	// its run's release holds. found is false, and nothing is locked, when
	// another transaction holds the attempt or it is no longer open, due, or
	// of a run that has not ended.
	LockQuestion(ctx context.Context, key AttemptKey) (q run.Question, found bool, err error)

	// MarkQuestioned records one more question about the attempt, due again
	// next after the database clock. It serves a dispatch question and a
	// re-sent cancel alike.
	MarkQuestioned(ctx context.Context, key AttemptKey, next time.Duration) error

	// DueStops returns up to limit cancelling runs of any tenant that have an
	// open attempt state asked to stop whose next check is due, the longest
	// due first.
	DueStops(ctx context.Context, limit int) ([]RunRef, error)

	// LockStops locks, FOR UPDATE SKIP LOCKED, every open attempt of the
	// cancelling run that state asked to stop, and returns them with the
	// run's cancelled_by and cancellation_reason: the cancel to send again,
	// naming each attempt that has not answered. found is false, and nothing
	// is held, when the run is no longer cancelling, another transaction holds
	// its attempts, none is open or none is due any more.
	LockStops(ctx context.Context, ref RunRef) (q run.StopQuestion, found bool, err error)

	// CountCancelling returns how many runs are cancelling and how many of
	// them were cancelled more than overdueAfter ago on the database clock.
	CountCancelling(ctx context.Context, overdueAfter time.Duration) (total, overdue int, err error)
}
