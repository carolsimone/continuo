package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// AttemptKey names one attempt of one task.
type AttemptKey struct {
	TaskID  uuid.UUID
	Attempt int32
}

// AttemptRepository serves state's attempt reconcile: the attempts due to be
// asked about, the runs that stalled, and, inside the unit of work's
// transaction, the lock and the record of one question. Due times compare
// against the database clock. The attempt questions (DueAttempts,
// LockQuestion) belong to the tenant the repository is bound to; StalledRuns
// reads every run of the install.
type AttemptRepository interface {
	// DueAttempts returns up to limit attempts that are dispatched, queued
	// or running, whose next check is due and whose run has not ended, the
	// longest due first.
	DueAttempts(ctx context.Context, limit int) ([]AttemptKey, error)

	// StalledRuns returns up to limit pending or running runs that have no
	// task dispatched, queued or running and whose progress clock is older
	// than idle, the oldest first. It reads the runs of every tenant: a run
	// names no tenant, and failing a stalled run reads no tenant's topology.
	StalledRuns(ctx context.Context, idle time.Duration, limit int) ([]uuid.UUID, error)

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

	// DueStops returns up to limit cancelling runs that have an open attempt
	// state asked to stop whose next check is due, the longest due first. It
	// reads the runs of every tenant: a cancel names no tenant.
	DueStops(ctx context.Context, limit int) ([]uuid.UUID, error)

	// LockStops locks, FOR UPDATE SKIP LOCKED, every open attempt of the
	// cancelling run that state asked to stop, and returns them with the
	// run's cancelled_by and cancellation_reason: the cancel to send again,
	// naming each attempt that has not answered. found is false, and nothing
	// is held, when the run is no longer cancelling, another transaction holds
	// its attempts, none is open or none is due any more.
	LockStops(ctx context.Context, runID uuid.UUID) (q run.StopQuestion, found bool, err error)

	// CountCancelling returns how many runs are cancelling and how many of
	// them were cancelled more than overdueAfter ago on the database clock.
	CountCancelling(ctx context.Context, overdueAfter time.Duration) (total, overdue int, err error)
}
