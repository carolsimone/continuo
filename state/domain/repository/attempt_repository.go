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
// against the database clock.
type AttemptRepository interface {
	// DueAttempts returns up to limit attempts that are dispatched, queued
	// or running, whose next check is due and whose run has not ended, the
	// longest due first.
	DueAttempts(ctx context.Context, limit int) ([]AttemptKey, error)

	// StalledRuns returns up to limit pending or running runs that have no
	// task dispatched, queued or running and whose progress clock is older
	// than idle, the oldest first.
	StalledRuns(ctx context.Context, idle time.Duration, limit int) ([]uuid.UUID, error)

	// LockQuestion locks the attempt FOR UPDATE SKIP LOCKED and returns what
	// a question about it carries, read from its task, its run and the node
	// its run's release holds. found is false, and nothing is locked, when
	// another transaction holds the attempt or it is no longer open, due, or
	// of a run that has not ended.
	LockQuestion(ctx context.Context, key AttemptKey) (q run.Question, found bool, err error)

	// MarkQuestioned records one more question about the attempt, due again
	// next after the database clock.
	MarkQuestioned(ctx context.Context, key AttemptKey, next time.Duration) error
}
