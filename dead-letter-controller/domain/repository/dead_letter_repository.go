// Package repository declares the persistence port for dead letters.
package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/google/uuid"
)

// DeadLetterRepository stores dead letters. Insert keeps the first row per
// DedupKey and reports whether it inserted. Get returns deadletter.ErrNotFound
// for an unknown id. LockForRedrive returns the existing rows among ids, locked
// until the transaction ends. DeleteExpired removes up to limit rows whose
// original message predates originalBefore and returns them. InsertBatch stores
// each dead letter not already stored, by DedupKey, and returns how many it
// inserted.
type DeadLetterRepository interface {
	Insert(ctx context.Context, dl deadletter.DeadLetter) (bool, error)
	InsertBatch(ctx context.Context, dls []deadletter.DeadLetter) (int, error)
	Get(ctx context.Context, id uuid.UUID) (deadletter.DeadLetter, error)
	List(ctx context.Context, f deadletter.Filter) ([]deadletter.DeadLetter, error)
	CountOpen(ctx context.Context) (int64, error)
	LockForRedrive(ctx context.Context, ids []uuid.UUID) ([]deadletter.DeadLetter, error)
	SaveRedrive(ctx context.Context, dl deadletter.DeadLetter) error
	DeleteExpired(ctx context.Context, originalBefore time.Time, limit int) ([]deadletter.DeadLetter, error)
	Backlog(ctx context.Context) ([]deadletter.BacklogRow, error)
}
