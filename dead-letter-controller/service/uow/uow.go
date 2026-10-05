// Package uow declares the transaction seam of dead-letter-controller. The
// Postgres implementation lives in adapters/postgres.
package uow

import (
	"context"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/outbox"
)

// UnitOfWork is one Postgres transaction and the repositories bound to it.
// Callers Begin, work, and Commit, with Rollback on any failure.
type UnitOfWork interface {
	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
	DeadLetters() repository.DeadLetterRepository
	Outbox() outbox.Repository
}
