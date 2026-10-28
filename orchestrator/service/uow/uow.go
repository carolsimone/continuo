package uow

import (
	"context"

	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
)

// UnitOfWork manages a single database transaction and exposes the
// transaction-scoped repositories handlers need. Bindings own its lifecycle:
// Begin → (work) → Commit, with Rollback on any failure.
type UnitOfWork interface {
	MessageProcessingRepo() messageprocessing.Repository
	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
}
