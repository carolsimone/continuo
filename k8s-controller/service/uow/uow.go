package uow

import (
	"context"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
)

// UnitOfWork manages a single database transaction and exposes the
// transaction-scoped repositories handlers need. Bindings own its lifecycle:
// Begin → (work) → Commit, with Rollback on any failure.
type UnitOfWork interface {
	OutboxRepo() pkgoutbox.Repository
	MessageProcessingRepo() messageprocessing.Repository
	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
}
