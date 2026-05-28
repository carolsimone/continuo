package uow

import (
	"context"

	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// UnitOfWork manages a single Postgres transaction and exposes the
// transaction-scoped repositories handlers need. Bindings own its
// lifecycle: Begin → (work) → Commit, with Rollback on any failure.
type UnitOfWork interface {
	ReleaseRepo() repository.ReleaseRepository
	CurrentProdRepo() repository.CurrentProdRepository
	OutboxRepo() pkgoutbox.Repository
	MessageProcessingRepo() messageprocessing.Repository
	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
}
