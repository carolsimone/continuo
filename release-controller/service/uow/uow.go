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
	ServiceProdRepo() repository.ServiceProdRepository
	OutboxRepo() pkgoutbox.Repository
	MessageProcessingRepo() messageprocessing.Repository
	Begin(ctx context.Context) error
	Commit() error
	Rollback() error

	// LockReleaseQueue acquires a tx-scoped advisory lock that serialises
	// AdvanceQueue across all callers of release-controller. Concurrent HTTP
	// POST + stream-consumer callers would otherwise both observe "no active
	// release", both promote the same queued row, and write duplicate
	// release.requested:v1 outbox entries. The lock is released automatically
	// on Commit or Rollback. Must be called inside Begin.
	LockReleaseQueue(ctx context.Context) error
}
