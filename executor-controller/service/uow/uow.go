// Package uow provides a Unit-of-Work abstraction over executor-controller's
// repositories so handlers can orchestrate over repos without seeing sqlx.
// Matches state/service/uow's UnitOfWork shape so the parser+binding+handler
// pattern is identical across services. The concrete Postgres implementation
// lives in adapters/postgres.
package uow

import (
	"context"

	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/jmoiron/sqlx"
)

// UnitOfWork exposes the repos executor-controller's handlers need plus
// tx lifecycle. Each repo accessor returns a fresh repo bound to the current tx
// (or the autocommit DB if no tx is active), mirroring state/service/uow's
// pattern.
type UnitOfWork interface {
	OutboxRepo() pkgoutbox.Repository
	DeploymentsRepo() repository.DeploymentRepository
	ValidationAggregateRepo() repository.ValidationAggregateRepository
	CancelledSchedulesRepo() repository.CancelledSchedulesRepository
	MessageProcessingRepo() messageprocessing.Repository

	// Tx returns the underlying *sqlx.Tx during a transaction, or nil otherwise.
	Tx() *sqlx.Tx

	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
}
