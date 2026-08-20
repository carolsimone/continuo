// executor-controller/service/uow/uow.go
// Package uow declares the Unit-of-Work port executor-controller's handlers
// orchestrate over, so they compose repository work into one transaction
// without seeing sqlx or any concrete adapter. The Postgres implementation
// lives in adapters/postgres. Matches state/service/uow's UnitOfWork shape so
// the parser+binding+handler pattern is identical across services.
package uow

import (
	"context"

	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/jmoiron/sqlx"
)

// UnitOfWork exposes the repos executor-controller's handlers need plus
// tx lifecycle. MessageProcessingRepo() returns a fresh repo bound to the
// current tx (or the autocommit DB if no tx is active), mirroring
// state/service/uow's pattern.
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
