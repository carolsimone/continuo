// execution-controller/service/uow/uow.go
// Package uow declares the Unit-of-Work port execution-controller's handlers
// orchestrate over, so they compose repository work into one transaction
// without seeing sqlx or any concrete adapter. The Postgres implementation
// lives in adapters/postgres. Matches run-controller/service/uow's UnitOfWork shape so
// the parser+binding+handler pattern is identical across services.
package uow

import (
	"context"

	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
)

// UnitOfWork exposes the repos execution-controller's handlers need plus
// tx lifecycle. MessageProcessingRepo() returns a fresh repo bound to the
// current tx (or the autocommit DB if no tx is active), mirroring
// run-controller/service/uow's pattern.
type UnitOfWork interface {
	OutboxRepo() pkgoutbox.Repository
	DeploymentsRepo() repository.DeploymentRepository
	ValidationAggregateRepo() repository.ValidationAggregateRepository
	CancelledRunsRepo() repository.CancelledRunsRepository
	MessageProcessingRepo() messageprocessing.Repository

	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
	// AfterCommit registers fn to run once the current transaction commits;
	// a rollback discards it. It records effects outside the database, such
	// as a metric, that must count only what committed.
	AfterCommit(fn func())
}
