// Package uow provides a Unit-of-Work abstraction over state's Postgres
// repositories so handlers can orchestrate over repos without seeing sqlx.
package uow

import (
	"context"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/jmoiron/sqlx"
)

// UnitOfWork exposes the repos state's handlers need plus tx lifecycle.
// Concrete implementations return the same repo instance regardless of tx
// state; the tx-aware behavior lives in repo methods that take a *sqlx.Tx
// parameter explicitly (the *Tx family). Handlers obtain the current tx via
// Tx() and pass it to those methods.
//
// MessageProcessingRepo returns a fresh repo bound to the current tx (or
// the autocommit DB if no tx is active), mirroring orchestrator's pattern.
//
// The aggregate-level accessors (Run, Catalog, Outbox, TaskCollection, Clock,
// TaskExecutions) are used by handlers that operate on domain aggregates.
type UnitOfWork interface {
	MessageProcessingRepo() messageprocessing.Repository

	// Aggregate-level accessors used by handler bodies.
	Run() repository.RunRepository
	Catalog() repository.ScheduleCatalogRepository
	Outbox() ports.OutboxPublisher
	TaskCollection() run.TaskCollection
	Clock() ports.Clock
	TaskExecutions() repository.TaskExecutionWriter

	// Tx returns the underlying *sqlx.Tx during a transaction, or nil otherwise.
	Tx() *sqlx.Tx

	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
}
