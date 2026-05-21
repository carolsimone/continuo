// Package uow provides a Unit-of-Work abstraction over state's Postgres
// repositories so handlers can orchestrate over repos without seeing the
// underlying database driver or transaction type.
package uow

import (
	"context"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	ports "github.com/carolsimone/continuo/state/service/ports"
)

// UnitOfWork exposes the repos state's handlers need plus tx lifecycle.
// Each accessor returns a repository bound to the UoW's active transaction:
// the repo's write/FOR-UPDATE methods run inside that tx, while read/query
// methods use the autocommit DB. Handlers never touch the transaction directly.
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

	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
}
