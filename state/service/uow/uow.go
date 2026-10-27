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
// The aggregate-level accessors (Run, Catalog, Outbox, TaskCollection, Clock)
// are used by handlers that operate on domain aggregates.
// Topology serves the promoted-topology tables and the live-release pointer;
// Materializations serves the record of what each node last loaded; TaskGraph
// serves a run's ready candidates and pending descendants.
type UnitOfWork interface {
	MessageProcessingRepo() messageprocessing.Repository

	// Aggregate-level accessors used by handler bodies.
	Run() repository.RunRepository
	Catalog() repository.ScheduleCatalogRepository
	Outbox() ports.OutboxPublisher
	// TaskCollection serves the tasks of the runs of tenantID inside the
	// transaction; the node write leases it takes are tenantID's.
	TaskCollection(tenantID string) run.TaskCollection
	Clock() ports.Clock
	Topology() repository.TopologyRepository
	Materializations() repository.MaterializationRepository
	// TaskGraph reads how the tasks of a run of tenantID connect over the
	// edges of the run's release, inside the transaction.
	TaskGraph(tenantID string) run.TaskGraph
	// Attempts serves the attempt reconcile: the due attempts, stalled runs
	// and due stops of every tenant, and the lock and record of one question,
	// under the tenant its key or run names, inside the transaction.
	Attempts() repository.AttemptRepository
	// Leases serves the lease waker across every tenant: the waiters a free
	// node can be granted to, the repair of leases whose holder task has
	// ended, and the count of tasks waiting for a lease.
	Leases() repository.LeaseRepository
	// CommandRequests serves the records of the commands that create or
	// cancel a run, inside the transaction.
	CommandRequests() repository.CommandRequestRepository

	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
	// AfterCommit registers fn to run once the current transaction commits;
	// a rollback, or a commit that fails, discards it. It records effects
	// outside the database, such as a metric, that must count only what
	// committed.
	AfterCommit(fn func())
}
