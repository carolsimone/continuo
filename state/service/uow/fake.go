package uow

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/ports"
	"github.com/jmoiron/sqlx"
)

// FakeUnitOfWork is an in-memory UnitOfWork for handler unit tests. Construct
// it with fake repo implementations satisfying the postgres.* interfaces.
// Tx() returns nil — handler-test repo fakes must accept and ignore the nil
// *sqlx.Tx parameter on *Tx methods without dereferencing it.
//
// The aggregate-level accessors (Run, Catalog, Outbox, TaskCollection, Clock)
// default to nil and can be set via the corresponding setter methods.
// Clock() falls back to ports.SystemClock{} when no override is provided.
type FakeUnitOfWork struct {
	Scheduler         postgres.SchedulerTrackerRepository
	Task              postgres.TaskTrackerRepository
	TaskExecution     postgres.TaskExecutionRepository
	ScheduleCatalog   postgres.ScheduleCatalogRepository
	OutboxStore       postgres.OutboxRepository
	MessageProcessing messageprocessing.Repository

	BeginCalled    int
	CommitCalled   int
	RollbackCalled int

	inTx bool

	runRepo     ports.RunRepository
	catalogRepo ports.ScheduleCatalogRepository
	outboxPub   ports.OutboxPublisher
	taskColl    run.TaskCollection
	clock       ports.Clock
}

func (f *FakeUnitOfWork) SchedulerRepo() postgres.SchedulerTrackerRepository {
	return f.Scheduler
}
func (f *FakeUnitOfWork) TaskRepo() postgres.TaskTrackerRepository { return f.Task }
func (f *FakeUnitOfWork) TaskExecutionRepo() postgres.TaskExecutionRepository {
	return f.TaskExecution
}
func (f *FakeUnitOfWork) ScheduleCatalogRepo() postgres.ScheduleCatalogRepository {
	return f.ScheduleCatalog
}
func (f *FakeUnitOfWork) OutboxRepo() postgres.OutboxRepository               { return f.OutboxStore }
func (f *FakeUnitOfWork) MessageProcessingRepo() messageprocessing.Repository { return f.MessageProcessing }
func (f *FakeUnitOfWork) Tx() *sqlx.Tx                                        { return nil }

func (f *FakeUnitOfWork) Begin(_ context.Context) error {
	if f.inTx {
		return errors.New("transaction already in progress")
	}
	f.inTx = true
	f.BeginCalled++
	return nil
}

func (f *FakeUnitOfWork) Commit() error {
	if !f.inTx {
		return errors.New("no transaction in progress")
	}
	f.inTx = false
	f.CommitCalled++
	return nil
}

func (f *FakeUnitOfWork) Rollback() error {
	if !f.inTx {
		return nil
	}
	f.inTx = false
	f.RollbackCalled++
	return nil
}

// Aggregate-level accessors. Return whatever was set via the setter methods.

func (f *FakeUnitOfWork) Run() ports.RunRepository                 { return f.runRepo }
func (f *FakeUnitOfWork) Outbox() ports.OutboxPublisher            { return f.outboxPub }
func (f *FakeUnitOfWork) TaskCollection() run.TaskCollection       { return f.taskColl }

// Catalog returns the aggregate-level ports.ScheduleCatalogRepository set via SetCatalogRepo.
func (f *FakeUnitOfWork) Catalog() ports.ScheduleCatalogRepository { return f.catalogRepo }

// Clock returns the configured clock, falling back to ports.SystemClock{} when
// no override has been set.
func (f *FakeUnitOfWork) Clock() ports.Clock {
	if f.clock == nil {
		return ports.SystemClock{}
	}
	return f.clock
}

// Setters allow individual tests to inject aggregate-level fakes without
// affecting tests that only need the low-level tracker repos.
func (f *FakeUnitOfWork) SetRunRepo(r ports.RunRepository)                 { f.runRepo = r }
func (f *FakeUnitOfWork) SetCatalogRepo(r ports.ScheduleCatalogRepository) { f.catalogRepo = r }
func (f *FakeUnitOfWork) SetOutboxPublisher(p ports.OutboxPublisher)       { f.outboxPub = p }
func (f *FakeUnitOfWork) SetTaskCollection(tc run.TaskCollection)          { f.taskColl = tc }
func (f *FakeUnitOfWork) SetClock(c ports.Clock)                           { f.clock = c }
