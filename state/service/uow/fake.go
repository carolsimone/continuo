package uow

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/jmoiron/sqlx"
)

// FakeUnitOfWork is an in-memory UnitOfWork for handler unit tests.
// Tx() returns nil — handler-test repo fakes must accept and ignore the nil
// *sqlx.Tx parameter on *Tx methods without dereferencing it.
//
// The aggregate-level accessors (Run, Catalog, Outbox, TaskCollection, Clock)
// default to nil and can be set via the corresponding setter methods.
// Clock() falls back to ports.SystemClock{} when no override is provided.
type FakeUnitOfWork struct {
	MessageProcessing messageprocessing.Repository

	BeginCalled    int
	CommitCalled   int
	RollbackCalled int

	inTx bool

	runRepo     repository.RunRepository
	catalogRepo repository.ScheduleCatalogRepository
	outboxPub   ports.OutboxPublisher
	taskColl    run.TaskCollection
	clock       ports.Clock

	// TaskExecutionWriter backs TaskExecutions(); set it to capture or assert
	// recorded-execution writes in handler tests.
	TaskExecutionWriter repository.TaskExecutionWriter
}

func (f *FakeUnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	return f.MessageProcessing
}
func (f *FakeUnitOfWork) Tx() *sqlx.Tx { return nil }

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

func (f *FakeUnitOfWork) Run() repository.RunRepository      { return f.runRepo }
func (f *FakeUnitOfWork) Outbox() ports.OutboxPublisher      { return f.outboxPub }
func (f *FakeUnitOfWork) TaskCollection() run.TaskCollection { return f.taskColl }

// TaskExecutions returns the aggregate-level repository.TaskExecutionWriter set
// via the TaskExecutionWriter field.
func (f *FakeUnitOfWork) TaskExecutions() repository.TaskExecutionWriter {
	return f.TaskExecutionWriter
}

// Catalog returns the aggregate-level repository.ScheduleCatalogRepository set via SetCatalogRepo.
func (f *FakeUnitOfWork) Catalog() repository.ScheduleCatalogRepository { return f.catalogRepo }

// Clock returns the configured clock, falling back to ports.SystemClock{} when
// no override has been set.
func (f *FakeUnitOfWork) Clock() ports.Clock {
	if f.clock == nil {
		return ports.SystemClock{}
	}
	return f.clock
}

// Setters allow individual tests to inject only the aggregate-level fakes they need.
func (f *FakeUnitOfWork) SetRunRepo(r repository.RunRepository)                 { f.runRepo = r }
func (f *FakeUnitOfWork) SetCatalogRepo(r repository.ScheduleCatalogRepository) { f.catalogRepo = r }
func (f *FakeUnitOfWork) SetOutboxPublisher(p ports.OutboxPublisher)            { f.outboxPub = p }
func (f *FakeUnitOfWork) SetTaskCollection(tc run.TaskCollection)               { f.taskColl = tc }
func (f *FakeUnitOfWork) SetClock(c ports.Clock)                                { f.clock = c }

// Compile-time interface assertion.
var _ UnitOfWork = (*FakeUnitOfWork)(nil)
