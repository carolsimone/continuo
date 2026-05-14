package uow

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/jmoiron/sqlx"
)

// FakeUnitOfWork is an in-memory UnitOfWork for handler unit tests. Construct
// it with fake repo implementations satisfying the postgres.* interfaces.
// Tx() returns nil — handler-test repo fakes must accept and ignore the nil
// *sqlx.Tx parameter on *Tx methods without dereferencing it.
type FakeUnitOfWork struct {
	Scheduler         postgres.SchedulerTrackerRepository
	Task              postgres.TaskTrackerRepository
	TaskExecution     postgres.TaskExecutionRepository
	Catalog           postgres.ScheduleCatalogRepository
	Outbox            postgres.OutboxRepository
	MessageProcessing messageprocessing.Repository

	BeginCalled    int
	CommitCalled   int
	RollbackCalled int

	inTx bool
}

func (f *FakeUnitOfWork) SchedulerRepo() postgres.SchedulerTrackerRepository {
	return f.Scheduler
}
func (f *FakeUnitOfWork) TaskRepo() postgres.TaskTrackerRepository { return f.Task }
func (f *FakeUnitOfWork) TaskExecutionRepo() postgres.TaskExecutionRepository {
	return f.TaskExecution
}
func (f *FakeUnitOfWork) ScheduleCatalogRepo() postgres.ScheduleCatalogRepository {
	return f.Catalog
}
func (f *FakeUnitOfWork) OutboxRepo() postgres.OutboxRepository               { return f.Outbox }
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
