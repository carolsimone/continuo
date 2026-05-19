// executor-controller/service/uow/fake.go
package uow

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/jmoiron/sqlx"
)

// FakeUnitOfWork is an in-memory UnitOfWork for handler unit tests.
// Construct it with fake repo implementations satisfying the
// pkgoutbox.Repository and messageprocessing.* interfaces. Tx() returns nil.
type FakeUnitOfWork struct {
	Outbox            pkgoutbox.Repository
	Cancelled         postgres.CancelledSchedulesRepository
	MessageProcessing messageprocessing.Repository

	BeginCalled    int
	CommitCalled   int
	RollbackCalled int

	inTx bool
}

func (f *FakeUnitOfWork) OutboxRepo() pkgoutbox.Repository { return f.Outbox }
func (f *FakeUnitOfWork) CancelledSchedulesRepo() postgres.CancelledSchedulesRepository {
	return f.Cancelled
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

// Compile-time assertion.
var _ UnitOfWork = (*FakeUnitOfWork)(nil)
