// Package uow provides a Unit-of-Work abstraction over state's Postgres
// repositories so handlers can orchestrate over repos without seeing sqlx.
package uow

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/adapters/postgres"
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
type UnitOfWork interface {
	SchedulerRepo() postgres.SchedulerTrackerRepository
	TaskRepo() postgres.TaskTrackerRepository
	TaskExecutionRepo() postgres.TaskExecutionRepository
	ScheduleCatalogRepo() postgres.ScheduleCatalogRepository
	OutboxRepo() postgres.OutboxRepository
	MessageProcessingRepo() messageprocessing.Repository

	// Tx returns the underlying *sqlx.Tx during a transaction, or nil otherwise.
	Tx() *sqlx.Tx

	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
}

// PostgresUnitOfWork implements UnitOfWork against a *sqlx.DB.
type PostgresUnitOfWork struct {
	db                *sqlx.DB
	tx                *sqlx.Tx
	schedulerRepo     postgres.SchedulerTrackerRepository
	taskRepo          postgres.TaskTrackerRepository
	taskExecutionRepo postgres.TaskExecutionRepository
	catalogRepo       postgres.ScheduleCatalogRepository
	outboxRepo        postgres.OutboxRepository
	logger            *slog.Logger
}

// NewPostgresUnitOfWork constructs a PostgresUnitOfWork. The passed-in repos
// are the autocommit (*sqlx.DB-bound) instances; tx-bound calls happen via
// the *Tx repo methods that take Tx() explicitly.
func NewPostgresUnitOfWork(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	taskExecutionRepo postgres.TaskExecutionRepository,
	catalogRepo postgres.ScheduleCatalogRepository,
	outboxRepo postgres.OutboxRepository,
	logger *slog.Logger,
) *PostgresUnitOfWork {
	return &PostgresUnitOfWork{
		db:                db,
		schedulerRepo:     schedulerRepo,
		taskRepo:          taskRepo,
		taskExecutionRepo: taskExecutionRepo,
		catalogRepo:       catalogRepo,
		outboxRepo:        outboxRepo,
		logger:            logger,
	}
}

func (u *PostgresUnitOfWork) SchedulerRepo() postgres.SchedulerTrackerRepository {
	return u.schedulerRepo
}
func (u *PostgresUnitOfWork) TaskRepo() postgres.TaskTrackerRepository { return u.taskRepo }
func (u *PostgresUnitOfWork) TaskExecutionRepo() postgres.TaskExecutionRepository {
	return u.taskExecutionRepo
}
func (u *PostgresUnitOfWork) ScheduleCatalogRepo() postgres.ScheduleCatalogRepository {
	return u.catalogRepo
}
func (u *PostgresUnitOfWork) OutboxRepo() postgres.OutboxRepository { return u.outboxRepo }

func (u *PostgresUnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	if u.tx != nil {
		return messageprocessing.NewPostgresRepository(u.tx, u.logger)
	}
	return messageprocessing.NewPostgresRepository(u.db, u.logger)
}

func (u *PostgresUnitOfWork) Tx() *sqlx.Tx { return u.tx }

func (u *PostgresUnitOfWork) Begin(ctx context.Context) error {
	if u.tx != nil {
		return fmt.Errorf("transaction already in progress")
	}
	tx, err := u.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	u.tx = tx
	return nil
}

func (u *PostgresUnitOfWork) Commit() error {
	if u.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}
	err := u.tx.Commit()
	u.tx = nil
	if err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func (u *PostgresUnitOfWork) Rollback() error {
	if u.tx == nil {
		return nil
	}
	err := u.tx.Rollback()
	u.tx = nil
	return err
}

// Compile-time interface assertions.
var (
	_ UnitOfWork = (*PostgresUnitOfWork)(nil)
	_ UnitOfWork = (*FakeUnitOfWork)(nil)
)
