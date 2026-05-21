// Package uow provides a Unit-of-Work abstraction over state's Postgres
// repositories so handlers can orchestrate over repos without seeing sqlx.
package uow

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/adapters/postgres"
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
// The aggregate-level accessors (Run, Catalog, Outbox, TaskCollection, Clock)
// are used by handlers that operate on domain aggregates rather than the
// low-level tracker repos.
type UnitOfWork interface {
	SchedulerRepo() postgres.SchedulerTrackerRepository
	TaskRepo() postgres.TaskTrackerRepository
	TaskExecutionRepo() postgres.TaskExecutionRepository
	ScheduleCatalogRepo() postgres.ScheduleCatalogRepository
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

// PostgresUnitOfWork implements UnitOfWork against a *sqlx.DB.
type PostgresUnitOfWork struct {
	db                *sqlx.DB
	tx                *sqlx.Tx
	schedulerRepo     postgres.SchedulerTrackerRepository
	taskRepo          postgres.TaskTrackerRepository
	taskExecutionRepo postgres.TaskExecutionRepository
	catalogRepo       postgres.ScheduleCatalogRepository
	runRepoPort       repository.RunRepository
	catalogRepoPort   repository.ScheduleCatalogRepository
	outboxPub         ports.OutboxPublisher
	clock             ports.Clock
	logger            *slog.Logger
}

// NewPostgresUnitOfWork constructs a PostgresUnitOfWork. The passed-in repos
// are the autocommit (*sqlx.DB-bound) instances; tx-bound calls happen via
// the *Tx repo methods that take Tx() explicitly.
//
// The aggregate-level ports (runRepoPort, catalogRepoPort, outboxPub, clock)
// power the Run(), Catalog(), Outbox(), and Clock() accessors used by
// aggregate-aware handlers. TaskCollection() is constructed fresh per call
// from taskRepo and the current transaction.
func NewPostgresUnitOfWork(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	taskExecutionRepo postgres.TaskExecutionRepository,
	catalogRepo postgres.ScheduleCatalogRepository,
	runRepoPort repository.RunRepository,
	catalogRepoPort repository.ScheduleCatalogRepository,
	outboxPub ports.OutboxPublisher,
	clock ports.Clock,
	logger *slog.Logger,
) *PostgresUnitOfWork {
	return &PostgresUnitOfWork{
		db:                db,
		schedulerRepo:     schedulerRepo,
		taskRepo:          taskRepo,
		taskExecutionRepo: taskExecutionRepo,
		catalogRepo:       catalogRepo,
		runRepoPort:       runRepoPort,
		catalogRepoPort:   catalogRepoPort,
		outboxPub:         outboxPub,
		clock:             clock,
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

func (u *PostgresUnitOfWork) Run() repository.RunRepository { return u.runRepoPort }
func (u *PostgresUnitOfWork) Catalog() repository.ScheduleCatalogRepository {
	return u.catalogRepoPort
}
func (u *PostgresUnitOfWork) Outbox() ports.OutboxPublisher { return u.outboxPub }
func (u *PostgresUnitOfWork) Clock() ports.Clock           { return u.clock }
func (u *PostgresUnitOfWork) TaskExecutions() repository.TaskExecutionWriter {
	return u.taskExecutionRepo
}

// TaskCollection returns a TaskCollectionAdapter bound to the current
// transaction (or nil tx if no transaction is in progress). Each call
// returns a fresh adapter so callers always see the active tx.
func (u *PostgresUnitOfWork) TaskCollection() run.TaskCollection {
	return postgres.NewTaskCollectionAdapter(u.taskRepo, u.tx)
}

// Compile-time interface assertions.
var (
	_ UnitOfWork = (*PostgresUnitOfWork)(nil)
	_ UnitOfWork = (*FakeUnitOfWork)(nil)
)
