package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/jmoiron/sqlx"
)

// PostgresUnitOfWork implements uow.UnitOfWork against a *sqlx.DB.
type PostgresUnitOfWork struct {
	db                *sqlx.DB
	tx                *sqlx.Tx
	taskRepo          TaskTrackerRepository
	taskExecutionRepo TaskExecutionRepository
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
	taskRepo TaskTrackerRepository,
	taskExecutionRepo TaskExecutionRepository,
	runRepoPort repository.RunRepository,
	catalogRepoPort repository.ScheduleCatalogRepository,
	outboxPub ports.OutboxPublisher,
	clock ports.Clock,
	logger *slog.Logger,
) *PostgresUnitOfWork {
	return &PostgresUnitOfWork{
		db:                db,
		taskRepo:          taskRepo,
		taskExecutionRepo: taskExecutionRepo,
		runRepoPort:       runRepoPort,
		catalogRepoPort:   catalogRepoPort,
		outboxPub:         outboxPub,
		clock:             clock,
		logger:            logger,
	}
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
func (u *PostgresUnitOfWork) Clock() ports.Clock            { return u.clock }
func (u *PostgresUnitOfWork) TaskExecutions() repository.TaskExecutionWriter {
	return u.taskExecutionRepo
}

// TaskCollection returns a TaskCollectionAdapter bound to the current
// transaction (or nil tx if no transaction is in progress). Each call
// returns a fresh adapter so callers always see the active tx.
func (u *PostgresUnitOfWork) TaskCollection() run.TaskCollection {
	return NewTaskCollectionAdapter(u.taskRepo, u.tx)
}

// Compile-time interface assertion.
var _ uow.UnitOfWork = (*PostgresUnitOfWork)(nil)
