package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/jmoiron/sqlx"
)

// PostgresUnitOfWork implements uow.UnitOfWork against a *sqlx.DB. It holds the
// low-level repositories the aggregate accessors compose; each accessor builds
// a fresh adapter bound to the current transaction so write methods run inside
// the active tx while reads use the autocommit db.
type PostgresUnitOfWork struct {
	db                *sqlx.DB
	tx                *sqlx.Tx
	schedulerRepo     SchedulerTrackerRepository
	taskRepo          TaskTrackerRepository
	taskExecutionRepo TaskExecutionRepository
	catalogRepo       ScheduleCatalogRepository
	clock             ports.Clock
	logger            *slog.Logger
}

// NewPostgresUnitOfWork constructs a PostgresUnitOfWork over the autocommit db
// and the low-level repositories its aggregate accessors compose. The
// Run(), Catalog(), Outbox(), TaskExecutions(), and TaskCollection() accessors
// each return an adapter bound to the current transaction.
func NewPostgresUnitOfWork(
	db *sqlx.DB,
	schedulerRepo SchedulerTrackerRepository,
	taskRepo TaskTrackerRepository,
	taskExecutionRepo TaskExecutionRepository,
	catalogRepo ScheduleCatalogRepository,
	clock ports.Clock,
	logger *slog.Logger,
) *PostgresUnitOfWork {
	return &PostgresUnitOfWork{
		db:                db,
		schedulerRepo:     schedulerRepo,
		taskRepo:          taskRepo,
		taskExecutionRepo: taskExecutionRepo,
		catalogRepo:       catalogRepo,
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

// Commit commits the current transaction. The transaction state is cleared
// unconditionally, even when the commit fails, so a failed commit never wedges
// the instance: the next Begin starts cleanly.
func (u *PostgresUnitOfWork) Commit() error {
	if u.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}
	tx := u.tx
	u.tx = nil
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// Rollback rolls back the current transaction. A deferred Rollback that runs
// after a failed Commit finds the transaction already finished; sql.ErrTxDone
// is treated as a successful no-op so that case does not surface an error.
func (u *PostgresUnitOfWork) Rollback() error {
	if u.tx == nil {
		return nil
	}
	tx := u.tx
	u.tx = nil
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		return err
	}
	return nil
}

// Run returns a RunRepository bound to the current transaction. Read methods
// use the autocommit db; LoadRunForUpdate/SaveRun run inside u.tx.
func (u *PostgresUnitOfWork) Run() repository.RunRepository {
	return NewRunRepository(u.tx, u.schedulerRepo)
}

// Catalog returns a ScheduleCatalogRepository bound to the current transaction.
func (u *PostgresUnitOfWork) Catalog() repository.ScheduleCatalogRepository {
	return NewCatalogRepositoryAdapter(u.db, u.tx, u.catalogRepo, u.logger)
}

// Outbox returns an OutboxPublisher bound to the current transaction.
func (u *PostgresUnitOfWork) Outbox() ports.OutboxPublisher {
	return NewOutboxPublisher(u.tx, u.logger)
}

func (u *PostgresUnitOfWork) Clock() ports.Clock { return u.clock }

// TaskExecutions returns a TaskExecutionWriter bound to the current transaction.
func (u *PostgresUnitOfWork) TaskExecutions() repository.TaskExecutionWriter {
	return NewTaskExecutionWriter(u.taskExecutionRepo, u.tx)
}

// TaskCollection returns a TaskCollectionAdapter bound to the current
// transaction (or nil tx if no transaction is in progress). Each call
// returns a fresh adapter so callers always see the active tx.
func (u *PostgresUnitOfWork) TaskCollection() run.TaskCollection {
	return NewTaskCollectionAdapter(u.taskRepo, u.tx)
}

// Compile-time interface assertion.
var _ uow.UnitOfWork = (*PostgresUnitOfWork)(nil)
