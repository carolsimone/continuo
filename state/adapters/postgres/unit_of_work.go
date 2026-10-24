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
	db            *sqlx.DB
	tx            *sqlx.Tx
	schedulerRepo SchedulerTrackerRepository
	taskRepo      TaskTrackerRepository
	catalogRepo   ScheduleCatalogRepository
	clock         ports.Clock
	logger        *slog.Logger
	// afterCommit holds the functions AfterCommit registered in the current
	// transaction.
	afterCommit []func()
}

// NewPostgresUnitOfWork constructs a PostgresUnitOfWork over the autocommit db
// and the low-level repositories its aggregate accessors compose. The
// Run(), Catalog(), Outbox() and TaskCollection() accessors
// each return an adapter bound to the current transaction.
func NewPostgresUnitOfWork(
	db *sqlx.DB,
	schedulerRepo SchedulerTrackerRepository,
	taskRepo TaskTrackerRepository,
	catalogRepo ScheduleCatalogRepository,
	clock ports.Clock,
	logger *slog.Logger,
) *PostgresUnitOfWork {
	return &PostgresUnitOfWork{
		db:            db,
		schedulerRepo: schedulerRepo,
		taskRepo:      taskRepo,
		catalogRepo:   catalogRepo,
		clock:         clock,
		logger:        logger,
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
	u.afterCommit = nil
	return nil
}

// Commit commits the current transaction, then runs the functions
// AfterCommit registered in it, in registration order. The transaction state
// and those functions are cleared unconditionally, even when the commit
// fails, so a failed commit never wedges the instance and runs none of them:
// the next Begin starts cleanly.
func (u *PostgresUnitOfWork) Commit() error {
	if u.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}
	tx := u.tx
	u.tx = nil
	hooks := u.afterCommit
	u.afterCommit = nil
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	for _, fn := range hooks {
		fn()
	}
	return nil
}

// AfterCommit registers fn to run once the current transaction commits; a
// rollback, or a commit that fails, discards it.
func (u *PostgresUnitOfWork) AfterCommit(fn func()) {
	u.afterCommit = append(u.afterCommit, fn)
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
	u.afterCommit = nil
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

// TaskCollection returns a TaskCollectionAdapter of tenantID bound to the
// current transaction (or nil tx if no transaction is in progress). Each call
// returns a fresh adapter so callers always see the active tx.
func (u *PostgresUnitOfWork) TaskCollection(tenantID string) run.TaskCollection {
	return NewTaskCollectionAdapter(u.taskRepo, u.tx, tenantID)
}

// Topology returns a TopologyRepository bound to the current transaction.
func (u *PostgresUnitOfWork) Topology() repository.TopologyRepository {
	return NewTopologyRepository(u.db, u.tx)
}

// Materializations returns a MaterializationRepository bound to the current
// transaction.
func (u *PostgresUnitOfWork) Materializations() repository.MaterializationRepository {
	return NewMaterializationRepository(u.db, u.tx)
}

// TaskGraph returns a TaskGraph of tenantID bound to the current transaction.
func (u *PostgresUnitOfWork) TaskGraph(tenantID string) run.TaskGraph {
	return NewTaskGraph(u.tx, tenantID)
}

// Attempts returns the attempt reconcile's repository for tenantID, bound to
// the current transaction.
func (u *PostgresUnitOfWork) Attempts(tenantID string) repository.AttemptRepository {
	return NewAttemptRepository(u.db, u.tx, tenantID)
}

// Compile-time interface assertion.
var _ uow.UnitOfWork = (*PostgresUnitOfWork)(nil)
