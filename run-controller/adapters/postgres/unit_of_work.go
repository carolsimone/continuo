package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/run-controller/domain/repository"
	ports "github.com/carolsimone/continuo/run-controller/service/ports"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/jmoiron/sqlx"
)

// PostgresUnitOfWork implements uow.UnitOfWork against a *sqlx.DB. Each
// accessor builds the repository of its port bound to the current transaction,
// so write methods run inside the active tx. A repository's reads run in that
// tx too when it is bound (each repository's conn helper) and on the pool
// otherwise; the reads that never take conn always run on the pool.
type PostgresUnitOfWork struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	clock  ports.Clock
	logger *slog.Logger
	// cronParser reads the stored cron of each schedule Schedules() loads.
	cronParser ports.CronParser
	// afterCommit holds the functions AfterCommit registered in the current
	// transaction.
	afterCommit []func()
}

// NewPostgresUnitOfWork constructs a PostgresUnitOfWork over the autocommit db.
// Run(), Outbox() and TaskCollection(tenantID) each return a repository bound
// to the current transaction; TaskCollection's reads and writes the tasks,
// attempts and node leases of tenantID. cronParser rebuilds the expression of
// each schedule Schedules() loads from its stored cron and timezone; a unit of
// work built with a nil one refuses to load a schedule, which only a caller
// that reads no schedule may accept.
func NewPostgresUnitOfWork(
	db *sqlx.DB,
	clock ports.Clock,
	logger *slog.Logger,
	cronParser ports.CronParser,
) *PostgresUnitOfWork {
	return &PostgresUnitOfWork{
		db:         db,
		clock:      clock,
		logger:     logger,
		cronParser: cronParser,
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

// Run returns a RunRepository bound to the current transaction. GetRun, GetActiveRun and HasActiveRun run in u.tx when one is open and on the
// pool otherwise; the other reads run on the pool; LoadRunForUpdate/SaveRun run
// inside u.tx.
func (u *PostgresUnitOfWork) Run() repository.RunRepository {
	return NewRunRepository(u.db, u.logger).WithTx(u.tx)
}

// Outbox returns an OutboxPublisher bound to the current transaction.
func (u *PostgresUnitOfWork) Outbox() ports.OutboxPublisher {
	return NewOutboxPublisher(u.tx, u.logger)
}

func (u *PostgresUnitOfWork) Clock() ports.Clock { return u.clock }

// TaskCollection returns the TaskCollection of tenantID bound to the current
// transaction (or nil tx if no transaction is in progress). Each call returns
// a fresh collection so callers always see the active tx.
func (u *PostgresUnitOfWork) TaskCollection(tenantID string) run.TaskCollection {
	return NewTaskCollection(u.db, u.logger).ForTenant(tenantID).WithTx(u.tx)
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

// Attempts returns the attempt reconcile's repository, bound to the current
// transaction.
func (u *PostgresUnitOfWork) Attempts() repository.AttemptRepository {
	return NewAttemptRepository(u.db, u.tx)
}

// Leases returns the lease waker's repository, bound to the current
// transaction.
func (u *PostgresUnitOfWork) Leases() repository.LeaseRepository {
	return NewLeaseRepository(u.db, u.tx)
}

// CommandRequests returns the command request repository, bound to the current
// transaction.
func (u *PostgresUnitOfWork) CommandRequests() repository.CommandRequestRepository {
	return NewCommandRequestRepository(u.db, u.tx)
}

// Schedules returns the schedule repository, bound to the current transaction.
func (u *PostgresUnitOfWork) Schedules() repository.ScheduleRepository {
	return NewScheduleRepository(u.db, u.tx, u.cronParser, u.logger)
}

// LiveScheduleModels returns the live release's model counts, read in the
// current transaction.
func (u *PostgresUnitOfWork) LiveScheduleModels() repository.LiveScheduleModels {
	return NewLiveScheduleModelRepository(u.db).WithTx(u.tx)
}

// Compile-time interface assertion.
var _ uow.UnitOfWork = (*PostgresUnitOfWork)(nil)
