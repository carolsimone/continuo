// executor-controller/service/uow/uow.go
// Package uow provides a Unit-of-Work abstraction over executor-controller's
// Postgres repositories so handlers can orchestrate over repos without seeing
// sqlx. Matches state/service/uow's UnitOfWork shape so the parser+binding+
// handler pattern is identical across services.
package uow

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/jmoiron/sqlx"
)

// UnitOfWork exposes the repos executor-controller's handlers need plus
// tx lifecycle. MessageProcessingRepo() returns a fresh repo bound to the
// current tx (or the autocommit DB if no tx is active), mirroring
// state/service/uow's pattern.
type UnitOfWork interface {
	OutboxRepo() postgres.OutboxRepository
	CancelledSchedulesRepo() postgres.CancelledSchedulesRepository
	MessageProcessingRepo() messageprocessing.Repository

	// Tx returns the underlying *sqlx.Tx during a transaction, or nil otherwise.
	Tx() *sqlx.Tx

	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
}

// PostgresUnitOfWork implements UnitOfWork against a *sqlx.DB.
type PostgresUnitOfWork struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	logger *slog.Logger
}

// NewPostgresUnitOfWork constructs a PostgresUnitOfWork over the given
// *sqlx.DB. The same instance can be returned from a factory closure for
// each inbound message; repos are constructed lazily per call so each
// invocation sees the current tx (or nil for autocommit).
func NewPostgresUnitOfWork(db *sqlx.DB, logger *slog.Logger) *PostgresUnitOfWork {
	return &PostgresUnitOfWork{db: db, logger: logger}
}

func (u *PostgresUnitOfWork) OutboxRepo() postgres.OutboxRepository {
	if u.tx != nil {
		return postgres.NewOutboxRepository(u.tx, u.logger)
	}
	return postgres.NewOutboxRepository(u.db, u.logger)
}

func (u *PostgresUnitOfWork) CancelledSchedulesRepo() postgres.CancelledSchedulesRepository {
	if u.tx != nil {
		return postgres.NewCancelledSchedulesRepository(u.tx)
	}
	return postgres.NewCancelledSchedulesRepository(u.db)
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

// -----------------------------------------------------------------------------
// Legacy TransactionRunner — consumed by handlers/deploy_handler.go and
// service/messagebus until Task 19 deletes them. Once removed, drop this block.
// -----------------------------------------------------------------------------

// Transaction exposes repositories bound to one database transaction.
type Transaction interface {
	OutboxRepo() postgres.OutboxRepository
}

// TransactionRunner executes work inside a fresh database transaction.
type TransactionRunner interface {
	WithinTransaction(ctx context.Context, fn func(Transaction) error) error
}

// PostgresTransactionRunner starts transaction scopes backed by PostgreSQL.
type PostgresTransactionRunner struct {
	db     *sqlx.DB
	logger *slog.Logger
}

type postgresTransaction struct {
	tx     *sqlx.Tx
	logger *slog.Logger
}

// NewPostgresTransactionRunner creates a PostgreSQL transaction runner.
func NewPostgresTransactionRunner(db *sqlx.DB, logger *slog.Logger) TransactionRunner {
	return &PostgresTransactionRunner{db: db, logger: logger}
}

func (r *PostgresTransactionRunner) WithinTransaction(ctx context.Context, fn func(Transaction) error) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		r.logger.Error("Failed to begin transaction", "error", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				r.logger.Error("Failed to rollback transaction", "error", rollbackErr)
			}
			panic(recovered)
		}
	}()

	scope := &postgresTransaction{tx: tx, logger: r.logger}
	if err := fn(scope); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			r.logger.Error("Failed to rollback transaction", "error", rollbackErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		r.logger.Error("Failed to commit transaction", "error", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (tx *postgresTransaction) OutboxRepo() postgres.OutboxRepository {
	return postgres.NewOutboxRepository(tx.tx, tx.logger)
}
