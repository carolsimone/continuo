package uow

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/jmoiron/sqlx"
)

// Transaction exposes repositories scoped to one database transaction.
type Transaction interface {
	OutboxRepo() pkgoutbox.Repository
	MessageProcessingRepo() messageprocessing.Repository
}

// TransactionRunner executes work inside a fresh database transaction.
type TransactionRunner interface {
	WithinTransaction(ctx context.Context, fn func(Transaction) error) error
}

// PostgresTransactionRunner creates transaction-scoped repositories for PostgreSQL work.
type PostgresTransactionRunner struct {
	db     *sqlx.DB
	logger *slog.Logger
}

type postgresTransaction struct {
	tx     *sqlx.Tx
	logger *slog.Logger
}

// NewPostgresTransactionRunner creates a PostgreSQL-backed transaction runner.
func NewPostgresTransactionRunner(db *sqlx.DB, logger *slog.Logger) TransactionRunner {
	return &PostgresTransactionRunner{db: db, logger: logger}
}

// WithinTransaction executes fn inside a fresh transaction and commits only on success.
func (r *PostgresTransactionRunner) WithinTransaction(ctx context.Context, fn func(Transaction) error) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		r.logger.Error("Failed to begin transaction", "error", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}
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

	committed = true
	return nil
}

func (tx *postgresTransaction) OutboxRepo() pkgoutbox.Repository {
	return pkgoutbox.NewPostgresRepository(tx.tx, "k8s_outbox", tx.logger)
}

func (tx *postgresTransaction) MessageProcessingRepo() messageprocessing.Repository {
	return messageprocessing.NewPostgresRepository(tx.tx, tx.logger)
}

// UnitOfWork manages a single database transaction and exposes the
// transaction-scoped repositories handlers need. Bindings own its lifecycle:
// Begin → (work) → Commit, with Rollback on any failure.
type UnitOfWork interface {
	OutboxRepo() pkgoutbox.Repository
	MessageProcessingRepo() messageprocessing.Repository
	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
}

// PostgresUnitOfWork implements UnitOfWork against PostgreSQL.
type PostgresUnitOfWork struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	logger *slog.Logger
	inTx   bool
}

// NewPostgresUnitOfWork creates a PostgreSQL-backed UnitOfWork.
func NewPostgresUnitOfWork(db *sqlx.DB, logger *slog.Logger) UnitOfWork {
	return &PostgresUnitOfWork{db: db, logger: logger}
}

// OutboxRepo returns an outbox repository bound to k8s_outbox. When a
// transaction is active the repository operates inside it.
func (u *PostgresUnitOfWork) OutboxRepo() pkgoutbox.Repository {
	if u.tx != nil {
		return pkgoutbox.NewPostgresRepository(u.tx, "k8s_outbox", u.logger)
	}
	return pkgoutbox.NewPostgresRepository(u.db, "k8s_outbox", u.logger)
}

// MessageProcessingRepo returns the message-processing repository, scoped to
// the active transaction when one is open.
func (u *PostgresUnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	if u.tx != nil {
		return messageprocessing.NewPostgresRepository(u.tx, u.logger)
	}
	return messageprocessing.NewPostgresRepository(u.db, u.logger)
}

// Begin starts a new transaction.
func (u *PostgresUnitOfWork) Begin(ctx context.Context) error {
	if u.inTx {
		return fmt.Errorf("transaction already in progress")
	}
	tx, err := u.db.BeginTxx(ctx, nil)
	if err != nil {
		u.logger.Error("Failed to begin transaction", "error", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	u.tx = tx
	u.inTx = true
	return nil
}

// Commit commits the active transaction.
func (u *PostgresUnitOfWork) Commit() error {
	if !u.inTx || u.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}
	if err := u.tx.Commit(); err != nil {
		u.logger.Error("Failed to commit transaction", "error", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	u.tx = nil
	u.inTx = false
	return nil
}

// Rollback rolls back the active transaction, if any.
func (u *PostgresUnitOfWork) Rollback() error {
	if !u.inTx || u.tx == nil {
		return nil
	}
	if err := u.tx.Rollback(); err != nil {
		u.logger.Error("Failed to rollback transaction", "error", err)
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}
	u.tx = nil
	u.inTx = false
	return nil
}
