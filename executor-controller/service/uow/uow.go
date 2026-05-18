package uow

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/jmoiron/sqlx"
)

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

// WithinTransaction executes fn in one fresh transaction.
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
