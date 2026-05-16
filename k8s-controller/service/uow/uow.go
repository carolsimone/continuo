package uow

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/jmoiron/sqlx"
)

// Transaction exposes repositories scoped to one database transaction.
type Transaction interface {
	OutboxRepo() postgres.OutboxRepository
	ProcessedEventsRepo() postgres.ProcessedEventsRepository
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
	return postgres.NewOutboxRepositoryWithTx(tx.tx, tx.logger)
}

func (tx *postgresTransaction) ProcessedEventsRepo() postgres.ProcessedEventsRepository {
	return postgres.NewProcessedEventsRepositoryWithTx(tx.tx, tx.logger)
}
