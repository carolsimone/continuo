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
