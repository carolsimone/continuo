package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/service/uow"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/jmoiron/sqlx"
)

// PostgresUnitOfWork implements uow.UnitOfWork for PostgreSQL.
type PostgresUnitOfWork struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	logger *slog.Logger
	inTx   bool
}

// NewPostgresUnitOfWork creates a new PostgreSQL-based uow.UnitOfWork.
func NewPostgresUnitOfWork(db *sqlx.DB, logger *slog.Logger) uow.UnitOfWork {
	return &PostgresUnitOfWork{
		db:     db,
		logger: logger,
	}
}

var _ uow.UnitOfWork = (*PostgresUnitOfWork)(nil)

// OutboxRepo returns a shared outbox repository bound to orchestrator_outbox.
// When a transaction is active the repository operates inside it, so outbox
// writes are atomic with the rest of the handler's changes.
func (u *PostgresUnitOfWork) OutboxRepo() pkgoutbox.Repository {
	if u.tx != nil {
		return pkgoutbox.NewPostgresRepository(u.tx, "orchestrator_outbox", u.logger)
	}
	return pkgoutbox.NewPostgresRepository(u.db, "orchestrator_outbox", u.logger)
}

// MessageProcessingRepo returns the message processing repository.
func (u *PostgresUnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	if u.tx != nil {
		return messageprocessing.NewPostgresRepository(u.tx, u.logger)
	}
	return messageprocessing.NewPostgresRepository(u.db, u.logger)
}

// Begin starts a new database transaction.
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
	u.logger.Debug("Transaction started")
	return nil
}

// Commit commits the current transaction.
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
	u.logger.Debug("Transaction committed")
	return nil
}

// Rollback rolls back the current transaction.
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
	u.logger.Debug("Transaction rolled back")
	return nil
}
