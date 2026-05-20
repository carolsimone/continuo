package uow

import (
	"context"
	"fmt"
	"log/slog"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/jmoiron/sqlx"
)

// UnitOfWork defines the interface for managing database transactions
type UnitOfWork interface {
	OutboxRepo() pkgoutbox.Repository
	MessageProcessingRepo() messageprocessing.Repository
	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
}

// PostgresUnitOfWork implements UnitOfWork for PostgreSQL
type PostgresUnitOfWork struct {
	db                *sqlx.DB
	tx                *sqlx.Tx
	msgProcessingRepo messageprocessing.Repository
	logger            *slog.Logger
	inTx              bool
}

// NewPostgresUnitOfWork creates a new PostgreSQL-based Unit of Work
func NewPostgresUnitOfWork(db *sqlx.DB, logger *slog.Logger) UnitOfWork {
	return &PostgresUnitOfWork{
		db:     db,
		logger: logger,
	}
}

// OutboxRepo returns a shared outbox repository bound to orchestrator_outbox.
// When a transaction is active the repository operates inside it, so outbox
// writes are atomic with the rest of the handler's changes.
func (uow *PostgresUnitOfWork) OutboxRepo() pkgoutbox.Repository {
	if uow.tx != nil {
		return pkgoutbox.NewPostgresRepository(uow.tx, "orchestrator_outbox", uow.logger)
	}
	return pkgoutbox.NewPostgresRepository(uow.db, "orchestrator_outbox", uow.logger)
}

// MessageProcessingRepo returns the message processing repository
func (uow *PostgresUnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	if uow.tx != nil {
		return messageprocessing.NewPostgresRepository(uow.tx, uow.logger)
	}
	return messageprocessing.NewPostgresRepository(uow.db, uow.logger)
}

// Begin starts a new database transaction
func (uow *PostgresUnitOfWork) Begin(ctx context.Context) error {
	if uow.inTx {
		return fmt.Errorf("transaction already in progress")
	}

	tx, err := uow.db.BeginTxx(ctx, nil)
	if err != nil {
		uow.logger.Error("Failed to begin transaction", "error", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	uow.tx = tx
	uow.inTx = true

	uow.logger.Debug("Transaction started")

	return nil
}

// Commit commits the current transaction
func (uow *PostgresUnitOfWork) Commit() error {
	if !uow.inTx || uow.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}

	if err := uow.tx.Commit(); err != nil {
		uow.logger.Error("Failed to commit transaction", "error", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	uow.tx = nil
	uow.inTx = false

	uow.logger.Debug("Transaction committed")

	return nil
}

// Rollback rolls back the current transaction
func (uow *PostgresUnitOfWork) Rollback() error {
	if !uow.inTx || uow.tx == nil {
		// No transaction to rollback, this is fine
		return nil
	}

	if err := uow.tx.Rollback(); err != nil {
		uow.logger.Error("Failed to rollback transaction", "error", err)
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	uow.tx = nil
	uow.inTx = false

	uow.logger.Debug("Transaction rolled back")

	return nil
}
