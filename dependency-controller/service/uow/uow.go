package uow

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/dependency-controller/adapters/postgres"
	"github.com/jmoiron/sqlx"
)

// UnitOfWork defines the interface for managing database transactions
type UnitOfWork interface {
	OutboxRepo() postgres.OutboxRepository
	MessageProcessingRepo() postgres.MessageProcessingRepository
	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
}

// PostgresUnitOfWork implements UnitOfWork for PostgreSQL
type PostgresUnitOfWork struct {
	db                *sqlx.DB
	tx                *sqlx.Tx
	outboxRepo        postgres.OutboxRepository
	msgProcessingRepo postgres.MessageProcessingRepository
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

// OutboxRepo returns the outbox repository
func (uow *PostgresUnitOfWork) OutboxRepo() postgres.OutboxRepository {
	if uow.tx != nil {
		// Return transactional repository
		return postgres.NewOutboxRepositoryWithExecutor(uow.tx, uow.logger)
	}
	return postgres.NewOutboxRepository(uow.db, uow.logger)
}

// MessageProcessingRepo returns the message processing repository
func (uow *PostgresUnitOfWork) MessageProcessingRepo() postgres.MessageProcessingRepository {
	if uow.tx != nil {
		// Return transactional repository
		return postgres.NewMessageProcessingRepository(uow.tx, uow.logger)
	}
	return postgres.NewMessageProcessingRepository(uow.db, uow.logger)
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
