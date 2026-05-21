package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/jmoiron/sqlx"
)

// PostgresUnitOfWork implements uow.UnitOfWork against PostgreSQL.
type PostgresUnitOfWork struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	logger *slog.Logger
	inTx   bool
}

// NewPostgresUnitOfWork creates a PostgreSQL-backed uow.UnitOfWork.
func NewPostgresUnitOfWork(db *sqlx.DB, logger *slog.Logger) uow.UnitOfWork {
	return &PostgresUnitOfWork{db: db, logger: logger}
}

var _ uow.UnitOfWork = (*PostgresUnitOfWork)(nil)

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
