package postgres

import (
	"context"
	"fmt"
	"log/slog"

	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/jmoiron/sqlx"
)

// UnitOfWork manages a single Postgres transaction and exposes all
// transaction-scoped repositories handlers need. Callers own its lifecycle:
// Begin → (work) → Commit, with Rollback on any failure.
type UnitOfWork struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	inTx   bool
	logger *slog.Logger
}

// NewUnitOfWork creates a new Postgres-backed UnitOfWork.
func NewUnitOfWork(db *sqlx.DB, logger *slog.Logger) *UnitOfWork {
	return &UnitOfWork{db: db, logger: logger}
}

var _ uow.UnitOfWork = (*UnitOfWork)(nil)

// queryer returns the active transaction when one is open, or the connection
// pool otherwise. This allows repositories to work identically in both modes.
func (u *UnitOfWork) queryer() Queryer {
	if u.tx != nil {
		return u.tx
	}
	return u.db
}

// ReleaseRepo returns the release repository bound to the current queryer.
func (u *UnitOfWork) ReleaseRepo() repository.ReleaseRepository {
	return NewReleaseRepository(u.queryer())
}

// CurrentProdRepo returns the current-prod repository bound to the current queryer.
func (u *UnitOfWork) CurrentProdRepo() repository.CurrentProdRepository {
	return NewCurrentProdRepository(u.queryer())
}

// OutboxRepo returns the outbox repository for the release_controller_outbox
// table. When a transaction is active the repository operates inside it, so
// outbox writes are atomic with the rest of the handler's changes.
func (u *UnitOfWork) OutboxRepo() pkgoutbox.Repository {
	if u.tx != nil {
		return pkgoutbox.NewPostgresRepository(u.tx, "release_controller_outbox", u.logger)
	}
	return pkgoutbox.NewPostgresRepository(u.db, "release_controller_outbox", u.logger)
}

// MessageProcessingRepo returns the message-processing repository. When a
// transaction is active the repository operates inside it.
func (u *UnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	if u.tx != nil {
		return messageprocessing.NewPostgresRepository(u.tx, u.logger)
	}
	return messageprocessing.NewPostgresRepository(u.db, u.logger)
}

// Begin starts a new database transaction.
func (u *UnitOfWork) Begin(ctx context.Context) error {
	if u.inTx {
		return fmt.Errorf("transaction already in progress")
	}
	tx, err := u.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	u.tx = tx
	u.inTx = true
	return nil
}

// Commit commits the current transaction.
func (u *UnitOfWork) Commit() error {
	if !u.inTx || u.tx == nil {
		return fmt.Errorf("no tx in progress")
	}
	err := u.tx.Commit()
	u.tx = nil
	u.inTx = false
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Rollback rolls back the current transaction. Calling Rollback when no
// transaction is in progress is a no-op.
func (u *UnitOfWork) Rollback() error {
	if !u.inTx || u.tx == nil {
		return nil
	}
	err := u.tx.Rollback()
	u.tx = nil
	u.inTx = false
	if err != nil {
		return fmt.Errorf("rollback: %w", err)
	}
	return nil
}
