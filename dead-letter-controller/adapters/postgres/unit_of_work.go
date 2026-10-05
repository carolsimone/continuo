package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/repository"
	"github.com/carolsimone/continuo/dead-letter-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
)

// UnitOfWork manages a single Postgres transaction and the repositories scoped
// to it.
type UnitOfWork struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	logger *slog.Logger
}

var _ uow.UnitOfWork = (*UnitOfWork)(nil)

// NewUnitOfWork constructs a Postgres-backed UnitOfWork.
func NewUnitOfWork(db *sqlx.DB, logger *slog.Logger) *UnitOfWork {
	return &UnitOfWork{db: db, logger: logger}
}

// Begin opens the transaction.
func (u *UnitOfWork) Begin(ctx context.Context) error {
	tx, err := u.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	u.tx = tx
	return nil
}

// Commit commits the transaction; it is a no-op when none is open.
func (u *UnitOfWork) Commit() error {
	if u.tx == nil {
		return nil
	}
	err := u.tx.Commit()
	u.tx = nil
	return err
}

// Rollback aborts the transaction; it is a no-op when none is open.
func (u *UnitOfWork) Rollback() error {
	if u.tx == nil {
		return nil
	}
	err := u.tx.Rollback()
	u.tx = nil
	return err
}

// DeadLetters returns the DeadLetterRepository bound to the current transaction.
func (u *UnitOfWork) DeadLetters() repository.DeadLetterRepository {
	return NewDeadLetterRepository(u.tx)
}

// OutboxTable is the outbox table this service writes its events to and its
// relay publishes from. Its insert trigger notifies the Postgres channel of the
// same name, on which the relay's waker listens.
const OutboxTable = "dead_letter_outbox"

// Outbox returns the pkg/outbox repository bound to dead_letter_outbox on the
// current transaction. The repository is constructed fresh on each call so it
// is always bound to the active tx.
func (u *UnitOfWork) Outbox() outbox.Repository {
	return outbox.NewPostgresRepository(u.tx, OutboxTable, u.logger)
}
