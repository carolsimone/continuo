package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/remediation/domain/repository"
	"github.com/carolsimone/continuo/remediation/service/uow"
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

func (u *UnitOfWork) Begin(ctx context.Context) error {
	tx, err := u.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	u.tx = tx
	return nil
}

func (u *UnitOfWork) Commit() error {
	if u.tx == nil {
		return nil
	}
	err := u.tx.Commit()
	u.tx = nil
	return err
}

func (u *UnitOfWork) Rollback() error {
	if u.tx == nil {
		return nil
	}
	err := u.tx.Rollback()
	u.tx = nil
	return err
}

// DecisionRepo returns the ClassificationDecisionRepository bound to the
// current transaction.
func (u *UnitOfWork) DecisionRepo() repository.ClassificationDecisionRepository {
	return NewDecisionRepository(u.tx)
}

// OutboxRepo returns the pkg/outbox repository bound to remediation_outbox on
// the current transaction. The repository is constructed fresh on each call so
// it is always bound to the active tx.
func (u *UnitOfWork) OutboxRepo() outbox.Repository {
	return outbox.NewPostgresRepository(u.tx, "remediation_outbox", u.logger)
}
