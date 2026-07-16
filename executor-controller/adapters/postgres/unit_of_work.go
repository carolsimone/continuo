package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/jmoiron/sqlx"
)

// UnitOfWork implements uow.UnitOfWork against a *sqlx.DB. Repos are
// constructed lazily per call so each invocation sees the current tx (or the
// autocommit DB when none is active).
type UnitOfWork struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	logger *slog.Logger
}

var _ uow.UnitOfWork = (*UnitOfWork)(nil)

// NewUnitOfWork constructs a UnitOfWork over the given *sqlx.DB. The same
// instance can be returned from a factory closure for each inbound message.
func NewUnitOfWork(db *sqlx.DB, logger *slog.Logger) *UnitOfWork {
	return &UnitOfWork{db: db, logger: logger}
}

// executor returns the current tx, or the autocommit DB when none is active.
func (u *UnitOfWork) executor() pkgoutbox.Executor {
	if u.tx != nil {
		return u.tx
	}
	return u.db
}

func (u *UnitOfWork) OutboxRepo() pkgoutbox.Repository {
	return pkgoutbox.NewPostgresRepository(u.executor(), "executor_outbox", u.logger)
}

func (u *UnitOfWork) DeploymentsRepo() repository.DeploymentRepository {
	return NewDeploymentsRepository(u.executor(), u.logger)
}

func (u *UnitOfWork) ValidationAggregateRepo() repository.ValidationAggregateRepository {
	if u.tx != nil {
		return NewValidationAggregateRepository(u.tx)
	}
	return NewValidationAggregateRepository(u.db)
}

func (u *UnitOfWork) CancelledSchedulesRepo() repository.CancelledSchedulesRepository {
	if u.tx != nil {
		return NewCancelledSchedulesRepository(u.tx)
	}
	return NewCancelledSchedulesRepository(u.db)
}

func (u *UnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	if u.tx != nil {
		return messageprocessing.NewPostgresRepository(u.tx, u.logger)
	}
	return messageprocessing.NewPostgresRepository(u.db, u.logger)
}

func (u *UnitOfWork) Tx() *sqlx.Tx { return u.tx }

func (u *UnitOfWork) Begin(ctx context.Context) error {
	if u.tx != nil {
		return fmt.Errorf("transaction already in progress")
	}
	tx, err := u.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	u.tx = tx
	return nil
}

func (u *UnitOfWork) Commit() error {
	if u.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}
	err := u.tx.Commit()
	u.tx = nil
	if err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// Rollback rolls back the current transaction. A deferred Rollback that runs
// after a failed Commit finds the transaction already finished; sql.ErrTxDone
// is treated as a successful no-op so that case does not surface an error.
func (u *UnitOfWork) Rollback() error {
	if u.tx == nil {
		return nil
	}
	tx := u.tx
	u.tx = nil
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		return err
	}
	return nil
}
