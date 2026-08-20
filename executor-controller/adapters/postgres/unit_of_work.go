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

// PostgresUnitOfWork implements uow.UnitOfWork against a *sqlx.DB.
type PostgresUnitOfWork struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	logger *slog.Logger
}

// NewPostgresUnitOfWork constructs a PostgresUnitOfWork over the given
// *sqlx.DB. The same instance can be returned from a factory closure for
// each inbound message; repos are constructed lazily per call so each
// invocation sees the current tx (or nil for autocommit).
func NewPostgresUnitOfWork(db *sqlx.DB, logger *slog.Logger) *PostgresUnitOfWork {
	return &PostgresUnitOfWork{db: db, logger: logger}
}

func (u *PostgresUnitOfWork) OutboxRepo() pkgoutbox.Repository {
	if u.tx != nil {
		return pkgoutbox.NewPostgresRepository(u.tx, "executor_outbox", u.logger)
	}
	return pkgoutbox.NewPostgresRepository(u.db, "executor_outbox", u.logger)
}

func (u *PostgresUnitOfWork) DeploymentsRepo() repository.DeploymentRepository {
	if u.tx != nil {
		return NewDeploymentsRepository(u.tx, u.logger)
	}
	return NewDeploymentsRepository(u.db, u.logger)
}

func (u *PostgresUnitOfWork) ValidationAggregateRepo() repository.ValidationAggregateRepository {
	if u.tx != nil {
		return NewValidationAggregateRepository(u.tx)
	}
	return NewValidationAggregateRepository(u.db)
}

func (u *PostgresUnitOfWork) CancelledSchedulesRepo() repository.CancelledSchedulesRepository {
	if u.tx != nil {
		return NewCancelledSchedulesRepository(u.tx)
	}
	return NewCancelledSchedulesRepository(u.db)
}

func (u *PostgresUnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	if u.tx != nil {
		return messageprocessing.NewPostgresRepository(u.tx, u.logger)
	}
	return messageprocessing.NewPostgresRepository(u.db, u.logger)
}

func (u *PostgresUnitOfWork) Tx() *sqlx.Tx { return u.tx }

func (u *PostgresUnitOfWork) Begin(ctx context.Context) error {
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

func (u *PostgresUnitOfWork) Commit() error {
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
func (u *PostgresUnitOfWork) Rollback() error {
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

var _ uow.UnitOfWork = (*PostgresUnitOfWork)(nil)
