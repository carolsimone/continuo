package postgres

import (
	"context"
	"errors"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// TestPostgresUnitOfWork_RecoversAfterFailedCommit reproduces issue #124: a
// failed Commit must still clear the transaction state so the same long-lived
// UnitOfWork instance can Begin again. The handler's deferred Rollback runs
// after the failed Commit and must not surface sql.ErrTxDone as an error.
func TestPostgresUnitOfWork_RecoversAfterFailedCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	u := NewPostgresUnitOfWork(sqlxDB, slog.Default())

	// First transaction: Commit fails. database/sql marks the tx done even on a
	// failed Commit, so the deferred Rollback that follows sees sql.ErrTxDone.
	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(errors.New("commit boom"))
	require.NoError(t, u.Begin(context.Background()))
	require.Error(t, u.Commit())
	require.NoError(t, u.Rollback(), "deferred rollback after a failed commit must not surface ErrTxDone")

	// Second transaction on the same instance must succeed: a failed commit must
	// not wedge the UnitOfWork.
	mock.ExpectBegin()
	mock.ExpectCommit()
	require.NoError(t, u.Begin(context.Background()), "Begin must succeed after a prior failed commit")
	require.NoError(t, u.Commit())

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgresUnitOfWork_RollbackSwallowsErrTxDone asserts Rollback treats an
// already-finished transaction as a successful no-op.
func TestPostgresUnitOfWork_RollbackSwallowsErrTxDone(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	u := NewPostgresUnitOfWork(sqlxDB, slog.Default())

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(sql.ErrTxDone)
	require.NoError(t, u.Begin(context.Background()))
	require.Error(t, u.Commit())
	require.NoError(t, u.Rollback())
}
