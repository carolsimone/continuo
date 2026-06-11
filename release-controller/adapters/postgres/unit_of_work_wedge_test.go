package postgres

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// TestUnitOfWork_RecoversAfterFailedCommit asserts a failed Commit clears the
// transaction state so the same instance can Begin again, and that a deferred
// Rollback after the failed Commit does not surface sql.ErrTxDone.
func TestUnitOfWork_RecoversAfterFailedCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	u := NewUnitOfWork(sqlxDB, slog.Default())

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(errors.New("commit boom"))
	require.NoError(t, u.Begin(context.Background()))
	require.Error(t, u.Commit())
	require.NoError(t, u.Rollback(), "rollback after failed commit must not surface ErrTxDone")

	mock.ExpectBegin()
	mock.ExpectCommit()
	require.NoError(t, u.Begin(context.Background()), "Begin must succeed after a prior failed commit")
	require.NoError(t, u.Commit())

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestUnitOfWork_RollbackSwallowsErrTxDone asserts Rollback treats an
// already-finished transaction as a successful no-op.
func TestUnitOfWork_RollbackSwallowsErrTxDone(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	u := NewUnitOfWork(sqlxDB, slog.Default())

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(sql.ErrTxDone)
	require.NoError(t, u.Begin(context.Background()))
	require.Error(t, u.Commit())
	require.NoError(t, u.Rollback())
}
