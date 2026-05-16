package uow

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestPostgresTransactionRunner_WithinTransaction_CommitsOnSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectCommit()

	runner := NewPostgresTransactionRunner(sqlxDB, slog.Default())
	err = runner.WithinTransaction(context.Background(), func(tx Transaction) error {
		require.NotNil(t, tx.OutboxRepo())
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresTransactionRunner_WithinTransaction_RollsBackOnCallbackError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectRollback()

	runner := NewPostgresTransactionRunner(sqlxDB, slog.Default())
	err = runner.WithinTransaction(context.Background(), func(Transaction) error {
		return errors.New("boom")
	})
	require.EqualError(t, err, "boom")
	require.NoError(t, mock.ExpectationsWereMet())
}
