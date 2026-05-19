package uow

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

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
		require.NotNil(t, tx.MessageProcessingRepo())
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

func TestPostgresTransactionRunner_WithinTransaction_RollsBackAndRepanicsOnCallbackPanic(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectRollback()

	runner := NewPostgresTransactionRunner(sqlxDB, slog.Default())
	require.PanicsWithValue(t, "boom", func() {
		_ = runner.WithinTransaction(context.Background(), func(Transaction) error {
			panic("boom")
		})
	})
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresTransactionRunner_WithinTransaction_AllowsConcurrentTransactions(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	sqlxDB.SetMaxOpenConns(2)
	mock.MatchExpectationsInOrder(false)
	mock.ExpectBegin()
	mock.ExpectBegin()
	mock.ExpectCommit()
	mock.ExpectCommit()

	runner := NewPostgresTransactionRunner(sqlxDB, slog.Default())
	scopes := make(chan *sqlx.Tx, 2)
	release := make(chan struct{})
	errs := make(chan error, 2)

	start := func() {
		go func() {
			errs <- runner.WithinTransaction(context.Background(), func(tx Transaction) error {
				scope, ok := tx.(*postgresTransaction)
				if !ok {
					return errors.New("unexpected transaction implementation")
				}
				scopes <- scope.tx
				<-release
				return nil
			})
		}()
	}

	start()
	start()

	first := receiveTransactionScope(t, scopes, errs)
	second := receiveTransactionScope(t, scopes, errs)
	require.NotSame(t, first, second)

	close(release)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	require.NoError(t, mock.ExpectationsWereMet())
}

func receiveTransactionScope(t *testing.T, scopes <-chan *sqlx.Tx, errs <-chan error) *sqlx.Tx {
	t.Helper()

	select {
	case scope := <-scopes:
		return scope
	case err := <-errs:
		require.NoError(t, err)
		return nil
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for concurrent transaction scope")
		return nil
	}
}
