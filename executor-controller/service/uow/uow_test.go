package uow

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestPostgresTransactionRunner_WithinTransaction_UsesTransactionBoundRepositoryAndCommits(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO deployment_outbox").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	runner := NewPostgresTransactionRunner(sqlxDB, slog.Default())
	err = runner.WithinTransaction(context.Background(), func(tx Transaction) error {
		return tx.OutboxRepo().Create(context.Background(), &model.DeploymentOutboxEntry{
			ID:         uuid.New(),
			TaskID:     uuid.New(),
			ScheduleID: uuid.New(),
		})
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

func TestPostgresTransactionRunner_WithinTransaction_RollsBackAndRepanicsOnPanic(t *testing.T) {
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
