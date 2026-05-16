package postgres

import (
	"context"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestNewOutboxRepositoryWithTx_UsesTransactionExecutorForWrites(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO deployment_outbox").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectRollback()

	tx, err := sqlxDB.BeginTxx(context.Background(), nil)
	require.NoError(t, err)

	repo := NewOutboxRepositoryWithTx(tx, slog.Default())
	require.Same(t, tx, repo.(*outboxRepository).executor)

	err = repo.Create(context.Background(), &model.DeploymentOutboxEntry{
		ID:         uuid.New(),
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
	})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}
