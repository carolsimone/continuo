package postgres

import (
	"context"
	"log/slog"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestPostgresUnitOfWork_CommitsOnSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectCommit()

	u := NewPostgresUnitOfWork(sqlxDB, slog.Default())
	require.NoError(t, u.Begin(context.Background()))
	require.NotNil(t, u.OutboxRepo())
	require.NotNil(t, u.MessageProcessingRepo())
	require.NoError(t, u.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresUnitOfWork_RollsBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectRollback()

	u := NewPostgresUnitOfWork(sqlxDB, slog.Default())
	require.NoError(t, u.Begin(context.Background()))
	require.NoError(t, u.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresUnitOfWork_BeginTwiceFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()

	u := NewPostgresUnitOfWork(sqlxDB, slog.Default())
	require.NoError(t, u.Begin(context.Background()))
	require.Error(t, u.Begin(context.Background()))
}

func TestPostgresUnitOfWork_CommitWithoutBeginFails(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	u := NewPostgresUnitOfWork(sqlxDB, slog.Default())
	require.Error(t, u.Commit())
}

func TestPostgresUnitOfWork_RollbackWithoutBeginIsNoop(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	u := NewPostgresUnitOfWork(sqlxDB, slog.Default())
	require.NoError(t, u.Rollback())
}

func TestPostgresUnitOfWork_AllowsConcurrentUnits(t *testing.T) {
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

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			u := NewPostgresUnitOfWork(sqlxDB, slog.Default())
			if err := u.Begin(context.Background()); err != nil {
				errs <- err
				return
			}
			errs <- u.Commit()
		}()
	}
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	require.NoError(t, mock.ExpectationsWereMet())
}
