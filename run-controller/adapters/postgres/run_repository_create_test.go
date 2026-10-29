package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// A run whose insert finds its row already stored (the INSERT does nothing)
// is reported as run.ErrRunAlreadyExists.
func TestRunRepository_SaveRun_ExistingRunIsErrRunAlreadyExists(t *testing.T) {
	rn, err := run.NewPromotedSeedsRun(pkgevents.DefaultTenantID, uuid.New(), "promote-seed-test", run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 1}, time.Now())
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO runs`).WillReturnResult(sqlmock.NewResult(0, 0))
	tx, err := sqlxDB.BeginTxx(context.Background(), nil)
	require.NoError(t, err)

	err = NewRunRepository(sqlxDB, discardLogger()).WithTx(tx).SaveRun(context.Background(), rn)
	if !errors.Is(err, run.ErrRunAlreadyExists) {
		t.Fatalf("SaveRun err = %v, want run.ErrRunAlreadyExists", err)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}
