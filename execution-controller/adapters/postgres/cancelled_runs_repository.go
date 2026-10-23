package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/google/uuid"
)

// cancelledExecutor is the read/write subset of sqlx.DB / sqlx.Tx that
// CancelledRunsRepository needs. Same shape as
// pkg/messageprocessing/postgres.go's executor.
type cancelledExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type cancelledRunsRepository struct {
	executor cancelledExecutor
}

// NewCancelledRunsRepository creates a repository.CancelledRunsRepository
// over cancelled_runs against any sqlx executor (*sqlx.DB for autocommit,
// *sqlx.Tx for transaction-scoped work).
func NewCancelledRunsRepository(executor cancelledExecutor) repository.CancelledRunsRepository {
	return &cancelledRunsRepository{executor: executor}
}

var _ repository.CancelledRunsRepository = (*cancelledRunsRepository)(nil)

func (r *cancelledRunsRepository) Insert(ctx context.Context, runID uuid.UUID) error {
	_, err := r.executor.ExecContext(ctx,
		`INSERT INTO cancelled_runs (run_id) VALUES ($1) ON CONFLICT DO NOTHING`, runID)
	return err
}

func (r *cancelledRunsRepository) Exists(ctx context.Context, runID uuid.UUID) (bool, error) {
	var exists bool
	err := r.executor.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM cancelled_runs WHERE run_id = $1)`, runID).Scan(&exists)
	return exists, err
}

func (r *cancelledRunsRepository) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	res, err := r.executor.ExecContext(ctx,
		`DELETE FROM cancelled_runs WHERE cancelled_at < NOW() - make_interval(secs => $1)`, age.Seconds())
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
