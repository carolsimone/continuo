package postgres

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type cancelledSchedulesRepository struct {
	db *sqlx.DB
}

// NewCancelledSchedulesRepository constructs a cancelled-schedules repository.
func NewCancelledSchedulesRepository(db *sqlx.DB) repository.CancelledSchedulesRepository {
	return &cancelledSchedulesRepository{db: db}
}

var _ repository.CancelledSchedulesRepository = (*cancelledSchedulesRepository)(nil)

func (r *cancelledSchedulesRepository) Insert(ctx context.Context, scheduleID uuid.UUID) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO cancelled_schedules (schedule_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		scheduleID)
	return err
}

func (r *cancelledSchedulesRepository) Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM cancelled_schedules WHERE schedule_id = $1)`,
		scheduleID).Scan(&exists)
	return exists, err
}

func (r *cancelledSchedulesRepository) DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error) {
	// The cutoff is computed in SQL (NOW() - interval) rather than with a Go
	// wall-clock value, so the comparison against cancelled_at (a DB-stamped
	// column) uses a single time authority and is immune to host/DB clock skew.
	result, err := r.db.ExecContext(ctx,
		`DELETE FROM cancelled_schedules WHERE cancelled_at < NOW() - make_interval(secs => $1)`,
		ttl.Seconds())
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return n, nil
}
