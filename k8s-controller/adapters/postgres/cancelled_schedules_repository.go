package postgres

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/domain/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type cancelledSchedulesRepository struct{ db *sqlx.DB }

func NewCancelledSchedulesRepository(db *sqlx.DB) repository.CancelledSchedulesRepository {
	return &cancelledSchedulesRepository{db: db}
}

var _ repository.CancelledSchedulesRepository = (*cancelledSchedulesRepository)(nil)

func (r *cancelledSchedulesRepository) Insert(ctx context.Context, id uuid.UUID) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO cancelled_schedules (schedule_id) VALUES ($1) ON CONFLICT DO NOTHING`, id)
	return err
}

func (r *cancelledSchedulesRepository) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM cancelled_schedules WHERE schedule_id = $1)`, id).Scan(&exists)
	return exists, err
}

func (r *cancelledSchedulesRepository) DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error) {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM cancelled_schedules WHERE cancelled_at < $1`, time.Now().Add(-ttl))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
