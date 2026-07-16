package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// CancelledSchedulesRepository tracks schedule IDs whose deploys should be
// dropped on receipt. Implementations operate against the cancelled_schedules
// table (id, schedule_id UNIQUE, cancelled_at).
type CancelledSchedulesRepository interface {
	Insert(ctx context.Context, scheduleID uuid.UUID) error
	Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error)
	DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error)
}
