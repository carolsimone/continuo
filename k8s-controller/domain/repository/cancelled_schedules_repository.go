// Package repository declares k8s-controller's domain repository ports.
// Implementations live in k8s-controller/adapters.
package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// CancelledSchedulesRepository records and queries schedules whose runs have
// been cancelled, so late job results for them can be absorbed.
type CancelledSchedulesRepository interface {
	Insert(ctx context.Context, scheduleID uuid.UUID) error
	Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error)
	DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error)
}
