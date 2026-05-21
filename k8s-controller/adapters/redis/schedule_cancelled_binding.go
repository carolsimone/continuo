package redis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/k8s-controller/domain/repository"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// NewScheduleCancelledBinding returns a pkg/redis.MessageHandler for
// schedule.cancelled:v1. It is a lightweight projection sink: parse the
// schedule_id and insert it into the cancelled_schedules guard table. The
// insert is idempotent, so no dedup or outbox transaction is needed. An invalid
// schedule_id is a permanent error (ACK + drop).
func NewScheduleCancelledBinding(
	repo repository.CancelledSchedulesRepository,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		idStr, _ := msg.Values["schedule_id"].(string)
		scheduleID, err := uuid.Parse(idStr)
		if err != nil {
			logger.Error("schedule_cancelled: invalid schedule_id — discarding", "id", idStr)
			return fmt.Errorf("%w: invalid schedule_id %q", pkgevents.ErrPermanent, idStr)
		}
		if err := repo.Insert(ctx, scheduleID); err != nil {
			return fmt.Errorf("insert cancelled schedule: %w", err)
		}
		return nil
	}
}
