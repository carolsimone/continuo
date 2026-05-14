package redis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// NewScheduleCancelledHandler returns a MessageHandler that records cancelled
// schedule IDs in the local cancelled_schedules table.
func NewScheduleCancelledHandler(
	repo repository.CancelledSchedulesRepository,
	logger *slog.Logger,
) MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		idStr, _ := msg.Values["schedule_id"].(string)
		scheduleID, err := uuid.Parse(idStr)
		if err != nil {
			logger.Error("schedule.cancelled: invalid schedule_id — discarding", "id", idStr)
			return nil
		}
		if err := repo.Insert(ctx, scheduleID); err != nil {
			return fmt.Errorf("insert cancelled schedule %s: %w", scheduleID, err)
		}
		logger.Info("Recorded cancelled schedule", "schedule_id", scheduleID)
		return nil
	}
}
