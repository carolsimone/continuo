// executor-controller/adapters/redis/schedule_cancelled_binding.go
package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/google/uuid"
)

// NewScheduleCancelledBinding wires ParseScheduleCancelled into the
// ScheduleCancelledHandler. No messageprocessing.Dedup is required —
// CancelledSchedulesRepository.Insert is INSERT … ON CONFLICT DO NOTHING,
// so duplicate deliveries are naturally idempotent. The binding does
// not call uow.Begin: a single autocommit INSERT is correct and
// avoids needless transaction overhead.
func NewScheduleCancelledBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.ScheduleCancelledHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseScheduleCancelled(msg)
		if err != nil {
			logger.Error("schedule.cancelled: parse failure", "message_id", msg.ID, "error", err)
			return fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}
		u := uowFactory()
		if err := handler.Handle(ctx, u, evt, uuid.Nil); err != nil {
			if errors.Is(err, pkgevents.ErrPermanent) {
				logger.Error("schedule.cancelled: permanent handler error",
					"message_id", msg.ID, "error", err)
			} else {
				logger.Error("schedule.cancelled: transient handler error",
					"message_id", msg.ID, "error", err)
			}
			return err
		}
		return nil
	}
}
