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
// ScheduleCancelledHandler inside a single Unit-of-Work transaction. The
// transaction is what makes the handler's row locks meaningful: it reads the
// schedule's in-flight deployments FOR UPDATE and cancels them, so the locks
// must be held until the cancellations commit together with the
// cancelled_schedules row.
//
// No messageprocessing.Dedup is required — the cancelled_schedules insert is
// INSERT … ON CONFLICT DO NOTHING and a redelivery finds the schedule's
// deployments already terminal, so it cancels nothing.
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
		if err := u.Begin(ctx); err != nil {
			return fmt.Errorf("begin uow: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				if rbErr := u.Rollback(); rbErr != nil {
					logger.Error("schedule.cancelled: rollback failed",
						"message_id", msg.ID, "error", rbErr)
				}
			}
		}()

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
		if err := u.Commit(); err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}
		committed = true
		return nil
	}
}
