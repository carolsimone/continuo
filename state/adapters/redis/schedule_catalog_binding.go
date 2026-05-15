package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// scheduleCatalogStreamName is the wire-stable name of the Redis stream
// whose messages this binding handles. It is also the value stored in the
// message_processing.stream_name column for dedup rows.
const scheduleCatalogStreamName = "schedules.loaded:v1"

// NewScheduleCatalogBinding returns a pkg/redis.MessageHandler that turns
// each schedules.loaded:v1 message into a typed event, runs dedup, and
// invokes ScheduleCatalogHandler inside a single Unit-of-Work transaction.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK
// policy: parse failures are wrapped with events.ErrPermanent (NACK and
// drop), while handler/repository failures propagate as-is (NACK and let
// the message stay pending for retry). On a duplicate the transaction is
// committed (empty txn) and nil is returned so the consumer ACKs.
func NewScheduleCatalogBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.ScheduleCatalogHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseScheduleCatalogLoaded(msg)
		if err != nil {
			logger.Error("schedule_catalog: parse failure", "message_id", msg.ID, "error", err)
			return fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}

		payload, _ := msg.Values["payload"].(string)
		u := uowFactory()
		if err := u.Begin(ctx); err != nil {
			return fmt.Errorf("begin uow: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				if rbErr := u.Rollback(); rbErr != nil {
					logger.Error("schedule_catalog: rollback failed",
						"message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		_, dup, err := messageprocessing.Dedup(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, scheduleCatalogStreamName, []byte(payload),
		)
		if err != nil {
			return err
		}
		if dup {
			if err := u.Commit(); err != nil {
				return fmt.Errorf("commit dedup tx: %w", err)
			}
			committed = true
			return nil
		}

		if err := handler.Handle(ctx, u, evt, uuid.Nil); err != nil {
			if errors.Is(err, pkgevents.ErrPermanent) {
				logger.Error("schedule_catalog: permanent handler error",
					"message_id", msg.ID, "error", err)
			} else {
				logger.Error("schedule_catalog: transient handler error",
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
