package redis

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewSchedulerStartedBinding wires ParseSchedulerStartedEvent into the
// HandleSchedulerStartedHandler. A parse failure is permanent
// (events.ErrPermanent): the binding logs and returns the error so the consumer
// ACKs and drops the poison message.
func NewSchedulerStartedBinding(
	handler *handlers.HandleSchedulerStartedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseSchedulerStartedEvent(msg.Values)
		if err != nil {
			logger.Error("scheduler.started: parse failure — discarding",
				"message_id", msg.ID, "error", err)
			return err
		}
		return handler.Handle(ctx, evt, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values))
	}
}
