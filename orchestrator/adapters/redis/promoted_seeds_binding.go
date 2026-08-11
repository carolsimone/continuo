package redis

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewPromotedSeedsBinding wires ParsePromotedSeedsRun into the
// HandlePromotedSeedsRunHandler. A parse failure is permanent
// (events.ErrPermanent): the binding logs and returns the error so the consumer
// ACKs and drops the poison message.
//
// outbox_entry_id is extracted from the message fields and threaded to the
// handler so the dedup layer catches re-XADDs of the same upstream outbox row
// arriving under a fresh Redis message ID.
func NewPromotedSeedsBinding(
	handler *handlers.HandlePromotedSeedsRunHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		cmd, err := ParsePromotedSeedsRun(msg)
		if err != nil {
			logger.Error("trigger.promoted_seeds: parse failure — discarding",
				"message_id", msg.ID, "error", err)
			return err
		}
		outboxEntryID := messageprocessing.ExtractOutboxEntryID(msg.Values)
		return handler.Handle(ctx, cmd, msg.ID, outboxEntryID)
	}
}
