package redis

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewNodeCompletedBinding wires ParseNodeCompleted into the
// HandleNodeCompletedHandler. A parse failure is permanent
// (events.ErrPermanent): the binding logs it and returns the error so the
// consumer dead-letters the poison message rather than re-delivering it from
// the PEL forever.
func NewNodeCompletedBinding(
	handler *handlers.HandleNodeCompletedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		cmd, err := ParseNodeCompleted(msg)
		if err != nil {
			logger.Error("node.updated: parse failure — dead-lettering",
				"message_id", msg.ID, "error", err)
			return err
		}
		return handler.Handle(ctx, cmd, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values))
	}
}
