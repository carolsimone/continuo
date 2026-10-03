package redis

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewRunFinalizedBinding wires ParseRunFinalized into the RunFinalizedHandler.
// A parse failure is permanent (events.ErrPermanent): the binding logs and
// returns the error so the consumer dead-letters the poison message.
func NewRunFinalizedBinding(
	handler *handlers.RunFinalizedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseRunFinalized(msg)
		if err != nil {
			logger.Error("run.finalized: parse failure — dead-lettering",
				"message_id", msg.ID, "error", err)
			return err
		}
		return handler.Handle(ctx, evt)
	}
}
