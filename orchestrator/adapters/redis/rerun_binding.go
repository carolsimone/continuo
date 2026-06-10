package redis

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewRerunBinding wires ParseRerun into the HandleRerunHandler. Parse failures
// are permanent (events.ErrPermanent): the binding logs and returns the error
// so the consumer ACKs and drops the poison message.
func NewRerunBinding(
	handler *handlers.HandleRerunHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		cmd, err := ParseRerun(msg)
		if err != nil {
			logger.Error("trigger.rerun: parse failure — discarding",
				"message_id", msg.ID, "error", err)
			return err
		}
		return handler.Handle(ctx, cmd, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values))
	}
}
