package redis

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewScheduleCancelledBinding wires ParseScheduleCancelled into the
// ScheduleCancelledHandler. Parse failures are silently ACKed (logged at
// error level), matching orchestrator's existing convention.
func NewScheduleCancelledBinding(
	handler *handlers.ScheduleCancelledHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseScheduleCancelled(msg)
		if err != nil {
			logger.Error("schedule.cancelled: parse failure — discarding",
				"message_id", msg.ID, "error", err)
			return nil // permanent error: ACK by returning nil (orchestrator convention)
		}
		return handler.Handle(ctx, evt)
	}
}
