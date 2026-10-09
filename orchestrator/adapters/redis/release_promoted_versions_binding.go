package redis

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewReleasePromotedVersionsBinding wires ParseReleasePromotedV2 into the
// ReleasePromotedVersionsHandler. A decode failure is permanent
// (events.ErrPermanent): the binding logs and returns the error so the consumer
// dead-letters the entry.
//
// outbox_entry_id is extracted from the message fields and threaded to the
// handler so the dedup layer can catch re-XADDs of the same upstream outbox row
// that arrive under a fresh Redis message ID.
func NewReleasePromotedVersionsBinding(
	handler *handlers.ReleasePromotedVersionsHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		in, err := ParseReleasePromotedV2(msg)
		if err != nil {
			logger.Error("release.promoted (versions): decode failure — dead-lettering",
				"message_id", msg.ID, "error", err)
			return err
		}
		return handler.Handle(ctx, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values), in)
	}
}
