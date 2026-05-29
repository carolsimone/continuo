package redis

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewReleasePromotedBinding wires ParseReleasePromoted into the
// ReleasePromotedHandler. Parse failures are permanently ACKed (logged at
// error level), matching orchestrator's existing convention.
//
// outbox_entry_id is extracted from the message fields and threaded to the
// handler so the dedup layer can catch re-XADDs of the same upstream outbox
// row that arrive under a fresh Redis message ID.
func NewReleasePromotedBinding(
	handler *handlers.ReleasePromotedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseReleasePromoted(msg)
		if err != nil {
			logger.Error("release.promoted: parse failure — discarding",
				"message_id", msg.ID, "error", err)
			return nil // permanent error: ACK by returning nil (orchestrator convention)
		}
		in := model.PromoteReleaseInput{
			ReleaseID: evt.ReleaseID,
			Topology:  evt.Topology,
			ImageTags: evt.ImageTags,
		}
		outboxEntryID := messageprocessing.ExtractOutboxEntryID(msg.Values)
		return handler.Handle(ctx, msg.ID, outboxEntryID, in)
	}
}
