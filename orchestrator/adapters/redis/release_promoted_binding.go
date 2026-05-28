package redis

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewReleasePromotedBinding wires ParseReleasePromoted into the
// ReleasePromotedHandler. Parse failures are permanently ACKed (logged at
// error level), matching orchestrator's existing convention.
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
		return handler.Handle(ctx, msg.ID, in)
	}
}
