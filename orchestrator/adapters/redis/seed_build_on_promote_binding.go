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

// NewSeedBuildOnPromoteBinding wires ParseReleasePromoted into the
// SeedBuildOnPromoteHandler. A parse failure is permanent (events.ErrPermanent):
// the binding logs and returns the error so the consumer ACKs and drops the
// poison message.
//
// outbox_entry_id is extracted from the message fields and threaded to the
// handler so the dedup layer can catch re-XADDs of the same upstream outbox
// row that arrive under a fresh Redis message ID.
func NewSeedBuildOnPromoteBinding(
	handler *handlers.SeedBuildOnPromoteHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseReleasePromoted(msg)
		if err != nil {
			logger.Error("release.promoted (seed-build): parse failure — discarding",
				"message_id", msg.ID, "error", err)
			return err
		}
		in := model.PromoteReleaseInput{
			ReleaseID:     evt.ReleaseID,
			Topology:      evt.Topology,
			ImageTags:     evt.ImageTags,
			Repo:          evt.Repo,
			CommitSHA:     evt.CommitSHA,
			PromotedAt:    evt.PromotedAt,
			CodeBundleURI: evt.CodeBundleURI,
			Bootstrap:     evt.Bootstrap,
		}
		outboxEntryID := messageprocessing.ExtractOutboxEntryID(msg.Values)
		return handler.Handle(ctx, msg.ID, outboxEntryID, in)
	}
}
