package redis

import (
	"context"
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// NewSeedBuildCompletedConsumer consumes seed.build.completed:v1 and dispatches to
// handlers.HandleSeedBuildResult (advancing to validation.requested or rejecting).
func NewSeedBuildCompletedConsumer(rc *goredis.Client, deps *handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	return pkgredis.NewStreamConsumer(rc, streams.SeedBuildCompletedV1,
		streams.ReleaseControllerSeedBuildCompleted, newSeedBuildCompletedHandler(deps, logger), logger)
}

func newSeedBuildCompletedHandler(deps *handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		var in handlers.HandleSeedBuildResultInput
		if err := decodePayload(msg, &in); err != nil {
			logger.Error("seed.build.completed:v1 decode failure — discarding", "message_id", msg.ID, "error", err)
			return nil
		}
		if err := handlers.HandleSeedBuildResult(ctx, deps, in); err != nil {
			return err
		}
		// Advance the queue after every seed-build result. On the success path the
		// release stays active (Validating or promoted), so this is a no-op. On the
		// failure path the release was just Rejected, so this unblocks the next
		// queued candidate.
		if err := handlers.AdvanceQueue(ctx, deps); err != nil {
			logger.Error("advance queue after seed.build.completed", "error", err)
			// Non-fatal: the periodic trigger in main.go will eventually advance
			// the queue; do not fail the ACK for this release.
		}
		return nil
	}
}
