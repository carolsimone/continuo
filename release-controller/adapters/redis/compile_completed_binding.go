package redis

import (
	"context"
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// NewCompileCompletedConsumer consumes compile.completed:v1 and dispatches to
// handlers.HandleCompileResult (advancing to release.requested or rejecting).
func NewCompileCompletedConsumer(rc *goredis.Client, deps *handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	return pkgredis.NewStreamConsumer(rc, streams.CompileCompletedV1,
		streams.ReleaseControllerCompileCompleted, newCompileCompletedHandler(deps, logger), logger)
}

func newCompileCompletedHandler(deps *handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		var in handlers.HandleCompileResultInput
		if err := decodePayload(msg, &in); err != nil {
			logger.Error("compile.completed:v1 decode failure — discarding", "message_id", msg.ID, "error", err)
			return nil
		}
		if err := handlers.HandleCompileResult(ctx, deps, in); err != nil {
			return err
		}
		// Advance the queue after every compile result. On the success path the
		// release stays active (Parsing → will receive manifest.loaded.candidate),
		// so this is a no-op. On the failure path the release was just Rejected,
		// so this unblocks the next queued candidate.
		if err := handlers.AdvanceQueue(ctx, deps); err != nil {
			logger.Error("advance queue after compile.completed", "error", err)
			// Non-fatal: the periodic trigger in main.go will eventually advance
			// the queue; do not fail the ACK for this release.
		}
		return nil
	}
}
