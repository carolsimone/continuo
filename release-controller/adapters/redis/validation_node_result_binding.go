package redis

import (
	"context"
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// NewValidationNodeResultConsumer constructs a StreamConsumer that reads
// validation.node.result:v1 and projects each per-node outcome into the release
// read model via handlers.HandleNodeValidationResult. Unlike the terminal
// validation.completed:v1 consumer, it makes no promote/reject decision and
// does not advance the release queue.
func NewValidationNodeResultConsumer(rc *goredis.Client, deps *handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	handler := newValidationNodeResultHandler(deps, logger)
	return pkgredis.NewStreamConsumer(
		rc,
		streams.ValidationNodeResultV1,
		streams.ReleaseControllerValidationNodeResult,
		handler,
		logger,
	)
}

// newValidationNodeResultHandler returns a MessageHandler that decodes the
// "payload" field of each validation.node.result:v1 message and dispatches it
// to handlers.HandleNodeValidationResult.
func newValidationNodeResultHandler(deps *handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		var in handlers.NodeValidationResultInput
		if err := decodePayload(msg, &in); err != nil {
			logger.Error("validation.node.result:v1 decode failure — discarding",
				"message_id", msg.ID, "error", err)
			return nil // permanent: ack by returning nil
		}
		return handlers.HandleNodeValidationResult(ctx, deps, in)
	}
}
