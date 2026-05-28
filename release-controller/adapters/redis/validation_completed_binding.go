package redis

import (
	"context"
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// NewValidationCompletedConsumer constructs a StreamConsumer that reads
// validation.completed:v1 and dispatches each message to
// handlers.HandleValidationResult. After each successful dispatch it calls
// handlers.AdvanceQueue so the next queued release can move forward immediately.
// The consumer group is created idempotently by StreamConsumer.Start; call
// Start(ctx) in a goroutine to begin consuming.
func NewValidationCompletedConsumer(
	rc *goredis.Client,
	deps *handlers.Deps,
	logger *slog.Logger,
) *pkgredis.StreamConsumer {
	handler := newValidationCompletedHandler(deps, logger)
	return pkgredis.NewStreamConsumer(
		rc,
		streams.ValidationCompletedV1,
		streams.ReleaseControllerValidationCompleted,
		handler,
		logger,
	)
}

// newValidationCompletedHandler returns a MessageHandler that decodes the
// "payload" field of each validation.completed:v1 message, calls
// handlers.HandleValidationResult, and on success advances the release queue.
func newValidationCompletedHandler(deps *handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		var in handlers.HandleValidationResultInput
		if err := decodePayload(msg, &in); err != nil {
			logger.Error("validation.completed:v1 decode failure — discarding",
				"message_id", msg.ID, "error", err)
			return nil // permanent: ACK by returning nil so it is not left in the PEL
		}
		if err := handlers.HandleValidationResult(ctx, deps, in); err != nil {
			return err
		}
		// Advance the queue immediately after a validation result is processed so
		// the next queued release begins parsing without waiting for an external
		// trigger.
		if err := handlers.AdvanceQueue(ctx, deps); err != nil {
			logger.Error("advance queue after validation.completed", "error", err)
			// Non-fatal: the periodic trigger in main.go will eventually advance
			// the queue; do not fail the ACK for this release.
		}
		return nil
	}
}
