package redis

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

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
		// The message ID's millisecond prefix dates when this terminal event was
		// first published and is stable across redeliveries, giving the handler a
		// state-free age for its completeness-barrier escape clock. A well-formed
		// Redis ID always parses; if it somehow does not, leave EmittedAt zero
		// (which keeps the barrier closed) and log.
		if emitted, ok := emittedAtFromMsgID(msg.ID); ok {
			in.EmittedAt = emitted
		} else {
			logger.Warn("validation.completed:v1 message ID has no parseable millis prefix; barrier escape clock disabled",
				"message_id", msg.ID)
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

// emittedAtFromMsgID parses the millisecond prefix of a Redis stream message ID
// ("<unixMillis>-<seq>") into the time the entry was first published. The prefix
// is stable across redeliveries (the same PEL entry keeps its original ID), so
// it dates the original publish regardless of how many times the message has
// been redelivered. Returns ok=false when the ID has no numeric millis prefix.
func emittedAtFromMsgID(id string) (time.Time, bool) {
	dash := strings.IndexByte(id, '-')
	if dash < 0 {
		return time.Time{}, false
	}
	ms, err := strconv.ParseInt(id[:dash], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}
