package redis

import (
	"context"
	"encoding/json"
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// NewValidationResultConsumer constructs a StreamConsumer that reads the unified
// validation.result:v1 stream and, per message kind, either projects one node's
// outcome into the release read model or decides the terminal promote-or-reject.
//
// Both kinds share one aggregate_id, so PerAggregateFIFO delivers them in order
// through a single consumer: every kind=node message lands before the trailing
// kind=complete message. That lets the terminal decision read a fully-projected
// store without any completeness barrier. Call Start(ctx) in a goroutine to
// begin consuming; the consumer group is created idempotently by Start.
func NewValidationResultConsumer(rc *goredis.Client, deps *handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	handler := newValidationResultHandler(deps, logger)
	return pkgredis.NewStreamConsumer(
		rc,
		streams.ValidationResultV1,
		streams.ReleaseControllerValidationResult,
		handler,
		logger,
	)
}

// newValidationResultHandler returns a MessageHandler that decodes the "payload"
// field of each validation.result:v1 message, inspects its "kind" discriminator,
// and routes:
//   - kind=node     → handlers.HandleNodeValidationResult (project one node)
//   - kind=complete → handlers.HandleValidationResult, then handlers.AdvanceQueue
//     on success so the next queued release moves forward immediately.
//
// A message whose payload cannot be decoded or carries an unknown kind is a
// permanent failure: the handler logs and returns nil so the consumer ACKs and
// drops it rather than redelivering forever.
func newValidationResultHandler(deps *handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		raw, ok := msg.Values["payload"].(string)
		if !ok {
			logger.Error("validation.result:v1 missing or non-string payload — discarding", "message_id", msg.ID)
			return nil
		}

		var envelope struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
			logger.Error("validation.result:v1 envelope decode failure — discarding",
				"message_id", msg.ID, "error", err)
			return nil
		}

		switch envelope.Kind {
		case "node":
			var in handlers.NodeValidationResultInput
			if err := json.Unmarshal([]byte(raw), &in); err != nil {
				logger.Error("validation.result:v1 node decode failure — discarding",
					"message_id", msg.ID, "error", err)
				return nil
			}
			return handlers.HandleNodeValidationResult(ctx, deps, in)

		case "complete":
			var in handlers.HandleValidationResultInput
			if err := json.Unmarshal([]byte(raw), &in); err != nil {
				logger.Error("validation.result:v1 complete decode failure — discarding",
					"message_id", msg.ID, "error", err)
				return nil
			}
			if err := handlers.HandleValidationResult(ctx, deps, in); err != nil {
				return err
			}
			// Advance the queue immediately after the terminal decision so the next
			// queued release begins without waiting for an external trigger.
			if err := handlers.AdvanceQueue(ctx, deps); err != nil {
				logger.Error("advance queue after validation.result complete", "error", err)
				// Non-fatal: the periodic trigger in main.go will eventually advance
				// the queue; do not fail the ACK for this release.
			}
			return nil

		default:
			logger.Error("validation.result:v1 unknown kind — discarding",
				"message_id", msg.ID, "kind", envelope.Kind)
			return nil
		}
	}
}
