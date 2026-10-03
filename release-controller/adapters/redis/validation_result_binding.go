package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// NewValidationResultConsumer constructs a StreamConsumer that reads the unified
// validation.result:v1 stream and, per message kind, either projects one node's
// outcome into the release read model or decides the terminal promote-or-reject.
//
// executor emits the kind=complete message last, after every kind=node row for
// the release; the per-node rows publish in parallel and, in the normal case,
// flush in the same created_at-ordered outbox batch as the terminal, so a single
// in-order consumer has already projected every node by the time it decides. The
// terminal decision reads only aggregate_status, so it does not depend on
// delivery order and needs no completeness barrier. Call Start(ctx) in a
// goroutine to begin consuming; the consumer group is created idempotently by Start.
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
// permanent failure: the handler returns events.ErrPermanent, so the consumer
// dead-letters it.
func newValidationResultHandler(deps *handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		raw, ok := msg.Values["payload"].(string)
		if !ok {
			return fmt.Errorf("%w: %s message has no payload field", pkgevents.ErrPermanent, streams.ValidationResultV1)
		}

		var envelope struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
			return fmt.Errorf("%w: %s envelope decode: %v", pkgevents.ErrPermanent, streams.ValidationResultV1, err)
		}

		switch envelope.Kind {
		case "node":
			var in handlers.NodeValidationResultInput
			if err := json.Unmarshal([]byte(raw), &in); err != nil {
				return fmt.Errorf("%w: %s node decode: %v", pkgevents.ErrPermanent, streams.ValidationResultV1, err)
			}
			return handlers.HandleNodeValidationResult(ctx, deps, in)

		case "complete":
			var in handlers.HandleValidationResultInput
			if err := json.Unmarshal([]byte(raw), &in); err != nil {
				return fmt.Errorf("%w: %s complete decode: %v", pkgevents.ErrPermanent, streams.ValidationResultV1, err)
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
			return fmt.Errorf("%w: %s unknown kind %q", pkgevents.ErrPermanent, streams.ValidationResultV1, envelope.Kind)
		}
	}
}
