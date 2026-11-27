package redis

import (
	"context"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation/service/handlers"
)

// NewRoundRequestedConsumer consumes round.requested:v1: a
// rejected release's stored rejection replayed with a round. It
// classifies the rejection's whole failed-node set exactly as the original
// rejection was, emitting one batched trigger for the round under the retry's
// tenant, so a "try again" walks the same path as the rejection itself, one
// round later.
func NewRoundRequestedConsumer(rc *goredis.Client, deps handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	return pkgredis.NewStreamConsumer(rc, streams.RoundRequestedV1, streams.RemediationRoundRequested,
		newRoundRequestedHandler(deps, logger), logger)
}

// newRoundRequestedHandler returns the MessageHandler that classifies the stored
// rejection of one round.requested:v1 message, stamping the
// retry's own round on its evidence and storing the trigger under the tenant of
// the message's envelope. An entry that cannot be read, one that names no
// tenant and one whose rejection cannot be decoded are permanent failures: the
// error wraps events.ErrPermanent, so the consumer dead-letters them. Any error
// from classification is returned as is, so the consumer redelivers it.
func newRoundRequestedHandler(deps handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		env, retry, err := pkgevents.DecodeRoundRequested(stringFields(msg.Values))
		if err != nil {
			return fmt.Errorf("%w: %s decode: %v", pkgevents.ErrPermanent, streams.RoundRequestedV1, err)
		}
		if env.TenantID == "" {
			return fmt.Errorf("%w: %s entry of release %s names no tenant", pkgevents.ErrPermanent, streams.RoundRequestedV1, retry.ReleaseID)
		}
		return classifyRejectionBody(ctx, deps, logger, streams.RoundRequestedV1, env.TenantID, retry.Rejection, retry.Round)
	}
}

// stringFields copies the message's string fields, the shape the typed decoders
// read.
func stringFields(values map[string]any) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}
