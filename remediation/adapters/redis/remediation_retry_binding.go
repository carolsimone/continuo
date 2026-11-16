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

// NewRemediationRetryConsumer consumes remediation.retry_requested:v2: a
// rejected release's stored rejection replayed with a remediation_round. It
// classifies the rejection's whole failed-node set exactly as the original
// rejection was, emitting one batched trigger for the round under the retry's
// tenant, so a "try again" walks the same path as the rejection itself, one
// round later.
func NewRemediationRetryConsumer(rc *goredis.Client, deps handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	return pkgredis.NewStreamConsumer(rc, streams.RemediationRetryRequestedV2, streams.RemediationRetryRequested,
		newRetryHandler(deps, logger), logger)
}

// newRetryHandler returns the MessageHandler that classifies the stored
// rejection of one remediation.retry_requested:v2 message, stamping the
// retry's own round on its evidence and storing the trigger under the tenant of
// the message's envelope. An entry that cannot be read, one that names no
// tenant and one whose rejection cannot be decoded are permanent failures: the
// error wraps events.ErrPermanent, so the consumer dead-letters them. Any error
// from classification is returned as is, so the consumer redelivers it.
func newRetryHandler(deps handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		env, retry, err := pkgevents.DecodeRemediationRetryRequested(stringFields(msg.Values))
		if err != nil {
			return fmt.Errorf("%w: %s decode: %v", pkgevents.ErrPermanent, streams.RemediationRetryRequestedV2, err)
		}
		if env.TenantID == "" {
			return fmt.Errorf("%w: %s entry of release %s names no tenant", pkgevents.ErrPermanent, streams.RemediationRetryRequestedV2, retry.ReleaseID)
		}
		return classifyRejectionBody(ctx, deps, logger, streams.RemediationRetryRequestedV2, env.TenantID, retry.Rejection, retry.RemediationRound)
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
