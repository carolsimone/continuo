package redis

import (
	"context"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
)

// NewRemediationRequestedConsumer constructs the StreamConsumer of
// remediation.requested:v3: each message is handed to accept, which records it
// as a remediation the worker drives. The consumer group is created
// idempotently by StreamConsumer.Start; call Start(ctx) in a goroutine to begin
// consuming.
func NewRemediationRequestedConsumer(rc *goredis.Client, accept func(context.Context, runner.Accepted) error, logger *slog.Logger) *pkgredis.StreamConsumer {
	return pkgredis.NewStreamConsumer(
		rc,
		streams.RemediationRequestedV3,
		streams.AgentRemediationRemediationRequested,
		newRemediationRequestedHandler(accept),
		logger,
	)
}

// requestedMessage decodes one remediation.requested:v3 message: its envelope,
// the heal trigger its payload carries and the payload's bytes. A message that
// does not decode is a permanent failure: the error wraps events.ErrPermanent,
// so the consumer dead-letters it.
func requestedMessage(msg goredis.XMessage) (pkgevents.Envelope, pkgevents.RemediationRequested, []byte, error) {
	fields := messageFields(msg)
	env, p, err := pkgevents.DecodeRemediationRequested(fields)
	if err != nil {
		return pkgevents.Envelope{}, pkgevents.RemediationRequested{}, nil,
			fmt.Errorf("%w: %s decode: %v", pkgevents.ErrPermanent, streams.RemediationRequestedV3, err)
	}
	return env, p, []byte(fields["payload"]), nil
}

// newRemediationRequestedHandler returns the MessageHandler that accepts
// each remediation.requested:v3 message's trigger as a remediation, unfixable
// nodes included, with the tenant the envelope carries (empty when it names
// none), the payload bytes, the message's dedup id, the producer's outbox entry
// id and the stream's fields. A message that does not decode is a permanent
// failure: the handler returns events.ErrPermanent, so the consumer
// dead-letters it. Any other error is returned as is, so the consumer
// redelivers it.
func newRemediationRequestedHandler(accept func(context.Context, runner.Accepted) error) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		env, p, payload, err := requestedMessage(msg)
		if err != nil {
			return err
		}
		trigger := serialization.TriggerFromRequested(p)
		trigger.TenantID = env.TenantID
		return accept(ctx, runner.Accepted{
			Trigger:       trigger,
			Payload:       payload,
			MessageID:     pkgredis.DedupMessageID(ctx, msg),
			OutboxEntryID: messageprocessing.ExtractOutboxEntryID(msg.Values),
			Fields:        streamFields(msg),
		})
	}
}

// messageFields copies the message's fields as strings.
func messageFields(msg goredis.XMessage) map[string]string {
	out := make(map[string]string, len(msg.Values))
	for k, v := range msg.Values {
		if s, ok := v.(string); ok {
			out[k] = s
			continue
		}
		out[k] = fmt.Sprint(v)
	}
	return out
}

// streamFields copies the message's fields as strings, without the payload,
// which a remediation stores on its own.
func streamFields(msg goredis.XMessage) map[string]string {
	out := messageFields(msg)
	delete(out, "payload")
	return out
}
