package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/agent-remediation/config"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
)

// failInFlightTimeout bounds the single-row recovery UPDATE the drop handler
// runs, so a slow database can never stall the consumer's reclaim sweep on the
// off-critical drop path.
const failInFlightTimeout = 10 * time.Second

// TriggerFromPayload decodes a remediation.requested:v2 payload — one batched
// trigger per rejected release, carrying its whole healable node set — into a
// handlers.Trigger. The dedup identity fields (MessageID, OutboxEntryID) are
// left unset: they belong to the Redis message that delivered the payload
// rather than to the payload itself, so a caller replaying stored bytes
// supplies an identity of its own. RawPayload is set to raw so a replayed
// trigger carries the same bytes the original did. Returns an error if the
// JSON is malformed.
func TriggerFromPayload(raw []byte) (handlers.Trigger, error) {
	var w handlers.TriggerWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return handlers.Trigger{}, fmt.Errorf("unmarshal remediation.requested payload: %w", err)
	}
	t := handlers.TriggerFromWire(w)
	t.RawPayload = raw
	return t, nil
}

// triggerFromRequested decodes a remediation.requested:v2 payload into a
// handlers.Trigger and stamps it with the dedup identity of the Redis message
// that delivered it.
func triggerFromRequested(messageID string, msg goredis.XMessage, raw []byte) (handlers.Trigger, error) {
	t, err := TriggerFromPayload(raw)
	if err != nil {
		return handlers.Trigger{}, err
	}
	t.MessageID = messageID
	t.OutboxEntryID = messageprocessing.ExtractOutboxEntryID(msg.Values)
	return t, nil
}

// NewRemediationRequestedConsumer constructs a StreamConsumer that reads
// remediation.requested:v2. With the runner engine each message is handed to
// accept, which records it as a remediation the worker drives. With the legacy
// engine each message proposes one fix attempt per rejected release via
// handlers.ProposeFix, and a message dead-lettered after its last delivery
// fails the in-flight attempt row it left behind. The consumer group is
// created idempotently by StreamConsumer.Start; call Start(ctx) in a goroutine
// to begin consuming.
func NewRemediationRequestedConsumer(rc *goredis.Client, deps handlers.Deps, accept func(context.Context, runner.Accepted) error, engine string, logger *slog.Logger) *pkgredis.StreamConsumer {
	var opts []pkgredis.ConsumerOption
	if engine != config.EngineRunner {
		opts = append(opts, pkgredis.WithOnDrop(failInFlightOnDrop(logger, func(ctx context.Context, releaseID, reason string) (int, error) {
			ctx, cancel := context.WithTimeout(ctx, failInFlightTimeout)
			defer cancel()
			return handlers.FailInFlight(ctx, deps, releaseID, reason)
		})))
	}
	return pkgredis.NewStreamConsumer(
		rc,
		streams.RemediationRequestedV2,
		streams.AgentRemediationRemediationRequested,
		newRemediationRequestedHandler(deps, accept, engine),
		logger,
		opts...,
	)
}

// newRemediationRequestedHandler returns the MessageHandler for each
// remediation.requested:v2 message: with the runner engine it accepts the
// decoded trigger as a remediation, otherwise it proposes one fix attempt for
// the release. A message with no payload field, or one whose payload cannot be
// decoded, is a permanent failure: the handler returns events.ErrPermanent, so
// the consumer dead-letters it. Any other error is returned as is, so the
// consumer redelivers it.
func newRemediationRequestedHandler(deps handlers.Deps, accept func(context.Context, runner.Accepted) error, engine string) pkgredis.MessageHandler {
	if engine == config.EngineRunner {
		return func(ctx context.Context, msg goredis.XMessage) error {
			raw, ok := msg.Values["payload"].(string)
			if !ok {
				return fmt.Errorf("%w: %s message has no payload field", pkgevents.ErrPermanent, streams.RemediationRequestedV2)
			}
			trigger, err := serialization.DecodeRemediationRequestedV2([]byte(raw))
			if err != nil {
				return fmt.Errorf("%w: %s decode: %v", pkgevents.ErrPermanent, streams.RemediationRequestedV2, err)
			}
			trigger.TenantID = tenantOf(msg)
			return accept(ctx, runner.Accepted{
				Trigger:       trigger,
				Payload:       []byte(raw),
				MessageID:     pkgredis.DedupMessageID(ctx, msg),
				OutboxEntryID: messageprocessing.ExtractOutboxEntryID(msg.Values),
				Fields:        streamFields(msg),
			})
		}
	}
	return func(ctx context.Context, msg goredis.XMessage) error {
		raw, ok := msg.Values["payload"].(string)
		if !ok {
			return fmt.Errorf("%w: %s message has no payload field", pkgevents.ErrPermanent, streams.RemediationRequestedV2)
		}
		trigger, err := triggerFromRequested(pkgredis.DedupMessageID(ctx, msg), msg, []byte(raw))
		if err != nil {
			return fmt.Errorf("%w: %s decode: %v", pkgevents.ErrPermanent, streams.RemediationRequestedV2, err)
		}
		return handlers.ProposeFix(ctx, deps, trigger)
	}
}

// tenantOf returns the tenant_id the message carries, or the default tenant.
func tenantOf(msg goredis.XMessage) string {
	if t, ok := msg.Values["tenant_id"].(string); ok && t != "" {
		return t
	}
	return pkgevents.DefaultTenantID
}

// streamFields copies the message's fields as strings, without the payload,
// which a remediation stores on its own.
func streamFields(msg goredis.XMessage) map[string]string {
	out := make(map[string]string, len(msg.Values))
	for k, v := range msg.Values {
		if k == "payload" {
			continue
		}
		if s, ok := v.(string); ok {
			out[k] = s
			continue
		}
		out[k] = fmt.Sprint(v)
	}
	return out
}

// failInFlightOnDrop builds the drop handler that closes out a remediation
// attempt the stream consumer abandoned. markGenerating commits an in-flight
// 'generating' row before the model is called; if the trigger then fails on
// every redelivery and is dead-lettered on its fifth delivery, that row is left
// reporting a fix as forever generating — and the release's "Try again" stays
// blocked behind it. This decodes the dead-lettered trigger's release id and
// fails that row via fail.
//
// A message with no payload, or one whose payload cannot be decoded, named no
// release and never created a row (markGenerating runs only after a successful
// decode), so it is ignored.
func failInFlightOnDrop(logger *slog.Logger, fail func(ctx context.Context, releaseID, reason string) (int, error)) pkgredis.DropHandler {
	return func(ctx context.Context, msg goredis.XMessage, cause error) {
		raw, ok := msg.Values["payload"].(string)
		if !ok {
			return
		}
		t, err := TriggerFromPayload([]byte(raw))
		if err != nil || t.ReleaseID == "" {
			return
		}
		reason := fmt.Sprintf("remediation trigger dead-lettered: %v", cause)
		n, ferr := fail(ctx, t.ReleaseID, reason)
		if ferr != nil {
			logger.Error("could not fail in-flight remediation row after its trigger was dead-lettered",
				"stream", streams.RemediationRequestedV2, "message_id", msg.ID, "release", t.ReleaseID, "error", ferr)
			return
		}
		if n > 0 {
			logger.Warn("failed in-flight remediation row after its trigger was dead-lettered",
				"stream", streams.RemediationRequestedV2, "message_id", msg.ID, "release", t.ReleaseID, "cause", cause)
		}
	}
}
