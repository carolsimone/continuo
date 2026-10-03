package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
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
func triggerFromRequested(msg goredis.XMessage, raw []byte) (handlers.Trigger, error) {
	t, err := TriggerFromPayload(raw)
	if err != nil {
		return handlers.Trigger{}, err
	}
	t.MessageID = msg.ID
	t.OutboxEntryID = messageprocessing.ExtractOutboxEntryID(msg.Values)
	return t, nil
}

// NewRemediationRequestedConsumer constructs a StreamConsumer that reads
// remediation.requested:v2 and proposes one fix attempt per rejected release
// via handlers.ProposeFix. The consumer group is created idempotently by
// StreamConsumer.Start; call Start(ctx) in a goroutine to begin consuming.
func NewRemediationRequestedConsumer(rc *goredis.Client, deps handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	return pkgredis.NewStreamConsumer(
		rc,
		streams.RemediationRequestedV2,
		streams.AgentRemediationRemediationRequested,
		newRemediationRequestedHandler(deps, logger),
		logger,
		pkgredis.WithOnDrop(failInFlightOnDrop(logger, func(ctx context.Context, releaseID, reason string) (int, error) {
			ctx, cancel := context.WithTimeout(ctx, failInFlightTimeout)
			defer cancel()
			return handlers.FailInFlight(ctx, deps, releaseID, reason)
		})),
	)
}

// newRemediationRequestedHandler returns the MessageHandler that proposes one
// fix attempt for each remediation.requested:v2 message. A message with no
// payload field, or one whose payload cannot be decoded, is a permanent
// failure: the handler returns events.ErrPermanent, so the consumer
// dead-letters it. Any error from handlers.ProposeFix is returned as is, so
// the consumer redelivers it.
func newRemediationRequestedHandler(deps handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		raw, ok := msg.Values["payload"].(string)
		if !ok {
			return fmt.Errorf("%w: %s message has no payload field", pkgevents.ErrPermanent, streams.RemediationRequestedV2)
		}
		trigger, err := triggerFromRequested(msg, []byte(raw))
		if err != nil {
			return fmt.Errorf("%w: %s decode: %v", pkgevents.ErrPermanent, streams.RemediationRequestedV2, err)
		}
		return handlers.ProposeFix(ctx, deps, trigger)
	}
}

// failInFlightOnDrop builds the drop handler that closes out a remediation
// attempt the stream consumer abandoned. markGenerating commits an in-flight
// 'generating' row before the model is called; if the trigger then fails on
// every redelivery and is poison-dropped, that row is left reporting a fix as
// forever generating — and the release's "Try again" stays blocked behind it.
// This decodes the dropped trigger's release id and fails that row via fail.
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
		reason := fmt.Sprintf("remediation trigger dropped after exhausting redelivery: %v", cause)
		n, ferr := fail(ctx, t.ReleaseID, reason)
		if ferr != nil {
			logger.Error("could not fail in-flight remediation row after its trigger was dropped",
				"stream", streams.RemediationRequestedV2, "message_id", msg.ID, "release", t.ReleaseID, "error", ferr)
			return
		}
		if n > 0 {
			logger.Warn("failed in-flight remediation row after its trigger was dropped",
				"stream", streams.RemediationRequestedV2, "message_id", msg.ID, "release", t.ReleaseID, "cause", cause)
		}
	}
}
