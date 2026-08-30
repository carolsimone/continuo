package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
)

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
	handler := func(ctx context.Context, msg goredis.XMessage) error {
		raw, ok := msg.Values["payload"].(string)
		if !ok {
			logger.Error("remediation.requested:v2 missing payload — discarding", "message_id", msg.ID)
			return nil // permanent: ACK by returning nil so the message is not left in the PEL
		}
		trigger, err := triggerFromRequested(msg, []byte(raw))
		if err != nil {
			logger.Error("remediation.requested:v2 decode failure — discarding", "message_id", msg.ID, "error", err)
			return nil // permanent: malformed payload cannot be retried
		}
		if err := handlers.ProposeFix(ctx, deps, trigger); err != nil {
			return err // transient: do not ACK; allow redelivery via PEL sweep
		}
		return nil
	}
	return pkgredis.NewStreamConsumer(
		rc,
		streams.RemediationRequestedV2,
		streams.AgentRemediationRemediationRequested,
		handler,
		logger,
	)
}
