package publisher

import (
	"context"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	goredis "github.com/redis/go-redis/v9"
)

// OutboxPublisher publishes orchestrator outbox entries to Redis streams.
// Each entry represents one event: its Payload is unmarshaled into a per-event
// field map and written via XADD to the stream named by entry.StreamName.
type OutboxPublisher struct {
	redis  *goredis.Client
	logger *slog.Logger
}

// NewOutboxPublisher constructs a publisher backed by a real Redis client.
func NewOutboxPublisher(r *goredis.Client, l *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{redis: r, logger: l}
}

// Publish dispatches on event type to build the Redis field map, then XADDs it
// to the stream stored in entry.StreamName.
func (p *OutboxPublisher) Publish(ctx context.Context, entry *outbox.Entry) error {
	args, err := p.xaddArgs(entry)
	if err != nil {
		return err
	}
	if _, err := p.redis.XAdd(ctx, args).Result(); err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
	}
	return nil
}

// PublishBatch pipelines one XADD per entry over a single Redis round trip and
// returns one error per entry, positionally aligned with entries. Commands are
// queued in slice order on a single connection, so per-aggregate FIFO (imposed
// by the outbox SELECT order) is preserved. A payload that fails to encode is
// reported in its slot without aborting the rest of the batch.
func (p *OutboxPublisher) PublishBatch(ctx context.Context, entries []*outbox.Entry) []error {
	errs := make([]error, len(entries))
	pipe := p.redis.Pipeline()
	cmds := make([]*goredis.StringCmd, len(entries))
	for i, entry := range entries {
		args, err := p.xaddArgs(entry)
		if err != nil {
			errs[i] = err
			continue
		}
		cmds[i] = pipe.XAdd(ctx, args)
	}
	// Exec flushes every queued command. A transport-level error is surfaced
	// per command below; we ignore Exec's aggregate error and read each result.
	_, _ = pipe.Exec(ctx)
	for i, cmd := range cmds {
		if cmd == nil {
			continue // encode failure already recorded
		}
		if err := cmd.Err(); err != nil {
			errs[i] = fmt.Errorf("xadd to %s: %w", entries[i].StreamName, err)
		}
	}
	return errs
}

// xaddArgs builds the XADD arguments for one entry, shared by single and batched
// publishes. It sets no length cap: the dead-letter-controller's trim loop
// bounds every stream.
func (p *OutboxPublisher) xaddArgs(entry *outbox.Entry) (*goredis.XAddArgs, error) {
	values, err := p.publishValues(entry)
	if err != nil {
		return nil, err
	}
	return &goredis.XAddArgs{
		Stream: entry.StreamName,
		Values: values,
	}, nil
}

// PayloadToValuesForTest is a test-only accessor that exposes publishValues
// for white-box unit tests without requiring a live Redis client.
func (p *OutboxPublisher) PayloadToValuesForTest(entry *outbox.Entry) (map[string]interface{}, error) {
	return p.publishValues(entry)
}

// publishValues returns the full XADD field map for entry: its rendered fields
// plus outbox_entry_id, so consumer-side pkg/messageprocessing.DedupWithOutboxEntryID
// catches a row republished with a fresh Redis message id.
func (p *OutboxPublisher) publishValues(entry *outbox.Entry) (map[string]interface{}, error) {
	values, err := p.Render(entry)
	if err != nil {
		return nil, err
	}
	values["outbox_entry_id"] = entry.ID.String()
	return values, nil
}

var _ outbox.Renderer = (*OutboxPublisher)(nil)

// Render returns the field map Publish XADDs for entry, without
// outbox_entry_id. The only event type orchestrator publishes is the dead
// letter the outbox processor writes for a row it gave up on. Every call
// returns a fresh map.
func (p *OutboxPublisher) Render(entry *outbox.Entry) (map[string]any, error) {
	switch entry.EventType {
	case outbox.DeadLetterEventType:
		// Dead-letter rows publish generically: their payload is already a flat
		// scalar map (outbox.DeadLetterPayload), expanded via DeadLetterValues.
		values, err := outbox.DeadLetterValues(entry)
		if err != nil {
			// Our own payload; a decode failure here is deterministic, never transient.
			return nil, fmt.Errorf("%w: dead-letter values: %v", pkgevents.ErrPermanent, err)
		}
		delete(values, "outbox_entry_id")
		return values, nil

	default:
		// Unknown event_type is retried, not dead-lettered: during a rolling
		// deployment an old replica can dequeue a row for a newly-introduced
		// event_type that a newer replica (already deployed) will publish
		// moments later once this row cycles back to it.
		return nil, fmt.Errorf("orchestrator publisher: unknown event_type %q (retryable — a newer replica may handle it during a rolling upgrade)", entry.EventType)
	}
}
