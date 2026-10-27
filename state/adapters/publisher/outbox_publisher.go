package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// producer names state as the producer of the enveloped streams it writes.
const producer = "state"

// OutboxPublisher publishes state's outbox entries to Redis streams. Each
// outbox row represents one event, rendered as a field map (Render) and
// written via XADD to the stream named by StreamName: a task.dispatch:v1 or
// run.cancelled:v1 row under an envelope, a dead-letter row as its dead-letter
// fields, and every other row's JSON payload flattened to scalar fields, its
// nested values re-encoded as JSON strings.
type OutboxPublisher struct {
	redis  *goredis.Client
	logger *slog.Logger
}

// NewOutboxPublisher constructs a publisher backed by a real Redis client.
func NewOutboxPublisher(redis *goredis.Client, logger *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{redis: redis, logger: logger}
}

// Publish renders the entry as its field map (Render), adds the entry's
// outbox_entry_id and XADDs it to entry.StreamName.
func (p *OutboxPublisher) Publish(ctx context.Context, entry *outbox.Entry) error {
	args, err := p.xaddArgs(entry)
	if err != nil {
		return err
	}
	if err := p.redis.XAdd(ctx, args).Err(); err != nil {
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

var _ outbox.Renderer = (*OutboxPublisher)(nil)

// Render returns the field map Publish XADDs for entry, without
// outbox_entry_id. A dead-letter row renders through outbox.DeadLetterValues;
// a task.dispatch:v1 row's payload is the dispatch and a run.cancelled:v1
// row's the cancel, each stored under its tenant (outbox.WrapTenant) and
// rendered under an envelope that names the row as the event, the tenant it
// stores and the time it was written; every other row's JSON payload is
// flattened to scalar fields, with nested values JSON-encoded. Every call
// returns a fresh map.
func (p *OutboxPublisher) Render(entry *outbox.Entry) (map[string]any, error) {
	switch entry.EventType {
	case outbox.DeadLetterEventType:
		values, err := outbox.DeadLetterValues(entry)
		if err != nil {
			// Our own payload; a decode failure here is deterministic, never transient.
			return nil, fmt.Errorf("%w: dead-letter values: %v", pkgevents.ErrPermanent, err)
		}
		delete(values, "outbox_entry_id")
		return values, nil
	case streams.TaskDispatchV1:
		tenantID, payload, err := outbox.UnwrapTenant(entry.Payload)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %w", pkgevents.ErrPermanent, streams.TaskDispatchV1, err)
		}
		var d pkgevents.TaskDispatch
		if err := json.Unmarshal(payload, &d); err != nil {
			return nil, fmt.Errorf("%w: unmarshal %s: %v", pkgevents.ErrPermanent, streams.TaskDispatchV1, err)
		}
		values, err := pkgevents.TaskDispatchFields(entry.ID.String(), tenantID, producer, entry.CreatedAt, d)
		if err != nil {
			return nil, fmt.Errorf("%w: render %s: %v", pkgevents.ErrPermanent, streams.TaskDispatchV1, err)
		}
		return values, nil
	case streams.RunCancelledV1:
		tenantID, payload, err := outbox.UnwrapTenant(entry.Payload)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %w", pkgevents.ErrPermanent, streams.RunCancelledV1, err)
		}
		var c pkgevents.RunCancelled
		if err := json.Unmarshal(payload, &c); err != nil {
			return nil, fmt.Errorf("%w: unmarshal %s: %v", pkgevents.ErrPermanent, streams.RunCancelledV1, err)
		}
		values, err := pkgevents.RunCancelledFields(entry.ID.String(), tenantID, producer, entry.CreatedAt, c)
		if err != nil {
			return nil, fmt.Errorf("%w: render %s: %v", pkgevents.ErrPermanent, streams.RunCancelledV1, err)
		}
		return values, nil
	}
	var fields map[string]interface{}
	if err := json.Unmarshal(entry.Payload, &fields); err != nil {
		return nil, fmt.Errorf("%w: unmarshal payload for stream %s: %v", pkgevents.ErrPermanent, entry.StreamName, err)
	}
	normalized, err := normalizeRedisFields(fields)
	if err != nil {
		return nil, fmt.Errorf("normalize fields for stream %s: %w", entry.StreamName, err)
	}
	return normalized, nil
}

// xaddArgs builds the XADD for entry: its rendered fields plus outbox_entry_id,
// so consumer-side pkg/messageprocessing.DedupWithOutboxEntryID catches a row
// republished with a fresh Redis message id. It sets no length cap: the
// dead-letter-controller's trim loop bounds every stream.
func (p *OutboxPublisher) xaddArgs(entry *outbox.Entry) (*goredis.XAddArgs, error) {
	values, err := p.Render(entry)
	if err != nil {
		return nil, err
	}
	values["outbox_entry_id"] = entry.ID.String()
	return &goredis.XAddArgs{
		Stream: entry.StreamName,
		Values: values,
	}, nil
}

// normalizeRedisFields returns a copy of fields where every value is a Redis-
// compatible scalar. Non-scalar values (maps, slices, structs) are JSON-encoded
// to strings so that XADD does not receive unsupported Go types.
func normalizeRedisFields(fields map[string]interface{}) (map[string]interface{}, error) {
	normalized := make(map[string]interface{}, len(fields))
	for key, value := range fields {
		scalar, err := normalizeRedisValue(value)
		if err != nil {
			return nil, fmt.Errorf("normalize field %q: %w", key, err)
		}
		normalized[key] = scalar
	}
	return normalized, nil
}

func normalizeRedisValue(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case nil, string, []byte, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return v, nil
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return string(encoded), nil
	}
}
