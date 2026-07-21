package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	goredis "github.com/redis/go-redis/v9"
)

// OutboxPublisher publishes state's outbox entries to Redis streams.
// Each outbox row represents one event: its Payload JSONB is unmarshaled into
// a field map and written via XADD to the stream named by StreamName.
// Nested non-scalar values (e.g. service_metadata maps) are re-encoded as JSON
// strings so that every Redis field is a plain scalar.
type OutboxPublisher struct {
	redis  *goredis.Client
	logger *slog.Logger
}

// NewOutboxPublisher constructs a publisher backed by a real Redis client.
func NewOutboxPublisher(redis *goredis.Client, logger *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{redis: redis, logger: logger}
}

// streamMaxLen caps each published stream at roughly this many entries
// (Approx/~ trimming). It bounds Redis memory for streams a consumer group may
// lag on. Trimming can drop the oldest entries before a lagging group reads
// them; 10000 is the accepted bound, matching the orchestrator's publisher.
const streamMaxLen = 10000

// Publish unmarshals the entry's JSONB payload into a field map, normalizes
// any nested structures to scalar strings, and XADDs them to entry.StreamName.
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

// xaddArgs unmarshals the entry's JSONB payload, injects outbox_entry_id for
// consumer-side dedup, normalizes nested values to scalars, and returns the
// XADD arguments (including the shared MaxLen cap so single and batched
// publishes trim identically).
func (p *OutboxPublisher) xaddArgs(entry *outbox.Entry) (*goredis.XAddArgs, error) {
	// A dead-letter row is published generically: its payload is already a flat
	// scalar map (outbox.DeadLetterPayload), so it is expanded onto the stream
	// via DeadLetterValues rather than routed through the map-unmarshal path
	// below (which would produce the identical result, but this branch keeps
	// the dead-letter wire shape explicit and independent of that path).
	if entry.EventType == outbox.DeadLetterEventType {
		values, err := outbox.DeadLetterValues(entry)
		if err != nil {
			// Our own payload; a decode failure here is deterministic, never transient.
			return nil, fmt.Errorf("%w: dead-letter values: %v", pkgevents.ErrPermanent, err)
		}
		return &goredis.XAddArgs{
			Stream: entry.StreamName,
			MaxLen: streamMaxLen,
			Approx: true,
			Values: values,
		}, nil
	}

	var fields map[string]interface{}
	if err := json.Unmarshal(entry.Payload, &fields); err != nil {
		return nil, fmt.Errorf("%w: unmarshal payload for stream %s: %v", pkgevents.ErrPermanent, entry.StreamName, err)
	}
	// Inject outbox_entry_id so consumer-side dedup via
	// pkg/messageprocessing.DedupWithOutboxEntryID can catch Processor-crash
	// redeliveries (same outbox row republished with a fresh Redis msg_id).
	fields["outbox_entry_id"] = entry.ID.String()
	normalized, err := normalizeRedisFields(fields)
	if err != nil {
		return nil, fmt.Errorf("normalize fields for stream %s: %w", entry.StreamName, err)
	}
	return &goredis.XAddArgs{
		Stream: entry.StreamName,
		MaxLen: streamMaxLen,
		Approx: true,
		Values: normalized,
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
