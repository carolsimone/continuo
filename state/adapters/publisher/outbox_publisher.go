package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

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

// Publish unmarshals the entry's JSONB payload into a field map, normalizes
// any nested structures to scalar strings, and XADDs them to entry.StreamName.
func (p *OutboxPublisher) Publish(ctx context.Context, entry *outbox.Entry) error {
	var fields map[string]interface{}
	if err := json.Unmarshal(entry.Payload, &fields); err != nil {
		return fmt.Errorf("unmarshal payload for stream %s: %w", entry.StreamName, err)
	}
	// Inject outbox_entry_id so consumer-side dedup via
	// pkg/messageprocessing.DedupWithOutboxEntryID can catch Processor-crash
	// redeliveries (same outbox row republished with a fresh Redis msg_id).
	fields["outbox_entry_id"] = entry.ID.String()
	normalized, err := normalizeRedisFields(fields)
	if err != nil {
		return fmt.Errorf("normalize fields for stream %s: %w", entry.StreamName, err)
	}
	if err := p.redis.XAdd(ctx, &goredis.XAddArgs{
		Stream: entry.StreamName,
		Values: normalized,
	}).Err(); err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
	}
	return nil
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
