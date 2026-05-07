package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	goredis "github.com/redis/go-redis/v9"
)

// publishFn abstracts the Redis XAdd call for testability.
type publishFn func(ctx context.Context, stream string, fields map[string]interface{}) error

// OutboxProcessor polls the state_outbox table and publishes pending entries to
// Redis Streams, implementing the transactional outbox pattern. It handles all
// event types stored in state_outbox (e.g. trigger.rerun:v1, scheduler.started:v1).
type OutboxProcessor struct {
	outboxRepo postgres.OutboxRepository
	publish    publishFn
	logger     *slog.Logger
}

// NewOutboxProcessor creates a processor backed by a real Redis client.
func NewOutboxProcessor(outboxRepo postgres.OutboxRepository, redisClient *goredis.Client, logger *slog.Logger) *OutboxProcessor {
	return &OutboxProcessor{
		outboxRepo: outboxRepo,
		publish: func(ctx context.Context, stream string, fields map[string]interface{}) error {
			return redisClient.XAdd(ctx, &goredis.XAddArgs{
				Stream: stream,
				Values: fields,
			}).Err()
		},
		logger: logger,
	}
}

// NewOutboxProcessorWithPublisher creates a processor with an injectable publish
// function — used in unit tests to avoid a real Redis dependency.
func NewOutboxProcessorWithPublisher(outboxRepo postgres.OutboxRepository, pub publishFn, logger *slog.Logger) *OutboxProcessor {
	return &OutboxProcessor{outboxRepo: outboxRepo, publish: pub, logger: logger}
}

// Run polls the outbox on a fixed interval until ctx is cancelled.
func (p *OutboxProcessor) Run(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.ProcessBatch(ctx); err != nil {
				p.logger.Error("outbox batch error", "error", err)
			}
		}
	}
}

// ProcessBatch is exported so unit tests can invoke it directly.
func (p *OutboxProcessor) ProcessBatch(ctx context.Context) error {
	entries, err := p.outboxRepo.ListPending(ctx, 10)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		var fields map[string]interface{}
		if err := json.Unmarshal(entry.Payload, &fields); err != nil {
			p.logger.Error("failed to unmarshal outbox payload", "id", entry.ID, "error", err)
			_ = p.outboxRepo.IncrementRetry(ctx, entry.ID)
			continue
		}
		redisFields, err := normalizeRedisFields(fields)
		if err != nil {
			p.logger.Error("failed to normalize outbox payload for redis", "id", entry.ID, "error", err)
			_ = p.outboxRepo.IncrementRetry(ctx, entry.ID)
			continue
		}
		if err := p.publish(ctx, entry.StreamName, redisFields); err != nil {
			p.logger.Error("failed to publish to redis", "id", entry.ID, "error", err)
			_ = p.outboxRepo.IncrementRetry(ctx, entry.ID)
			continue
		}
		_ = p.outboxRepo.MarkPublished(ctx, entry.ID)
	}
	return nil
}

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
