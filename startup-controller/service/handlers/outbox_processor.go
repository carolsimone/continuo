package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/startup-controller/adapters/postgres"
	"github.com/carolsimone/continuo/startup-controller/adapters/redis"
	"github.com/carolsimone/continuo/startup-controller/domain/model"
)

// OutboxProcessor processes pending outbox entries and publishes them to Redis
type OutboxProcessor struct {
	outboxRepo postgres.OutboxRepository
	producers  map[string]*redis.Producer // keyed by stream name
	logger     *slog.Logger
}

// NewOutboxProcessor creates a new OutboxProcessor.
// producers maps stream names to their respective Redis producers.
func NewOutboxProcessor(
	outboxRepo postgres.OutboxRepository,
	producers map[string]*redis.Producer,
	logger *slog.Logger,
) *OutboxProcessor {
	return &OutboxProcessor{
		outboxRepo: outboxRepo,
		producers:  producers,
		logger:     logger,
	}
}

// Run starts the outbox processor loop
func (p *OutboxProcessor) Run(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	p.logger.Info("Starting outbox processor")

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Outbox processor context cancelled, stopping")
			return ctx.Err()
		case <-ticker.C:
			if err := p.ProcessBatch(ctx); err != nil {
				p.logger.Error("Failed to process outbox batch", "error", err)
			}
		}
	}
}

// ProcessBatch processes a batch of pending outbox entries (exported for testing)
func (p *OutboxProcessor) ProcessBatch(ctx context.Context) error {
	// Step 1: Fetch pending entries
	entries, err := p.outboxRepo.GetPendingBatch(ctx, 100)
	if err != nil {
		return fmt.Errorf("failed to fetch outbox entries: %w", err)
	}

	if len(entries) == 0 {
		return nil // Nothing to process
	}

	p.logger.Info("Processing outbox batch", "count", len(entries))

	// Step 2: Process each entry
	for _, entry := range entries {
		if err := p.processEntry(ctx, entry); err != nil {
			p.logger.Error("Failed to process outbox entry",
				"entry_id", entry.ID,
				"error", err,
			)

			// Increment retry count
			if entry.RetryCount >= entry.MaxRetries {
				// Mark as failed permanently
				if markErr := p.outboxRepo.MarkFailed(ctx, entry.ID, err.Error()); markErr != nil {
					p.logger.Error("Failed to mark entry as failed",
						"entry_id", entry.ID,
						"error", markErr,
					)
				}
			} else {
				if incrErr := p.outboxRepo.IncrementRetry(ctx, entry.ID); incrErr != nil {
					p.logger.Error("Failed to increment retry count",
						"entry_id", entry.ID,
						"error", incrErr,
					)
				}
			}
			continue
		}

		// Step 3: Mark as processed on success
		if err := p.outboxRepo.MarkProcessed(ctx, entry.ID); err != nil {
			p.logger.Error("Failed to mark entry as processed",
				"entry_id", entry.ID,
				"error", err,
			)
		}
	}

	return nil
}

// processEntry processes a single outbox entry, routing it to the correct
// Redis producer based on the entry's StreamName.
func (p *OutboxProcessor) processEntry(ctx context.Context, entry *model.OutboxEntry) error {
	producer, ok := p.producers[entry.StreamName]
	if !ok {
		return fmt.Errorf("no producer registered for stream %q", entry.StreamName)
	}

	// Build the Redis message values from the raw JSON payload.
	// Always unpack the JSON payload into top-level Redis message fields
	// so consumers can read them directly (e.g. msg.Values["schedule_name"]).
	var fields map[string]interface{}
	if err := json.Unmarshal(entry.Payload, &fields); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	values := map[string]interface{}{
		"outbox_entry_id": entry.ID.String(),
	}
	for k, v := range fields {
		switch tv := v.(type) {
		case string, bool, float64:
			values[k] = tv
		default:
			// Complex values (maps, arrays) → serialize to JSON string
			b, err := json.Marshal(tv)
			if err != nil {
				return fmt.Errorf("failed to marshal field %q: %w", k, err)
			}
			values[k] = string(b)
		}
	}

	messageID, err := producer.Publish(ctx, values)
	if err != nil {
		return fmt.Errorf("failed to publish to Redis: %w", err)
	}

	p.logger.Info("Published event to Redis",
		"stream", entry.StreamName,
		"message_id", messageID,
		"event_type", entry.EventType,
	)

	return nil
}
