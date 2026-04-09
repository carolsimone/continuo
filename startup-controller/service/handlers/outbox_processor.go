package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/startup-controller/adapters/postgres"
	"github.com/carolsimone/continuo/startup-controller/adapters/redis"
	"github.com/carolsimone/continuo/startup-controller/domain/event"
	"github.com/carolsimone/continuo/startup-controller/domain/model"
)

// OutboxProcessor processes pending outbox entries and publishes them to Redis
type OutboxProcessor struct {
	outboxRepo postgres.OutboxRepository
	producer   *redis.Producer
	logger     *slog.Logger
}

// NewOutboxProcessor creates a new OutboxProcessor
func NewOutboxProcessor(
	outboxRepo postgres.OutboxRepository,
	producer *redis.Producer,
	logger *slog.Logger,
) *OutboxProcessor {
	return &OutboxProcessor{
		outboxRepo: outboxRepo,
		producer:   producer,
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

// processEntry processes a single outbox entry
func (p *OutboxProcessor) processEntry(ctx context.Context, entry *model.OutboxEntry) error {
	// Unmarshal payload to get event data
	var evt event.NodeReadyForExecution
	if err := json.Unmarshal(entry.Payload, &evt); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	// Publish to Redis stream
	values := map[string]interface{}{
		"outbox_entry_id": entry.ID.String(),
		"schedule_id":     evt.ScheduleID,
		"schedule_name":   evt.ScheduleName,
		"service_name":    evt.ServiceName,
		"schema":          evt.Schema,
		"table_name":      evt.TableName,
		"task_id":         evt.TaskID,
		"job_name":        evt.JobName,
		"node_type":       evt.NodeType,
	}

	messageID, err := p.producer.Publish(ctx, values)
	if err != nil {
		return fmt.Errorf("failed to publish to Redis: %w", err)
	}

	p.logger.Info("Published event to Redis",
		"stream", entry.StreamName,
		"message_id", messageID,
		"table", evt.TableName,
	)

	return nil
}
