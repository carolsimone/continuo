package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	goredis "github.com/redis/go-redis/v9"
)

// errAlreadyClaimed signals that another processor instance claimed this entry.
// ProcessBatch must NOT call MarkProcessed or IncrementRetry in this case.
var errAlreadyClaimed = errors.New("outbox entry already claimed by another processor")

// OutboxProcessor processes pending outbox entries and publishes them to Redis
type OutboxProcessor struct {
	outboxRepo    postgres.OutboxRepository
	publishedRepo postgres.PublishedMessagesRepository
	redisClient   *goredis.Client
	logger        *slog.Logger
}

// NewOutboxProcessor creates a new OutboxProcessor
func NewOutboxProcessor(
	outboxRepo postgres.OutboxRepository,
	publishedRepo postgres.PublishedMessagesRepository,
	redisClient *goredis.Client,
	logger *slog.Logger,
) *OutboxProcessor {
	return &OutboxProcessor{
		outboxRepo:    outboxRepo,
		publishedRepo: publishedRepo,
		redisClient:   redisClient,
		logger:        logger,
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
			if errors.Is(err, errAlreadyClaimed) {
				// Another instance claimed this entry — no action needed.
				continue
			}
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

// processEntry processes a single outbox entry with deduplication
func (p *OutboxProcessor) processEntry(ctx context.Context, entry *domain.OutboxEntry) error {
	// Step 1: Check if already published
	exists, err := p.publishedRepo.Exists(ctx, entry.ID)
	if err != nil {
		return fmt.Errorf("failed to check published messages: %w", err)
	}

	if exists {
		p.logger.Info("Outbox entry already published, marking as processed",
			"entry_id", entry.ID,
		)
		// Just mark as processed, skip actual publishing
		return p.outboxRepo.MarkProcessed(ctx, entry.ID)
	}

	// Step 2: Mark as publishing (optimistic lock)
	err = p.outboxRepo.UpdateStatus(ctx, entry.ID, "publishing", "pending")
	if err != nil {
		// Another processor instance claimed this entry — skip silently.
		p.logger.Debug("Failed to claim outbox entry, skipping",
			"entry_id", entry.ID,
			"error", err,
		)
		return errAlreadyClaimed
	}

	// Step 3: Build values map from payload based on event type
	values, err := p.payloadToValues(entry)
	if err != nil {
		return fmt.Errorf("failed to build values from payload: %w", err)
	}

	// Step 4: Publish to the stream name stored in the outbox entry
	messageID, err := p.publishToStream(ctx, entry.StreamName, values)
	if err != nil {
		return fmt.Errorf("failed to publish to Redis: %w", err)
	}

	p.logger.Info("Published event to Redis",
		"stream", entry.StreamName,
		"message_id", messageID,
		"outbox_entry_id", entry.ID,
		"event_type", entry.EventType,
	)

	// Step 5: Record successful publish
	publishedMsg := &domain.PublishedMessage{
		OutboxEntryID:  entry.ID,
		RedisMessageID: &messageID,
	}

	if err := p.publishedRepo.Create(ctx, publishedMsg); err != nil {
		// Published to Redis but failed to record.
		// Next run will check published_messages and skip.
		p.logger.Error("Published to Redis but failed to record",
			"entry_id", entry.ID,
			"error", err,
		)
		// Don't return error — message was published successfully
	}

	return nil
}

// payloadToValues dispatches on event type to produce the Redis stream field map.
func (p *OutboxProcessor) payloadToValues(entry *domain.OutboxEntry) (map[string]interface{}, error) {
	switch entry.EventType {
	case "node_ready_for_execution":
		var evt domain.NodeReadyForExecution
		if err := json.Unmarshal(entry.Payload, &evt); err != nil {
			return nil, fmt.Errorf("failed to unmarshal node_ready_for_execution payload: %w", err)
		}
		return map[string]interface{}{
			"outbox_entry_id":  entry.ID.String(),
			"schedule_id":      evt.ScheduleID,
			"schedule_name":    evt.ScheduleName,
			"service_name":     evt.ServiceName,
			"schema_name":      evt.SchemaName,
			"table_name":       evt.TableName,
			"task_id":          evt.TaskID,
			"job_name":         evt.JobName,
			"node_type":        evt.NodeType,
			"image_tag":        evt.ImageTag,
			"manifest_version": evt.ManifestVersion,
		}, nil

	case "cascade_task_skipped":
		var evt domain.CascadeTaskSkipped
		if err := json.Unmarshal(entry.Payload, &evt); err != nil {
			return nil, fmt.Errorf("failed to unmarshal cascade_task_skipped payload: %w", err)
		}
		return map[string]interface{}{
			"outbox_entry_id": entry.ID.String(),
			"task_id":         evt.TaskID,
			"schedule_id":     evt.ScheduleID,
			"status":          "skipped",
			"retry_count":     "0",
		}, nil

	case "topology_ingested", "run_initialized", "rerun_ready",
		"run_entries_dispatched", "run_rerun_dispatched",
		"run_entries_dispatch_failed":
		// These event types use a generic payload field consumed by state
		return map[string]interface{}{
			"outbox_entry_id": entry.ID.String(),
			"payload":         string(entry.Payload),
		}, nil

	default:
		return nil, fmt.Errorf("unknown event type: %s", entry.EventType)
	}
}

// publishToStream publishes values to the given Redis stream using XADD.
func (p *OutboxProcessor) publishToStream(
	ctx context.Context,
	streamName string,
	values map[string]interface{},
) (string, error) {
	messageID, err := p.redisClient.XAdd(ctx, &goredis.XAddArgs{
		Stream: streamName,
		MaxLen: 10000,
		Approx: true,
		Values: values,
	}).Result()
	if err != nil {
		return "", fmt.Errorf("failed to publish to %s: %w", streamName, err)
	}
	return messageID, nil
}
