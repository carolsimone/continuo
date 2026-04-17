package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/adapters/grpc"
	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/domain/event"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
)

// StateUpdater defines the interface for updating task status via gRPC
type StateUpdater interface {
	UpdateTaskWithRetry(ctx context.Context, taskID uuid.UUID, status statev1.TaskStatus, retryCount int32) error
	CreateTaskExecution(ctx context.Context, params *grpc.TaskExecutionParams) error
}

// EventPublisher defines the interface for publishing events to specific Redis streams
type EventPublisher interface {
	PublishToStream(ctx context.Context, streamName string, values map[string]interface{}) (string, error)
}

// OutboxProcessor processes pending k8s status outbox entries
type OutboxProcessor struct {
	outboxRepo  postgres.OutboxRepository
	stateClient StateUpdater
	producer    EventPublisher
	logger      *slog.Logger
}

// NewOutboxProcessor creates a new OutboxProcessor
func NewOutboxProcessor(
	outboxRepo postgres.OutboxRepository,
	stateClient StateUpdater,
	producer EventPublisher,
	logger *slog.Logger,
) *OutboxProcessor {
	return &OutboxProcessor{
		outboxRepo:  outboxRepo,
		stateClient: stateClient,
		producer:    producer,
		logger:      logger,
	}
}

// Run starts the outbox processor loop
func (p *OutboxProcessor) Run(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	p.logger.Info("Starting k8s status outbox processor")

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

// ProcessBatch processes a batch of pending outbox entries
func (p *OutboxProcessor) ProcessBatch(ctx context.Context) error {
	// Step 1: Fetch pending entries
	entries, err := p.outboxRepo.GetPendingBatch(ctx, 100)
	if err != nil {
		return fmt.Errorf("failed to fetch outbox entries: %w", err)
	}

	if len(entries) == 0 {
		return nil // Nothing to process
	}

	p.logger.Info("Processing k8s status outbox batch", "count", len(entries))

	// Step 2: Process each entry
	for _, entry := range entries {
		if err := p.processEntry(ctx, entry); err != nil {
			p.logger.Error("Failed to process outbox entry",
				"entry_id", entry.ID,
				"task_id", entry.TaskID,
				"event_type", entry.EventType,
				"error", err,
			)

			// Check retry count
			if entry.OutboxRetryCount >= entry.MaxRetries {
				// Mark as failed permanently
				if markErr := p.outboxRepo.MarkFailed(ctx, entry.ID, err.Error()); markErr != nil {
					p.logger.Error("Failed to mark entry as failed",
						"entry_id", entry.ID,
						"error", markErr,
					)
				}
			} else {
				// Increment retry count
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
func (p *OutboxProcessor) processEntry(ctx context.Context, entry *model.K8sStatusOutboxEntry) error {
	p.logger.Info("Processing k8s status outbox entry",
		"entry_id", entry.ID,
		"task_id", entry.TaskID,
		"event_type", entry.EventType,
		"stream_name", entry.StreamName,
	)

	// Step 1: Update task status via gRPC if flagged
	if entry.UpdateTaskStatus {
		status := p.parseTaskStatus(entry.NewTaskStatus)
		if err := p.stateClient.UpdateTaskWithRetry(ctx, entry.TaskID, status, int32(entry.NewRetryCount)); err != nil {
			return fmt.Errorf("failed to update task status: %w", err)
		}
		p.logger.Debug("Updated task status",
			"task_id", entry.TaskID,
			"status", entry.NewTaskStatus,
			"retry_count", entry.NewRetryCount,
		)
	}

	// Step 2: Create task execution via gRPC if flagged
	if entry.CreateExecution {
		executionIDStr := ""
		if entry.ExecutionID != uuid.Nil {
			executionIDStr = entry.ExecutionID.String()
		}
		params := &grpc.TaskExecutionParams{
			TaskID:           entry.TaskID.String(),
			StartedAt:        entry.ExecutionStartedAt,
			CompletedAt:      entry.ExecutionCompletedAt,
			ExecutionSeconds: entry.ExecutionSeconds,
			K8sJobName:       entry.JobName,
			ErrorMessage:     entry.ErrorMessage,
			ExecutionID:      executionIDStr,
			LogS3Key:         entry.LogS3Key,
		}
		if err := p.stateClient.CreateTaskExecution(ctx, params); err != nil {
			return fmt.Errorf("failed to create task execution: %w", err)
		}
		p.logger.Debug("Created task execution",
			"task_id", entry.TaskID,
			"job_name", entry.JobName,
		)
	}

	// Step 3: Publish event to Redis stream (if stream_name is not empty)
	if entry.StreamName != "" {
		evt := p.buildEvent(entry)
		if _, err := p.producer.PublishToStream(ctx, entry.StreamName, evt); err != nil {
			return fmt.Errorf("failed to publish event to %s: %w", entry.StreamName, err)
		}
	}

	p.logger.Info("Successfully processed outbox entry",
		"entry_id", entry.ID,
		"task_id", entry.TaskID,
		"event_type", entry.EventType,
		"stream_name", entry.StreamName,
	)

	return nil
}

// ProcessOnce processes a single batch of outbox entries synchronously
// This is useful for testing to avoid running the continuous loop
func (p *OutboxProcessor) ProcessOnce(ctx context.Context, batchSize int) error {
	entries, err := p.outboxRepo.GetPendingBatch(ctx, batchSize)
	if err != nil {
		return fmt.Errorf("failed to get pending outbox entries: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	for _, entry := range entries {
		if err := p.processEntry(ctx, entry); err != nil {
			p.logger.Error("Failed to process outbox entry",
				"entry_id", entry.ID,
				"task_id", entry.TaskID,
				"error", err,
			)

			// Check retry count
			if entry.OutboxRetryCount >= entry.MaxRetries {
				// Mark as failed permanently
				if markErr := p.outboxRepo.MarkFailed(ctx, entry.ID, err.Error()); markErr != nil {
					p.logger.Error("Failed to mark entry as failed",
						"entry_id", entry.ID,
						"error", markErr,
					)
				}
			} else {
				// Increment retry count
				if incrErr := p.outboxRepo.IncrementRetry(ctx, entry.ID); incrErr != nil {
					p.logger.Error("Failed to increment retry count",
						"entry_id", entry.ID,
						"error", incrErr,
					)
				}
			}
			continue
		}

		// Mark as processed on success
		if err := p.outboxRepo.MarkProcessed(ctx, entry.ID); err != nil {
			p.logger.Error("Failed to mark entry as processed",
				"entry_id", entry.ID,
				"error", err,
			)
		}
	}

	return nil
}

// parseTaskStatus converts string status to protobuf TaskStatus
func (p *OutboxProcessor) parseTaskStatus(status string) statev1.TaskStatus {
	switch status {
	case "running":
		return statev1.TaskStatus_TASK_STATUS_RUNNING
	case "succeeded":
		return statev1.TaskStatus_TASK_STATUS_SUCCEEDED
	case "failed":
		return statev1.TaskStatus_TASK_STATUS_FAILED
	default:
		p.logger.Warn("Unknown task status", "status", status)
		return statev1.TaskStatus_TASK_STATUS_PENDING
	}
}

// buildEvent builds the event map for Redis based on event type
func (p *OutboxProcessor) buildEvent(entry *model.K8sStatusOutboxEntry) map[string]interface{} {
	switch entry.EventType {
	case "task_retry":
		return event.TaskRetry{
			TaskID:       entry.TaskID.String(),
			ScheduleID:   entry.ScheduleID.String(),
			ScheduleName: entry.ScheduleName,
			ServiceName:  entry.ServiceName,
			SchemaName:   entry.SchemaName,
			TableName:    entry.TableName,
			JobName:      entry.JobName,
			RetryCount:   entry.NewRetryCount,
			NodeType:     entry.NodeType,
		}.ToMap()

	case "task_failed":
		return event.TaskFailed{
			TaskID:       entry.TaskID.String(),
			ScheduleID:   entry.ScheduleID.String(),
			ScheduleName: entry.ScheduleName,
			ServiceName:  entry.ServiceName,
			SchemaName:   entry.SchemaName,
			TableName:    entry.TableName,
			JobName:      entry.JobName,
			ErrorMessage: entry.ErrorMessage,
			RetryCount:   entry.TaskRetryCount,
		}.ToMap()

	case "check_delayed":
		return event.JobCheckRequest{
			OutboxEntryID: entry.ID.String(),
			TaskID:        entry.TaskID.String(),
			ScheduleID:    entry.ScheduleID.String(),
			ScheduleName:  entry.ScheduleName,
			ServiceName:   entry.ServiceName,
			SchemaName:    entry.SchemaName,
			TableName:     entry.TableName,
			JobName:       entry.JobName,
			CheckAfter:    entry.CheckAfter,
			NodeType:      entry.NodeType,
		}.ToMap()

	case "node_status_updated":
		return event.NodeStatusUpdated{
			TaskID:       entry.TaskID.String(),
			ScheduleID:   entry.ScheduleID.String(),
			ScheduleName: entry.ScheduleName,
			ServiceName:  entry.ServiceName,
			SchemaName:   entry.SchemaName,
			TableName:    entry.TableName,
			Status:       entry.NewTaskStatus,
		}.ToMap()

	default:
		p.logger.Warn("Unknown event type", "event_type", entry.EventType)
		// Return basic event map
		return map[string]interface{}{
			"task_id":       entry.TaskID.String(),
			"schedule_id":   entry.ScheduleID.String(),
			"schedule_name": entry.ScheduleName,
			"service_name":  entry.ServiceName,
			"schema_name":   entry.SchemaName,
			"table_name":    entry.TableName,
			"job_name":      entry.JobName,
		}
	}
}
