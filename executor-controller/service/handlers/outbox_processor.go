package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/event"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
)

// errPermanentFailure signals that processEntry already called MarkFailed.
// ProcessBatch must not increment retry or call MarkFailed again.
var errPermanentFailure = errors.New("permanent failure — already marked")

// K8sDeployer defines the interface for K8s job deployment
type K8sDeployer interface {
	CreateQueryJob(ctx context.Context, params k8s.JobParams) error
}

// StateUpdater defines the interface for updating task status
type StateUpdater interface {
	UpdateTaskStatus(ctx context.Context, taskID uuid.UUID, status statev1.TaskStatus) error
}

// EventPublisher defines the interface for publishing events to Redis
type EventPublisher interface {
	Publish(ctx context.Context, values map[string]interface{}) (string, error)
}

// OutboxProcessor processes pending outbox entries and deploys K8s Jobs
type OutboxProcessor struct {
	outboxRepo   postgres.OutboxRepository
	k8sClient    K8sDeployer
	stateClient  StateUpdater
	producer     EventPublisher
	logger       *slog.Logger
	k8sNamespace string
	PollInterval time.Duration
}

// NewOutboxProcessor creates a new OutboxProcessor
func NewOutboxProcessor(
	outboxRepo postgres.OutboxRepository,
	k8sClient K8sDeployer,
	stateClient StateUpdater,
	producer EventPublisher,
	k8sNamespace string,
	logger *slog.Logger,
) *OutboxProcessor {
	return &OutboxProcessor{
		outboxRepo:   outboxRepo,
		k8sClient:    k8sClient,
		stateClient:  stateClient,
		producer:     producer,
		k8sNamespace: k8sNamespace,
		logger:       logger,
		PollInterval: 5 * time.Second, // Default to 5s for production
	}
}

// Run starts the outbox processor loop
func (p *OutboxProcessor) Run(ctx context.Context) error {
	pollInterval := p.PollInterval
	if pollInterval == 0 {
		pollInterval = 5 * time.Second
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	p.logger.Info("Starting outbox processor", "poll_interval", pollInterval)

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

	p.logger.Info("Processing outbox batch", "count", len(entries))

	// Step 2: Process each entry
	for _, entry := range entries {
		if err := p.processEntry(ctx, entry); err != nil {
			if errors.Is(err, errPermanentFailure) {
				continue // MarkFailed already called inside processEntry
			}

			p.logger.Error("Failed to process outbox entry",
				"entry_id", entry.ID,
				"task_id", entry.TaskID,
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
func (p *OutboxProcessor) processEntry(ctx context.Context, entry *model.DeploymentOutboxEntry) error {
	p.logger.Info("Processing deployment outbox entry",
		"entry_id", entry.ID,
		"task_id", entry.TaskID,
		"job_name", entry.JobName,
	)

	// Step 1: Deploy K8s Job (idempotent operation)
	nodeType, err := pkg_model.ParseNodeType(entry.NodeType)
	if err != nil {
		// Data corruption — entry was validated at write time; retrying will never succeed.
		p.logger.Error("Deployment outbox entry has invalid node_type (data corruption)",
			"entry_id", entry.ID, "node_type", entry.NodeType, "error", err)
		if markErr := p.outboxRepo.MarkFailed(ctx, entry.ID, fmt.Sprintf("data corruption: %v", err)); markErr != nil {
			p.logger.Error("Failed to mark corrupt entry as failed", "entry_id", entry.ID, "error", markErr)
		}
		return errPermanentFailure
	}

	params := k8s.JobParams{
		JobName:      entry.JobName,
		TaskID:       entry.TaskID.String(),
		ScheduleID:   entry.ScheduleID.String(),
		ScheduleName: entry.ScheduleName,
		ServiceName:  entry.ServiceName,
		Schema:       entry.Schema,
		TableName:    entry.TableName,
		Namespace:    p.k8sNamespace,
		NodeType:     nodeType,
	}

	if err := p.k8sClient.CreateQueryJob(ctx, params); err != nil {
		return fmt.Errorf("k8s deployment failed: %w", err)
	}

	// Step 2: Update task_tracker to "running" via gRPC
	if err := p.stateClient.UpdateTaskStatus(ctx, entry.TaskID, statev1.TaskStatus_TASK_STATUS_RUNNING); err != nil {
		// K8s job created but DB update failed - will retry
		// On retry, K8s job creation will be idempotent (already exists)
		return fmt.Errorf("failed to update task status: %w", err)
	}

	// Step 3: Publish event for k8s-controller
	evt := event.JobDeployed{
		OutboxEntryID: entry.ID.String(),
		TaskID:        entry.TaskID.String(),
		ScheduleID:    entry.ScheduleID.String(),
		ScheduleName:  entry.ScheduleName,
		ServiceName:   entry.ServiceName,
		Schema:        entry.Schema,
		TableName:     entry.TableName,
		JobName:       entry.JobName,
		NodeType:      entry.NodeType,
	}

	if _, err := p.producer.Publish(ctx, evt.ToMap()); err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	p.logger.Info("Successfully processed deployment outbox entry",
		"entry_id", entry.ID,
		"task_id", entry.TaskID,
		"job_name", entry.JobName,
	)

	return nil
}
