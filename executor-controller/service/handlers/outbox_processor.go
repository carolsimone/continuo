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
	pkgevents "github.com/carolsimone/continuo/pkg/events"
)

// errPermanentFailure signals that processEntry already called MarkFailed.
// ProcessBatch must not increment retry or call MarkFailed again.
var errPermanentFailure = errors.New("permanent failure — already marked")

// K8sDeployer defines the interface for K8s job deployment
type K8sDeployer interface {
	CreateQueryJob(ctx context.Context, params k8s.JobParams) error
}

// EventPublisher defines the interface for publishing events to Redis streams
type EventPublisher interface {
	Publish(ctx context.Context, stream string, values map[string]interface{}) (string, error)
}

// OutboxProcessor processes pending outbox entries and deploys K8s Jobs
type OutboxProcessor struct {
	outboxRepo        postgres.OutboxRepository
	k8sClient         K8sDeployer
	publisher         EventPublisher
	jobDeployedStream string
	taskStatusStream  string
	nodeUpdatedStream string // node.updated:v1 — used for terminal dispatch failures
	logger            *slog.Logger
	k8sNamespace      string
	PollInterval      time.Duration
}

// NewOutboxProcessor creates a new OutboxProcessor
func NewOutboxProcessor(
	outboxRepo postgres.OutboxRepository,
	k8sClient K8sDeployer,
	publisher EventPublisher,
	jobDeployedStream string,
	taskStatusStream string,
	nodeUpdatedStream string,
	k8sNamespace string,
	logger *slog.Logger,
) *OutboxProcessor {
	return &OutboxProcessor{
		outboxRepo:        outboxRepo,
		k8sClient:         k8sClient,
		publisher:         publisher,
		jobDeployedStream: jobDeployedStream,
		taskStatusStream:  taskStatusStream,
		nodeUpdatedStream: nodeUpdatedStream,
		k8sNamespace:      k8sNamespace,
		logger:            logger,
		PollInterval:      5 * time.Second,
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

// MarkTaskTerminallyFailed propagates a terminal task failure originating in
// the executor (e.g. permanent dispatch error or retry-exhaustion). Best-effort:
// each publish step logs and continues on error so a single transient hiccup
// doesn't leave the outbox entry stuck. Only outboxRepo.MarkFailed's error
// propagates back to the caller — failing to mark the outbox row is the only
// failure that genuinely blocks forward progress.
//
// Short-circuits the normal executor → k8s-controller → orchestrator chain
// because we never created a pod: state needs the task tracker updated, and
// orchestrator needs CheckScheduleCompletion to fire so the schedule advances.
func (p *OutboxProcessor) MarkTaskTerminallyFailed(
	ctx context.Context,
	entry *model.DeploymentOutboxEntry,
	reason string,
) error {
	// 1) state's task tracker → FAILED
	statusEvt := pkgevents.TaskStatusUpdated{
		TaskID:     entry.TaskID.String(),
		ScheduleID: entry.ScheduleID.String(),
		Status:     "FAILED",
		RetryCount: int32(entry.TaskRetryCount),
	}
	if _, err := p.publisher.Publish(ctx, p.taskStatusStream, statusEvt.ToMap()); err != nil {
		p.logger.Error("Failed to publish task.status.updated:v1 (FAILED) — continuing with node.updated and outbox mark",
			"entry_id", entry.ID, "task_id", entry.TaskID, "error", err)
	}

	// 2) orchestrator's HandleNodeCompletedHandler → advances schedule (CheckScheduleCompletion)
	nodeEvt := map[string]interface{}{
		"task_id":       entry.TaskID.String(),
		"schedule_id":   entry.ScheduleID.String(),
		"schedule_name": entry.ScheduleName,
		"service_name":  entry.ServiceName,
		"schema_name":   entry.SchemaName,
		"table_name":    entry.TableName,
		"status":        "FAILED",
	}
	if _, err := p.publisher.Publish(ctx, p.nodeUpdatedStream, nodeEvt); err != nil {
		p.logger.Error("Failed to publish node.updated:v1 (FAILED) — continuing with outbox mark",
			"entry_id", entry.ID, "task_id", entry.TaskID, "error", err)
	}

	// 3) outbox row → failed (this is the only error worth bubbling)
	if err := p.outboxRepo.MarkFailed(ctx, entry.ID, reason); err != nil {
		p.logger.Error("Failed to mark outbox entry failed",
			"entry_id", entry.ID, "task_id", entry.TaskID, "error", err)
		return err
	}
	return nil
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
			if entry.OutboxRetryCount >= entry.OutboxMaxRetries {
				// Retry budget exhausted — propagate terminal failure to state + orchestrator.
				if termErr := p.MarkTaskTerminallyFailed(ctx, entry, err.Error()); termErr != nil {
					p.logger.Error("Failed to mark task terminally failed after retry exhaustion",
						"entry_id", entry.ID, "error", termErr)
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
		SchemaName:   entry.SchemaName,
		TableName:    entry.TableName,
		Namespace:    p.k8sNamespace,
		NodeType:     nodeType,
		ImageTag:     entry.ImageTag,
	}

	if err := p.k8sClient.CreateQueryJob(ctx, params); err != nil {
		if errors.Is(err, pkgevents.ErrPermanent) {
			p.logger.Error("Permanent dispatch failure — terminating task on attempt 1",
				"entry_id", entry.ID,
				"task_id", entry.TaskID,
				"error", err,
			)
			if termErr := p.MarkTaskTerminallyFailed(ctx, entry, err.Error()); termErr != nil {
				// MarkFailed itself failed — let ProcessBatch retry-increment so
				// we eventually get unstuck on a future tick.
				return fmt.Errorf("k8s deployment permanent + terminal mark failed: %w", err)
			}
			return errPermanentFailure
		}
		return fmt.Errorf("k8s deployment failed: %w", err)
	}

	// Step 2: Publish task.status.updated:v1 (RUNNING) for state service
	statusEvt := pkgevents.TaskStatusUpdated{
		TaskID:     entry.TaskID.String(),
		ScheduleID: entry.ScheduleID.String(),
		Status:     "RUNNING",
		RetryCount: int32(entry.TaskRetryCount), // task-level retry count, not outbox delivery count
	}
	if _, err := p.publisher.Publish(ctx, p.taskStatusStream, statusEvt.ToMap()); err != nil {
		// K8s job created but status event failed — will retry; K8s creation is idempotent.
		return fmt.Errorf("failed to publish task status event: %w", err)
	}

	// Step 3: Publish event for k8s-controller (includes task_retry_count so k8s-controller
	// does not need to call state gRPC to discover the current retry count)
	evt := event.JobDeployed{
		OutboxEntryID:  entry.ID.String(),
		TaskID:         entry.TaskID.String(),
		ScheduleID:     entry.ScheduleID.String(),
		ScheduleName:   entry.ScheduleName,
		ServiceName:    entry.ServiceName,
		SchemaName:     entry.SchemaName,
		TableName:      entry.TableName,
		JobName:        entry.JobName,
		ImageTag:       entry.ImageTag,
		NodeType:       entry.NodeType,
		TaskRetryCount: entry.TaskRetryCount,
		MaxRetries:     entry.TaskMaxRetries,
	}

	if _, err := p.publisher.Publish(ctx, p.jobDeployedStream, evt.ToMap()); err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	p.logger.Info("Successfully processed deployment outbox entry",
		"entry_id", entry.ID,
		"task_id", entry.TaskID,
		"job_name", entry.JobName,
	)

	return nil
}
