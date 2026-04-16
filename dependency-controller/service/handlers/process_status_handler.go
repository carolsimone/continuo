package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"

	"github.com/carolsimone/continuo/dependency-controller/adapters/grpc"
	"github.com/carolsimone/continuo/dependency-controller/domain/command"
	"github.com/carolsimone/continuo/dependency-controller/domain/event"
	"github.com/carolsimone/continuo/dependency-controller/domain/model"
	"github.com/carolsimone/continuo/dependency-controller/service/uow"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
)

// StateTaskClient defines the interface for task operations on the state service
type StateTaskClient interface {
	GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error)
	UpdateSchedulerStatus(ctx context.Context, scheduleID uuid.UUID, schedulerStatus statev1.SchedulerStatus) error
	GetSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID) (string, error)
}

// GraphNodeClient defines the interface for graph node operations via the graph service gRPC API
type GraphNodeClient interface {
	UpdateNodeStatus(ctx context.Context, scheduleName, schema, tableName, status, runID string) error
	GetReadyDownstream(ctx context.Context, scheduleName, schema, tableName, runID string) ([]model.DownstreamNode, error)
	CheckScheduleCompletion(ctx context.Context, scheduleName, runID string) (bool, bool, error)
	FinalizeRun(ctx context.Context, runID, terminalStatus string) error
}

// ProcessStatusHandler handles the ProcessNodeStatus command
type ProcessStatusHandler struct {
	uow         uow.UnitOfWork
	graphClient GraphNodeClient
	stateClient StateTaskClient
	logger      *slog.Logger
}

// NewProcessStatusHandler creates a new ProcessStatusHandler
func NewProcessStatusHandler(
	uow uow.UnitOfWork,
	graphClient GraphNodeClient,
	stateClient StateTaskClient,
	logger *slog.Logger,
) *ProcessStatusHandler {
	return &ProcessStatusHandler{
		uow:         uow,
		graphClient: graphClient,
		stateClient: stateClient,
		logger:      logger,
	}
}

// handleMessageDeduplication checks if message was already processed
// Returns (messageProcessingID, shouldSkip, error)
func (h *ProcessStatusHandler) handleMessageDeduplication(
	ctx context.Context,
	messageID string,
	messagePayload []byte,
) (uuid.UUID, bool, error) {
	// Try to insert message processing record
	msgProc := &model.MessageProcessing{
		MessageID:  messageID,
		StreamName: "node.updated:v1",
		State:      model.MessageProcessingStateProcessing,
		Payload:    messagePayload,
	}

	id, inserted, err := h.uow.MessageProcessingRepo().InsertIfNotExists(ctx, msgProc)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("failed to insert message processing: %w", err)
	}

	if !inserted {
		// Message already being processed or completed
		existing, err := h.uow.MessageProcessingRepo().GetByMessageID(ctx, messageID)
		if err != nil {
			return uuid.Nil, false, fmt.Errorf("failed to get existing message: %w", err)
		}

		// If already completed or acked, skip processing
		if existing.State == model.MessageProcessingStateCompleted ||
			existing.State == model.MessageProcessingStateAcked {
			h.logger.Info("Message already processed, skipping",
				"message_id", messageID,
				"state", existing.State,
			)
			return existing.ID, true, nil
		}

		// Still processing (another instance), abort
		h.logger.Warn("Message being processed by another instance",
			"message_id", messageID,
		)
		return existing.ID, true, nil
	}

	return id, false, nil
}

// Handle processes the ProcessNodeStatus command
func (h *ProcessStatusHandler) Handle(ctx context.Context, cmd command.ProcessNodeStatus, messageID string) error {
	h.logger.Info("Processing node status update",
		"message_id", messageID,
		"task_id", cmd.TaskID,
		"schedule_name", cmd.ScheduleName,
		"schema", cmd.Schema,
		"table_name", cmd.TableName,
		"status", cmd.Status,
	)

	// Serialize command as payload for message_processing
	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	// Begin transaction
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback()

	// Check for duplicate message
	msgProcessingID, shouldSkip, err := h.handleMessageDeduplication(ctx, messageID, payload)
	if err != nil {
		return fmt.Errorf("message deduplication failed: %w", err)
	}
	if shouldSkip {
		// Message already processed, commit and return
		// Don't rollback - we want to keep the transaction minimal
		return nil
	}

	// Step 1: Update the node status in graph service (outside transaction, idempotent)
	if err := h.graphClient.UpdateNodeStatus(ctx, cmd.ScheduleName, cmd.Schema, cmd.TableName, cmd.Status, cmd.ScheduleID.String()); err != nil {
		return fmt.Errorf("failed to update node status: %w", err)
	}

	// Step 2: If the node succeeded, trigger ready downstream nodes.
	if cmd.Status == "SUCCEEDED" {
		downstreamNodes, err := h.graphClient.GetReadyDownstream(ctx, cmd.ScheduleName, cmd.Schema, cmd.TableName, cmd.ScheduleID.String())
		if err != nil {
			return fmt.Errorf("failed to query ready downstream nodes: %w", err)
		}

		h.logger.Info("Found downstream nodes ready for execution",
			"count", len(downstreamNodes),
			"trigger_table", cmd.TableName,
		)

		for _, node := range downstreamNodes {
			nodeType, err := pkg_model.ParseNodeType(node.NodeType)
			if err != nil {
				h.logger.Error("Skipping downstream node with invalid node_type",
					"table", node.TableName, "node_type", node.NodeType, "error", err)
				continue
			}

			taskIDStr, err := h.ensureTask(ctx, cmd.ScheduleID, node.ServiceName, node.Schema, node.TableName)
			if err != nil {
				return fmt.Errorf("failed to ensure task for %s.%s: %w", node.Schema, node.TableName, err)
			}

			jobName, err := pkgDomain.ComputeJobName(node.ServiceName, node.Schema, node.TableName)
			if err != nil {
				return fmt.Errorf("failed to compute job_name for %s.%s: %w", node.Schema, node.TableName, err)
			}

			evt := event.NodeReadyForExecution{
				ScheduleID:   cmd.ScheduleID.String(),
				ScheduleName: cmd.ScheduleName,
				ServiceName:  node.ServiceName,
				Schema:       node.Schema,
				TableName:    node.TableName,
				TaskID:       taskIDStr,
				JobName:      jobName,
				NodeType:     string(nodeType),
			}

			evtPayload, err := json.Marshal(evt)
			if err != nil {
				return fmt.Errorf("failed to marshal event: %w", err)
			}

			outboxEntry := &model.OutboxEntry{
				ID:                  uuid.New(),
				MessageProcessingID: &msgProcessingID,
				AggregateType:       "dependency",
				AggregateID:         cmd.ScheduleID,
				EventType:           "node_ready_for_execution",
				Payload:             evtPayload,
				StreamName:          "query.model:v1",
				Status:              string(model.OutboxStatusPending),
				MaxRetries:          3,
			}

			if err := h.uow.OutboxRepo().Create(ctx, outboxEntry); err != nil {
				return fmt.Errorf("failed to write to outbox: %w", err)
			}

			h.logger.Debug("Wrote event to outbox",
				"outbox_id", outboxEntry.ID,
				"downstream_table", node.TableName,
			)
		}
	} else {
		h.logger.Info("Node did not succeed, skipping downstream check",
			"table_name", cmd.TableName,
			"status", cmd.Status,
		)
	}

	// Update message processing state to completed
	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, model.MessageProcessingStateCompleted); err != nil {
		return fmt.Errorf("failed to update message state: %w", err)
	}

	// Commit PostgreSQL transaction
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	h.logger.Info("Node status processing completed", "trigger_table", cmd.TableName)

	// Step 5 (NEW): After commit, check if the entire schedule is now drained.
	// Only SUCCEEDED and FAILED are terminal node statuses; RUNNING cannot complete a schedule.
	if cmd.Status == "SUCCEEDED" || cmd.Status == "FAILED" {
		if err := h.checkAndFinalizeSchedule(ctx, cmd); err != nil {
			// Non-fatal: main work is committed. A future reconciliation job recovers stale records.
			h.logger.Error("Failed to finalize schedule completion",
				"schedule_id", cmd.ScheduleID,
				"schedule_name", cmd.ScheduleName,
				"error", err,
			)
		}
	}

	return nil
}

// checkAndFinalizeSchedule queries the graph for drain status and, if complete,
// updates scheduler_tracker to SUCCEEDED or FAILED via gRPC.
func (h *ProcessStatusHandler) checkAndFinalizeSchedule(ctx context.Context, cmd command.ProcessNodeStatus) error {
	// Guard: skip finalization while a re-run is being set up.
	// Prevents overwriting RUNNING status with FAILED/SUCCEEDED before
	// startup-controller has finished resetting graph nodes.
	// The guard is lifted once startup-controller sets init_status = "completed".
	initStatus, err := h.stateClient.GetSchedulerInitStatus(ctx, cmd.ScheduleID)
	if err != nil {
		return fmt.Errorf("failed to get scheduler init status: %w", err)
	}
	if initStatus != "completed" {
		h.logger.Debug("Skipping finalization: init status not completed",
			"schedule_id", cmd.ScheduleID,
			"init_status", initStatus,
		)
		return nil
	}

	isComplete, hasFailed, err := h.graphClient.CheckScheduleCompletion(ctx, cmd.ScheduleName, cmd.ScheduleID.String())
	if err != nil {
		return fmt.Errorf("failed to check schedule completion: %w", err)
	}

	if !isComplete {
		h.logger.Debug("Schedule not yet complete", "schedule_name", cmd.ScheduleName)
		return nil
	}

	schedulerStatus := statev1.SchedulerStatus_SCHEDULER_STATUS_SUCCEEDED
	if hasFailed {
		schedulerStatus = statev1.SchedulerStatus_SCHEDULER_STATUS_FAILED
	}

	if err := h.stateClient.UpdateSchedulerStatus(ctx, cmd.ScheduleID, schedulerStatus); err != nil {
		return fmt.Errorf("failed to update scheduler status: %w", err)
	}

	terminalStatus := "SUCCEEDED"
	if hasFailed {
		terminalStatus = "FAILED"
	}
	if err := h.graphClient.FinalizeRun(ctx, cmd.ScheduleID.String(), terminalStatus); err != nil {
		h.logger.Error("Failed to finalize run snapshot",
			"run_id", cmd.ScheduleID,
			"error", err,
		)
	}

	h.logger.Info("Schedule finalized and run stamped",
		"schedule_id", cmd.ScheduleID,
		"schedule_name", cmd.ScheduleName,
		"status", schedulerStatus,
	)

	return nil
}

// ensureTask gets a pre-registered task for the given node, returns the task ID string
func (h *ProcessStatusHandler) ensureTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (string, error) {
	task, err := h.stateClient.GetTask(ctx, scheduleID, serviceName, schemaName, tableName)
	if errors.Is(err, grpc.ErrTaskNotFound) {
		return "", fmt.Errorf("task not found - pre-registration should have created it: %w", err)
	}
	if err != nil {
		return "", fmt.Errorf("failed to get task: %w", err)
	}

	return task.TaskId, nil
}
