package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// HandleSchedulerStartedHandler consumes scheduler.started:v1 events.
// It snapshots the run graph, emits run.entries.dispatched:v1 with all tasks,
// and dispatches the initial seed/root nodes via query.model:v1.
// This absorbs the role previously owned by startup-controller.
type HandleSchedulerStartedHandler struct {
	uow     uow.UnitOfWork
	runRepo run.Repository
	logger  *slog.Logger
}

// NewHandleSchedulerStartedHandler creates a new HandleSchedulerStartedHandler.
func NewHandleSchedulerStartedHandler(
	u uow.UnitOfWork,
	runRepo run.Repository,
	logger *slog.Logger,
) *HandleSchedulerStartedHandler {
	return &HandleSchedulerStartedHandler{
		uow:     u,
		runRepo: runRepo,
		logger:  logger,
	}
}

// Handle processes a SchedulerStartedCmd.
func (h *HandleSchedulerStartedHandler) Handle(ctx context.Context, cmd domainCmd.SchedulerStartedCmd, messageID string) error {
	h.logger.Info("Processing scheduler started",
		"message_id", messageID,
		"schedule_id", cmd.ScheduleID,
		"schedule_name", cmd.ScheduleName,
	)

	// Marshal command payload for message_processing record.
	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	// Begin transaction.
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	// Dedup: skip if already processed.
	msgProcessingID, shouldSkip, err := h.dedup(ctx, messageID, payload)
	if err != nil {
		return fmt.Errorf("message deduplication failed: %w", err)
	}
	if shouldSkip {
		return nil
	}

	// Snapshot the graph for this run (creates Run + EXECUTES edges, pre-assigns task UUIDs).
	if err := h.runRepo.SnapshotGraph(ctx, cmd.ScheduleID.String(), cmd.ScheduleName, cmd.Kind, cmd.SourceRunID); err != nil {
		return fmt.Errorf("failed to snapshot graph: %w", err)
	}

	// Get all/root/seed initialization nodes for the schedule.
	initNodes, err := h.runRepo.GetScheduleInitNodes(ctx, cmd.ScheduleName, cmd.ScheduleID.String())
	if err != nil {
		return fmt.Errorf("failed to get schedule init nodes: %w", err)
	}

	// Build and write run.entries.dispatched:v1 outbox entry.
	dispatchedPayload, err := h.buildRunEntriesDispatchedPayload(cmd, initNodes)
	if err != nil {
		return fmt.Errorf("failed to build run.entries.dispatched payload: %w", err)
	}

	entriesDispatchedEntry := &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         cmd.ScheduleID,
		EventType:           "run_entries_dispatched",
		Payload:             dispatchedPayload,
		StreamName:          "run.entries.dispatched:v1",
		Status:              "pending",
		MaxRetries:          3,
	}
	if err := h.uow.OutboxRepo().Create(ctx, entriesDispatchedEntry); err != nil {
		return fmt.Errorf("failed to write run.entries.dispatched outbox entry: %w", err)
	}

	// Determine which nodes to dispatch first: seeds take priority over roots.
	dispatchNodes := initNodes.SeedNodes
	if len(dispatchNodes) == 0 {
		dispatchNodes = initNodes.RootNodes
	}

	// Dispatch each seed/root node as a query.model:v1 outbox entry.
	for _, node := range dispatchNodes {
		nodeType, err := pkgModel.ParseNodeType(node.NodeType)
		if err != nil {
			h.logger.Error("Skipping dispatch node with invalid node_type",
				"table", node.TableName, "node_type", node.NodeType, "error", err)
			continue
		}

		jobName, err := pkgDomain.ComputeJobName(node.ServiceName, node.SchemaName, node.TableName, cmd.ScheduleID.String())
		if err != nil {
			return fmt.Errorf("failed to compute job_name for %s.%s: %w", node.SchemaName, node.TableName, err)
		}

		evt := domain.NodeReadyForExecution{
			ScheduleID:      cmd.ScheduleID.String(),
			ScheduleName:    node.ScheduleName,
			ServiceName:     node.ServiceName,
			SchemaName:      node.SchemaName,
			TableName:       node.TableName,
			TaskID:          node.TaskID,
			JobName:         jobName,
			NodeType:        string(nodeType),
			ManifestVersion: node.ManifestVersion,
			ImageTag:        node.ImageTag,
		}

		evtPayload, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("failed to marshal NodeReadyForExecution for %s.%s: %w", node.SchemaName, node.TableName, err)
		}

		outboxEntry := &domain.OutboxEntry{
			ID:                  uuid.New(),
			MessageProcessingID: &msgProcessingID,
			AggregateType:       "orchestrator",
			AggregateID:         cmd.ScheduleID,
			EventType:           "node_ready_for_execution",
			Payload:             evtPayload,
			StreamName:          "query.model:v1",
			Status:              "pending",
			MaxRetries:          3,
		}
		if err := h.uow.OutboxRepo().Create(ctx, outboxEntry); err != nil {
			return fmt.Errorf("failed to write query.model outbox entry for %s.%s: %w", node.SchemaName, node.TableName, err)
		}

		h.logger.Debug("Dispatched initial node",
			"table", node.TableName,
			"schema", node.SchemaName,
			"task_id", node.TaskID,
		)
	}

	// Mark message processing as completed.
	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("failed to update message state: %w", err)
	}

	// Commit the Postgres transaction.
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	h.logger.Info("Scheduler started processing finished",
		"schedule_id", cmd.ScheduleID,
		"schedule_name", cmd.ScheduleName,
		"dispatched_count", len(dispatchNodes),
	)

	return nil
}

// dedup checks if the message was already processed.
// Returns (messageProcessingID, shouldSkip, error).
func (h *HandleSchedulerStartedHandler) dedup(
	ctx context.Context,
	messageID string,
	messagePayload []byte,
) (uuid.UUID, bool, error) {
	msgProc := &domain.MessageProcessing{
		MessageID:  messageID,
		StreamName: "scheduler.started:v1",
		State:      "processing",
		Payload:    messagePayload,
	}

	id, inserted, err := h.uow.MessageProcessingRepo().InsertIfNotExists(ctx, msgProc)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("failed to insert message processing: %w", err)
	}

	if !inserted {
		existing, err := h.uow.MessageProcessingRepo().GetByMessageID(ctx, messageID)
		if err != nil {
			return uuid.Nil, false, fmt.Errorf("failed to get existing message: %w", err)
		}

		if existing.State == "completed" || existing.State == "acked" {
			h.logger.Info("Message already processed, skipping",
				"message_id", messageID,
				"state", existing.State,
			)
			return existing.ID, true, nil
		}

		h.logger.Warn("Message being processed by another instance",
			"message_id", messageID,
		)
		return existing.ID, true, nil
	}

	return id, false, nil
}

// buildRunEntriesDispatchedPayload constructs the JSON payload for run.entries.dispatched:v1.
func (h *HandleSchedulerStartedHandler) buildRunEntriesDispatchedPayload(
	cmd domainCmd.SchedulerStartedCmd,
	initNodes *run.ScheduleInitNodes,
) ([]byte, error) {
	var allTasks []pkgevents.DispatchedTask
	if initNodes != nil {
		for _, node := range initNodes.AllNodes {
			allTasks = append(allTasks, pkgevents.DispatchedTask{
				TaskID:          node.TaskID,
				ServiceName:     node.ServiceName,
				SchemaName:      node.SchemaName,
				TableName:       node.TableName,
				NodeType:        node.NodeType,
				ManifestVersion: node.ManifestVersion,
				ImageTag:        node.ImageTag,
			})
		}
	}

	evt := pkgevents.RunEntriesDispatched{
		ScheduleID:     cmd.ScheduleID.String(),
		ScheduleName:   cmd.ScheduleName,
		AllTasks:       allTasks,
		TotalTaskCount: int32(len(allTasks)),
	}

	return json.Marshal(evt)
}
