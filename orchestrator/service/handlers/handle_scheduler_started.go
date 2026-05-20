package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// HandleSchedulerStartedHandler consumes scheduler.started:v1 events.
// On success, it snapshots the run graph, emits run.entries.dispatched:v1
// with all tasks, and dispatches the initial seed/root nodes via query.model:v1.
// On empty projection (no active :Table nodes), it emits
// run.entries.dispatch_failed:v1 with reason "empty_projection", allowing
// state's RunEntriesDispatchFailedHandler to finalise the run as failed.
type HandleSchedulerStartedHandler struct {
	uow         uow.UnitOfWork
	runQueries  run.RunQueryPort
	snapshotSvc SnapshotService
	logger      *slog.Logger
}

// NewHandleSchedulerStartedHandler creates a new HandleSchedulerStartedHandler.
func NewHandleSchedulerStartedHandler(
	u uow.UnitOfWork,
	runQueries run.RunQueryPort,
	snapshotSvc SnapshotService,
	logger *slog.Logger,
) *HandleSchedulerStartedHandler {
	return &HandleSchedulerStartedHandler{
		uow:         u,
		runQueries:  runQueries,
		snapshotSvc: snapshotSvc,
		logger:      logger,
	}
}

// Handle processes a domain.SchedulerStarted event.
func (h *HandleSchedulerStartedHandler) Handle(ctx context.Context, evt domain.SchedulerStarted, messageID string, outboxEntryID *uuid.UUID) error {
	h.logger.Info("Processing scheduler started",
		"message_id", messageID,
		"schedule_id", evt.ScheduleID,
		"schedule_name", evt.ScheduleName,
	)

	// Marshal event payload for message_processing record.
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Begin transaction.
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	// Dedup: skip if already processed.
	msgProcessingID, shouldSkip, err := h.dedup(ctx, messageID, payload, outboxEntryID)
	if err != nil {
		return fmt.Errorf("message deduplication failed: %w", err)
	}
	if shouldSkip {
		return nil
	}

	// Snapshot the graph for this run via the unified routine: LatestFullDAG
	// selector enumerates active :Tables + upstream dbt-seeds, mints fresh
	// task_ids, and the materialiser writes :Run + :EXECUTES edges in one tx.
	projection, err := h.snapshotSvc.Snapshot(ctx, snapshot.Params{
		RunID:        evt.ScheduleID.String(),
		ScheduleName: evt.ScheduleName,
		Kind:         evt.Kind,
		SourceRunID:  evt.SourceRunID,
		Selector:     snapshot.LatestFullDAG{},
	})
	if err != nil {
		if reason, ok := dispatchFailedReason(err); ok {
			if ferr := EmitDispatchFailed(ctx, h.uow, h.logger, DispatchFailedParams{
				RunID:               evt.ScheduleID.String(),
				ScheduleName:        evt.ScheduleName,
				MessageProcessingID: msgProcessingID,
				Reason:              reason,
			}); ferr != nil {
				return ferr
			}
			if cerr := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); cerr != nil {
				return fmt.Errorf("mark completed after %s: %w", reason, cerr)
			}
			return h.uow.Commit()
		}
		return fmt.Errorf("failed to snapshot graph: %w", err)
	}

	// Get root/seed dispatch ordering for the schedule. The AllNodes data is
	// redundant with the projection above; we ignore it. Only RootNodes/SeedNodes
	// are used (seeds dispatch first, otherwise roots).
	initNodes, err := h.runQueries.GetScheduleInitNodes(ctx, evt.ScheduleName, evt.ScheduleID.String())
	if err != nil {
		return fmt.Errorf("failed to get schedule init nodes: %w", err)
	}

	// Build and write run.entries.dispatched:v1 outbox entry from the projection.
	dispatchedPayload, err := h.buildRunEntriesDispatchedPayload(evt, projection)
	if err != nil {
		return fmt.Errorf("failed to build run.entries.dispatched payload: %w", err)
	}

	entriesDispatchedEntry := &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         evt.ScheduleID,
		EventType:           "run_entries_dispatched",
		Payload:             dispatchedPayload,
		StreamName:          streams.RunEntriesDispatchedV1,
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

		jobName, err := pkgDomain.ComputeJobName(node.ServiceName, node.SchemaName, node.TableName, evt.ScheduleID.String())
		if err != nil {
			return fmt.Errorf("failed to compute job_name for %s.%s: %w", node.SchemaName, node.TableName, err)
		}

		nodeEvt := domain.NodeReadyForExecution{
			ScheduleID:      evt.ScheduleID.String(),
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

		evtPayload, err := json.Marshal(nodeEvt)
		if err != nil {
			return fmt.Errorf("failed to marshal NodeReadyForExecution for %s.%s: %w", node.SchemaName, node.TableName, err)
		}

		outboxEntry := &pkgoutbox.Entry{
			ID:                  uuid.New(),
			MessageProcessingID: &msgProcessingID,
			AggregateType:       "orchestrator",
			AggregateID:         evt.ScheduleID,
			EventType:           "node_ready_for_execution",
			Payload:             evtPayload,
			StreamName:          streams.QueryModelV1,
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
		"schedule_id", evt.ScheduleID,
		"schedule_name", evt.ScheduleName,
		"dispatched_count", len(dispatchNodes),
	)

	return nil
}

// dedup checks if the message was already processed. Delegates to
// pkg/messageprocessing.DedupWithOutboxEntryID so that producer-side
// Processor-crash redeliveries (same outbox row republished with a fresh
// Redis msg_id) are caught by the outbox_entry_id secondary uniqueness key.
func (h *HandleSchedulerStartedHandler) dedup(
	ctx context.Context,
	messageID string,
	messagePayload []byte,
	outboxEntryID *uuid.UUID,
) (uuid.UUID, bool, error) {
	return messageprocessing.DedupWithOutboxEntryID(
		ctx, h.uow.MessageProcessingRepo(), h.logger,
		messageID, "scheduler.started:v1", messagePayload, outboxEntryID,
	)
}

// buildRunEntriesDispatchedPayload constructs the JSON payload for run.entries.dispatched:v1
// from the snapshot projection. All cron-path tasks are PENDING with no inherit
// pointer — Status defaults to "" (omitted) on the wire, which the consumer
// treats as PENDING (backward-compat).
func (h *HandleSchedulerStartedHandler) buildRunEntriesDispatchedPayload(
	evt domain.SchedulerStarted,
	projection []snapshot.TaskProjection,
) ([]byte, error) {
	allTasks := make([]pkgevents.DispatchedTask, 0, len(projection))
	for _, t := range projection {
		allTasks = append(allTasks, pkgevents.DispatchedTask{
			TaskID:          t.TaskID.String(),
			ServiceName:     t.ServiceName,
			SchemaName:      t.SchemaName,
			TableName:       t.TableName,
			NodeType:        t.NodeType,
			MaxRetries:      t.MaxRetries,
			ManifestVersion: t.ManifestVersion,
			ImageTag:        t.ImageTag,
		})
	}

	dispatched := pkgevents.RunEntriesDispatched{
		ScheduleID:     evt.ScheduleID.String(),
		ScheduleName:   evt.ScheduleName,
		AllTasks:       allTasks,
		TotalTaskCount: int32(len(allTasks)),
	}

	return json.Marshal(dispatched)
}
