package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// HandleSingleNodeRunHandler consumes trigger.single_node_run:v1 messages.
type HandleSingleNodeRunHandler struct {
	uow         uow.UnitOfWork
	snapshotSvc SnapshotService
	logger      *slog.Logger
}

// NewHandleSingleNodeRunHandler creates a new HandleSingleNodeRunHandler.
func NewHandleSingleNodeRunHandler(u uow.UnitOfWork, snapshotSvc SnapshotService, logger *slog.Logger) *HandleSingleNodeRunHandler {
	return &HandleSingleNodeRunHandler{uow: u, snapshotSvc: snapshotSvc, logger: logger}
}

// Handle processes a SingleNodeRunInput derived from a
// trigger.single_node_run:v1 message.
//
// It runs the dedup→snapshot→outbox flow:
//  1. Marshal cmd for dedup record.
//  2. Begin transaction; defer Rollback.
//  3. Dedup on messageID with the streams.TriggerSingleNodeRunV1 stream.
//  4. Parse cmd.SourceRunID → *uuid.UUID (nil if empty).
//  5. Call snapshotSvc.Snapshot with a SingleNode selector.
//     - ErrTargetNotFound or ErrEmptyProjection: emit run.entries.dispatch_failed:v1,
//     mark dedup completed, commit, return nil.
//     - Other errors: return wrapped error (triggers retry).
//  6. Emit run.entries.dispatched:v1.
//  7. Emit query.model:v1 with NodeReadyForExecution payload.
//  8. Mark dedup state="completed".
//  9. Commit.
func (h *HandleSingleNodeRunHandler) Handle(ctx context.Context, cmd domainModel.SingleNodeRunInput, messageID string, outboxEntryID *uuid.UUID) error {
	h.logger.Info("Processing single-node run",
		"message_id", messageID,
		"run_id", cmd.RunID,
		"target", fmt.Sprintf("%s.%s.%s", cmd.ServiceName, cmd.SchemaName, cmd.TableName),
		"metadata_source", cmd.MetadataSource,
	)

	cmdPayload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}

	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	msgProcessingID, shouldSkip, err := h.dedup(ctx, messageID, cmdPayload, outboxEntryID)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if shouldSkip {
		return nil
	}

	var sourceRunUUID *uuid.UUID
	if cmd.SourceRunID != "" {
		parsed, perr := uuid.Parse(cmd.SourceRunID)
		if perr != nil {
			return fmt.Errorf("invalid source_run_id %q: %w", cmd.SourceRunID, perr)
		}
		sourceRunUUID = &parsed
	}

	projection, snapErr := h.snapshotSvc.Snapshot(ctx, snapshot.Params{
		RunID:        cmd.RunID,
		ScheduleName: cmd.ScheduleName,
		Kind:         "single_node_run",
		SourceRunID:  sourceRunUUID,
		Selector: snapshot.SingleNode{
			ServiceName:    cmd.ServiceName,
			SchemaName:     cmd.SchemaName,
			TableName:      cmd.TableName,
			MetadataSource: cmd.MetadataSource,
		},
	})
	if snapErr != nil {
		if reason, ok := dispatchFailedReason(snapErr); ok {
			if ferr := EmitDispatchFailed(ctx, h.uow, h.logger, DispatchFailedParams{
				RunID:               cmd.RunID,
				ScheduleName:        cmd.ScheduleName,
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
		return fmt.Errorf("snapshot single-node run: %w", snapErr)
	}
	if len(projection) != 1 {
		return fmt.Errorf("single-node-run: expected exactly 1 task in projection, got %d", len(projection))
	}
	sole := projection[0]
	taskID := sole.TaskID.String()
	imageTag := sole.ImageTag
	manifestVersion := sole.ManifestVersion
	nodeType := sole.NodeType

	scheduleUUID, err := uuid.Parse(cmd.RunID)
	if err != nil {
		return fmt.Errorf("invalid run_id %q: %w", cmd.RunID, err)
	}

	// Outbox 1: run.entries.dispatched:v1 — reuses the existing state consumer
	// RunEntriesDispatchedHandler which:
	//   1. creates the task_tracker row (with manifest_version + image_tag)
	//   2. sets total_task_count = 1
	//   3. sets initialization_status = "completed"
	// This is required for the finalization guard (initCompleted) to pass.
	dispatchedEvt := pkgEvents.RunEntriesDispatched{
		ScheduleID:   cmd.RunID,
		ScheduleName: cmd.ScheduleName,
		AllTasks: []pkgEvents.DispatchedTask{
			{
				TaskID:          taskID,
				ServiceName:     cmd.ServiceName,
				SchemaName:      cmd.SchemaName,
				TableName:       cmd.TableName,
				NodeType:        nodeType,
				MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
				ManifestVersion: manifestVersion,
				ImageTag:        imageTag,
			},
		},
		TotalTaskCount: 1,
	}
	dispatchedPayload, err := json.Marshal(dispatchedEvt)
	if err != nil {
		return fmt.Errorf("marshal run.entries.dispatched: %w", err)
	}
	if err := h.uow.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           "run_entries_dispatched",
		Payload:             dispatchedPayload,
		StreamName:          streams.RunEntriesDispatchedV1,
		Status:              "pending",
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write run.entries.dispatched to outbox: %w", err)
	}

	// Outbox 2: query.model:v1
	jobName, err := pkgDomain.ComputeJobName(cmd.ServiceName, cmd.SchemaName, cmd.TableName, cmd.RunID)
	if err != nil {
		return fmt.Errorf("compute job name: %w", err)
	}
	queryEvt := domain.NodeReadyForExecution{
		ScheduleID:      cmd.RunID,
		ScheduleName:    cmd.ScheduleName,
		ServiceName:     cmd.ServiceName,
		SchemaName:      cmd.SchemaName,
		TableName:       cmd.TableName,
		TaskID:          taskID,
		JobName:         jobName,
		NodeType:        nodeType,
		ManifestVersion: manifestVersion,
		ImageTag:        imageTag,
	}
	queryPayload, err := json.Marshal(queryEvt)
	if err != nil {
		return fmt.Errorf("marshal query.model: %w", err)
	}
	if err := h.uow.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           "node_ready_for_execution",
		Payload:             queryPayload,
		StreamName:          streams.QueryModelV1,
		Status:              "pending",
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write query.model to outbox: %w", err)
	}

	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}

	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	h.logger.Info("Single-node run processing finished",
		"run_id", cmd.RunID,
		"target", fmt.Sprintf("%s.%s.%s", cmd.ServiceName, cmd.SchemaName, cmd.TableName),
		"task_id", taskID,
	)

	return nil
}

// dedup checks for duplicate delivery. Delegates to
// pkg/messageprocessing.DedupWithOutboxEntryID so producer-side
// Processor-crash redeliveries (same outbox row republished with a fresh
// Redis msg_id) are caught by the outbox_entry_id secondary uniqueness key.
func (h *HandleSingleNodeRunHandler) dedup(
	ctx context.Context,
	messageID string,
	payload []byte,
	outboxEntryID *uuid.UUID,
) (uuid.UUID, bool, error) {
	return messageprocessing.DedupWithOutboxEntryID(
		ctx, h.uow.MessageProcessingRepo(), h.logger,
		messageID, streams.TriggerSingleNodeRunV1, payload, outboxEntryID,
	)
}
