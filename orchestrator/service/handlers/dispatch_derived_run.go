package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// DerivedRunDispatch carries the parameters for DispatchDerivedRun: the new
// run's identity, the projection materialised by the selector, and the
// labels for the dispatch_failed stream the caller uses on empty projections.
type DerivedRunDispatch struct {
	RunID               string
	ScheduleName        string
	Kind                string // "rerun" | "rebase"
	StreamForFailed     string // run.entries.dispatch_failed:v1
	EventTypeForFailed  string // run_entries_dispatch_failed
	MessageProcessingID uuid.UUID
	Projection          []snapshot.TaskProjection
}

// DispatchDerivedRun writes ONE run.entries.dispatched:v1 outbox entry covering
// the full projection (per-task Status drives state.task_tracker.status:
// "pending" for rebased, terminal states preserved verbatim for inherited
// rows) plus N × query.model:v1 entries for the PENDING (rebased) rows only.
// Inherited terminal rows MUST round-trip verbatim — coercing them to
// "pending" creates task_tracker rows the executor never runs, blocking
// run finalize.
func DispatchDerivedRun(ctx context.Context, u uow.UnitOfWork, logger *slog.Logger, d DerivedRunDispatch) error {
	scheduleUUID, err := uuid.Parse(d.RunID)
	if err != nil {
		return fmt.Errorf("invalid run_id %q: %w", d.RunID, err)
	}

	allTasks := make([]pkgEvents.DispatchedTask, 0, len(d.Projection))
	queryModelTasks := make([]snapshot.TaskProjection, 0)
	for _, t := range d.Projection {
		inheritedStr := ""
		if t.InheritedFromTaskID != nil {
			inheritedStr = t.InheritedFromTaskID.String()
		}
		allTasks = append(allTasks, pkgEvents.DispatchedTask{
			TaskID:              t.TaskID.String(),
			ServiceName:         t.ServiceName,
			SchemaName:          t.SchemaName,
			TableName:           t.TableName,
			NodeType:            t.NodeType,
			MaxRetries:          t.MaxRetries,
			ManifestVersion:     t.ManifestVersion,
			ImageTag:            t.ImageTag,
			Status:              projectionStatusLower(t.InitialStatus),
			InheritedFromTaskID: inheritedStr,
		})
		if t.InitialStatus == "PENDING" {
			queryModelTasks = append(queryModelTasks, t)
		}
	}

	dispatchedEvt := pkgEvents.RunEntriesDispatched{
		ScheduleID:     d.RunID,
		ScheduleName:   d.ScheduleName,
		AllTasks:       allTasks,
		TotalTaskCount: int32(len(allTasks)),
	}
	dispatchedPayload, err := json.Marshal(dispatchedEvt)
	if err != nil {
		return fmt.Errorf("marshal run.entries.dispatched: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &d.MessageProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           "run_entries_dispatched",
		Payload:             dispatchedPayload,
		StreamName:          "run.entries.dispatched:v1",
		Status:              "pending",
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write run.entries.dispatched: %w", err)
	}

	for _, t := range queryModelTasks {
		jobName, err := pkgDomain.ComputeJobName(t.ServiceName, t.SchemaName, t.TableName, d.RunID)
		if err != nil {
			return fmt.Errorf("compute job name for %s.%s: %w", t.SchemaName, t.TableName, err)
		}
		queryEvt := domain.NodeReadyForExecution{
			ScheduleID:      d.RunID,
			ScheduleName:    d.ScheduleName,
			ServiceName:     t.ServiceName,
			SchemaName:      t.SchemaName,
			TableName:       t.TableName,
			TaskID:          t.TaskID.String(),
			JobName:         jobName,
			NodeType:        t.NodeType,
			ManifestVersion: t.ManifestVersion,
			ImageTag:        t.ImageTag,
		}
		queryPayload, err := json.Marshal(queryEvt)
		if err != nil {
			return fmt.Errorf("marshal query.model: %w", err)
		}
		if err := u.OutboxRepo().Create(ctx, &domain.OutboxEntry{
			ID:                  uuid.New(),
			MessageProcessingID: &d.MessageProcessingID,
			AggregateType:       "orchestrator",
			AggregateID:         scheduleUUID,
			EventType:           "node_ready_for_execution",
			Payload:             queryPayload,
			StreamName:          "query.model:v1",
			Status:              "pending",
			MaxRetries:          3,
		}); err != nil {
			return fmt.Errorf("write query.model for %s.%s: %w", t.SchemaName, t.TableName, err)
		}
	}

	logger.Info("Derived run dispatched",
		"kind", d.Kind, "run_id", d.RunID,
		"total_tasks", len(allTasks), "dispatched_tasks", len(queryModelTasks),
	)
	return nil
}

// DispatchFailed carries the parameters for EmitDispatchFailed.
type DispatchFailed struct {
	RunID               string
	ScheduleName        string
	Reason              string
	StreamName          string
	EventType           string
	MessageProcessingID uuid.UUID
}

// EmitDispatchFailed writes a run.entries.dispatch_failed:v1 outbox entry.
// Used when a selector returns ErrEmptyProjection.
func EmitDispatchFailed(ctx context.Context, u uow.UnitOfWork, logger *slog.Logger, df DispatchFailed) error {
	scheduleUUID, err := uuid.Parse(df.RunID)
	if err != nil {
		return fmt.Errorf("invalid run_id %q: %w", df.RunID, err)
	}
	evt := pkgEvents.RunEntriesDispatchFailed{
		ScheduleID:   df.RunID,
		ScheduleName: df.ScheduleName,
		Reason:       pkgEvents.DispatchFailedReason(df.Reason),
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &df.MessageProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           df.EventType,
		Payload:             payload,
		StreamName:          df.StreamName,
		Status:              "pending",
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write %s: %w", df.StreamName, err)
	}
	logger.Info("Emitted dispatch_failed", "run_id", df.RunID, "reason", df.Reason)
	return nil
}

// projectionStatusLower maps a TaskProjection InitialStatus to its wire-format
// (lowercased) string. Kept private to this package (shared by handle_rerun
// and handle_rebase indirectly through DispatchDerivedRun).
func projectionStatusLower(initialStatus string) string {
	switch initialStatus {
	case "PENDING":
		return "pending"
	case "SUCCEEDED":
		return "succeeded"
	case "FAILED":
		return "failed"
	case "CANCELLED":
		return "cancelled"
	case "SKIPPED":
		return "skipped"
	default:
		return strings.ToLower(initialStatus)
	}
}
