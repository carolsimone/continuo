package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// DerivedRunDispatch carries the parameters for DispatchDerivedRun: the new
// run's identity, the projection materialised by the selector, and the
// kind label that distinguishes rerun from rebase in log output.
type DerivedRunDispatch struct {
	RunID               string
	ScheduleName        string
	Kind                string // "rerun" | "rebase"
	MessageProcessingID uuid.UUID
	Projection          []snapshot.TaskProjection
}

// DispatchDerivedRun writes ONE run.entries.dispatched:v1 outbox entry covering
// the full projection (per-task Status drives state.task_tracker.status:
// "pending" for rebased, terminal states preserved verbatim for inherited
// rows) plus N × query.model:v1 entries for the rebased rows on the dispatch
// frontier only (PENDING and ReadyToDispatch). A blocked rebased node — one
// whose upstream is itself rebased/PENDING — is intentionally not dispatched
// here: once its upstreams complete, the run aggregate emits NodeUnblocked for
// it, or cascade-skips it if an upstream fails. Dispatching the whole rebase
// subtree up front would run downstream nodes that should wait (and be skipped
// when an upstream re-fails).
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
		if t.InitialStatus == "PENDING" && t.ReadyToDispatch {
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
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &d.MessageProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           "run_entries_dispatched",
		Payload:             dispatchedPayload,
		StreamName:          streams.RunEntriesDispatchedV1,
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
		if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
			ID:                  uuid.New(),
			MessageProcessingID: &d.MessageProcessingID,
			AggregateType:       "orchestrator",
			AggregateID:         scheduleUUID,
			EventType:           "node_ready_for_execution",
			Payload:             queryPayload,
			StreamName:          streams.QueryModelV1,
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
