package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/events"
	"github.com/carolsimone/continuo/run-controller/domain/seeds"
	"github.com/carolsimone/continuo/run-controller/service/ports"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
)

// AttemptReportHandler applies one task.status.updated:v2 report in its run's
// transaction: it locks the run, lets the run record the report
// (Run.RecordAttemptReport: the attempt by rank, the task, the next attempt
// of a retryable failure, the dispatch of the tasks a success makes ready, the
// skip of what a final failure blocks, and finalization), saves the run and
// writes the dispatches it recorded to the outbox. When the report ends a
// task of a run run-controller planned with an operation that writes the
// table, succeeded or failed for good, it reads the task and records what the
// end means for the warehouse:
//   - a success records the version the task loaded into the warehouse, which
//     the seed reconcile compares with the live release, and closes the
//     node's view outage;
//   - the success of a seed load that drops its table opens an outage for each
//     view the run rebuilds after it, missing on a schema_bound engine and
//     stale on a late_binding one;
//   - a seed load that ended counts its action and outcome, and a task of a
//     seed reload that a seed rule stopped counts its reason.
//
// When the report ends a cancelling run, it observes how long the cancel
// took. Every count and observation is made once the transaction commits.
type AttemptReportHandler struct {
	tasks   run.TaskDefaults
	metrics ports.RunMetrics
	reloads ports.SeedReloadMetrics
	family  warehouse.Family
	logger  *slog.Logger
}

// NewAttemptReportHandler constructs the handler; tasks names the timeout of a
// dispatch whose node declares none, and family is the drop family of the
// install's warehouse engine.
func NewAttemptReportHandler(tasks run.TaskDefaults, metrics ports.RunMetrics, reloads ports.SeedReloadMetrics,
	family warehouse.Family, logger *slog.Logger) *AttemptReportHandler {
	return &AttemptReportHandler{tasks: tasks, metrics: metrics, reloads: reloads, family: family, logger: logger}
}

// Handle applies evt inside u's transaction; the caller commits. It locks the
// run evt names under the tenant evt carries, the tenant of the report's
// envelope, and applies the report under the run's tenant. The error of
// locking a run with no row, run.ErrTaskNotHeld, run.ErrAttemptNotIssued and
// run.ErrStopNotRequested propagate wrapped: a report naming a run run-controller never
// created, a run of another tenant than its envelope's, a task the run does
// not hold, an attempt run-controller never dispatched or a stopped attempt run-controller never
// asked to stop can never be applied, so the stream binding marks them
// permanent (permanentWhenNeverValid) and the consumer dead-letters the report
// at once.
func (h *AttemptReportHandler) Handle(ctx context.Context, u uow.UnitOfWork, evt events.AttemptReported, msgProcID uuid.UUID) error {
	r, err := u.Run().LoadRunForUpdate(ctx, evt.TenantID, evt.RunID)
	if err != nil {
		return fmt.Errorf("load run: %w", err)
	}
	tenantID := r.TenantID()
	domainEvents, ended, err := r.RecordAttemptReport(ctx, u.TaskCollection(tenantID), u.TaskGraph(tenantID), evt.Report, h.tasks, u.Clock().Now())
	if err != nil {
		return fmt.Errorf("record attempt report: %w", err)
	}
	if err := u.Run().SaveRun(ctx, r); err != nil {
		return fmt.Errorf("save run: %w", err)
	}
	if err := u.Outbox().Append(ctx, domainEvents, msgProcID); err != nil {
		return fmt.Errorf("append outbox: %w", err)
	}
	h.observeCancel(u, r, domainEvents, u.Clock().Now())
	if (ended == run.TaskStatusSucceeded || ended == run.TaskStatusFailed) && recordsBuiltVersions(r) {
		if err := h.recordTaskEnd(ctx, u, r, evt.Report, ended); err != nil {
			return err
		}
	}
	h.logger.Info("task.status.updated: report processed",
		"tenant_id", tenantID, "run_id", evt.RunID, "task_id", evt.Report.TaskID, "attempt", evt.Report.Attempt,
		"status", evt.Report.Status, "failure_reason", evt.Report.FailureReason)
	return nil
}

// observeCancel observes the time from the cancel to now when events end r
// cancelled, once u's transaction commits: a report whose transaction rolls
// back, and is redelivered, counts once.
func (h *AttemptReportHandler) observeCancel(u uow.UnitOfWork, r *run.Run, events []run.DomainEvent, now time.Time) {
	if r.CancelledAt() == nil {
		return
	}
	for _, evt := range events {
		if fin, ok := evt.(run.RunFinalized); ok && fin.Outcome == run.RunStatusCancelled {
			d := now.Sub(*r.CancelledAt())
			u.AfterCommit(func() { h.metrics.RunCancelled(d) })
			return
		}
	}
}

// recordTaskEnd records what the end of a task of r, a run that writes its
// nodes, means for the warehouse: on success the version the task loaded, the
// close of its node's outage and, for a seed load that drops its table, the
// outages of the views the run rebuilds after it; then the count of a seed
// reload's task. A task whose row is gone records nothing.
func (h *AttemptReportHandler) recordTaskEnd(ctx context.Context, u uow.UnitOfWork, r *run.Run, rep run.AttemptReport, ended run.TaskStatus) error {
	tenantID := r.TenantID()
	task, err := u.TaskCollection(tenantID).GetByID(ctx, rep.TaskID)
	if errors.Is(err, run.ErrTaskNotFound) {
		h.logger.Warn("task status: task row not found; nothing recorded of its end",
			"task_id", rep.TaskID, "run_id", r.RunID(), "release_id", r.ReleaseID())
		return nil
	}
	if err != nil {
		return fmt.Errorf("read task %s: %w", rep.TaskID, err)
	}
	if ended == run.TaskStatusSucceeded {
		if err := recordLoadedVersion(ctx, u, tenantID, h.logger, r, task); err != nil {
			return err
		}
		if err := h.closeOutage(ctx, u, tenantID, task); err != nil {
			return err
		}
		if err := h.openRebuildOutages(ctx, u, r, task); err != nil {
			return err
		}
	}
	h.countReload(u, r, task, rep, ended)
	return nil
}

// closeOutage closes the outage of the task's node and observes, once the
// transaction commits, how long it lasted. A node with no outage observes
// nothing.
func (h *AttemptReportHandler) closeOutage(ctx context.Context, u uow.UnitOfWork, tenantID string, task run.Task) error {
	key := task.NodeKey
	since, found, err := u.ViewOutages().Close(ctx, tenantID, key)
	if err != nil {
		return fmt.Errorf("close the outage of %s: %w", key, err)
	}
	if !found {
		return nil
	}
	after := u.Clock().Now().Sub(since)
	u.AfterCommit(func() { h.reloads.ViewRebuilt(after) })
	h.logger.Info("task status: view rebuilt; outage closed", "tenant_id", tenantID, "node_key", key, "outage_seconds", after.Seconds())
	return nil
}

// openRebuildOutages opens, when task is a seed load that drops its table, an
// outage for each view of r rebuilt after it, at the clock's now, in the state
// the install's warehouse family leaves such a view in.
func (h *AttemptReportHandler) openRebuildOutages(ctx context.Context, u uow.UnitOfWork, r *run.Run, task run.Task) error {
	if task.SeedPreflight == nil {
		return nil
	}
	seed := task.NodeKey
	state := seeds.OutageStateFor(h.family)
	n, err := u.ViewOutages().OpenForRebuilds(ctx, r.TenantID(), r.RunID(), seed, state, u.Clock().Now())
	if err != nil {
		return fmt.Errorf("open the outages of the views rebuilt after %s: %w", seed, err)
	}
	h.logger.Info("task status: seed loaded; outages opened for its rebuilds",
		"tenant_id", r.TenantID(), "run_id", r.RunID(), "node_key", seed, "state", state, "outages", n)
	return nil
}

// countReload counts, once the transaction commits, the end of a task of a
// seed reload: a seed load (a task with a seed preflight) counts its action
// and outcome, and a seed load or a view's rebuild that a seed rule stopped
// counts its reason and is logged at WARN with the objects outside the run
// its preflight found. Any other task counts nothing.
func (h *AttemptReportHandler) countReload(u uow.UnitOfWork, r *run.Run, task run.Task, rep run.AttemptReport, ended run.TaskStatus) {
	seedLoad := task.SeedPreflight != nil
	if !seedLoad && task.RebuildOf == "" {
		return
	}
	var reason model.FailureReason
	if ended == run.TaskStatusFailed {
		reason = rep.FailureReason
	}
	stopped := seedRuleStopped(reason)
	action, outcome := reloadAction(task.Operation), ""
	if seedLoad {
		outcome = seedLoadOutcome(ended, reason)
	}
	if outcome == "" && !stopped {
		return
	}
	var outside []string
	if rep.SeedPreflight != nil {
		for _, d := range rep.SeedPreflight.Outside() {
			outside = append(outside, d.Object())
		}
	}
	u.AfterCommit(func() {
		if outcome != "" {
			h.reloads.SeedReloadEnded(action, outcome)
		}
		if stopped {
			h.reloads.SeedReloadStopped(string(reason))
			h.logger.Warn("seed reload stopped: a seed rule ended the task before it changed the warehouse",
				"tenant_id", r.TenantID(), "release_id", r.ReleaseID(), "run_id", r.RunID(), "node_key", task.NodeKey,
				"failure_reason", reason, "outside_objects", outside)
		}
	})
}

// reloadAction is the action of a seed load whose task applies op:
// full_refresh for a full refresh, load otherwise.
func reloadAction(op model.Operation) string {
	if op == model.OperationFullRefresh {
		return "full_refresh"
	}
	return "load"
}

// seedLoadOutcome is the outcome of a seed load whose task ended, failed with
// reason or succeeded: loaded on success; stopped when its preflight stopped
// it; refused when no command can apply it; failed otherwise.
func seedLoadOutcome(ended run.TaskStatus, reason model.FailureReason) string {
	if ended == run.TaskStatusSucceeded {
		return "loaded"
	}
	switch reason {
	case model.FailureReasonUnownedDependents, model.FailureReasonEngineMismatch:
		return "stopped"
	case model.FailureReasonSeedFullRefreshDisabled, model.FailureReasonSeedFullRefreshUndefined, model.FailureReasonFullRefreshUndefined:
		return "refused"
	}
	return "failed"
}

// seedRuleStopped reports whether a task that failed with reason was stopped
// by a seed rule before it changed the warehouse.
func seedRuleStopped(reason model.FailureReason) bool {
	switch reason {
	case model.FailureReasonUnownedDependents, model.FailureReasonEngineMismatch, model.FailureReasonSeedFullRefreshDisabled,
		model.FailureReasonSeedFullRefreshUndefined, model.FailureReasonFullRefreshUndefined:
		return true
	}
	return false
}
