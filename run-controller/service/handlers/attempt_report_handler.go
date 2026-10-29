package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/events"
	"github.com/carolsimone/continuo/run-controller/service/ports"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
)

// AttemptReportHandler applies one task.status.updated:v2 report in its run's
// transaction: it locks the run, lets the run record the report
// (Run.RecordAttemptReport: the attempt by rank, the task, the next attempt
// of a retryable failure, the dispatch of the tasks a success makes ready, the
// skip of what a final failure blocks, and finalization), saves the run and
// writes the dispatches it recorded to the outbox. When the report makes a
// task of a run run-controller planned with an operation that writes the table
// succeeded, it also records the version the task loaded into the warehouse,
// which the seed reconcile compares with the live release. When the report
// ends a cancelling run, it observes how long the cancel took once the
// transaction commits.
type AttemptReportHandler struct {
	tasks   run.TaskDefaults
	metrics ports.RunMetrics
	logger  *slog.Logger
}

// NewAttemptReportHandler constructs the handler; tasks names the timeout of a
// dispatch whose node declares none.
func NewAttemptReportHandler(tasks run.TaskDefaults, metrics ports.RunMetrics, logger *slog.Logger) *AttemptReportHandler {
	return &AttemptReportHandler{tasks: tasks, metrics: metrics, logger: logger}
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
	domainEvents, succeeded, err := r.RecordAttemptReport(ctx, u.TaskCollection(tenantID), u.TaskGraph(tenantID), evt.Report, h.tasks, u.Clock().Now())
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
	if succeeded && recordsBuiltVersions(r) {
		if err := recordLoadedVersion(ctx, u, tenantID, h.logger, r, evt.Report.TaskID); err != nil {
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
