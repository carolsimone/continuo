package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// AttemptReportHandler applies one task.status.updated:v2 report in its run's
// transaction: it locks the run, lets the run record the report
// (Run.RecordAttemptReport: the attempt by rank, the task, the next attempt
// of a retryable failure, the dispatch of the tasks a success makes ready, the
// skip of what a final failure blocks, and finalization), saves the run and
// writes the dispatches it recorded to the outbox. When the report makes a
// task of a run state planned with an operation that writes the table
// succeeded, it also records the version the task loaded into the warehouse,
// which the seed reconcile compares with the live release.
type AttemptReportHandler struct {
	tenantID string
	tasks    run.TaskDefaults
	logger   *slog.Logger
}

// NewAttemptReportHandler constructs the handler for the runs of tenantID;
// tasks names the timeout of a dispatch whose node declares none.
func NewAttemptReportHandler(tenantID string, tasks run.TaskDefaults, logger *slog.Logger) *AttemptReportHandler {
	return &AttemptReportHandler{tenantID: tenantID, tasks: tasks, logger: logger}
}

// Handle applies evt inside u's transaction; the caller commits.
// run.ErrTaskNotHeld and run.ErrAttemptNotIssued propagate as they are: a
// report either names can never be applied, so the stream binding marks both
// permanent (permanentWhenNeverValid) and the consumer dead-letters the report
// at once.
func (h *AttemptReportHandler) Handle(ctx context.Context, u uow.UnitOfWork, evt events.AttemptReported, msgProcID uuid.UUID) error {
	r, err := u.Run().LoadRunForUpdate(ctx, evt.RunID)
	if err != nil {
		return fmt.Errorf("load run: %w", err)
	}
	domainEvents, succeeded, err := r.RecordAttemptReport(ctx, u.TaskCollection(), u.TaskGraph(h.tenantID), evt.Report, h.tasks, u.Clock().Now())
	if err != nil {
		return fmt.Errorf("record attempt report: %w", err)
	}
	if err := u.Run().SaveRun(ctx, r); err != nil {
		return fmt.Errorf("save run: %w", err)
	}
	if err := u.Outbox().Append(ctx, domainEvents, msgProcID); err != nil {
		return fmt.Errorf("append outbox: %w", err)
	}
	if succeeded && recordsBuiltVersions(r) {
		if err := recordLoadedVersion(ctx, u, h.tenantID, h.logger, r, evt.Report.TaskID); err != nil {
			return err
		}
	}
	h.logger.Info("task.status.updated: report processed",
		"run_id", evt.RunID, "task_id", evt.Report.TaskID, "attempt", evt.Report.Attempt,
		"status", evt.Report.Status, "failure_reason", evt.Report.FailureReason)
	return nil
}
