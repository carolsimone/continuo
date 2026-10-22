package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/events"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/carolsimone/continuo/execution-controller/service/reports"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// TaskDispatchHandler accepts each attempt of a task once. A dispatch of an
// attempt it holds is answered with the attempt's last report; a dispatch of
// an attempt older than one it holds creates nothing; a dispatch that cannot
// run is recorded failed; a dispatch whose Job exists without a record has
// its record rebuilt; a new dispatch state still knows as dispatched is
// queued for its Job; and an attempt state knows as acknowledged that it
// holds no trace of is reported attempt_lost. Dispatches of a cancelled run
// are dropped.
type TaskDispatchHandler struct {
	jobs   ports.JobCache
	logger *slog.Logger
	now    func() time.Time
}

// NewTaskDispatchHandler constructs the handler; jobs is the watch's cache,
// read for a Job of the attempt.
func NewTaskDispatchHandler(jobs ports.JobCache, logger *slog.Logger) *TaskDispatchHandler {
	return &TaskDispatchHandler{jobs: jobs, logger: logger, now: time.Now}
}

// Handle applies one task.dispatch:v1 message inside u's transaction; the
// caller commits. msgProcID is the dedup row's id, kept on the deployment for
// provenance.
func (h *TaskDispatchHandler) Handle(ctx context.Context, u uow.UnitOfWork, evt events.TaskDispatch, msgProcID uuid.UUID) error {
	cancelled, err := u.CancelledSchedulesRepo().Exists(ctx, evt.RunID)
	if err != nil {
		return fmt.Errorf("cancelled-schedule check: %w", err)
	}
	if cancelled {
		h.logger.Info("run cancelled — dropping task.dispatch", "run_id", evt.RunID, "task_id", evt.TaskID, "attempt", evt.Attempt)
		return nil
	}
	cmd := evt.Command()
	repo := u.DeploymentsRepo()
	held, err := repo.LoadByTaskAttempt(ctx, cmd.TaskID, cmd.Attempt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	laterHeld := false
	if held == nil {
		if laterHeld, err = repo.HasAttemptAfter(ctx, cmd.TaskID, cmd.Attempt); err != nil {
			return err
		}
	}
	problem, message := model.DispatchProblem(cmd, evt.KnownStatus)
	jobOfAttempt := h.jobs.Observe(cmd.JobName).RunsAttempt(cmd.TaskID, cmd.Attempt)
	var procID *uuid.UUID
	if msgProcID != uuid.Nil {
		procID = &msgProcID
	}
	now := h.now()

	action := model.DecideDispatch(held != nil, laterHeld, problem, jobOfAttempt, evt.KnownStatus)
	switch action {
	case model.ResendReport:
		if err := reports.Resend(ctx, u.OutboxRepo(), held); err != nil {
			return err
		}
		return repo.Save(ctx, held)
	case model.IgnoreOlder:
		h.logger.Info("A later attempt of the task is held — the dispatch creates nothing",
			"task_id", cmd.TaskID, "attempt", cmd.Attempt)
		return nil
	case model.RefuseInvalid:
		return h.add(ctx, u, model.NewRefusedDeployment(cmd, procID, problem, message, now), model.ReportFailed, message)
	case model.AdoptJob:
		h.logger.Warn("No record of the attempt, but its Job exists — the record is rebuilt from it",
			"task_id", cmd.TaskID, "attempt", cmd.Attempt, "job_name", cmd.JobName)
		return h.add(ctx, u, model.NewAdoptedDeployment(cmd, procID, now), model.ReportQueued, "")
	case model.CreateAttempt:
		return h.add(ctx, u, model.NewDeployment(cmd, procID, now), model.ReportQueued, "")
	case model.ReportLost:
		message := fmt.Sprintf("state knows attempt %d as %s, and execution-controller holds neither its record nor its Job",
			cmd.Attempt, evt.KnownStatus)
		h.logger.Error("An acknowledged attempt is lost", "task_id", cmd.TaskID, "attempt", cmd.Attempt, "known_status", evt.KnownStatus)
		return h.add(ctx, u, model.NewRefusedDeployment(cmd, procID, pkgmodel.FailureReasonAttemptLost, message, now), model.ReportFailed, message)
	}
	return fmt.Errorf("dispatch of attempt %d of task %s: no action %d", cmd.Attempt, cmd.TaskID, action)
}

// add stores d, the record of a new attempt, with its first report, and
// writes the report. A record of the attempt stored meanwhile (another
// replica's delivery of the same attempt) is answered instead.
func (h *TaskDispatchHandler) add(ctx context.Context, u uow.UnitOfWork, d *model.Deployment, status model.ReportStatus, message string) error {
	d.RecordReport(d.ReportOf(status, message))
	added, err := u.DeploymentsRepo().AddAttempt(ctx, d)
	if err != nil {
		return fmt.Errorf("add attempt %d of task %s: %w", d.Command().Attempt, d.Command().TaskID, err)
	}
	if !added {
		held, err := u.DeploymentsRepo().LoadByTaskAttempt(ctx, d.Command().TaskID, d.Command().Attempt)
		if err != nil {
			return err
		}
		return reports.Resend(ctx, u.OutboxRepo(), held)
	}
	return reports.Resend(ctx, u.OutboxRepo(), d)
}
