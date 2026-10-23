package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
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
// holds no trace of, neither a record nor a Job nor a pod the cache or a live
// read of the API shows, is reported attempt_lost. A dispatch of a fenced run
// (one state cancelled) that is not held creates no Job: the attempt is
// stored stopped, or, when its Job exists, adopted to be stopped. A dispatch
// that may store an attempt takes its run's lock shared before it reads the
// fence, so a fence being written is waited for and met, and a fence written
// after sees the attempt. Each attempt stored stopped is counted once the
// transaction commits.
type TaskDispatchHandler struct {
	jobs    ports.JobCache
	lookup  ports.JobLookup
	metrics ports.StopMetrics
	logger  *slog.Logger
	now     func() time.Time
}

// NewTaskDispatchHandler constructs the handler; jobs is the watch's cache,
// read for a Job of the attempt, and lookup reads the Job from the Kubernetes
// API before an attempt is reported lost.
func NewTaskDispatchHandler(jobs ports.JobCache, lookup ports.JobLookup, metrics ports.StopMetrics, logger *slog.Logger) *TaskDispatchHandler {
	return &TaskDispatchHandler{jobs: jobs, lookup: lookup, metrics: metrics, logger: logger, now: time.Now}
}

// Handle applies one task.dispatch:v1 message inside u's transaction; the
// caller commits. msgProcID is the dedup row's id, kept on the deployment for
// provenance.
func (h *TaskDispatchHandler) Handle(ctx context.Context, u uow.UnitOfWork, evt events.TaskDispatch, msgProcID uuid.UUID) error {
	cmd := evt.Command()
	repo := u.DeploymentsRepo()
	held, found, err := repo.LoadByTaskAttempt(ctx, cmd.TaskID, cmd.Attempt)
	if err != nil {
		return err
	}
	laterHeld := false
	if !found {
		if laterHeld, err = repo.HasAttemptAfter(ctx, cmd.TaskID, cmd.Attempt); err != nil {
			return err
		}
	}
	var procID *uuid.UUID
	if msgProcID != uuid.Nil {
		procID = &msgProcID
	}
	now := h.now()
	if !found && !laterHeld {
		fence := u.CancelledRunsRepo()
		if err := fence.HoldRunForDispatch(ctx, evt.RunID); err != nil {
			return err
		}
		fenced, err := fence.Exists(ctx, evt.RunID)
		if err != nil {
			return fmt.Errorf("read the fence of run %s: %w", evt.RunID, err)
		}
		if fenced {
			return h.acceptFenced(ctx, u, cmd, procID, now)
		}
	}
	problem, message := model.DispatchProblem(cmd, evt.KnownStatus)
	cached := h.jobs.Observe(cmd.JobName)

	action := model.DecideDispatch(found, laterHeld, problem, cached.RunsAttempt(cmd.TaskID, cmd.Attempt),
		cached.HasUnfinishedPod(), evt.KnownStatus)
	if action == model.ReportLost {
		// The cache can trail the API, so a Job or a pod it does not hold may
		// still run: the attempt is reported lost only once a live read finds
		// neither. A read that fails leaves the dispatch unanswered, and its
		// message is delivered again.
		live, err := h.lookup.ObserveLive(ctx, cmd.JobName)
		if err != nil {
			return fmt.Errorf("read Job %s of attempt %d of task %s: %w", cmd.JobName, cmd.Attempt, cmd.TaskID, err)
		}
		action = model.DecideDispatch(found, laterHeld, problem, live.RunsAttempt(cmd.TaskID, cmd.Attempt),
			live.HasUnfinishedPod(), evt.KnownStatus)
	}
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
		return h.add(ctx, u, model.NewRefusedDeployment(cmd, procID, problem, message, now), pkgmodel.AttemptStatusFailed, message)
	case model.AdoptJob:
		h.logger.Warn("No record of the attempt, but its Job exists — the record is rebuilt from it",
			"task_id", cmd.TaskID, "attempt", cmd.Attempt, "job_name", cmd.JobName)
		return h.add(ctx, u, model.NewAdoptedDeployment(cmd, procID, now), pkgmodel.AttemptStatusQueued, "")
	case model.CreateAttempt:
		return h.add(ctx, u, model.NewDeployment(cmd, procID, now), pkgmodel.AttemptStatusQueued, "")
	case model.AwaitOrphanPod:
		h.logger.Error("An acknowledged attempt has no record and no Job, but a pod of it has not finished — "+
			"nothing is reported until the pod is gone; state asks again",
			"task_id", cmd.TaskID, "attempt", cmd.Attempt, "job_name", cmd.JobName, "known_status", evt.KnownStatus)
		return nil
	case model.ReportLost:
		message := fmt.Sprintf("state knows attempt %d as %s, and execution-controller holds neither its record nor its Job",
			cmd.Attempt, evt.KnownStatus)
		h.logger.Error("An acknowledged attempt is lost", "task_id", cmd.TaskID, "attempt", cmd.Attempt, "known_status", evt.KnownStatus)
		return h.add(ctx, u, model.NewRefusedDeployment(cmd, procID, pkgmodel.FailureReasonAttemptLost, message, now), pkgmodel.AttemptStatusFailed, message)
	}
	return fmt.Errorf("dispatch of attempt %d of task %s: no action %d", cmd.Attempt, cmd.TaskID, action)
}

// acceptFenced answers a dispatch of a fenced run that holds no record of the
// attempt, and creates no Job for it. When a Job of the attempt exists (the
// cache, then a live read), its record is rebuilt and moved to terminating
// with the stop recorded: the observer deletes the Job and reports the
// attempt stopped once its pods are gone. With no Job but a pod of the
// attempt that has not finished (its Job was deleted with orphan
// propagation), nothing is written while the pod can still run; state's next
// cancel names the attempt again. Otherwise the attempt is stored done with
// the outcome stopped and reported stopped.
func (h *TaskDispatchHandler) acceptFenced(ctx context.Context, u uow.UnitOfWork, cmd command.DeployTask, procID *uuid.UUID, now time.Time) error {
	job, obs, err := jobOfAttempt(ctx, h.jobs, h.lookup, cmd)
	if err != nil {
		return err
	}
	if job {
		d := model.NewAdoptedDeployment(cmd, procID, now)
		if _, err := d.Stop(now); err != nil {
			return err
		}
		h.logger.Info("A dispatch of a cancelled run found its Job — the Job is stopped",
			"run_id", cmd.ScheduleID, "task_id", cmd.TaskID, "attempt", cmd.Attempt, "job_name", cmd.JobName)
		return h.add(ctx, u, d, pkgmodel.AttemptStatusQueued, "")
	}
	if obs.HasUnfinishedPod() {
		h.logger.Error("A dispatch of a cancelled run has no record and no Job, but a pod of it has not finished — "+
			"nothing is reported until the pod is gone; state asks again",
			"run_id", cmd.ScheduleID, "task_id", cmd.TaskID, "attempt", cmd.Attempt, "job_name", cmd.JobName)
		return nil
	}
	h.logger.Info("A dispatch of a cancelled run is stored stopped and creates no Job",
		"run_id", cmd.ScheduleID, "task_id", cmd.TaskID, "attempt", cmd.Attempt)
	return h.add(ctx, u, model.NewStoppedDeployment(cmd, procID, now), pkgmodel.AttemptStatusStopped, "")
}

// jobOfAttempt reports whether a Job carrying cmd's task and attempt exists:
// the cache first, then, since the cache can trail the API, a live read. It
// returns the observation that decided: the cache's when it holds the Job,
// otherwise the live read's, whose pods are listed even when the Job is gone.
func jobOfAttempt(ctx context.Context, jobs ports.JobCache, lookup ports.JobLookup, cmd command.DeployTask) (bool, model.JobObservation, error) {
	if cached := jobs.Observe(cmd.JobName); cached.RunsAttempt(cmd.TaskID, cmd.Attempt) {
		return true, cached, nil
	}
	live, err := lookup.ObserveLive(ctx, cmd.JobName)
	if err != nil {
		return false, model.JobObservation{}, fmt.Errorf("read Job %s of attempt %d of task %s: %w", cmd.JobName, cmd.Attempt, cmd.TaskID, err)
	}
	return live.RunsAttempt(cmd.TaskID, cmd.Attempt), live, nil
}

// add stores d, the record of a new attempt, with its first report, and
// writes the report; a first report of stopped is counted once the
// transaction commits. A record of the attempt stored meanwhile (another
// replica's delivery of the same attempt) is answered instead.
func (h *TaskDispatchHandler) add(ctx context.Context, u uow.UnitOfWork, d *model.Deployment, status pkgmodel.AttemptStatus, message string) error {
	d.RecordReport(d.ReportOf(status, message))
	added, err := u.DeploymentsRepo().AddAttempt(ctx, d)
	if err != nil {
		return fmt.Errorf("add attempt %d of task %s: %w", d.Command().Attempt, d.Command().TaskID, err)
	}
	if !added {
		held, found, err := u.DeploymentsRepo().LoadByTaskAttempt(ctx, d.Command().TaskID, d.Command().Attempt)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("attempt %d of task %s is stored, yet it cannot be loaded", d.Command().Attempt, d.Command().TaskID)
		}
		return reports.Resend(ctx, u.OutboxRepo(), held)
	}
	if status == pkgmodel.AttemptStatusStopped {
		u.AfterCommit(h.metrics.AttemptStopped)
	}
	return reports.Resend(ctx, u.OutboxRepo(), d)
}
