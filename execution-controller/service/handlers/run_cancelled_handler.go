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

// RunCancelledHandler fences a run state cancelled and stops the run's work.
// In one transaction it takes the run's lock exclusively, waiting for every
// dispatch of the run that may be storing an attempt, inserts the fence,
// which every gate that starts work reads, then stops each production row of
// the run that has not ended, waiting for a row a launch holds: a row with no
// Job yet (pending or reserved) ends stopped and is reported stopped; a row
// whose Job exists moves to terminating, and its Job is deleted once the
// transaction commits. Each attempt the cancel names is answered too: a row
// that has ended has its last report sent again; an attempt with no row but a
// Job has its record rebuilt and moved to terminating; one with neither is
// stored stopped and reported stopped, unless a pod of it has not finished,
// in which case nothing is written for it and state's next cancel names it
// again. Each attempt reported stopped is counted once the transaction
// commits.
type RunCancelledHandler struct {
	jobs       ports.JobCache
	lookup     ports.JobLookup
	terminator ports.JobTerminator
	metrics    ports.StopMetrics
	logger     *slog.Logger
	now        func() time.Time
}

// NewRunCancelledHandler constructs the handler; jobs is the watch's cache and
// lookup the Kubernetes API behind it, read for the Job of a named attempt
// that has no row, and terminator deletes the Jobs of the stopped rows.
func NewRunCancelledHandler(jobs ports.JobCache, lookup ports.JobLookup, terminator ports.JobTerminator,
	metrics ports.StopMetrics, logger *slog.Logger) *RunCancelledHandler {
	return &RunCancelledHandler{jobs: jobs, lookup: lookup, terminator: terminator, metrics: metrics, logger: logger,
		now: time.Now}
}

// Handle applies one run.cancelled:v1 message inside u's transaction; the
// caller commits, then passes deleteJobs to DeleteJobs. deleteJobs names the
// Job of every row the transaction moved to terminating. msgProcID is the
// dedup row's id, kept on a row the cancel creates for provenance.
func (h *RunCancelledHandler) Handle(ctx context.Context, u uow.UnitOfWork, evt events.RunCancelled, msgProcID uuid.UUID) (deleteJobs []string, err error) {
	fence := u.CancelledRunsRepo()
	if err := fence.HoldRunForFence(ctx, evt.RunID); err != nil {
		return nil, err
	}
	if err := fence.Insert(ctx, evt.RunID); err != nil {
		return nil, fmt.Errorf("fence run %s: %w", evt.RunID, err)
	}
	open, err := u.DeploymentsRepo().LockOpenOfRun(ctx, evt.RunID.String())
	if err != nil {
		return nil, err
	}
	now := h.now()
	stopped := map[attemptOf]bool{}
	for _, d := range open {
		job, err := h.stopRow(ctx, u, d, now)
		if err != nil {
			return nil, err
		}
		if job != "" {
			deleteJobs = append(deleteJobs, job)
		}
		stopped[attemptOf{d.Command().TaskID, d.Command().Attempt}] = true
	}
	var procID *uuid.UUID
	if msgProcID != uuid.Nil {
		procID = &msgProcID
	}
	for _, a := range evt.Attempts {
		if stopped[attemptOf{a.TaskID, a.Attempt}] {
			continue
		}
		cmd := command.DeployTask{TaskID: a.TaskID, ScheduleID: evt.RunID.String(), JobName: a.JobName, Attempt: a.Attempt}
		job, err := h.stopNamed(ctx, u, cmd, procID, now)
		if err != nil {
			return nil, err
		}
		if job != "" {
			deleteJobs = append(deleteJobs, job)
		}
	}
	h.logger.Info("Run cancelled — its work is stopped", "run_id", evt.RunID, "cancelled_by", evt.CancelledBy,
		"reason", evt.Reason, "open_rows", len(open), "named_attempts", len(evt.Attempts), "jobs_to_delete", len(deleteJobs))
	return deleteJobs, nil
}

// DeleteJobs deletes each named Job with its pods, after the transaction
// that moved its row to terminating committed. A failed deletion is logged
// and left to the observer, which deletes the Job of a terminating row again
// on its next observation of it.
func (h *RunCancelledHandler) DeleteJobs(ctx context.Context, names []string) {
	for _, name := range names {
		if err := h.terminator.DeleteJob(ctx, name); err != nil {
			h.logger.Warn("Deleting the Job of a stopped attempt failed — the observer deletes it again",
				"job_name", name, "error", err)
		}
	}
}

// attemptOf identifies one attempt of a task.
type attemptOf struct {
	taskID  string
	attempt int
}

// stopRow stops d, a locked row of the run, and saves it. A row that has not
// ended is stopped: one with no Job ends stopped and is reported stopped, one
// whose Job exists moves to terminating and its Job's name is returned for
// deletion, one terminating keeps its reason and gains the stop. A row that
// has ended has its last report sent again.
func (h *RunCancelledHandler) stopRow(ctx context.Context, u uow.UnitOfWork, d *model.Deployment, now time.Time) (string, error) {
	if !d.Status().InFlight() && d.Status() != model.StatusPending {
		if err := reports.Resend(ctx, u.OutboxRepo(), d); err != nil {
			return "", err
		}
		return "", u.DeploymentsRepo().Save(ctx, d)
	}
	effect, err := d.Stop(now)
	if err != nil {
		return "", err
	}
	job := ""
	switch effect {
	case model.StopEnded:
		if err := reports.Write(ctx, u.OutboxRepo(), d, pkgmodel.AttemptStatusStopped, ""); err != nil {
			return "", err
		}
		u.AfterCommit(h.metrics.AttemptStopped)
	case model.StopTerminate:
		job = d.JobName()
	}
	return job, u.DeploymentsRepo().Save(ctx, d)
}

// stopNamed answers an attempt the cancel names that is not among the run's
// open rows. A row of it that has ended has its last report sent again. With
// no row, a Job of the attempt (the cache, then a live read) has its record
// rebuilt and moved to terminating, and its name is returned for deletion;
// with neither, the attempt is stored stopped and reported stopped, unless a
// pod of it has not finished (its Job was deleted with orphan propagation):
// nothing is written while that pod can still run. A row of the attempt
// another transaction stores meanwhile is stopped as a row of the run.
func (h *RunCancelledHandler) stopNamed(ctx context.Context, u uow.UnitOfWork, cmd command.DeployTask, procID *uuid.UUID, now time.Time) (string, error) {
	repo := u.DeploymentsRepo()
	held, found, err := repo.LoadByTaskAttempt(ctx, cmd.TaskID, cmd.Attempt)
	if err != nil {
		return "", err
	}
	if found {
		return h.stopRow(ctx, u, held, now)
	}
	job, obs, err := jobOfAttempt(ctx, h.jobs, h.lookup, cmd)
	if err != nil {
		return "", err
	}
	if !job && obs.HasUnfinishedPod() {
		h.logger.Error("A named attempt of a cancelled run has no record and no Job, but a pod of it has not finished — "+
			"nothing is reported until the pod is gone; state asks again",
			"run_id", cmd.ScheduleID, "task_id", cmd.TaskID, "attempt", cmd.Attempt, "job_name", cmd.JobName)
		return "", nil
	}
	var d *model.Deployment
	if job {
		d = model.NewAdoptedDeployment(cmd, procID, now)
		if _, err := d.Stop(now); err != nil {
			return "", err
		}
	} else {
		d = model.NewStoppedDeployment(cmd, procID, now)
		d.RecordReport(d.ReportOf(pkgmodel.AttemptStatusStopped, ""))
	}
	added, err := repo.AddAttempt(ctx, d)
	if err != nil {
		return "", fmt.Errorf("add attempt %d of task %s: %w", cmd.Attempt, cmd.TaskID, err)
	}
	if !added {
		held, found, err := repo.LoadByTaskAttempt(ctx, cmd.TaskID, cmd.Attempt)
		if err != nil {
			return "", err
		}
		if !found {
			return "", fmt.Errorf("attempt %d of task %s is stored, yet it cannot be loaded", cmd.Attempt, cmd.TaskID)
		}
		return h.stopRow(ctx, u, held, now)
	}
	if job {
		h.logger.Info("A named attempt of a cancelled run has no record but its Job exists — the Job is stopped",
			"run_id", cmd.ScheduleID, "task_id", cmd.TaskID, "attempt", cmd.Attempt, "job_name", cmd.JobName)
		return cmd.JobName, nil
	}
	u.AfterCommit(h.metrics.AttemptStopped)
	return "", reports.Resend(ctx, u.OutboxRepo(), d)
}
