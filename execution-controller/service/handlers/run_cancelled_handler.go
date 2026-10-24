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
// the run that has not ended, waiting for a row a launch holds. A row whose
// Job exists moves to terminating, and its Job is deleted once the
// transaction commits: a starting or running row, or a pending or reserved
// one whose attempt's Job the run's read of the API finds (a launch created
// it and was lost before its commit). That read (ports.RunJobLookup) lists
// the run's Jobs and pods once per cancel, after every row is locked, so no
// launch can create a Job of a locked row after it, and its cost does not
// grow with the number of rows. A pending or reserved row with
// neither a Job nor an unfinished pod ends stopped and is reported stopped;
// one with an unfinished pod but no Job is left for state's next cancel. Each
// attempt the cancel names is answered too: a row
// that has ended has its last report sent again; an attempt with no row but a
// Job has its record rebuilt and moved to terminating; one with neither is
// stored stopped and reported stopped, unless a pod of it has not finished,
// in which case nothing is written for it and state's next cancel names it
// again. Each attempt reported stopped is counted once the transaction
// commits.
type RunCancelledHandler struct {
	runJobs    ports.RunJobLookup
	terminator ports.JobTerminator
	metrics    ports.StopMetrics
	logger     *slog.Logger
	now        func() time.Time
}

// NewRunCancelledHandler constructs the handler; runJobs reads the run's Jobs
// and pods from the Kubernetes API, for the Job of a row that awaits its
// launch and of a named attempt that has no row, and terminator deletes the
// Jobs of the stopped rows.
func NewRunCancelledHandler(runJobs ports.RunJobLookup, terminator ports.JobTerminator,
	metrics ports.StopMetrics, logger *slog.Logger) *RunCancelledHandler {
	return &RunCancelledHandler{runJobs: runJobs, terminator: terminator, metrics: metrics, logger: logger,
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
	snap := &runSnapshot{lookup: h.runJobs, runID: evt.RunID.String()}
	stopped := map[attemptOf]bool{}
	for _, d := range open {
		job, err := h.stopRow(ctx, u, snap, d, now)
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
		job, err := h.stopNamed(ctx, u, snap, cmd, procID, now)
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

// runSnapshot is the run's Jobs and pods, read from the API once, the first
// time a row or a named attempt needs them. It is first read after the run's
// open rows are locked.
type runSnapshot struct {
	lookup ports.RunJobLookup
	runID  string
	read   *model.RunObservation
}

// jobOfAttempt reports whether the read shows a Job carrying cmd's task and
// attempt, and returns what it shows of cmd's Job and pods.
func (s *runSnapshot) jobOfAttempt(ctx context.Context, cmd command.DeployTask) (bool, model.JobObservation, error) {
	if s.read == nil {
		read, err := s.lookup.ObserveRun(ctx, s.runID)
		if err != nil {
			return false, model.JobObservation{}, fmt.Errorf("read the Jobs of run %s: %w", s.runID, err)
		}
		s.read = &read
	}
	obs := s.read.Of(cmd.JobName)
	return obs.RunsAttempt(cmd.TaskID, cmd.Attempt), obs, nil
}

// attemptOf identifies one attempt of a task.
type attemptOf struct {
	taskID  string
	attempt int
}

// stopRow stops d, a locked row of the run, and saves it. A row that awaits
// its launch (pending or reserved) records no Job, yet a launch lost before
// its commit may have created one, so its attempt's Job is looked for first
// in the run's read of the API (snap): a Job that exists is adopted, the row moves
// to terminating and the Job's name is returned for deletion; with neither a
// Job nor an unfinished pod the row ends stopped and is reported stopped; an
// unfinished pod without its Job leaves the row as it is, unsaved, while the
// pod can still run, and state's next cancel stops it. A row whose Job exists
// (starting or running) moves to terminating and its Job's name is returned
// for deletion; one terminating keeps its reason and gains the stop. A row
// that has ended has its last report sent again.
func (h *RunCancelledHandler) stopRow(ctx context.Context, u uow.UnitOfWork, snap *runSnapshot, d *model.Deployment, now time.Time) (string, error) {
	if !d.Status().InFlight() && d.Status() != model.StatusPending {
		if err := reports.Resend(ctx, u.OutboxRepo(), d); err != nil {
			return "", err
		}
		return "", u.DeploymentsRepo().Save(ctx, d)
	}
	var effect model.StopEffect
	var err error
	if d.AwaitsLaunch() {
		job, obs, lookErr := snap.jobOfAttempt(ctx, d.Command())
		if lookErr != nil {
			return "", lookErr
		}
		effect, err = d.StopBeforeLaunch(now, model.DecideFenced(job, obs.HasUnfinishedPod()))
	} else {
		effect, err = d.Stop(now)
	}
	if err != nil {
		return "", err
	}
	cmd := d.Command()
	job := ""
	switch effect {
	case model.StopEnded:
		if err := reports.Write(ctx, u.OutboxRepo(), d, pkgmodel.AttemptStatusStopped, ""); err != nil {
			return "", err
		}
		u.AfterCommit(h.metrics.AttemptStopped)
	case model.StopTerminate:
		job = d.JobName()
	case model.StopAwaitPod:
		h.logger.Error("A row of a cancelled run awaits its launch and has no Job, but a pod of its attempt has not finished — "+
			"nothing is reported until the pod is gone; state asks again",
			"run_id", cmd.ScheduleID, "task_id", cmd.TaskID, "attempt", cmd.Attempt, "job_name", d.JobName(), "status", d.Status())
		return "", nil
	}
	return job, u.DeploymentsRepo().Save(ctx, d)
}

// stopNamed answers an attempt the cancel names that is not among the run's
// open rows. A row of it that has ended has its last report sent again. With
// no row, a Job of the attempt in the run's read of the API (snap) has its
// record rebuilt and moved to terminating, and its name is returned for
// deletion; with neither, the attempt is stored stopped and reported
// stopped, unless a pod of it has not finished (its Job was deleted with
// orphan propagation): nothing is written while that pod can still run. A
// row of the attempt another transaction stores meanwhile is stopped as a
// row of the run.
func (h *RunCancelledHandler) stopNamed(ctx context.Context, u uow.UnitOfWork, snap *runSnapshot, cmd command.DeployTask, procID *uuid.UUID, now time.Time) (string, error) {
	repo := u.DeploymentsRepo()
	held, found, err := repo.LoadByTaskAttempt(ctx, cmd.TaskID, cmd.Attempt)
	if err != nil {
		return "", err
	}
	if found {
		return h.stopRow(ctx, u, snap, held, now)
	}
	job, obs, err := snap.jobOfAttempt(ctx, cmd)
	if err != nil {
		return "", err
	}
	action := model.DecideFenced(job, obs.HasUnfinishedPod())
	var d *model.Deployment
	switch action {
	case model.StopAdopted:
		if d, err = stoppedAdoption(cmd, procID, now); err != nil {
			return "", err
		}
	case model.AwaitFencedPod:
		h.logger.Error("A named attempt of a cancelled run has no record and no Job, but a pod of it has not finished — "+
			"nothing is reported until the pod is gone; state asks again",
			"run_id", cmd.ScheduleID, "task_id", cmd.TaskID, "attempt", cmd.Attempt, "job_name", cmd.JobName)
		return "", nil
	case model.StoreStopped:
		d = model.NewStoppedDeployment(cmd, procID, now)
		d.RecordReport(d.ReportOf(pkgmodel.AttemptStatusStopped, ""))
	default:
		return "", fmt.Errorf("named attempt %d of task %s: no fenced action %d", cmd.Attempt, cmd.TaskID, action)
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
		return h.stopRow(ctx, u, snap, held, now)
	}
	if action == model.StopAdopted {
		h.logger.Info("A named attempt of a cancelled run has no record but its Job exists — the Job is stopped",
			"run_id", cmd.ScheduleID, "task_id", cmd.TaskID, "attempt", cmd.Attempt, "job_name", cmd.JobName)
		return cmd.JobName, nil
	}
	u.AfterCommit(h.metrics.AttemptStopped)
	return "", reports.Resend(ctx, u.OutboxRepo(), d)
}
