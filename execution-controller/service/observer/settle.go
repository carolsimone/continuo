package observer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/outcomes"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/num"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// settle records how an attempt ended on its deployment, which releases the
// slot, and writes the result. A release leg's node settles through
// outcomes.Recorder. A production task is reported SUCCEEDED or FAILED with
// its execution record, unless its run was cancelled; a failure with retries
// left queues the task's next attempt in the same transaction.
func (o *Observer) settle(ctx context.Context, u uow.UnitOfWork, d *model.Deployment, v model.Verdict, now time.Time) error {
	if d.Mode() != model.ModeProduction {
		return o.settleCandidate(ctx, u, d, v)
	}
	outcome := "failed"
	if v.Succeeded {
		outcome = "ok"
	}
	if err := d.Finish(outcome, now); err != nil {
		return fmt.Errorf("finish deployment %s: %w", d.ID(), err)
	}
	if err := u.DeploymentsRepo().Save(ctx, d); err != nil {
		return err
	}
	cancelled, err := o.cancelled(ctx, u, d)
	if err != nil || cancelled {
		return err
	}
	cmd := d.Command()
	status := "SUCCEEDED"
	if !v.Succeeded {
		status = "FAILED"
	}
	if err := writeTaskStatus(ctx, u.OutboxRepo(), cmd, status); err != nil {
		return err
	}
	if err := o.writeExecution(ctx, u.OutboxRepo(), d, v); err != nil {
		return err
	}
	o.deps.Logger.Info("Attempt ended", "task_id", cmd.TaskID, "job_name", cmd.JobName, "status", status,
		"failure_class", d.Facts().Class(), "failure_reason", d.Facts().Reason)
	if v.Succeeded || cmd.TaskRetryCount >= retryBudget(cmd.TaskMaxRetries) {
		return nil
	}
	next, err := cmd.NextAttempt()
	if err != nil {
		return err
	}
	// An older version may have queued the next attempt when it reported this
	// failure; a task never gets two.
	if _, err := u.DeploymentsRepo().LoadByJobName(ctx, next.JobName); err == nil {
		o.deps.Logger.Info("Next attempt already queued", "task_id", cmd.TaskID, "job_name", next.JobName)
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := u.DeploymentsRepo().Add(ctx, model.NewDeployment(next, nil, now)); err != nil {
		return fmt.Errorf("queue the next attempt of task %s: %w", cmd.TaskID, err)
	}
	return nil
}

// settleCandidate records a release leg's node outcome. The deployment's
// facts are saved first, so outcomes.Recorder, which loads the row afresh
// within this transaction, keeps them.
func (o *Observer) settleCandidate(ctx context.Context, u uow.UnitOfWork, d *model.Deployment, v model.Verdict) error {
	if err := u.DeploymentsRepo().Save(ctx, d); err != nil {
		return err
	}
	outcome := "failed"
	if v.Succeeded {
		outcome = "ok"
	}
	vc, f := d.ValidationCommand(), d.Facts()
	in := outcomes.NodeOutcome{Mode: d.Mode(), ReleaseID: vc.ReleaseID, NodeID: vc.NodeID, Outcome: outcome,
		DBTLogURI: f.LogKey, RunResultsURI: f.RunResultsKey}
	if d.Mode() == model.ModeCompile {
		in.FailedContainer = f.FailedContainer
	}
	return o.deps.Outcomes.Record(ctx, u, in)
}

// reportRunning announces that a production task's main container started,
// unless its run was cancelled.
func (o *Observer) reportRunning(ctx context.Context, u uow.UnitOfWork, d *model.Deployment) error {
	if d.Mode() != model.ModeProduction {
		return nil
	}
	cancelled, err := o.cancelled(ctx, u, d)
	if err != nil || cancelled {
		return err
	}
	return writeTaskStatus(ctx, u.OutboxRepo(), d.Command(), "RUNNING")
}

// cancelled reports whether d's run was cancelled; its attempt is then
// observed to its end without announcing anything.
func (o *Observer) cancelled(ctx context.Context, u uow.UnitOfWork, d *model.Deployment) (bool, error) {
	runID, err := uuid.Parse(d.Command().ScheduleID)
	if err != nil {
		return false, fmt.Errorf("deployment %s: schedule_id %q: %w", d.ID(), d.Command().ScheduleID, err)
	}
	cancelled, err := u.CancelledSchedulesRepo().Exists(ctx, runID)
	if err != nil {
		return false, fmt.Errorf("cancelled schedules check: %w", err)
	}
	return cancelled, nil
}

// retryBudget is a task's retry budget, or pkgevents.DefaultTaskMaxRetries
// when its dispatch carried none.
func retryBudget(maxRetries int) int {
	if maxRetries <= 0 {
		return int(pkgevents.DefaultTaskMaxRetries)
	}
	return maxRetries
}

// writeTaskStatus writes a task_status_updated row for the attempt cmd runs,
// stamped with its retry count.
func writeTaskStatus(ctx context.Context, repo pkgoutbox.Repository, cmd command.DeployTask, status string) error {
	taskID, err := uuid.Parse(cmd.TaskID)
	if err != nil {
		return fmt.Errorf("task_id %q: %w", cmd.TaskID, err)
	}
	retryCount, err := num.Int32(cmd.TaskRetryCount, "task_retry_count")
	if err != nil {
		return err
	}
	payload, err := json.Marshal(pkgevents.TaskStatusUpdated{
		TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, Status: status, RetryCount: retryCount,
	})
	if err != nil {
		return fmt.Errorf("marshal task_status_updated: %w", err)
	}
	if err := repo.Create(ctx, &pkgoutbox.Entry{AggregateType: "task", AggregateID: taskID,
		EventType: event.EventTypeTaskStatusUpdated, Payload: payload, StreamName: streams.TaskStatusUpdatedV1}); err != nil {
		return fmt.Errorf("task_status_updated %s: %w", status, err)
	}
	return nil
}

// writeExecution writes the attempt's task_execution_recorded row. Its
// execution id is the deployment's id, so a repeated record is recognised as
// the same execution.
func (o *Observer) writeExecution(ctx context.Context, repo pkgoutbox.Repository, d *model.Deployment, v model.Verdict) error {
	cmd, f := d.Command(), d.Facts()
	taskID, err := uuid.Parse(cmd.TaskID)
	if err != nil {
		return fmt.Errorf("task_id %q: %w", cmd.TaskID, err)
	}
	rec := pkgevents.TaskExecutionRecorded{
		ExecutionID: d.ID().String(), TaskID: cmd.TaskID, JobName: cmd.JobName,
		LogS3Key: f.LogKey, RunResultsURI: f.RunResultsKey,
		ParseCache: f.ParseCache, ParseCacheReason: f.ParseCacheReason,
	}
	if f.StartedAt != nil {
		rec.StartedAt = f.StartedAt.UTC().Format(time.RFC3339)
	}
	if f.FinishedAt != nil {
		rec.CompletedAt = f.FinishedAt.UTC().Format(time.RFC3339)
		if f.StartedAt != nil {
			rec.ExecutionSeconds = f.FinishedAt.Sub(*f.StartedAt).Seconds()
		}
	}
	if !v.Succeeded {
		rec.ErrorMessage = o.errorMessage(f)
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal task_execution_recorded: %w", err)
	}
	if err := repo.Create(ctx, &pkgoutbox.Entry{AggregateType: "task", AggregateID: taskID,
		EventType: event.EventTypeTaskExecutionRecorded, Payload: payload, StreamName: streams.TaskExecutionRecordedV1}); err != nil {
		return fmt.Errorf("task_execution_recorded: %w", err)
	}
	return nil
}

// errorMessage is the message a failed attempt reports: the log's (its result
// block's message, else its tail), else what Kubernetes said.
func (o *Observer) errorMessage(f model.AttemptFacts) string {
	if f.LogMessage != "" {
		return f.LogMessage
	}
	return o.truncate(f.Message)
}

// truncate cuts msg to ErrorMessageMaxLen.
func (o *Observer) truncate(msg string) string {
	if o.cfg.ErrorMessageMaxLen > 0 && len(msg) > o.cfg.ErrorMessageMaxLen {
		return msg[:o.cfg.ErrorMessageMaxLen] + "...[truncated]"
	}
	return msg
}
