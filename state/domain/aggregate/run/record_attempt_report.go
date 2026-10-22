package run

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/google/uuid"
)

// RecordAttemptReport applies one report of execution-controller to the
// attempt it names and decides what the run does next, in the transaction
// that locked r. It locks the task row, then the attempt row.
//
// The attempt takes the report by rank (Attempt.Apply): a report that ranks
// higher moves its status, one of equal rank on an attempt that has not ended
// moves only its facts and its next check, and anything else changes nothing.
// A report for an attempt older than the task's current one updates only that
// attempt's row. A report that moves the task's current attempt:
//   - queued or running makes the task queued or running;
//   - succeeded makes the task succeeded and dispatches attempt 0 of each task
//     the success makes ready;
//   - failed, when the failure's class is Retryable and the attempt is below
//     the task's budget, adds the next attempt, dispatched and due for its
//     first check, makes the task dispatched at that attempt and records its
//     TaskDispatched, with the timeout the run's release gives the node;
//     otherwise the task fails and the pending tasks below it are skipped.
//
// Every move of a task stamps the run's progress clock. A task that has ended
// counts terminal once, and the run finalizes when every task has ended:
// failed if any failed or was cancelled, succeeded otherwise. Dispatching the
// tasks a success makes ready and skipping the tasks a final failure blocks
// read the run's graph only on a run whose tasks state planned; the next
// attempt of a retryable failure reads its node from the run's release on
// every run.
//
// A report for a run that has ended changes nothing: every attempt of such a
// run has ended. The boolean result reports whether the report made the task
// succeeded. Returns ErrTaskNotHeld when the run holds no such task, and
// ErrAttemptNotIssued when the task has no such attempt.
func (r *Run) RecordAttemptReport(
	ctx context.Context,
	tasks TaskCollection,
	graph TaskGraph,
	rep AttemptReport,
	d TaskDefaults,
	now time.Time,
) ([]DomainEvent, bool, error) {
	if r.IsTerminal() {
		return nil, false, nil
	}
	task, exists, err := tasks.LoadAttemptState(ctx, rep.TaskID)
	if err != nil {
		return nil, false, fmt.Errorf("load task attempt: %w", err)
	}
	if !exists {
		return nil, false, ErrTaskNotHeld
	}
	a, held, err := tasks.LoadAttempt(ctx, rep.TaskID, rep.Attempt)
	if err != nil {
		return nil, false, fmt.Errorf("load attempt %d: %w", rep.Attempt, err)
	}
	if !held {
		return nil, false, fmt.Errorf("%w: attempt %d of task %s", ErrAttemptNotIssued, rep.Attempt, rep.TaskID)
	}
	applied, changed := a.Apply(rep)
	if !applied {
		return nil, false, nil
	}
	if err := tasks.SaveAttempt(ctx, a); err != nil {
		return nil, false, fmt.Errorf("save attempt %d: %w", a.Number, err)
	}
	if !changed || rep.Attempt != task.Attempt {
		return nil, false, nil
	}

	switch a.Status {
	case AttemptQueued, AttemptRunning:
		return nil, false, r.moveTask(ctx, tasks, rep.TaskID, TaskStatus(a.Status), a.Number, now)
	case AttemptSucceeded:
		events, err := r.endTask(ctx, tasks, graph, rep.TaskID, task, TaskStatusSucceeded, d, now)
		return events, err == nil, err
	case AttemptFailed:
		if Retryable(rep.FailureReason.Class()) && a.Number < task.MaxRetries {
			events, err := r.dispatchNextAttempt(ctx, tasks, graph, rep.TaskID, task, d, now)
			return events, false, err
		}
		events, err := r.endTask(ctx, tasks, graph, rep.TaskID, task, TaskStatusFailed, d, now)
		return events, false, err
	}
	return nil, false, nil
}

// moveTask writes status and attempt n on the task and stamps r's progress
// clock.
func (r *Run) moveTask(ctx context.Context, tasks TaskCollection, taskID uuid.UUID, status TaskStatus, n int32, now time.Time) error {
	if _, err := tasks.SetStatusAndAttempt(ctx, taskID, status, n); err != nil {
		return fmt.Errorf("set task status: %w", err)
	}
	r.recordProgress(now)
	return nil
}

// endTask makes the task final with status and moves the run: a success
// dispatches what it makes ready, a failure skips the pending tasks it blocks,
// and the run finalizes once every task has ended.
func (r *Run) endTask(ctx context.Context, tasks TaskCollection, graph TaskGraph, taskID uuid.UUID, task TaskAttemptState,
	status TaskStatus, d TaskDefaults, now time.Time) ([]DomainEvent, error) {
	if err := r.moveTask(ctx, tasks, taskID, status, task.Attempt, now); err != nil {
		return nil, err
	}
	r.fillSlot()
	var events []DomainEvent
	if r.plannedAt != nil {
		switch status {
		case TaskStatusSucceeded:
			dispatched, err := r.dispatchReady(ctx, tasks, graph, task.NodeKey, d, now)
			if err != nil {
				return nil, err
			}
			events = dispatched
		case TaskStatusFailed:
			if err := r.skipBlocked(ctx, tasks, graph, task.NodeKey); err != nil {
				return nil, err
			}
		}
	}
	finalized, err := r.finalizeWhenEveryTaskEnded(ctx, tasks, now)
	if err != nil {
		return nil, err
	}
	return append(events, finalized...), nil
}

// dispatchNextAttempt adds the attempt after the task's current one,
// dispatched and due for its first check, makes the task dispatched at it and
// records its TaskDispatched, built from the node the run's release holds.
func (r *Run) dispatchNextAttempt(ctx context.Context, tasks TaskCollection, graph TaskGraph, taskID uuid.UUID,
	task TaskAttemptState, d TaskDefaults, now time.Time) ([]DomainEvent, error) {
	node, found, err := graph.Node(ctx, r.release.ReleaseID, task.NodeKey)
	if err != nil {
		return nil, fmt.Errorf("read node %s of release %s: %w", task.NodeKey, r.release.ReleaseID, err)
	}
	if !found {
		return nil, fmt.Errorf("retry task %s: release %s holds no node %s", taskID, r.release.ReleaseID, task.NodeKey)
	}
	next := task.Attempt + 1
	jobName, err := domain.ComputeJobName(task.ServiceName, task.SchemaName, task.TableName, r.scheduleID.String(), int(next))
	if err != nil {
		return nil, fmt.Errorf("name attempt %d of task %s: %w", next, taskID, err)
	}
	if err := tasks.AddAttempts(ctx, []Attempt{{TaskID: taskID, Number: next, JobName: jobName,
		Status: AttemptDispatched, NextCheckIn: NextCheckAfter(AttemptDispatched, 0)}}); err != nil {
		return nil, fmt.Errorf("add attempt %d of task %s: %w", next, taskID, err)
	}
	if err := r.moveTask(ctx, tasks, taskID, TaskStatusDispatched, next, now); err != nil {
		return nil, err
	}
	return []DomainEvent{r.dispatchEvent(taskID, next, node, jobName, d)}, nil
}

// finalizeWhenEveryTaskEnded finalizes r when every task has ended, the plan
// is stored and r is still running: failed when any task failed or was
// cancelled, succeeded otherwise.
func (r *Run) finalizeWhenEveryTaskEnded(ctx context.Context, tasks TaskCollection, now time.Time) ([]DomainEvent, error) {
	if r.totalTaskCount == nil || r.terminalTaskCount != *r.totalTaskCount ||
		r.initStatus != InitStatusCompleted || r.status != SchedulerStatusRunning {
		return nil, nil
	}
	anyFailed, err := tasks.HasFailed(ctx, r.scheduleID)
	if err != nil {
		return nil, fmt.Errorf("has failed: %w", err)
	}
	outcome := SchedulerStatusSucceeded
	if anyFailed {
		outcome = SchedulerStatusFailed
	}
	return r.finalize(outcome, now), nil
}

// FailStalled fails r as stalled when it is active, has no attempt in flight
// and has made no progress for StallAfter: only a defect in state leaves a run
// so. Its pending tasks are skipped and count terminal. It reports whether it
// failed the run; a run that has ended, has an attempt in flight or moved
// within StallAfter is left as it is.
func (r *Run) FailStalled(ctx context.Context, tasks TaskCollection, now time.Time) ([]DomainEvent, bool, error) {
	if !r.IsActive() || r.lastHeartbeatAt == nil || now.Sub(*r.lastHeartbeatAt) < StallAfter {
		return nil, false, nil
	}
	inFlight, err := tasks.HasInFlight(ctx, r.scheduleID)
	if err != nil {
		return nil, false, fmt.Errorf("has in flight: %w", err)
	}
	if inFlight {
		return nil, false, nil
	}
	skipped, err := tasks.SkipPending(ctx, r.scheduleID)
	if err != nil {
		return nil, false, fmt.Errorf("skip the pending tasks of stalled run %s: %w", r.scheduleID, err)
	}
	if skipped > 0 {
		r.terminalTaskCount += num.ClampInt32(skipped)
		r.changes.terminalTaskCountDirty = true
	}
	reason := RunFailureStalled
	r.failureReason = &reason
	return r.finalize(SchedulerStatusFailed, now), true, nil
}
