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
// A cancelling run starts nothing: its tasks in flight are stopping, and a
// report of a stopping task's current attempt ends the task with what really
// happened. Queued or running moves only the attempt's facts: the task stays
// stopping and the attempt keeps the next check its stop set. Stopped cancels
// the task, succeeded makes it succeeded and failed fails it, never retried;
// nothing is dispatched and nothing skipped. A stopped report of an open
// attempt state never asked to stop is refused with ErrStopNotRequested
// before anything is written.
//
// A task of a run that writes its nodes holds the write lease on its node from
// its dispatch until the report that ends it: that report releases the lease,
// whether it ends the task succeeded, failed or cancelled; a retry keeps the
// lease, and a stopping task keeps it until its report ends it. Each task a
// success makes ready claims its own node's lease as it is dispatched, as
// Start describes, before the ended task's lease is released.
//
// Every move of a task stamps the run's progress clock. A task that has ended
// counts terminal once, and the run finalizes when every task has ended: a
// cancelling run cancelled, a running one failed if any task failed or was
// cancelled and succeeded otherwise. Dispatching the tasks a success makes
// ready and skipping the tasks a final failure blocks read the run's graph
// only on a running run whose tasks state planned; the next attempt of a
// retryable failure reads its node from the run's release on every running
// run.
//
// A report for a run that has ended changes nothing: every attempt of such a
// run has ended. The boolean result reports whether the report made the task
// succeeded. Returns ErrTaskNotHeld when the run holds no such task (no task
// has the report's id, or another run holds it), before the attempt is read,
// and ErrAttemptNotIssued when the task has no such attempt.
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
	if task.RunID != r.scheduleID {
		return nil, false, fmt.Errorf("%w: task %s belongs to run %s", ErrTaskNotHeld, rep.TaskID, task.RunID)
	}
	a, held, err := tasks.LoadAttempt(ctx, rep.TaskID, rep.Attempt)
	if err != nil {
		return nil, false, fmt.Errorf("load attempt %d: %w", rep.Attempt, err)
	}
	if !held {
		return nil, false, fmt.Errorf("%w: attempt %d of task %s", ErrAttemptNotIssued, rep.Attempt, rep.TaskID)
	}
	stopping := task.Status == TaskStatusStopping && rep.Attempt == task.Attempt
	if rep.Status == AttemptStopped && !a.Status.IsFinal() && !stopping {
		return nil, false, fmt.Errorf("%w: attempt %d of task %s", ErrStopNotRequested, rep.Attempt, rep.TaskID)
	}
	applied, changed := a.Apply(rep)
	if !applied {
		return nil, false, nil
	}
	if stopping && !a.Status.IsFinal() {
		// The attempt is asked about on the stop's schedule, which the cancel
		// and each re-send of it set.
		a.NextCheckIn = 0
	}
	if err := tasks.SaveAttempt(ctx, a); err != nil {
		return nil, false, fmt.Errorf("save attempt %d: %w", a.Number, err)
	}
	if !changed || rep.Attempt != task.Attempt {
		return nil, false, nil
	}

	switch a.Status {
	case AttemptQueued, AttemptRunning:
		if stopping {
			return nil, false, nil
		}
		return nil, false, r.moveTask(ctx, tasks, rep.TaskID, TaskStatus(a.Status), a.Number, now)
	case AttemptSucceeded:
		events, err := r.endTask(ctx, tasks, graph, rep.TaskID, task, TaskStatusSucceeded, d, now)
		return events, err == nil, err
	case AttemptFailed:
		if r.status == SchedulerStatusRunning && Retryable(rep.FailureReason.Class()) && a.Number < task.MaxRetries {
			events, err := r.dispatchNextAttempt(ctx, tasks, graph, rep.TaskID, task, d, now)
			return events, false, err
		}
		events, err := r.endTask(ctx, tasks, graph, rep.TaskID, task, TaskStatusFailed, d, now)
		return events, false, err
	case AttemptStopped:
		events, err := r.endTask(ctx, tasks, graph, rep.TaskID, task, TaskStatusCancelled, d, now)
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

// endTask makes the task final with status and moves the run: on a running
// run a success dispatches what it makes ready, taking their nodes' leases,
// and a failure skips the pending tasks it blocks; then the task's write lease
// on its node is released, and the run finalizes once every task has ended.
// The release follows every lease the report takes, so the report's
// transaction takes all its leases before it deletes one, as a run's start
// does, and cannot wait in a cycle with a concurrent take of the same nodes.
// The run releases a task's lease nowhere else, so a task keeps its lease
// across every retry and while it is stopping, until the report that ends it.
func (r *Run) endTask(ctx context.Context, tasks TaskCollection, graph TaskGraph, taskID uuid.UUID, task TaskAttemptState,
	status TaskStatus, d TaskDefaults, now time.Time) ([]DomainEvent, error) {
	if err := r.moveTask(ctx, tasks, taskID, status, task.Attempt, now); err != nil {
		return nil, err
	}
	r.fillSlot()
	var events []DomainEvent
	if r.plannedAt != nil && r.status == SchedulerStatusRunning {
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
	if err := tasks.ReleaseLease(ctx, taskID); err != nil {
		return nil, fmt.Errorf("release the lease of task %s: %w", taskID, err)
	}
	finalized, err := r.finalizeWhenEveryTaskEnded(ctx, tasks, now)
	if err != nil {
		return nil, err
	}
	return append(events, finalized...), nil
}

// dispatchNextAttempt adds the attempt after the task's current one,
// dispatched and due for its first check, makes the task dispatched at it and
// records its TaskDispatched, built from the node the run's release holds. The
// task keeps the write lease it holds on its node.
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
// is stored and r has not ended: a cancelling run cancelled; a running one
// failed when any task failed or was cancelled, succeeded otherwise.
func (r *Run) finalizeWhenEveryTaskEnded(ctx context.Context, tasks TaskCollection, now time.Time) ([]DomainEvent, error) {
	if r.totalTaskCount == nil || r.terminalTaskCount != *r.totalTaskCount || r.initStatus != InitStatusCompleted {
		return nil, nil
	}
	if r.status == SchedulerStatusCancelling {
		return r.finalize(SchedulerStatusCancelled, now), nil
	}
	if r.status != SchedulerStatusRunning {
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

// StallCheck is what FailStalled did to a run.
type StallCheck struct {
	// Events are what the run recorded: the dispatch of each waiting task it
	// granted its node's lease, or the run's end.
	Events []DomainEvent
	// Granted are the waits it ended by granting the task its node's write
	// lease and dispatching it; a run granted any is not stalled.
	Granted []LeaseWait
	// Stalled reports whether it ended the run.
	Stalled bool
}

// FailStalled ends r when it is active, has made no progress for StallAfter
// and has no work in flight: only a defect in state leaves a run so.
//
// A running run's task that waits for a node lease and is next in line for a
// free node (GrantableWaits: no task holds the node, and the task is its
// oldest waiter among the waiters of running runs) is first granted that
// node's lease and dispatched (GrantLease), as the lease waker would grant it,
// so a node released after the waker's last pass, or one a waker pass left for
// its next, is served under r's lock instead of reading as a stall. A run
// granted any task is not stalled.
//
// Work in flight (HasInFlight) is a task dispatched, queued, running or
// stopping, a task waiting for a node lease whose holder is one of those, and
// a task waiting behind an older waiter of a running run for its node, which
// the node's next grant serves first. A lease wait therefore ends its run only
// when no grant can serve it: its node is held by a task that has ended
// without releasing it, or the run's release holds no such node for the
// tenant, which only a defect in state causes.
//
// A cancelling run ends cancelled, with no failure reason: its cancel asked
// for nothing more. Any other run fails as stalled, its pending tasks skipped
// and counted terminal. A run that has ended, has work in flight or moved
// within StallAfter is left as it is.
func (r *Run) FailStalled(ctx context.Context, tasks TaskCollection, graph TaskGraph, d TaskDefaults, now time.Time) (StallCheck, error) {
	if !r.IsActive() || r.lastHeartbeatAt == nil || now.Sub(*r.lastHeartbeatAt) < StallAfter {
		return StallCheck{}, nil
	}
	events, granted, err := r.grantWaitingTasks(ctx, tasks, graph, d, now)
	if err != nil {
		return StallCheck{}, err
	}
	if len(granted) > 0 {
		return StallCheck{Events: events, Granted: granted}, nil
	}
	inFlight, err := tasks.HasInFlight(ctx, r.scheduleID)
	if err != nil {
		return StallCheck{}, fmt.Errorf("has in flight: %w", err)
	}
	if inFlight {
		return StallCheck{}, nil
	}
	if r.status == SchedulerStatusCancelling {
		return StallCheck{Events: r.finalize(SchedulerStatusCancelled, now), Stalled: true}, nil
	}
	skipped, err := tasks.SkipPending(ctx, r.scheduleID)
	if err != nil {
		return StallCheck{}, fmt.Errorf("skip the pending tasks of stalled run %s: %w", r.scheduleID, err)
	}
	if skipped > 0 {
		r.terminalTaskCount += num.ClampInt32(skipped)
		r.changes.terminalTaskCountDirty = true
	}
	reason := RunFailureStalled
	r.failureReason = &reason
	return StallCheck{Events: r.finalize(SchedulerStatusFailed, now), Stalled: true}, nil
}
