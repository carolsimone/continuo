package run

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// LeaseClaim is a task's claim on the write lease of the node it writes: the
// task, the run that holds it, and the node. At most one task holds a node's
// lease, from the dispatch of its attempt 0 until the task ends, so two runs
// never write one node at once.
type LeaseClaim struct {
	TaskID  uuid.UUID
	RunID   uuid.UUID
	NodeKey topology.NodeKey
}

// LeaseWait is a task waiting for the write lease on its node: the task, the
// node, and when it began to wait.
type LeaseWait struct {
	TaskID       uuid.UUID
	NodeKey      topology.NodeKey
	WaitingSince time.Time
}

// writes reports whether r's tasks write their nodes: every operation but test
// does, so each task of r holds its node's write lease while it runs. A
// promote-seed run, which names no operation, writes.
func (r *Run) writes() bool { return r.operation != model.OperationTest }

// takeLeases claims the write lease on the node of each ready task of r and
// returns, in their given order, the tasks that hold their node's lease
// (TakeLeases). Every other task waits for its node's lease (WaitForLeases)
// and is not dispatched.
func (r *Run) takeLeases(ctx context.Context, tasks TaskCollection, ready []readyTask) ([]readyTask, error) {
	claims := make([]LeaseClaim, 0, len(ready))
	for _, t := range ready {
		claims = append(claims, LeaseClaim{TaskID: t.taskID, RunID: r.runID, NodeKey: t.node.Key})
	}
	taken, err := tasks.TakeLeases(ctx, claims)
	if err != nil {
		return nil, fmt.Errorf("take the leases of %d ready tasks of run %s: %w", len(claims), r.runID, err)
	}
	holds := make(map[uuid.UUID]bool, len(taken))
	for _, id := range taken {
		holds[id] = true
	}
	leased := make([]readyTask, 0, len(taken))
	var waiting []uuid.UUID
	for _, t := range ready {
		if holds[t.taskID] {
			leased = append(leased, t)
		} else {
			waiting = append(waiting, t.taskID)
		}
	}
	if len(waiting) > 0 {
		if err := tasks.WaitForLeases(ctx, waiting); err != nil {
			return nil, fmt.Errorf("make %d ready tasks of run %s wait for their leases: %w", len(waiting), r.runID, err)
		}
	}
	return leased, nil
}

// GrantLease gives the task taskID of r, which waits for its node's write
// lease, that lease and dispatches it, in the transaction that locked r. It
// acts only on a running run, and only while the task, whose row it locks
// (LoadAttemptState), is still pending, undispatched and waiting for its
// node's lease. It takes the lease for the task as the waiter being served
// (TakeLeaseForWaiter); once the task holds it, the task is made dispatched at
// now, which ends its wait, its attempt 0 is stored dispatched and due for its
// first check, a TaskDispatched of attempt 0 is recorded with the timeout r's
// release gives its node, and r's progress clock is stamped.
//
// The boolean result reports whether it dispatched the task. A run that is
// not running is left before the task is read, and a task that no longer
// waits, or whose node another task still holds, is left as it is. Returns
// ErrTaskNotHeld when r holds no such task, and fails, so the caller rolls the
// grant back, when r's release holds no such node or the task it took the
// lease for is not made dispatched.
func (r *Run) GrantLease(
	ctx context.Context,
	tasks TaskCollection,
	graph TaskGraph,
	taskID uuid.UUID,
	d TaskDefaults,
	now time.Time,
) ([]DomainEvent, bool, error) {
	if r.status != RunStatusRunning {
		return nil, false, nil
	}
	task, exists, err := tasks.LoadAttemptState(ctx, taskID)
	if err != nil {
		return nil, false, fmt.Errorf("load task %s: %w", taskID, err)
	}
	if !exists {
		return nil, false, fmt.Errorf("%w: task %s", ErrTaskNotHeld, taskID)
	}
	if task.RunID != r.runID {
		return nil, false, fmt.Errorf("%w: task %s belongs to run %s", ErrTaskNotHeld, taskID, task.RunID)
	}
	if task.Status != TaskStatusPending || task.DispatchedAt != nil || task.WaitReason != WaitReasonNodeLease {
		return nil, false, nil
	}
	taken, err := tasks.TakeLeaseForWaiter(ctx, LeaseClaim{TaskID: taskID, RunID: r.runID, NodeKey: task.NodeKey})
	if err != nil {
		return nil, false, fmt.Errorf("take the lease of %s for task %s: %w", task.NodeKey, taskID, err)
	}
	if !taken {
		return nil, false, nil
	}
	node, found, err := graph.Node(ctx, r.release.ReleaseID, task.NodeKey)
	if err != nil {
		return nil, false, fmt.Errorf("read node %s of release %s: %w", task.NodeKey, r.release.ReleaseID, err)
	}
	if !found {
		return nil, false, fmt.Errorf("grant task %s: release %s holds no node %s", taskID, r.release.ReleaseID, task.NodeKey)
	}
	events, err := r.dispatchAttemptZero(ctx, tasks, []readyTask{{taskID: taskID, node: node, jobName: task.JobName}}, d, now)
	if err != nil {
		return nil, false, fmt.Errorf("dispatch task %s: %w", taskID, err)
	}
	if len(events) == 0 {
		return nil, false, fmt.Errorf("grant task %s: the task was not dispatched", taskID)
	}
	r.recordProgress(now)
	return events, true, nil
}

// grantWaitingTasks grants each task of r that is next in line for a free
// node (GrantableWaits) that node's write lease and dispatches it
// (GrantLease), in the transaction that locked r, and returns the dispatches
// and the waits it ended. A run that is not running grants nothing, and a task
// whose node another task took after GrantableWaits read it keeps waiting.
func (r *Run) grantWaitingTasks(ctx context.Context, tasks TaskCollection, graph TaskGraph, d TaskDefaults,
	now time.Time) ([]DomainEvent, []LeaseWait, error) {
	if r.status != RunStatusRunning {
		return nil, nil, nil
	}
	waits, err := tasks.GrantableWaits(ctx, r.runID)
	if err != nil {
		return nil, nil, fmt.Errorf("read the lease waits of run %s next in line: %w", r.runID, err)
	}
	var events []DomainEvent
	var granted []LeaseWait
	for _, w := range waits {
		dispatched, ok, err := r.GrantLease(ctx, tasks, graph, w.TaskID, d, now)
		if err != nil {
			return nil, nil, fmt.Errorf("grant the lease of %s to task %s of run %s: %w", w.NodeKey, w.TaskID, r.runID, err)
		}
		if ok {
			events = append(events, dispatched...)
			granted = append(granted, w)
		}
	}
	return events, granted, nil
}
