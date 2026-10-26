// Package attemptreconcile is state's attempt reconcile: every pass it asks
// execution-controller about each attempt nothing was heard of within its
// window, by dispatching the attempt again with its known status, and fails
// each run left with nothing in flight and no progress as stalled. Under the
// lock of each run it would fail, it first grants every free node the run's
// waiting task is next in line for (run.Run.FailStalled), so a run waiting for
// a node lease is never failed while a grant can serve it; a grant that keeps
// failing ends its run once the run has been idle for
// run.StallAfterFailedGrant. Before it looks for stalled runs it runs a hook,
// which state wires to a lease waker pass, so most freed nodes are granted
// before the scan lists their waiters.
package attemptreconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// Limits of one pass.
const (
	// DueLimit is how many due attempts one pass asks about; the rest wait
	// for the next pass, oldest due first.
	DueLimit = 500
	// StalledLimit is how many stalled runs one pass fails.
	StalledLimit = 100
	// StopLimit is how many cancelling runs one pass sends the cancel of
	// again.
	StopLimit = 100
	// CancelOverdueAfter is how long a cancelling run may wait for its
	// attempts to stop before it counts as overdue and is logged.
	CancelOverdueAfter = 15 * time.Minute
)

// Loop runs the attempt reconcile of one tenant.
type Loop struct {
	newUoW      func() uow.UnitOfWork
	tenantID    string
	tasks       run.TaskDefaults
	metrics     ports.RunMetrics
	beforeStall func(context.Context) error
	logger      *slog.Logger
}

// New builds the loop; tasks names the timeout of a dispatch whose node
// declares none, and beforeStall runs at the start of every stall scan (nil
// runs nothing).
func New(newUoW func() uow.UnitOfWork, tenantID string, tasks run.TaskDefaults, metrics ports.RunMetrics,
	beforeStall func(context.Context) error, logger *slog.Logger) *Loop {
	return &Loop{newUoW: newUoW, tenantID: tenantID, tasks: tasks, metrics: metrics, beforeStall: beforeStall, logger: logger}
}

// Run passes now and every interval until ctx ends.
func (l *Loop) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := l.Pass(ctx); err != nil && ctx.Err() == nil {
			l.logger.Error("Attempt reconcile pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Pass asks about every due attempt, sends the cancel again for every
// cancelling run whose stops are unanswered, runs beforeStall and then fails
// every stalled run, or grants its waiting tasks their free nodes, and sets
// the cancelling gauges, each question and stall check in a transaction of its
// own. A failed beforeStall is logged and the stall scan still runs. Two
// passes at once cost at most a repeated question, which execution-controller
// answers twice.
func (l *Loop) Pass(ctx context.Context) error {
	reader := l.newUoW().Attempts(l.tenantID)
	due, err := reader.DueAttempts(ctx, DueLimit)
	if err != nil {
		return err
	}
	var errs []error
	for _, key := range due {
		if err := l.ask(ctx, key); err != nil {
			errs = append(errs, err)
		}
	}
	stops, err := reader.DueStops(ctx, StopLimit)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	l.runBeforeStall(ctx)
	stalled, err := reader.StalledRuns(ctx, run.StallAfter, StalledLimit)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for _, id := range stalled {
		if err := l.failStalled(ctx, id); err != nil {
			errs = append(errs, err)
		}
	}
	for _, id := range stops {
		if err := l.resendStop(ctx, id); err != nil {
			errs = append(errs, err)
		}
	}
	total, overdue, err := reader.CountCancelling(ctx, CancelOverdueAfter)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	l.metrics.RunsCancelling(total, overdue)
	return errors.Join(errs...)
}

// runBeforeStall runs the beforeStall hook and logs its failure; a failure
// does not stop the pass.
func (l *Loop) runBeforeStall(ctx context.Context) {
	if l.beforeStall == nil {
		return
	}
	if err := l.beforeStall(ctx); err != nil && ctx.Err() == nil {
		l.logger.Error("The hook before the stall scan failed; the stall scan still runs", "error", err)
	}
}

// ask dispatches one due attempt again, with its known status, and moves its
// next check to its next window.
func (l *Loop) ask(ctx context.Context, key repository.AttemptKey) error {
	return l.inTx(ctx, func(u uow.UnitOfWork) error {
		q, found, err := u.Attempts(l.tenantID).LockQuestion(ctx, key)
		if err != nil || !found {
			return err
		}
		dispatch, next := q.Dispatch(l.tasks)
		if err := u.Attempts(l.tenantID).MarkQuestioned(ctx, key, next); err != nil {
			return err
		}
		if err := u.Outbox().Append(ctx, []run.DomainEvent{dispatch}, uuid.Nil); err != nil {
			return fmt.Errorf("append the question about attempt %d of task %s: %w", key.Attempt, key.TaskID, err)
		}
		l.logger.Info("Asked about a quiet attempt", "run_id", q.RunID, "task_id", q.TaskID, "attempt", q.Attempt,
			"known_status", q.Status, "checks_sent", q.ChecksSent+1, "next_check_in", next.String())
		return nil
	})
}

// resendStop sends the cancel of one cancelling run again, naming each attempt
// asked to stop that has not answered, and moves each of those attempts to its
// next stop window. A run past CancelOverdueAfter is also logged as an error,
// with the attempts it waits for.
func (l *Loop) resendStop(ctx context.Context, runID uuid.UUID) error {
	return l.inTx(ctx, func(u uow.UnitOfWork) error {
		q, found, err := u.Attempts(l.tenantID).LockStops(ctx, runID)
		if err != nil || !found {
			return err
		}
		for _, t := range q.Attempts {
			key := repository.AttemptKey{TaskID: t.TaskID, Attempt: t.Attempt}
			if err := u.Attempts(l.tenantID).MarkQuestioned(ctx, key, run.NextStopCheckAfter(t.ChecksSent+1)); err != nil {
				return err
			}
		}
		if err := u.Outbox().Append(ctx, []run.DomainEvent{q.Cancel()}, uuid.Nil); err != nil {
			return fmt.Errorf("append the cancel of run %s again: %w", runID, err)
		}
		waiting := stopTargets(q.Attempts)
		if waited := u.Clock().Now().Sub(q.CancelledAt); waited > CancelOverdueAfter {
			l.logger.Error("A cancelling run is still waiting for attempts to stop",
				"run_id", runID, "schedule_name", q.ScheduleName, "waiting", waited.String(), "attempts", waiting)
			return nil
		}
		l.logger.Info("Sent the cancel again", "run_id", runID, "schedule_name", q.ScheduleName, "attempts", waiting)
		return nil
	})
}

// stopTargets renders the attempts a cancel waits for as "task/attempt (job)".
func stopTargets(targets []run.StopTarget) []string {
	out := make([]string, 0, len(targets))
	for _, t := range targets {
		out = append(out, fmt.Sprintf("%s/%d (%s)", t.TaskID, t.Attempt, t.JobName))
	}
	return out
}

// failStalled locks one run the scan listed and checks it under the lock
// (run.Run.FailStalled): a task of it next in line for a free node is granted
// that node's lease and dispatched, and a run granted none that is still
// stalled is failed. A check whose grant fails (run.ErrGrantFailed) is rolled
// back and tried once more at once, in a transaction of its own, since a
// transient error rarely repeats; when the grant fails again the run is
// checked without its grant (run.Run.FailStalledWithoutGrants), which fails a
// run idle for run.StallAfterFailedGrant, so a grant that keeps failing ends
// its run and leaves the node's queue to the next waiter. A run idle for less
// is left for the next pass, and the grant's error is returned. Once a check
// has committed, it observes and logs each grant as the lease waker does, and
// counts and logs a failed run.
func (l *Loop) failStalled(ctx context.Context, runID uuid.UUID) error {
	c, err := l.checkStall(ctx, runID, true)
	if errors.Is(err, run.ErrGrantFailed) {
		c, err = l.checkStall(ctx, runID, true)
	}
	var grantErr error
	if errors.Is(err, run.ErrGrantFailed) {
		grantErr = err
		if c, err = l.checkStall(ctx, runID, false); err != nil {
			return errors.Join(grantErr, err)
		}
		if !c.check.Stalled {
			return grantErr
		}
	}
	if err != nil {
		return err
	}
	for _, w := range c.check.Granted {
		// The wait began on the database clock; a clock behind it reads as no
		// wait rather than a negative one.
		waited := max(c.now.Sub(w.WaitingSince), 0)
		l.metrics.LeaseGranted(waited)
		l.logger.Info("Granted a node lease to the task next in line for it, under the lock of a run idle for five minutes",
			"node_key", w.NodeKey, "task_id", w.TaskID, "run_id", runID, "waited", waited.String())
	}
	if !c.check.Stalled {
		return nil
	}
	l.metrics.RunStalled()
	if grantErr != nil {
		l.logger.Error("Failed a stalled run whose waiting task could not be granted its node's lease, which a defect in state causes",
			"run_id", runID, "schedule_name", c.r.ScheduleName(), "last_progress", c.r.LastHeartbeatAt(),
			"idle_for_at_least", run.StallAfterFailedGrant.String(), "grant_error", grantErr)
		return nil
	}
	l.logger.Error("Failed a stalled run: it had no attempt in flight and no progress for five minutes, which a defect in state causes",
		"run_id", runID, "schedule_name", c.r.ScheduleName(), "last_progress", c.r.LastHeartbeatAt())
	return nil
}

// stallCheck is one committed check of a run: the run as the check left it,
// what the check did, and the clock it read.
type stallCheck struct {
	r     *run.Run
	check run.StallCheck
	now   time.Time
}

// checkStall locks the run and checks it, with its grant step
// (run.Run.FailStalled) or without it (run.Run.FailStalledWithoutGrants), in
// a transaction of its own; a run the check granted a task or ended is saved
// with its events.
func (l *Loop) checkStall(ctx context.Context, runID uuid.UUID, grant bool) (stallCheck, error) {
	var c stallCheck
	err := l.inTx(ctx, func(u uow.UnitOfWork) error {
		var err error
		if c.r, err = u.Run().LoadRunForUpdate(ctx, runID); err != nil {
			return fmt.Errorf("load run %s: %w", runID, err)
		}
		c.now = u.Clock().Now()
		if grant {
			c.check, err = c.r.FailStalled(ctx, u.TaskCollection(l.tenantID), u.TaskGraph(l.tenantID), l.tasks, c.now)
		} else {
			c.check, err = c.r.FailStalledWithoutGrants(ctx, u.TaskCollection(l.tenantID), c.now)
		}
		if err != nil {
			return fmt.Errorf("check run %s for a stall: %w", runID, err)
		}
		if !c.check.Stalled && len(c.check.Granted) == 0 {
			return nil
		}
		if err := u.Run().SaveRun(ctx, c.r); err != nil {
			return fmt.Errorf("save run %s: %w", runID, err)
		}
		if err := u.Outbox().Append(ctx, c.check.Events, uuid.Nil); err != nil {
			return fmt.Errorf("append the events of run %s: %w", runID, err)
		}
		return nil
	})
	return c, err
}

// inTx runs fn in a transaction of its own and commits it when fn succeeds.
func (l *Loop) inTx(ctx context.Context, fn func(uow.UnitOfWork) error) error {
	u := l.newUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := fn(u); err != nil {
		_ = u.Rollback()
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
