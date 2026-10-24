// Package attemptreconcile is state's attempt reconcile: every pass it asks
// execution-controller about each attempt nothing was heard of within its
// window, by dispatching the attempt again with its known status, and fails
// each run left with nothing in flight and no progress as stalled.
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
	newUoW   func() uow.UnitOfWork
	tenantID string
	tasks    run.TaskDefaults
	metrics  ports.RunMetrics
	logger   *slog.Logger
}

// New builds the loop; tasks names the timeout of a dispatch whose node
// declares none.
func New(newUoW func() uow.UnitOfWork, tenantID string, tasks run.TaskDefaults, metrics ports.RunMetrics, logger *slog.Logger) *Loop {
	return &Loop{newUoW: newUoW, tenantID: tenantID, tasks: tasks, metrics: metrics, logger: logger}
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
// cancelling run whose stops are unanswered, fails every stalled run and sets
// the cancelling gauges, each question and failure in a transaction of its
// own. Two passes at once cost at most a repeated question, which
// execution-controller answers twice.
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

// failStalled fails one run as stalled, once its lock confirms it still is,
// and counts and logs it once the transaction that failed it has committed.
func (l *Loop) failStalled(ctx context.Context, runID uuid.UUID) error {
	var failed *run.Run
	if err := l.inTx(ctx, func(u uow.UnitOfWork) error {
		r, err := u.Run().LoadRunForUpdate(ctx, runID)
		if err != nil {
			return fmt.Errorf("load run %s: %w", runID, err)
		}
		events, stalled, err := r.FailStalled(ctx, u.TaskCollection(), u.Clock().Now())
		if err != nil || !stalled {
			return err
		}
		if err := u.Run().SaveRun(ctx, r); err != nil {
			return fmt.Errorf("save stalled run %s: %w", runID, err)
		}
		if err := u.Outbox().Append(ctx, events, uuid.Nil); err != nil {
			return fmt.Errorf("append the events of stalled run %s: %w", runID, err)
		}
		failed = r
		return nil
	}); err != nil || failed == nil {
		return err
	}
	l.metrics.RunStalled()
	l.logger.Error("Failed a stalled run: it had no attempt in flight and no progress for five minutes, which a defect in state causes",
		"run_id", runID, "schedule_name", failed.ScheduleName(), "last_progress", failed.LastHeartbeatAt())
	return nil
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
