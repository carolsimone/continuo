// Package leasewaker is state's lease waker: it grants the write lease on each
// node no task holds to the task that has waited longest for it, dispatching
// that task, and deletes the leases whose holder task has ended without
// releasing them. A pass covers every tenant: each tenant's node is its own,
// and each grant runs under the tenant of the waiter it serves. A pass runs on
// every notification that leases were released, every interval, and before
// each stall scan of the attempt reconcile.
package leasewaker

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

// GrantLimit is how many waiters one pass grants their node to, the longest
// waiting first; the rest wait for the next pass.
const GrantLimit = 200

// Waker grants the node leases of every tenant.
type Waker struct {
	newUoW  func() uow.UnitOfWork
	tasks   run.TaskDefaults
	metrics ports.RunMetrics
	logger  *slog.Logger
}

// New builds the waker; tasks names the timeout of a dispatch whose node
// declares none.
func New(newUoW func() uow.UnitOfWork, tasks run.TaskDefaults, metrics ports.RunMetrics, logger *slog.Logger) *Waker {
	return &Waker{newUoW: newUoW, tasks: tasks, metrics: metrics, logger: logger}
}

// Run passes now, then on every signal from wake and every interval, until
// ctx ends. A nil wake leaves only the interval.
func (w *Waker) Run(ctx context.Context, wake <-chan struct{}, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := w.Pass(ctx); err != nil && ctx.Err() == nil {
			w.logger.Error("Lease waker pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

// Pass deletes, counts and logs every lease of any tenant whose holder task
// has ended, then grants each free node of each tenant to its oldest waiter
// whose run is running, up to GrantLimit of them, each grant in a transaction
// of its own under the waiter's tenant, and sets the waiter gauge to the count
// of every tenant's waiters. A failed repair or grant does not stop the pass;
// Pass returns every failure. Two passes at once grant a node once: the
// second finds its waiter dispatched, or its node held, under the run's lock.
func (w *Waker) Pass(ctx context.Context) error {
	leases := w.newUoW().Leases()
	var errs []error
	repaired, err := leases.RepairEnded(ctx)
	if err != nil {
		errs = append(errs, err)
	}
	for _, l := range repaired {
		w.metrics.LeaseRepaired()
		w.logger.Error("Deleted the node lease of a task that ended without releasing it, which a defect in state causes",
			"tenant_id", l.TenantID, "node_key", l.NodeKey, "task_id", l.TaskID, "run_id", l.RunID, "task_status", l.TaskStatus,
			"acquired_at", l.AcquiredAt)
	}
	waiters, err := leases.Grantable(ctx, GrantLimit)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for _, waiter := range waiters {
		if err := w.grant(ctx, waiter); err != nil {
			errs = append(errs, err)
		}
	}
	n, err := leases.CountWaiters(ctx)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	w.metrics.LeaseWaiters(n)
	return errors.Join(errs...)
}

// grant locks the waiter's run and grants its task the node's lease, which
// dispatches the task, then saves the run and writes the dispatch to the
// outbox, all under the waiter's tenant. A run that is no longer running, a
// task that no longer waits and a node another task took first are left as
// they are. The wait is observed, and the grant logged, once the transaction
// has committed.
func (w *Waker) grant(ctx context.Context, waiter repository.LeaseWaiter) error {
	var waited time.Duration
	granted := false
	if err := w.inTx(ctx, func(u uow.UnitOfWork) error {
		r, err := u.Run().LoadRunForUpdate(ctx, waiter.TenantID, waiter.RunID)
		if err != nil {
			return fmt.Errorf("load run %s of tenant %s: %w", waiter.RunID, waiter.TenantID, err)
		}
		now := u.Clock().Now()
		events, ok, err := r.GrantLease(ctx, u.TaskCollection(waiter.TenantID), u.TaskGraph(waiter.TenantID), waiter.TaskID, w.tasks, now)
		if err != nil {
			return fmt.Errorf("grant the lease of %s to task %s of run %s: %w", waiter.NodeKey, waiter.TaskID, waiter.RunID, err)
		}
		if !ok {
			return nil
		}
		if err := u.Run().SaveRun(ctx, r); err != nil {
			return fmt.Errorf("save run %s: %w", waiter.RunID, err)
		}
		if err := u.Outbox().Append(ctx, events, uuid.Nil); err != nil {
			return fmt.Errorf("append the dispatch of task %s of run %s: %w", waiter.TaskID, waiter.RunID, err)
		}
		// The wait began on the database clock; a clock behind it reads as no
		// wait rather than a negative one.
		waited = max(now.Sub(waiter.WaitingSince), 0)
		granted = true
		return nil
	}); err != nil || !granted {
		return err
	}
	w.metrics.LeaseGranted(waited)
	w.logger.Info("Granted a node lease to the task that waited longest for it", "tenant_id", waiter.TenantID,
		"node_key", waiter.NodeKey, "task_id", waiter.TaskID, "run_id", waiter.RunID, "waited", waited.String())
	return nil
}

// inTx runs fn in a transaction of its own and commits it when fn succeeds.
func (w *Waker) inTx(ctx context.Context, fn func(uow.UnitOfWork) error) error {
	u := w.newUoW()
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
