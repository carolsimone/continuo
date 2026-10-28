// Package cronloop is state's cron loop: it fires the schedules of every
// tenant whose next fire has come. Each schedule row stores the instant of its
// next slot; a pass fires each due schedule in a transaction of its own that
// locks the schedule's row, starts the run of the slot (stamped with it, so a
// slot never has two runs) and moves the next fire on to the slot after it.
// Only a schedule's latest due slot fires: older slots it missed, while state
// was down, are counted and logged. In maintenance mode, or while the
// schedule's previous run is active, the slot is consumed with no run. Each
// fire is bounded by FireTimeout. The loop sleeps until the earliest next
// fire after its last pass, a wake signal (a change to the schedules) or its
// tick.
package cronloop

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
)

// DueLimit is how many due schedules one pass fires, the earliest next fire
// first; the rest fire on the passes after it.
const DueLimit = 100

// FireTimeout bounds one schedule's fire. A fire waits for the topology lock
// while a promotion holds it, and the fires of a pass run one after another,
// so without a bound one tenant's promotion would hold up every fire after it
// in the pass. A fire still going at FireTimeout rolls back and leaves its
// slot due, and a later pass fires it.
const FireTimeout = 30 * time.Second

// The outcomes a fire counts, as continuo_cron_fires_total names them.
const (
	outcomeStarted            = "started"
	outcomeSkippedActive      = "skipped_active"
	outcomeSkippedMaintenance = "skipped_maintenance"
	outcomeEmptySelection     = "empty_selection"
	outcomeInvalidNodeType    = "invalid_node_type"
	outcomeMissed             = "missed"
)

// Loop fires the due schedules of every tenant.
type Loop struct {
	newUoW      func() uow.UnitOfWork
	starter     *handlers.StartRunHandler
	metrics     ports.RunMetrics
	maintenance bool
	logger      *slog.Logger
	// fireTimeout bounds each fire: FireTimeout.
	fireTimeout time.Duration
}

// New builds the loop. starter starts the run of each slot; with maintenance
// on, no run is started and every due slot is consumed.
func New(newUoW func() uow.UnitOfWork, starter *handlers.StartRunHandler, metrics ports.RunMetrics, maintenance bool, logger *slog.Logger) *Loop {
	return &Loop{newUoW: newUoW, starter: starter, metrics: metrics, maintenance: maintenance, logger: logger, fireTimeout: FireTimeout}
}

// Run passes now, then again when the earliest next fire after the last pass
// comes, on every signal from wake and at least every tick, until ctx ends. A
// nil wake leaves the next fire and the tick. A schedule a pass leaves due,
// one past DueLimit or one it could not fire (its stored cron no longer
// parses, its fire failed or ran out of time, or another transaction held its
// row), is tried again by the pass a wake signal or the tick brings. With wake
// the schedules channel, as state runs it, every committed fire notifies it,
// so a pass that fired DueLimit schedules is followed by another at once.
func (l *Loop) Run(ctx context.Context, wake <-chan struct{}, tick time.Duration) {
	for {
		now := l.newUoW().Clock().Now()
		if err := l.Pass(ctx, now); err != nil && ctx.Err() == nil {
			l.logger.Error("Cron loop pass failed", "error", err)
		}
		timer := time.NewTimer(l.untilNextFire(ctx, now, tick))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// untilNextFire is how long Run sleeps after a pass at passedAt: until the
// earliest next fire after passedAt, no time at all when that has already
// come, at most tick, and tick when there is none or it cannot be read.
//
// A schedule the pass left due has a next fire at or before passedAt, so it
// does not count, and its next try comes with a wake signal or the tick: a
// schedule no pass can fire, such as one whose stored cron no longer parses,
// neither delays the others nor makes the loop pass again and again. No
// shortest sleep is needed against such a spin either. A next fire after
// passedAt that has come by now, such as one that came due while the pass
// ran, makes the loop pass again at once; that pass fires it, and if it
// cannot, the fire is at or before that pass's own instant and no longer
// counts. So each next fire brings at most one pass that fires nothing.
func (l *Loop) untilNextFire(ctx context.Context, passedAt time.Time, tick time.Duration) time.Duration {
	u := l.newUoW()
	next, ok, err := u.Schedules().EarliestFireAfter(ctx, passedAt)
	if err != nil {
		if ctx.Err() == nil {
			l.logger.Error("Cron loop could not read the next fire; it passes again at its tick", "error", err)
		}
		return tick
	}
	if !ok {
		return tick
	}
	return min(max(next.Sub(u.Clock().Now()), 0), tick)
}

// Pass fires every schedule of any tenant due at now, up to DueLimit of them,
// each in a transaction of its own that FireTimeout bounds, then sets the
// schedules gauge. The due schedules are read without a lock, leaving out
// those another pass holds; each fire locks its schedule's row and reads it
// again, so two passes fire a slot once. A schedule whose fire fails or runs
// out of time keeps its slot for a later pass and does not stop the others;
// Pass returns every failure.
func (l *Loop) Pass(ctx context.Context, now time.Time) error {
	schedules := l.newUoW().Schedules()
	due, err := schedules.ListDue(ctx, now, DueLimit)
	if err != nil {
		return fmt.Errorf("read the due schedules: %w", err)
	}
	var errs []error
	for _, s := range due {
		if err := l.fireWithin(ctx, s.TenantID(), s.Name(), now); err != nil {
			errs = append(errs, err)
		}
	}
	scheduled, paused, err := schedules.Count(ctx)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	l.metrics.Schedules(scheduled, paused)
	return errors.Join(errs...)
}

// fireWithin fires tenantID's schedule name as fire does, within the loop's
// fire timeout. A fire that runs out of time has rolled back, and its error
// says so.
func (l *Loop) fireWithin(ctx context.Context, tenantID, name string, now time.Time) error {
	fireCtx, cancel := context.WithTimeout(ctx, l.fireTimeout)
	defer cancel()
	err := l.fire(fireCtx, tenantID, name, now)
	if err != nil && ctx.Err() == nil && errors.Is(fireCtx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("the fire of schedule %q of tenant %q did not end within %s and rolled back; a later pass fires its slot: %w",
			name, tenantID, l.fireTimeout, err)
	}
	return err
}

// fire fires the slot tenantID's schedule name is due for at now, in one
// transaction: it locks the schedule's row, reads its latest due slot, starts
// the slot's run unless maintenance mode is on or the schedule's previous run
// is active, and moves the next fire on to the slot after it. A schedule that
// is gone, or no longer due because another pass fired it or an edit moved its
// next fire, is left as it is. A run another caller started at the moment of
// the insert rolls the fire back; a later pass finds that run active. The
// outcome is counted and logged once the transaction has committed.
func (l *Loop) fire(ctx context.Context, tenantID, name string, now time.Time) error {
	u := l.newUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin the fire of schedule %q of tenant %q: %w", name, tenantID, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()
	s, ok, err := u.Schedules().LoadForUpdate(ctx, tenantID, name)
	if err != nil {
		return fmt.Errorf("lock schedule %q of tenant %q: %w", name, tenantID, err)
	}
	if !ok {
		return nil
	}
	slot, missed, due := s.Due(now)
	if !due {
		return nil
	}
	firstMissed, _ := s.NextFireAt()

	var fired handlers.CronFireResult
	outcome := outcomeSkippedMaintenance
	if !l.maintenance {
		res, err := l.starter.HandleCronFire(ctx, u, tenantID, name, slot)
		if errors.Is(err, run.ErrScheduleHasActiveRun) {
			l.logger.Info("Cron fire rolled back: a run of the schedule started at the moment of the fire; the next pass consumes the slot",
				"tenant_id", tenantID, "schedule_name", name, "slot", slot)
			return nil
		}
		if err != nil {
			return fmt.Errorf("fire the %s slot of schedule %q of tenant %q: %w", slot.UTC().Format(time.RFC3339), name, tenantID, err)
		}
		fired, outcome = res, outcomeOf(res)
	}
	// The row is locked, and a fire keeps the version, so the save at the
	// version read always matches.
	s.Fired(slot)
	if err := u.Schedules().Save(ctx, s, s.Version()); err != nil {
		return fmt.Errorf("move the next fire of schedule %q of tenant %q: %w", name, tenantID, err)
	}
	next, _ := s.NextFireAt()
	u.AfterCommit(func() {
		l.record(tenantID, name, slot, next, fired, outcome, missed, firstMissed)
	})
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit the fire of schedule %q of tenant %q: %w", name, tenantID, err)
	}
	committed = true
	return nil
}

// outcomeOf is the outcome a fire counts for what starting its run did: a
// refused run counts its failure reason, and a slot that already had its run
// counts none.
func outcomeOf(fired handlers.CronFireResult) string {
	switch fired.Outcome {
	case handlers.OutcomeRunStarted:
		return outcomeStarted
	case handlers.OutcomeRunRefused:
		if fired.FailureReason == run.RunFailureInvalidNodeType {
			return outcomeInvalidNodeType
		}
		return outcomeEmptySelection
	case handlers.OutcomeSkippedActive:
		return outcomeSkippedActive
	default:
		return ""
	}
}

// record counts and logs a committed fire: its outcome, and the missed slots
// before it, from firstMissed on.
func (l *Loop) record(tenantID, name string, slot, next time.Time, fired handlers.CronFireResult, outcome string, missed int, firstMissed time.Time) {
	attrs := []any{"tenant_id", tenantID, "schedule_name", name, "slot", slot, "next_fire_at", next}
	if missed > 0 {
		l.metrics.CronFires(outcomeMissed, missed)
		l.logger.Warn("Cron fire skipped missed slots: only the latest due slot fires",
			append(attrs, "missed", missed, "first_missed", firstMissed)...)
	}
	switch outcome {
	case outcomeStarted:
		l.logger.Info("Cron fire started a run", append(attrs, "run_id", fired.RunID)...)
	case outcomeEmptySelection:
		l.logger.Warn("Cron fire recorded a failed run: the live release gives the schedule nothing to run",
			append(attrs, "run_id", fired.RunID, "failure_reason", string(fired.FailureReason))...)
	case outcomeInvalidNodeType:
		l.logger.Warn("Cron fire recorded a failed run: the schedule's selection holds a node of a type the node_type vocabulary does not declare",
			append(attrs, "run_id", fired.RunID, "failure_reason", string(fired.FailureReason))...)
	case outcomeSkippedActive:
		l.logger.Info("Cron fire skipped: the schedule's previous run is still active", attrs...)
	case outcomeSkippedMaintenance:
		l.logger.Info("Cron fire skipped: maintenance mode is on", attrs...)
	default:
		l.logger.Warn("Cron fire skipped: the slot already has its run", attrs...)
	}
	if outcome != "" {
		l.metrics.CronFire(outcome)
	}
}
