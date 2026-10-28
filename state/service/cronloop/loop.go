// Package cronloop is state's cron loop: it fires the schedules of every
// tenant whose next fire has come. Each schedule row stores the instant of its
// next slot; a pass fires each due schedule in a transaction of its own that
// locks the schedule's row, starts the run of the slot (stamped with it, so a
// slot never has two runs) and moves the next fire on to the slot after it.
// Only a schedule's latest due slot fires: older slots it missed, while state
// was down, are counted and logged. In maintenance mode, or while the
// schedule's previous run is active, the slot is consumed with no run. The
// loop sleeps until the earliest next fire, a wake signal (a change to the
// schedules) or its tick.
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
	"github.com/google/uuid"
)

// DueLimit is how many due schedules one pass fires, the earliest next fire
// first; the rest fire on the passes after it.
const DueLimit = 100

// minWait is the shortest sleep between two passes when a schedule is still
// due after a pass: one the pass could not fire, such as a schedule whose
// stored cron no longer parses, or one past DueLimit. It keeps such a schedule
// from making the loop pass again at once.
const minWait = time.Second

// The outcomes a fire counts, as continuo_cron_fires_total names them.
const (
	outcomeStarted            = "started"
	outcomeSkippedActive      = "skipped_active"
	outcomeSkippedMaintenance = "skipped_maintenance"
	outcomeEmptySelection     = "empty_selection"
	outcomeMissed             = "missed"
)

// Loop fires the due schedules of every tenant.
type Loop struct {
	newUoW      func() uow.UnitOfWork
	starter     *handlers.StartRunHandler
	metrics     ports.RunMetrics
	maintenance bool
	logger      *slog.Logger
}

// New builds the loop. starter starts the run of each slot; with maintenance
// on, no run is started and every due slot is consumed.
func New(newUoW func() uow.UnitOfWork, starter *handlers.StartRunHandler, metrics ports.RunMetrics, maintenance bool, logger *slog.Logger) *Loop {
	return &Loop{newUoW: newUoW, starter: starter, metrics: metrics, maintenance: maintenance, logger: logger}
}

// Run passes now, then again when the earliest next fire of any schedule
// comes, on every signal from wake and at least every tick, until ctx ends. A
// nil wake leaves the next fire and the tick. When a schedule is still due
// after a pass, Run waits minWait before the next one.
func (l *Loop) Run(ctx context.Context, wake <-chan struct{}, tick time.Duration) {
	for {
		if err := l.Pass(ctx); err != nil && ctx.Err() == nil {
			l.logger.Error("Cron loop pass failed", "error", err)
		}
		timer := time.NewTimer(l.untilNextFire(ctx, tick))
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

// untilNextFire is how long Run sleeps after a pass: until the earliest next
// fire of any schedule, at most tick, and tick when no schedule has a next
// fire or it cannot be read. A next fire that is not after now is a schedule
// the pass left due, which waits minWait.
func (l *Loop) untilNextFire(ctx context.Context, tick time.Duration) time.Duration {
	u := l.newUoW()
	next, ok, err := u.Schedules().EarliestFire(ctx)
	if err != nil {
		if ctx.Err() == nil {
			l.logger.Error("Cron loop could not read the next fire; it passes again at its tick", "error", err)
		}
		return tick
	}
	if !ok {
		return tick
	}
	wait := next.Sub(u.Clock().Now())
	if wait <= 0 {
		wait = minWait
	}
	return min(wait, tick)
}

// Pass fires every schedule of any tenant due at now, up to DueLimit of them,
// each in a transaction of its own, then sets the schedules gauge. The due
// schedules are read without a lock, leaving out those another pass holds;
// each fire locks its schedule's row and reads it again, so two passes fire a
// slot once. A schedule whose fire fails keeps its slot for a later pass and
// does not stop the others; Pass returns every failure.
func (l *Loop) Pass(ctx context.Context) error {
	u := l.newUoW()
	schedules := u.Schedules()
	now := u.Clock().Now()
	due, err := schedules.LockDue(ctx, now, DueLimit)
	if err != nil {
		return fmt.Errorf("read the due schedules: %w", err)
	}
	var errs []error
	for _, s := range due {
		if err := l.fire(ctx, s.TenantID(), s.Name(), now); err != nil {
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

// fire fires the slot tenantID's schedule name is due for at now, in one
// transaction: it locks the schedule's row, reads its latest due slot, starts
// the slot's run unless maintenance mode is on or the schedule's previous run
// is active, and moves the next fire on to the slot after it. A schedule that
// is gone, or no longer due because another pass fired it or an edit moved its
// next fire, is left as it is. A run another caller started at the moment of
// the insert rolls the fire back; the next pass finds that run active. The
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
	s, ok, err := u.Schedules().GetForUpdate(ctx, tenantID, name)
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

	var runID uuid.UUID
	outcome := outcomeSkippedMaintenance
	if !l.maintenance {
		id, started, err := l.starter.HandleCronFire(ctx, u, tenantID, name, slot)
		if errors.Is(err, run.ErrScheduleHasActiveRun) {
			l.logger.Info("Cron fire rolled back: a run of the schedule started at the moment of the fire; the next pass consumes the slot",
				"tenant_id", tenantID, "schedule_name", name, "slot", slot)
			return nil
		}
		if err != nil {
			return fmt.Errorf("fire the %s slot of schedule %q of tenant %q: %w", slot.UTC().Format(time.RFC3339), name, tenantID, err)
		}
		runID, outcome = id, outcomeOf(started)
	}
	// The row is locked, and a fire keeps the version, so the save at the
	// version read always matches.
	s.Fired(slot)
	if err := u.Schedules().Save(ctx, s, s.Version()); err != nil {
		return fmt.Errorf("move the next fire of schedule %q of tenant %q: %w", name, tenantID, err)
	}
	next, _ := s.NextFireAt()
	u.AfterCommit(func() {
		l.record(tenantID, name, slot, next, runID, outcome, missed, firstMissed)
	})
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit the fire of schedule %q of tenant %q: %w", name, tenantID, err)
	}
	committed = true
	return nil
}

// outcomeOf is the outcome a fire counts for what starting its run did; a
// slot that already had its run counts none.
func outcomeOf(started handlers.ActivationOutcome) string {
	switch started {
	case handlers.OutcomeRunStarted:
		return outcomeStarted
	case handlers.OutcomeRunRefused:
		return outcomeEmptySelection
	case handlers.OutcomeSkippedActive:
		return outcomeSkippedActive
	default:
		return ""
	}
}

// record counts and logs a committed fire: its outcome, and the missed slots
// before it, from firstMissed on.
func (l *Loop) record(tenantID, name string, slot, next time.Time, runID uuid.UUID, outcome string, missed int, firstMissed time.Time) {
	attrs := []any{"tenant_id", tenantID, "schedule_name", name, "slot", slot, "next_fire_at", next}
	if missed > 0 {
		for range missed {
			l.metrics.CronFire(outcomeMissed)
		}
		l.logger.Warn("Cron fire skipped missed slots: only the latest due slot fires",
			append(attrs, "missed", missed, "first_missed", firstMissed)...)
	}
	switch outcome {
	case outcomeStarted:
		l.logger.Info("Cron fire started a run", append(attrs, "run_id", runID)...)
	case outcomeEmptySelection:
		l.logger.Warn("Cron fire recorded a failed run: the live release gives the schedule nothing to run",
			append(attrs, "run_id", runID)...)
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
