package backup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
)

const (
	// maxSleep bounds one wait of the scheduler: it reads the clock again at
	// least this often, so a wall clock that moved (a host that slept) is
	// noticed within it.
	maxSleep = time.Minute
	// slotTolerance is how late the scheduler may wake for a slot and still
	// run it. A slot it wakes for later than that, because the process or
	// its host was not running at the slot's time, is missed, not run late.
	slotTolerance = 5 * time.Minute
)

// Scheduler exports every tenant at each slot of a cron schedule, in UTC.
type Scheduler struct {
	spec     string
	schedule ports.BackupSchedule
	tenants  ports.GraphTenants
	ex       *Exporter
	clock    func() time.Time
	logger   *slog.Logger
	sleep    func(ctx context.Context, d time.Duration) error
}

// NewScheduler returns a Scheduler for spec, which parser reads. It returns
// the parser's reason when spec cannot be a backup schedule.
func NewScheduler(spec string, parser ports.BackupScheduleParser, tenants ports.GraphTenants, ex *Exporter, clock func() time.Time, logger *slog.Logger) (*Scheduler, error) {
	schedule, err := parser.Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("catalog export schedule %q: %w", spec, err)
	}
	return &Scheduler{
		spec: strings.TrimSpace(spec), schedule: schedule, tenants: tenants, ex: ex,
		clock: clock, logger: logger, sleep: sleepCtx,
	}, nil
}

// Run exports every tenant at each slot until ctx ends. The next slot is
// always the schedule's first after the time the previous run ended, so a
// slot that passed while catalog was down, or while the previous run still
// ran, is never run late; the next slot runs instead. An export in flight
// when ctx ends is cancelled, which aborts its upload and releases its
// lease.
func (s *Scheduler) Run(ctx context.Context) {
	after := s.clock().UTC()
	for {
		slot := s.schedule.Next(after)
		if slot.IsZero() {
			s.logger.Error("the catalog export schedule names no later time: no export runs", "schedule", s.spec)
			return
		}
		s.logger.Info("next catalog export", "slot", slot, "schedule", s.spec)
		if err := s.waitUntil(ctx, slot); err != nil {
			return
		}
		if late := s.clock().UTC().Sub(slot); late > slotTolerance {
			s.logger.Warn("catalog export slot missed: catalog was not running at its time", "slot", slot, "late_by", late.String())
		} else {
			s.exportAll(ctx)
		}
		after = s.clock().UTC()
		// A clock that stepped back never repeats a slot.
		if after.Before(slot) {
			after = slot
		}
		if next := s.schedule.Next(slot); next.Before(after) {
			s.logger.Warn("catalog exports ran past their next slot, which is skipped", "slot", next, "ended_at", after)
		}
	}
}

// waitUntil sleeps until the clock reads slot, reading it again at least
// every maxSleep.
func (s *Scheduler) waitUntil(ctx context.Context, slot time.Time) error {
	for {
		d := slot.Sub(s.clock())
		if d <= 0 {
			return nil
		}
		if err := s.sleep(ctx, min(d, maxSleep)); err != nil {
			return err
		}
	}
}

// exportAll exports every tenant in turn; one tenant's failure does not stop
// the others. Each export logs and observes its own outcome.
func (s *Scheduler) exportAll(ctx context.Context) {
	tenants, err := s.tenants.Tenants(ctx)
	if err != nil {
		s.logger.Error("catalog export: the tenants could not be listed; no tenant is exported until the next slot", "error", err)
		return
	}
	s.ex.ExportEach(ctx, tenants, func(string, Result, error) {})
}

// sleepCtx sleeps d, or until ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
