// Package watchdog cancels active runs that have made no lifecycle progress
// within a configured window.
package watchdog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
)

// Clock returns the current time. Injected for deterministic tests.
type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Config holds the watchdog tunables.
type Config struct {
	Enabled       bool
	Interval      time.Duration
	NoProgressFor time.Duration
}

// Watchdog cancels active runs that have stopped making progress. It depends
// only on domain-typed ports; the gRPC wire types live in the adapter that
// implements StuckScheduleReader and RunCanceller.
type Watchdog struct {
	cfg       Config
	reader    ports.StuckScheduleReader
	canceller ports.RunCanceller
	clock     Clock
	logger    *slog.Logger
}

// NewWatchdog constructs a Watchdog. Uses the real clock.
func NewWatchdog(cfg Config, reader ports.StuckScheduleReader, canceller ports.RunCanceller, logger *slog.Logger) *Watchdog {
	return &Watchdog{cfg: cfg, reader: reader, canceller: canceller, clock: realClock{}, logger: logger}
}

// SetClockForTest replaces the clock for tests in this package and tests
// in the watchdog_test external package. Not for production use.
func SetClockForTest(w *Watchdog, c Clock) { w.clock = c }

// TickForTest exposes a single iteration of tick for unit tests.
func TickForTest(w *Watchdog, ctx context.Context) error { return w.tick(ctx) }

// Run starts the watchdog loop. Returns when ctx is cancelled or, if
// Enabled is false, immediately.
func (w *Watchdog) Run(ctx context.Context) error {
	if !w.cfg.Enabled {
		w.logger.Info("Dispatch watchdog disabled by config")
		return nil
	}
	t := time.NewTicker(w.cfg.Interval)
	defer t.Stop()
	w.logger.Info("Dispatch watchdog starting",
		"interval", w.cfg.Interval, "no_progress_for", w.cfg.NoProgressFor)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := w.tick(ctx); err != nil {
				w.logger.Error("Watchdog tick failed", "error", err)
			}
		}
	}
}

// tickTimeout bounds a single tick so a slow state service never stalls the
// watchdog loop across ticks. It is the interval less a small margin, capped so
// the deadline is always positive.
func (w *Watchdog) tickTimeout() time.Duration {
	d := w.cfg.Interval - time.Second
	if d <= 0 {
		d = w.cfg.Interval
	}
	if d <= 0 {
		d = 5 * time.Second
	}
	return d
}

// tick asks state for the active runs with no progress since now minus
// NoProgressFor and no running task, then cancels each by its run id. A cancel
// writes the outbox rows that publish schedule.cancelled:v1 and
// run.finalized:v1; the cancelled_schedules guards in the consumers absorb
// in-flight messages. A run that reached a terminal status after the read is
// skipped.
func (w *Watchdog) tick(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, w.tickTimeout())
	defer cancel()

	cutoff := w.clock.Now().Add(-w.cfg.NoProgressFor)
	candidates, err := w.reader.ListStuckCandidates(ctx, cutoff)
	if err != nil {
		return fmt.Errorf("list stuck candidates: %w", err)
	}
	reason := fmt.Sprintf("watchdog: no run progress for >%dm", int(w.cfg.NoProgressFor.Minutes()))
	for _, c := range candidates {
		w.logger.Warn("Watchdog cancelling stalled run",
			"schedule_name", c.ScheduleName,
			"schedule_id", c.RunID,
			"reason", reason,
		)
		if err := w.canceller.CancelRun(ctx, c.RunID, "watchdog", reason); err != nil {
			if errors.Is(err, ports.ErrRunNotCancellable) {
				w.logger.Info("Watchdog skipped run already terminal",
					"schedule_name", c.ScheduleName, "schedule_id", c.RunID, "error", err)
				continue
			}
			w.logger.Error("Watchdog CancelRun failed",
				"schedule_name", c.ScheduleName, "schedule_id", c.RunID, "error", err)
			continue
		}
	}
	return nil
}
