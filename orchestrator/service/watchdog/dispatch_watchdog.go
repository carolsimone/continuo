// Package watchdog detects schedules whose dispatch has silently stalled
// and terminates them via the cancellation pathway.
package watchdog

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"google.golang.org/grpc"
)

// StateClient is the slimmest gRPC surface the watchdog needs.
type StateClient interface {
	ListAllSchedules(ctx context.Context, req *statev1.ListAllSchedulesRequest, opts ...grpc.CallOption) (*statev1.ListAllSchedulesResponse, error)
	ListTasks(ctx context.Context, req *statev1.ListTasksRequest, opts ...grpc.CallOption) (*statev1.ListTasksResponse, error)
	CancelSchedule(ctx context.Context, req *statev1.CancelScheduleRequest, opts ...grpc.CallOption) (*statev1.CancelScheduleResponse, error)
}

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

// Watchdog terminates schedules whose dispatch has silently stalled.
type Watchdog struct {
	cfg    Config
	state  StateClient
	clock  Clock
	logger *slog.Logger
}

// NewWatchdog constructs a Watchdog. Uses the real clock.
func NewWatchdog(cfg Config, state StateClient, logger *slog.Logger) *Watchdog {
	return &Watchdog{cfg: cfg, state: state, clock: realClock{}, logger: logger}
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

// tick scans for stuck schedules and cancels them via state.CancelSchedule.
// state.CancelSchedule writes an outbox row that publishes schedule.cancelled:v1
// — the existing cancellation pathway absorbs in-flight messages via the
// cancelled_schedules guards in the consumers.
func (w *Watchdog) tick(ctx context.Context) error {
	resp, err := w.state.ListAllSchedules(ctx, &statev1.ListAllSchedulesRequest{})
	if err != nil {
		return fmt.Errorf("list all schedules: %w", err)
	}
	now := w.clock.Now()
	for _, sched := range resp.GetSchedules() {
		if !sched.GetIsRunning() {
			continue
		}
		runID := sched.GetLastRunId()
		if runID == "" {
			continue // shouldn't happen if is_running=true, but skip defensively
		}
		tasksResp, err := w.state.ListTasks(ctx, &statev1.ListTasksRequest{ScheduleId: runID})
		if err != nil {
			w.logger.Error("ListTasks failed", "schedule_name", sched.GetScheduleName(), "schedule_id", runID, "error", err)
			continue
		}
		if !IsScheduleStuck(tasksResp.GetTasks(), now, w.cfg.NoProgressFor) {
			continue
		}
		reason := fmt.Sprintf("watchdog: no task progress for >%dm", int(w.cfg.NoProgressFor.Minutes()))
		w.logger.Warn("Watchdog terminating stuck schedule",
			"schedule_name", sched.GetScheduleName(),
			"schedule_id", runID,
			"reason", reason,
		)
		if _, err := w.state.CancelSchedule(ctx, &statev1.CancelScheduleRequest{
			ScheduleName:       sched.GetScheduleName(),
			CancelledBy:        "watchdog",
			CancellationReason: reason,
		}); err != nil {
			w.logger.Error("Watchdog CancelSchedule failed",
				"schedule_name", sched.GetScheduleName(), "error", err)
			continue
		}
	}
	return nil
}

// IsScheduleStuck returns true iff both predicates hold:
//
//  1. No task is currently in TASK_STATUS_RUNNING. A running task means a
//     long-running model is in flight — that's not stuck.
//  2. The most recent task's created_at is older than noProgressFor. The
//     state proto Task message has no updated_at, so created_at is the
//     only "progress" signal available — and it's enough for the bug we
//     care about: tasks created, dispatcher silently failed, no RUNNING
//     transition ever happens.
//
// Empty input returns false — a schedule with no tasks isn't stuck, it
// just hasn't started.
func IsScheduleStuck(tasks []*statev1.Task, now time.Time, noProgressFor time.Duration) bool {
	if len(tasks) == 0 {
		return false
	}
	mostRecent := time.Time{}
	for _, t := range tasks {
		if t.GetStatus() == statev1.TaskStatus_TASK_STATUS_RUNNING {
			return false
		}
		ts := t.GetCreatedAt().AsTime()
		if ts.After(mostRecent) {
			mostRecent = ts
		}
	}
	return now.Sub(mostRecent) >= noProgressFor
}
