package ports

import (
	"context"
	"time"
)

// StuckSchedule identifies one active run the watchdog should consider
// terminating: its dispatch has silently stalled. ScheduleName drives the
// cancellation; RunID is carried for logging/traceability.
type StuckSchedule struct {
	ScheduleName string
	RunID        string
}

// StuckScheduleReader returns active runs whose dispatch has silently stalled —
// no task is running and the most recent task is older than the cutoff. The
// state service answers this with a single indexed query, so the watchdog issues
// O(1) RPCs per tick instead of fanning out one ListTasks call per schedule, and
// it considers ALL of a run's tasks rather than only the newest page.
type StuckScheduleReader interface {
	ListStuckCandidates(ctx context.Context, cutoff time.Time) ([]StuckSchedule, error)
}

// ScheduleCanceller cancels the active run of a named schedule. The watchdog
// uses it to terminate stalled dispatches via state's cancellation pathway.
type ScheduleCanceller interface {
	CancelSchedule(ctx context.Context, scheduleName, cancelledBy, reason string) error
}
