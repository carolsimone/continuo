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

// StuckScheduleReader returns the active runs that have made no lifecycle
// progress since the cutoff and have no task running. A run's progress time is
// its last dispatch or applied task status change, or its creation when it has
// never progressed, so a run whose dispatch never arrived is returned too. The
// state service answers with one server-side query, so the watchdog issues one
// read per tick however many runs are active.
type StuckScheduleReader interface {
	ListStuckCandidates(ctx context.Context, cutoff time.Time) ([]StuckSchedule, error)
}

// ScheduleCanceller cancels the active run of a named schedule. The watchdog
// uses it to terminate stalled dispatches via state's cancellation pathway.
type ScheduleCanceller interface {
	CancelSchedule(ctx context.Context, scheduleName, cancelledBy, reason string) error
}
