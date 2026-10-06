package ports

import (
	"context"
	"errors"
	"time"
)

// StuckSchedule identifies one active run the watchdog cancels: it has made no
// lifecycle progress since the cutoff and has no task running. RunID names the
// run to cancel; ScheduleName is carried for logging.
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

// ErrRunNotCancellable means the run a cancel names is already terminal or is
// unknown to state, so there is nothing left to cancel.
var ErrRunNotCancellable = errors.New("run not cancellable")

// RunCanceller cancels one run by its id through state's cancellation pathway.
// The watchdog uses it to cancel stalled runs.
type RunCanceller interface {
	// CancelRun cancels the run with id runID, recording cancelledBy and reason.
	// The error wraps ErrRunNotCancellable when the run is already terminal or
	// unknown.
	CancelRun(ctx context.Context, runID, cancelledBy, reason string) error
}
