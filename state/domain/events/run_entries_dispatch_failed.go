package events

import (
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

// RunEntriesDispatchFailed is the typed form of run.entries.dispatch_failed:v1.
// Reason reuses the pkg/events enum since it's a wire-stable code. Benign marks
// a "no work to dispatch" outcome that is not a failure (no_tests): the run
// finalizes as `skipped` rather than `failed`.
type RunEntriesDispatchFailed struct {
	ScheduleID   uuid.UUID
	ScheduleName string
	Reason       pkgevents.DispatchFailedReason
	Benign       bool
}
