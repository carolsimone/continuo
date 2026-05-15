package events

import (
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

// RunEntriesDispatchFailed is the typed form of run.entries.dispatch_failed:v1.
// Reason reuses the pkg/events enum since it's a wire-stable code.
type RunEntriesDispatchFailed struct {
	ScheduleID   uuid.UUID
	ScheduleName string
	Reason       pkgevents.DispatchFailedReason
}
