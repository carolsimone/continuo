package events

import "github.com/google/uuid"

// ScheduleCancelled is the parsed schedule.cancelled:v1 stream payload.
// Carries only the cancelled schedule ID; the binding inserts it into
// cancelled_schedules so the deploy bindings can drop in-flight messages.
type ScheduleCancelled struct {
	ScheduleID uuid.UUID
}
