package events

import "github.com/google/uuid"

// ScheduleCancelled is the parsed schedule.cancelled:v1 stream payload.
// Carries only the cancelled run's ID; the binding inserts it into
// cancelled_runs, the run fence every gate that starts work reads.
type ScheduleCancelled struct {
	ScheduleID uuid.UUID
}
