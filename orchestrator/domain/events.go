package domain

import "github.com/google/uuid"

// SchedulerStarted carries the data from a scheduler.started:v1 stream message.
// This is an event (a fact: state has activated a schedule) — not a command.
type SchedulerStarted struct {
	ScheduleID   uuid.UUID
	ScheduleName string
	Kind         string     // "cron" | "trigger" | "rerun" | "rebase" | "single_node_run"; defaults to "cron" if missing on incoming message
	SourceRunID  *uuid.UUID // populated for rerun, rebase, stale-mode single_node_run; nil otherwise
	// InitiatedBy is the user who triggered the run, or the "system" sentinel
	// for cron / platform-initiated runs. Defaults to "system" when absent on
	// the incoming message (runs that predate provenance tracking).
	InitiatedBy string
}

// ScheduleCancelled is the typed form of schedule.cancelled:v1.
type ScheduleCancelled struct {
	ScheduleID uuid.UUID
	// CancelledBy is the user who cancelled the schedule, or the "system"
	// sentinel for a platform-initiated cancellation (e.g. the watchdog).
	CancelledBy string
}

// RunFinalized is the typed form of run.finalized:v1.
// ScheduleID and Status stay as strings — the Neo4j FinalizeRun signature
// already takes strings, so converting to uuid.UUID + model enum would be
// pointless round-tripping.
type RunFinalized struct {
	ScheduleID string
	Status     string
}
