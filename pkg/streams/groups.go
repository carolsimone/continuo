// Package streams holds the canonical names of Redis stream consumer groups
// used across the continuo services. Each group identifies which service
// (and which logical handler within that service) is reading a given stream.
// These names are code-level identifiers — they are NOT deployment-tunable:
// changing one orphans any existing pending-entry-list (PEL) for the old name.
package streams

// State service consumer groups.
const (
	StateScheduleCatalog          = "state-schedule-catalog"
	StateRunEntriesDispatched     = "state-run-entries-dispatched"
	StateRunEntriesDispatchFailed = "state-run-entries-dispatch-failed"
	StateTaskStatusUpdated        = "state-task-status-updated"
	StateTaskExecutionRecorded    = "state-task-execution-recorded"
)
