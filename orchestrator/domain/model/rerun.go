package model

// RerunInput carries the handler-input data for a rerun operation. The
// consumer hand-builds it from the trigger.rerun:v1 stream's scalar fields.
//
// Rerun mints a new :Run on the source's schedule with kind='rerun'.
// SourceRunID is the source run's schedule_id, which the orchestrator
// handler uses to drive Snapshot(SourcePinnedDAG{}). The selector picks
// the work set from source state — no caller-supplied target node.
type RerunInput struct {
	ScheduleName string
	RunID        string // schedule_id of the NEW run (target of Snapshot)
	SourceRunID  string // schedule_id of the SOURCE run (read by SourcePinnedDAG)
}
