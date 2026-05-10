package model

// RebaseInput is the handler input the orchestrator builds in response to
// a trigger.rebase:v1 message. The consumer reads loose Redis stream
// fields and populates this struct directly — there is no wire-format
// payload struct.
//
// The orchestrator handler reads the source run's :EXECUTES set + latest
// topology via Snapshot(RebasePartition{...}); no target identity is
// carried because the rebase partition is computed deterministically
// from source state.
type RebaseInput struct {
	RunID        string // == new ScheduleID.String()
	ScheduleName string // copied from source
	SourceRunID  string // schedule_id of the failed/cancelled source run
}
