package run

import "slices"

// RunFailureReason says why a run failed when no task of it failed. A run that
// failed because a task failed records none.
type RunFailureReason string

const (
	// RunFailureStalled: the run had no attempt in flight and made no progress
	// for StallAfter, which only a defect in state can cause.
	RunFailureStalled RunFailureReason = "stalled"
	// RunFailureEmptySelection: a cron fire's selection held no node to run.
	RunFailureEmptySelection RunFailureReason = "empty_selection"
	// RunFailureInvalidNodeType: a cron fire's selection held a node whose
	// type the node_type vocabulary does not declare.
	RunFailureInvalidNodeType RunFailureReason = "invalid_node_type"
)

// IsValid reports whether f is one of RunFailureReasons.
func (f RunFailureReason) IsValid() bool {
	return slices.Contains(RunFailureReasons(), f)
}

// RunFailureReasons lists every RunFailureReason. The CHECK constraint on
// scheduler_tracker.failure_reason lists exactly these.
func RunFailureReasons() []RunFailureReason {
	return []RunFailureReason{RunFailureStalled, RunFailureEmptySelection, RunFailureInvalidNodeType}
}
