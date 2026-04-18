package finalization

// Decide returns the outcome status ("succeeded" or "failed") when all termination
// conditions are met, or "" when no transition should occur.
//
//   - terminalCount: current terminal_task_count after the latest increment
//   - totalCount: total_task_count for the run (must be > 0)
//   - anyFailed: true when at least one task in the run has status = 'failed'
//   - initCompleted: true when initialization_status = 'completed'
//   - currentStatus: the scheduler_tracker.status value (must equal "running")
func Decide(terminalCount, totalCount int32, anyFailed, initCompleted bool, currentStatus string) string {
	if !initCompleted || currentStatus != "running" || totalCount <= 0 || terminalCount != totalCount {
		return ""
	}
	if anyFailed {
		return "failed"
	}
	return "succeeded"
}
