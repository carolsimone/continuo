package ports

import "time"

// RunMetrics counts what state's reconcile loop and report handler do to runs.
type RunMetrics interface {
	// RunStalled counts one run failed as stalled.
	RunStalled()

	// RunsCancelling sets the gauges of the runs that are cancelling: how many
	// there are and how many of them have waited too long for their attempts
	// to stop.
	RunsCancelling(total, overdue int)

	// RunCancelled observes how long a run took from its cancel to its end.
	RunCancelled(after time.Duration)
}
