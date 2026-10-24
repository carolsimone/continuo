package ports

import "time"

// RunMetrics counts what state's reconcile loop, report handler and lease
// waker do to runs.
type RunMetrics interface {
	// RunStalled counts one run failed as stalled.
	RunStalled()

	// RunsCancelling sets the gauges of the runs that are cancelling: how many
	// there are and how many of them have waited too long for their attempts
	// to stop.
	RunsCancelling(total, overdue int)

	// RunCancelled observes how long a run took from its cancel to its end.
	RunCancelled(after time.Duration)

	// LeaseWaiters sets the gauge of the tasks waiting for a node's write
	// lease.
	LeaseWaiters(n int)

	// LeaseGranted observes how long a task waited for the node lease it was
	// granted.
	LeaseGranted(waited time.Duration)

	// LeaseRepaired counts one lease deleted because the task that held it had
	// ended without releasing it.
	LeaseRepaired()
}
